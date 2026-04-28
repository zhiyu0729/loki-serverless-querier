//go:build loki_serverless

package awsruntime

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"path"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/aws/signer/v4"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"

	"github.com/grafana/loki/v3/pkg/serverless/protocol"
)

type LambdaInvoker struct {
	cfg          aws.Config
	client       *http.Client
	functionName string
	region       string
	signer       *v4.Signer
}

func NewLambdaInvoker(region, functionName string) (*LambdaInvoker, error) {
	if functionName == "" {
		return nil, fmt.Errorf("lambda function name is required")
	}
	cfg, err := loadAWSConfig(context.Background(), region)
	if err != nil {
		return nil, err
	}
	if cfg.Region == "" {
		return nil, fmt.Errorf("aws region is required for lambda invoke")
	}
	return &LambdaInvoker{
		cfg:          cfg,
		client:       http.DefaultClient,
		functionName: functionName,
		region:       cfg.Region,
		signer:       v4.NewSigner(),
	}, nil
}

func (i *LambdaInvoker) Invoke(ctx context.Context, payload []byte) ([]byte, error) {
	endpoint := fmt.Sprintf("https://lambda.%s.amazonaws.com/2015-03-31/functions/%s/invocations", i.region, url.PathEscape(i.functionName))
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(payload))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Amz-Invocation-Type", "RequestResponse")

	creds, err := i.cfg.Credentials.Retrieve(ctx)
	if err != nil {
		return nil, err
	}
	payloadHash := sha256Hex(payload)
	if err := i.signer.SignHTTP(ctx, creds, req, payloadHash, "lambda", i.region, time.Now()); err != nil {
		return nil, err
	}

	resp, err := i.client.Do(req)
	if err != nil {
		return nil, retryableError{message: err.Error(), retryable: true}
	}
	defer resp.Body.Close()
	body, readErr := io.ReadAll(resp.Body)
	if readErr != nil {
		return nil, readErr
	}
	if functionErr := resp.Header.Get("X-Amz-Function-Error"); functionErr != "" {
		return nil, retryableError{message: fmt.Sprintf("lambda function error %s: %s", functionErr, string(body)), retryable: false}
	}
	if resp.StatusCode >= 500 || resp.StatusCode == http.StatusTooManyRequests {
		return nil, retryableError{message: fmt.Sprintf("lambda invoke returned %s: %s", resp.Status, string(body)), retryable: true}
	}
	if resp.StatusCode >= 400 {
		return nil, fmt.Errorf("lambda invoke returned %s: %s", resp.Status, string(body))
	}
	return body, nil
}

type S3Store struct {
	client *s3.Client
	bucket string
	prefix string
	region string
}

func NewS3Store(region, bucket, prefix string) (*S3Store, error) {
	if bucket == "" {
		return nil, fmt.Errorf("s3 bucket is required")
	}
	cfg, err := loadAWSConfig(context.Background(), region)
	if err != nil {
		return nil, err
	}
	return &S3Store{
		client: s3.NewFromConfig(cfg),
		bucket: bucket,
		prefix: strings.Trim(prefix, "/"),
		region: cfg.Region,
	}, nil
}

func (s *S3Store) Put(ctx context.Context, keyHint string, body []byte, contentType string) (*protocol.ObjectRef, error) {
	key := s.key(keyHint)
	_, err := s.client.PutObject(ctx, &s3.PutObjectInput{
		Bucket:      aws.String(s.bucket),
		Key:         aws.String(key),
		Body:        bytes.NewReader(body),
		ContentType: aws.String(contentType),
	})
	if err != nil {
		return nil, classifyAWSError(err)
	}
	return &protocol.ObjectRef{
		Bucket:      s.bucket,
		Key:         key,
		Region:      s.region,
		ContentType: contentType,
		SizeBytes:   int64(len(body)),
	}, nil
}

func (s *S3Store) Get(ctx context.Context, ref protocol.ObjectRef) ([]byte, error) {
	bucket := ref.Bucket
	if bucket == "" {
		bucket = s.bucket
	}
	out, err := s.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(ref.Key),
	})
	if err != nil {
		return nil, classifyAWSError(err)
	}
	defer out.Body.Close()
	return io.ReadAll(out.Body)
}

func (s *S3Store) key(keyHint string) string {
	token := make([]byte, 16)
	if _, err := rand.Read(token); err != nil {
		panic(err)
	}
	hint := strings.Trim(strings.ReplaceAll(keyHint, "\\", "/"), "/")
	hint = strings.ReplaceAll(hint, "..", "")
	if hint == "" {
		hint = "payload"
	}
	return path.Join(s.prefix, hint, time.Now().UTC().Format("20060102T150405.000000000Z")+"-"+hex.EncodeToString(token))
}

func loadAWSConfig(ctx context.Context, region string) (aws.Config, error) {
	var opts []func(*awsconfig.LoadOptions) error
	if region != "" {
		opts = append(opts, awsconfig.WithRegion(region))
	}
	return awsconfig.LoadDefaultConfig(ctx, opts...)
}

type retryableError struct {
	message   string
	retryable bool
}

func (e retryableError) Error() string     { return e.message }
func (e retryableError) IsRetryable() bool { return e.retryable }

func classifyAWSError(err error) error {
	if err == nil {
		return nil
	}
	return retryableError{message: err.Error(), retryable: true}
}

func sha256Hex(body []byte) string {
	sum := sha256.Sum256(body)
	return hex.EncodeToString(sum[:])
}

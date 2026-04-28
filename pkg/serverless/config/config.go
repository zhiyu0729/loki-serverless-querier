package config

import (
	"errors"
	"flag"
	"time"
)

const (
	DefaultInlineResponseLimitBytes = 4 * 1024 * 1024
	DefaultInlineRequestLimitBytes  = 4 * 1024 * 1024
	DefaultMaxInvokePayloadBytes    = 6 * 1024 * 1024
	DefaultMaxInterval              = 15 * time.Minute
	DefaultMinInterval              = time.Minute
	DefaultMaxConcurrent            = 16

	ProviderAWSLambda = "aws-lambda"
	ObjectStoreS3     = "s3"
)

type StoreConfig struct {
	Enabled                  bool              `yaml:"enabled,omitempty"`
	Provider                 string            `yaml:"provider,omitempty"`
	AWS                      AWSConfig         `yaml:"aws,omitempty"`
	ObjectStore              ObjectStoreConfig `yaml:"object_store,omitempty"`
	MaxInterval              time.Duration     `yaml:"max_interval,omitempty"`
	MinInterval              time.Duration     `yaml:"min_interval,omitempty"`
	MaxConcurrent            int               `yaml:"max_concurrent,omitempty"`
	InlineRequestLimitBytes  int               `yaml:"inline_request_limit_bytes,omitempty"`
	InlineResponseLimitBytes int64             `yaml:"inline_response_limit_bytes,omitempty"`
	FallbackOnFailure        bool              `yaml:"fallback_on_failure,omitempty"`
}

type AWSConfig struct {
	Region             string `yaml:"region,omitempty"`
	LambdaFunctionName string `yaml:"lambda_function_name,omitempty"`
}

type ObjectStoreConfig struct {
	Type   string `yaml:"type,omitempty"`
	Bucket string `yaml:"bucket,omitempty"`
	Prefix string `yaml:"prefix,omitempty"`
	Region string `yaml:"region,omitempty"`
}

func (c *StoreConfig) RegisterFlags(f *flag.FlagSet) {
	f.BoolVar(&c.Enabled, "serverless.store.enabled", false, "Enable remote serverless execution for Loki store query intervals.")
	f.StringVar(&c.Provider, "serverless.store.provider", ProviderAWSLambda, "Remote execution provider. Supported values: aws-lambda.")
	f.StringVar(&c.AWS.Region, "serverless.store.aws.region", "", "AWS region for the aws-lambda provider. If empty, the AWS SDK default region resolution is used.")
	f.StringVar(&c.AWS.LambdaFunctionName, "serverless.store.aws.lambda-function-name", "", "AWS Lambda function name or ARN used for synchronous store-only query execution.")
	f.StringVar(&c.ObjectStore.Type, "serverless.store.object-store.type", ObjectStoreS3, "Object store used for request and result payload references. Supported values: s3.")
	f.StringVar(&c.ObjectStore.Bucket, "serverless.store.object-store.bucket", "", "Object store bucket used for request and result payload references.")
	f.StringVar(&c.ObjectStore.Prefix, "serverless.store.object-store.prefix", "loki-serverless-querier", "Object store key prefix used for request and result payload references.")
	f.StringVar(&c.ObjectStore.Region, "serverless.store.object-store.region", "", "Object store region. If empty, provider region resolution is used.")
	f.DurationVar(&c.MaxInterval, "serverless.store.max-interval", DefaultMaxInterval, "Maximum store interval sent to a single remote invocation.")
	f.DurationVar(&c.MinInterval, "serverless.store.min-interval", DefaultMinInterval, "Minimum store interval for retry-by-smaller-interval splitting.")
	f.IntVar(&c.MaxConcurrent, "serverless.store.max-concurrent", DefaultMaxConcurrent, "Maximum concurrent synchronous remote invocations per store query.")
	f.IntVar(&c.InlineRequestLimitBytes, "serverless.store.inline-request-limit-bytes", DefaultInlineRequestLimitBytes, "Maximum request payload size sent inline before spilling request to object storage.")
	f.Int64Var(&c.InlineResponseLimitBytes, "serverless.store.inline-response-limit-bytes", DefaultInlineResponseLimitBytes, "Maximum response payload size returned inline before spilling result to object storage.")
	f.BoolVar(&c.FallbackOnFailure, "serverless.store.fallback-on-failure", false, "Fallback to local Loki store execution when serverless store execution fails.")
}

func (c *StoreConfig) SetDefaults() {
	if c.Provider == "" {
		c.Provider = ProviderAWSLambda
	}
	if c.ObjectStore.Type == "" {
		c.ObjectStore.Type = ObjectStoreS3
	}
	if c.ObjectStore.Prefix == "" {
		c.ObjectStore.Prefix = "loki-serverless-querier"
	}
	if c.MaxInterval == 0 {
		c.MaxInterval = DefaultMaxInterval
	}
	if c.MinInterval == 0 {
		c.MinInterval = DefaultMinInterval
	}
	if c.MaxConcurrent == 0 {
		c.MaxConcurrent = DefaultMaxConcurrent
	}
	if c.InlineRequestLimitBytes == 0 {
		c.InlineRequestLimitBytes = DefaultInlineRequestLimitBytes
	}
	if c.InlineResponseLimitBytes == 0 {
		c.InlineResponseLimitBytes = DefaultInlineResponseLimitBytes
	}
}

func (c StoreConfig) Validate() error {
	if !c.Enabled {
		return nil
	}
	switch c.Provider {
	case ProviderAWSLambda:
		if c.AWS.LambdaFunctionName == "" {
			return errors.New("aws lambda function name is required when serverless store is enabled")
		}
	default:
		return errors.New("unsupported serverless store provider")
	}
	switch c.ObjectStore.Type {
	case ObjectStoreS3:
		if c.ObjectStore.Bucket == "" {
			return errors.New("object store bucket is required when serverless store is enabled")
		}
	default:
		return errors.New("unsupported serverless object store")
	}
	if c.MaxInterval <= 0 {
		return errors.New("max interval must be positive")
	}
	if c.MinInterval <= 0 {
		return errors.New("min interval must be positive")
	}
	if c.MinInterval > c.MaxInterval {
		return errors.New("min interval must be less than or equal to max interval")
	}
	if c.MaxConcurrent <= 0 {
		return errors.New("max concurrent must be positive")
	}
	if c.InlineRequestLimitBytes <= 0 {
		return errors.New("inline request limit must be positive")
	}
	if c.InlineResponseLimitBytes <= 0 {
		return errors.New("inline response limit must be positive")
	}
	return nil
}

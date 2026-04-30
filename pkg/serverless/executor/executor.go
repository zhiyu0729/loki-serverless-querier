package executor

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/go-kit/log/level"

	"github.com/grafana/loki/v3/pkg/serverless/interval"
	"github.com/grafana/loki/v3/pkg/serverless/objectstore"
	"github.com/grafana/loki/v3/pkg/serverless/protocol"
	util_log "github.com/grafana/loki/v3/pkg/util/log"
)

const (
	DefaultInlineRequestLimitBytes  = 4 * 1024 * 1024
	DefaultInlineResponseLimitBytes = 4 * 1024 * 1024
	DefaultMaxInvokePayloadBytes    = 6 * 1024 * 1024
	slowInvokeThreshold             = 5 * time.Second
)

type Invoker interface {
	Invoke(ctx context.Context, payload []byte) ([]byte, error)
}

type Options struct {
	MaxInterval              time.Duration
	MinInterval              time.Duration
	MaxConcurrent            int
	InlineRequestLimitBytes  int
	InlineResponseLimitBytes int64
	MaxInvokePayloadBytes    int
	FallbackOnFailure        bool
}

func DefaultOptions() Options {
	return Options{
		MaxInterval:              15 * time.Minute,
		MinInterval:              time.Minute,
		MaxConcurrent:            16,
		InlineRequestLimitBytes:  DefaultInlineRequestLimitBytes,
		InlineResponseLimitBytes: DefaultInlineResponseLimitBytes,
		MaxInvokePayloadBytes:    DefaultMaxInvokePayloadBytes,
	}
}

type Executor struct {
	invoker Invoker
	store   objectstore.Store
	opts    Options
}

type ServerlessStoreExecutor = Executor

func New(invoker Invoker, store objectstore.Store, opts Options) *Executor {
	defaults := DefaultOptions()
	if opts.MaxInterval == 0 {
		opts.MaxInterval = defaults.MaxInterval
	}
	if opts.MinInterval == 0 {
		opts.MinInterval = defaults.MinInterval
	}
	if opts.MaxConcurrent == 0 {
		opts.MaxConcurrent = defaults.MaxConcurrent
	}
	if opts.InlineRequestLimitBytes == 0 {
		opts.InlineRequestLimitBytes = defaults.InlineRequestLimitBytes
	}
	if opts.InlineResponseLimitBytes == 0 {
		opts.InlineResponseLimitBytes = defaults.InlineResponseLimitBytes
	}
	if opts.MaxInvokePayloadBytes == 0 {
		opts.MaxInvokePayloadBytes = defaults.MaxInvokePayloadBytes
	}
	return &Executor{invoker: invoker, store: store, opts: opts}
}

func NewServerlessStoreExecutor(invoker Invoker, store objectstore.Store, opts Options) *ServerlessStoreExecutor {
	return New(invoker, store, opts)
}

func (e *Executor) Execute(ctx context.Context, req protocol.ServerlessQueryRequest) ([]*protocol.ServerlessQueryResponse, error) {
	if e.invoker == nil {
		return nil, errors.New("remote invoker is required")
	}
	req.SetDefaults()
	if err := req.Validate(); err != nil {
		return nil, err
	}

	parts, err := interval.SplitRequest(req, e.opts.MaxInterval)
	if err != nil {
		return nil, err
	}

	results := make([][]*protocol.ServerlessQueryResponse, len(parts))
	errs := make([]error, len(parts))
	sem := make(chan struct{}, e.opts.MaxConcurrent)
	var wg sync.WaitGroup

	for i, part := range parts {
		i, part := i, part
		wg.Add(1)
		go func() {
			defer wg.Done()
			select {
			case sem <- struct{}{}:
				defer func() { <-sem }()
			case <-ctx.Done():
				errs[i] = ctx.Err()
				return
			}
			results[i], errs[i] = e.executeOne(ctx, *part)
		}()
	}
	wg.Wait()

	var out []*protocol.ServerlessQueryResponse
	for i, err := range errs {
		if err != nil {
			return nil, err
		}
		out = append(out, results[i]...)
	}
	return out, nil
}

func (e *Executor) executeOne(ctx context.Context, req protocol.ServerlessQueryRequest) ([]*protocol.ServerlessQueryResponse, error) {
	resp, err := e.invokeOnce(ctx, req)
	if err != nil {
		if isRetryable(err) && canSplit(req, e.opts.MinInterval) {
			return e.splitAndRetry(ctx, req)
		}
		return nil, err
	}

	if resp.Status == protocol.StatusOK {
		return []*protocol.ServerlessQueryResponse{resp}, nil
	}
	if resp.Error != nil && resp.Error.Retryable && canSplit(req, e.opts.MinInterval) {
		return e.splitAndRetry(ctx, req)
	}
	return nil, resp.AsError()
}

func (e *Executor) splitAndRetry(ctx context.Context, req protocol.ServerlessQueryRequest) ([]*protocol.ServerlessQueryResponse, error) {
	halves, err := interval.HalveRequest(req)
	if err != nil {
		return nil, err
	}
	if len(halves) == 1 {
		return nil, fmt.Errorf("retry requested but interval cannot be split further")
	}

	var out []*protocol.ServerlessQueryResponse
	for _, half := range halves {
		res, err := e.executeOne(ctx, *half)
		if err != nil {
			return nil, err
		}
		out = append(out, res...)
	}
	return out, nil
}

func (e *Executor) invokeOnce(ctx context.Context, req protocol.ServerlessQueryRequest) (*protocol.ServerlessQueryResponse, error) {
	envelope := protocol.InvokeEnvelope{
		ProtocolVersion:          protocol.ProtocolVersion,
		LokiVersion:              req.LokiVersion,
		Request:                  &req,
		InlineResponseLimitBytes: e.opts.InlineResponseLimitBytes,
	}
	payload, err := json.Marshal(envelope)
	if err != nil {
		return nil, err
	}

	if len(payload) > e.opts.InlineRequestLimitBytes {
		if e.store == nil {
			return nil, fmt.Errorf("request payload is %d bytes and no object store is configured", len(payload))
		}
		reqBody, err := json.Marshal(req)
		if err != nil {
			return nil, err
		}
		ref, err := e.store.Put(ctx, "requests", reqBody, "application/json")
		if err != nil {
			return nil, err
		}
		envelope.Request = nil
		envelope.RequestRef = ref
		payload, err = json.Marshal(envelope)
		if err != nil {
			return nil, err
		}
	}
	if e.opts.MaxInvokePayloadBytes > 0 && len(payload) > e.opts.MaxInvokePayloadBytes {
		return nil, fmt.Errorf("remote invoke payload is %d bytes, above %d byte limit", len(payload), e.opts.MaxInvokePayloadBytes)
	}

	started := time.Now()
	raw, err := e.invoker.Invoke(ctx, payload)
	invokeDuration := time.Since(started)
	if err != nil {
		logSlowInvoke(req, len(payload), len(raw), "", false, invokeDuration, err)
		return nil, err
	}

	var resp protocol.ServerlessQueryResponse
	if err := json.Unmarshal(raw, &resp); err != nil {
		logSlowInvoke(req, len(payload), len(raw), "", false, invokeDuration, err)
		return nil, fmt.Errorf("decode lambda response: %w", err)
	}
	if err := resp.Validate(); err != nil {
		logSlowInvoke(req, len(payload), len(raw), resp.Status, resp.ResultRef != nil, invokeDuration, err)
		return nil, fmt.Errorf("invalid lambda response: %w", err)
	}
	resultRef := resp.ResultRef != nil
	if resp.ResultRef != nil {
		if e.store == nil {
			logSlowInvoke(req, len(payload), len(raw), resp.Status, resultRef, invokeDuration, errors.New("remote execution returned result_ref but no object store is configured"))
			return nil, errors.New("remote execution returned result_ref but no object store is configured")
		}
		body, err := e.store.Get(ctx, *resp.ResultRef)
		if err != nil {
			logSlowInvoke(req, len(payload), len(raw), resp.Status, resultRef, invokeDuration, err)
			return nil, err
		}
		resp.InlineResponse = body
		resp.ResultRef = nil
	}
	logSlowInvoke(req, len(payload), len(raw), resp.Status, resultRef, invokeDuration, nil)
	return &resp, nil
}

func logSlowInvoke(req protocol.ServerlessQueryRequest, payloadBytes, responseBytes int, status string, resultRef bool, duration time.Duration, err error) {
	if duration < slowInvokeThreshold {
		return
	}
	fields := []interface{}{
		"msg", "slow serverless store invoke",
		"duration", duration,
		"tenant", req.TenantID,
		"operation", req.Operation,
		"query_type", req.QueryType,
		"query", req.Query,
		"start", req.StartTime().Format(time.RFC3339Nano),
		"end", req.EndTime().Format(time.RFC3339Nano),
		"length", req.Duration(),
		"payload_bytes", payloadBytes,
		"response_bytes", responseBytes,
		"status", status,
		"result_ref", resultRef,
	}
	if err != nil {
		fields = append(fields, "err", err)
	}
	level.Warn(util_log.Logger).Log(fields...)
}

func canSplit(req protocol.ServerlessQueryRequest, min time.Duration) bool {
	return req.QueryType == protocol.QueryTypeRange && req.Duration() > min
}

type retryable interface {
	IsRetryable() bool
}

func isRetryable(err error) bool {
	var target retryable
	return errors.As(err, &target) && target.IsRetryable()
}

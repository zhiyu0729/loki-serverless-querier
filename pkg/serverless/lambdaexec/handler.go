package lambdaexec

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/grafana/loki/v3/pkg/serverless/objectstore"
	"github.com/grafana/loki/v3/pkg/serverless/protocol"
)

type StoreOnlyRunner interface {
	RunStoreOnly(ctx context.Context, req protocol.ServerlessQueryRequest) (json.RawMessage, error)
}

type RetryableError interface {
	error
	IsRetryable() bool
}

type Handler struct {
	runner                   StoreOnlyRunner
	requestStore             objectstore.Store
	resultStore              objectstore.Store
	inlineResponseLimitBytes int64
	lokiVersion              string
	overlayVersion           string
}

func NewHandler(runner StoreOnlyRunner, requestStore objectstore.Store, resultStore objectstore.Store, inlineLimit int64, lokiVersion, overlayVersion string) *Handler {
	if inlineLimit <= 0 {
		inlineLimit = 4 * 1024 * 1024
	}
	return &Handler{
		runner:                   runner,
		requestStore:             requestStore,
		resultStore:              resultStore,
		inlineResponseLimitBytes: inlineLimit,
		lokiVersion:              lokiVersion,
		overlayVersion:           overlayVersion,
	}
}

func (h *Handler) HandleRaw(ctx context.Context, payload []byte) ([]byte, error) {
	resp := h.Handle(ctx, payload)
	return json.Marshal(resp)
}

func (h *Handler) Handle(ctx context.Context, payload []byte) *protocol.ServerlessQueryResponse {
	req, err := h.decodeRequest(ctx, payload)
	if err != nil {
		return protocol.ErrorResponse(h.lokiVersion, "bad_request", err.Error(), false)
	}
	ctx, cancel := contextWithRequestDeadline(ctx, req.DeadlineUnixNano)
	defer cancel()

	body, err := h.runner.RunStoreOnly(ctx, *req)
	if err != nil {
		return h.errorResponse(req.LokiVersion, err)
	}
	if len(body) == 0 {
		body = json.RawMessage("null")
	}

	limit := h.inlineResponseLimitBytes
	if limit <= 0 {
		limit = 4 * 1024 * 1024
	}
	if int64(len(body)) <= limit {
		return protocol.OKInline(req.LokiVersion, body)
	}
	if h.resultStore == nil {
		return protocol.ErrorResponse(req.LokiVersion, "result_too_large", fmt.Sprintf("result is %d bytes and no result store is configured", len(body)), false)
	}
	ref, err := h.resultStore.Put(ctx, "results", body, "application/json")
	if err != nil {
		return protocol.ErrorResponse(req.LokiVersion, "result_store_error", err.Error(), true)
	}
	return protocol.OKRef(req.LokiVersion, ref)
}

func (h *Handler) decodeRequest(ctx context.Context, payload []byte) (*protocol.ServerlessQueryRequest, error) {
	var envelope protocol.InvokeEnvelope
	if err := json.Unmarshal(payload, &envelope); err != nil {
		return nil, err
	}
	envelope.SetDefaults()
	if err := envelope.Validate(); err != nil {
		return nil, err
	}

	if envelope.Request != nil {
		req := *envelope.Request
		req.SetDefaults()
		if req.OverlayVersion == "" {
			req.OverlayVersion = h.overlayVersion
		}
		return &req, nil
	}
	if h.requestStore == nil {
		return nil, errors.New("request_ref received but no request store is configured")
	}
	raw, err := h.requestStore.Get(ctx, *envelope.RequestRef)
	if err != nil {
		return nil, err
	}
	var req protocol.ServerlessQueryRequest
	if err := json.Unmarshal(raw, &req); err != nil {
		return nil, err
	}
	req.SetDefaults()
	if err := req.Validate(); err != nil {
		return nil, err
	}
	return &req, nil
}

func contextWithRequestDeadline(ctx context.Context, deadlineUnixNano int64) (context.Context, context.CancelFunc) {
	if deadlineUnixNano <= 0 {
		return ctx, func() {}
	}
	deadline := time.Unix(0, deadlineUnixNano)
	if current, ok := ctx.Deadline(); ok && current.Before(deadline) {
		return ctx, func() {}
	}
	return context.WithDeadline(ctx, deadline)
}

func (h *Handler) errorResponse(lokiVersion string, err error) *protocol.ServerlessQueryResponse {
	if errors.Is(err, context.Canceled) {
		return protocol.CanceledResponse(lokiVersion, err.Error())
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return protocol.ErrorResponse(lokiVersion, "deadline_exceeded", err.Error(), true)
	}
	var retryable RetryableError
	if errors.As(err, &retryable) {
		return protocol.ErrorResponse(lokiVersion, "runner_error", err.Error(), retryable.IsRetryable())
	}
	return protocol.ErrorResponse(lokiVersion, "runner_error", err.Error(), false)
}

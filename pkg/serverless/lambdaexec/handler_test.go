package lambdaexec

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/grafana/loki/v3/pkg/serverless/objectstore"
	"github.com/grafana/loki/v3/pkg/serverless/protocol"
)

type staticRunner struct {
	body json.RawMessage
	err  error
}

func (r staticRunner) RunStoreOnly(context.Context, protocol.ServerlessQueryRequest) (json.RawMessage, error) {
	return r.body, r.err
}

type temporaryError struct{}

func (temporaryError) Error() string     { return "temporary" }
func (temporaryError) IsRetryable() bool { return true }

type deadlineRunner struct {
	deadline time.Time
	ok       bool
}

func (r *deadlineRunner) RunStoreOnly(ctx context.Context, _ protocol.ServerlessQueryRequest) (json.RawMessage, error) {
	r.deadline, r.ok = ctx.Deadline()
	return json.RawMessage(`{"ok":true}`), nil
}

func TestHandlerSpillsLargeResult(t *testing.T) {
	store := objectstore.NewMemoryStore("spill")
	handler := NewHandler(staticRunner{body: json.RawMessage(`{"data":"abcdef"}`)}, nil, store, 4, "v3.4.0", "test")
	req := protocol.ServerlessQueryRequest{
		LokiVersion:    "v3.4.0",
		TenantID:       "tenant-a",
		Query:          `{app="api"}`,
		StartUnixNanos: time.Unix(0, 0).UnixNano(),
		EndUnixNanos:   time.Unix(1, 0).UnixNano(),
	}
	req.SetDefaults()
	payload, err := json.Marshal(protocol.InvokeEnvelope{ProtocolVersion: protocol.ProtocolVersion, LokiVersion: req.LokiVersion, Request: &req})
	if err != nil {
		t.Fatal(err)
	}

	resp := handler.Handle(context.Background(), payload)
	if err := resp.Validate(); err != nil {
		t.Fatalf("validate response: %v", err)
	}
	if resp.ResultRef == nil {
		t.Fatal("expected spilled result ref")
	}
	body, err := store.Get(context.Background(), *resp.ResultRef)
	if err != nil {
		t.Fatalf("get spilled result: %v", err)
	}
	if string(body) != `{"data":"abcdef"}` {
		t.Fatalf("spilled body = %s", string(body))
	}
}

func TestHandlerAppliesRequestDeadline(t *testing.T) {
	runner := &deadlineRunner{}
	handler := NewHandler(runner, nil, nil, 1024, "v3.4.0", "test")
	wantDeadline := time.Now().Add(time.Hour).Round(0)
	req := protocol.ServerlessQueryRequest{
		LokiVersion:      "v3.4.0",
		TenantID:         "tenant-a",
		Query:            `{app="api"}`,
		StartUnixNanos:   time.Unix(0, 0).UnixNano(),
		EndUnixNanos:     time.Unix(1, 0).UnixNano(),
		DeadlineUnixNano: wantDeadline.UnixNano(),
	}
	req.SetDefaults()
	payload, _ := json.Marshal(protocol.InvokeEnvelope{ProtocolVersion: protocol.ProtocolVersion, LokiVersion: req.LokiVersion, Request: &req})

	resp := handler.Handle(context.Background(), payload)
	if resp.Status != protocol.StatusOK {
		t.Fatalf("status = %s, error = %#v", resp.Status, resp.Error)
	}
	if !runner.ok {
		t.Fatal("runner context did not have a deadline")
	}
	if !runner.deadline.Equal(wantDeadline) {
		t.Fatalf("deadline = %s, want %s", runner.deadline, wantDeadline)
	}
}

func TestHandlerMarksRetryableErrors(t *testing.T) {
	handler := NewHandler(staticRunner{err: temporaryError{}}, nil, nil, 1024, "v3.4.0", "test")
	req := protocol.ServerlessQueryRequest{
		LokiVersion:    "v3.4.0",
		TenantID:       "tenant-a",
		Query:          `{app="api"}`,
		StartUnixNanos: time.Unix(0, 0).UnixNano(),
		EndUnixNanos:   time.Unix(1, 0).UnixNano(),
	}
	req.SetDefaults()
	payload, _ := json.Marshal(protocol.InvokeEnvelope{ProtocolVersion: protocol.ProtocolVersion, LokiVersion: req.LokiVersion, Request: &req})

	resp := handler.Handle(context.Background(), payload)
	if resp.Error == nil || !resp.Error.Retryable {
		t.Fatalf("expected retryable error, got %#v", resp.Error)
	}
}

func TestHandlerRejectsRequestRefWithoutStore(t *testing.T) {
	handler := NewHandler(staticRunner{}, nil, nil, 1024, "v3.4.0", "test")
	payload, _ := json.Marshal(protocol.InvokeEnvelope{
		ProtocolVersion: protocol.ProtocolVersion,
		LokiVersion:     "v3.4.0",
		RequestRef:      &protocol.ObjectRef{Bucket: "b", Key: "k"},
	})

	resp := handler.Handle(context.Background(), payload)
	if resp.Status != protocol.StatusError {
		t.Fatalf("status = %s", resp.Status)
	}
	if resp.Error == nil || resp.Error.Code != "bad_request" {
		t.Fatalf("expected bad_request, got %#v", resp.Error)
	}
}

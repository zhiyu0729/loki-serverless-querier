package executor

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/grafana/loki/v3/pkg/serverless/lambdaexec"
	"github.com/grafana/loki/v3/pkg/serverless/objectstore"
	"github.com/grafana/loki/v3/pkg/serverless/protocol"
)

type handlerInvoker struct {
	handler *lambdaexec.Handler
}

type staticRunner struct {
	body json.RawMessage
	err  error
}

func (r staticRunner) RunStoreOnly(context.Context, protocol.ServerlessQueryRequest) (json.RawMessage, error) {
	return r.body, r.err
}

func (i handlerInvoker) Invoke(ctx context.Context, payload []byte) ([]byte, error) {
	return i.handler.HandleRaw(ctx, payload)
}

func TestExecutorSplitsAndFetchesResultRef(t *testing.T) {
	store := objectstore.NewMemoryStore("objects")
	handler := lambdaexec.NewHandler(staticRunner{body: json.RawMessage(`{"streams":[]}`)}, store, store, 4, "v3.4.0", "test")
	exec := New(handlerInvoker{handler: handler}, store, Options{
		MaxInterval:              10 * time.Minute,
		MinInterval:              time.Minute,
		MaxConcurrent:            2,
		InlineRequestLimitBytes:  1024,
		InlineResponseLimitBytes: 4,
	})

	req := baseRequest()
	req.EndUnixNanos = time.Unix(30*60, 0).UnixNano()

	responses, err := exec.Execute(context.Background(), req)
	if err != nil {
		t.Fatalf("execute: %v", err)
	}
	if len(responses) != 3 {
		t.Fatalf("got %d responses, want 3", len(responses))
	}
	for _, resp := range responses {
		if string(resp.InlineResponse) != `{"streams":[]}` {
			t.Fatalf("inline response = %s", string(resp.InlineResponse))
		}
	}
}

func TestExecutorSpillsLargeRequest(t *testing.T) {
	store := objectstore.NewMemoryStore("objects")
	handler := lambdaexec.NewHandler(staticRunner{body: json.RawMessage(`{"ok":true}`)}, store, store, 1024, "v3.4.0", "test")
	exec := New(handlerInvoker{handler: handler}, store, Options{
		MaxInterval:              time.Hour,
		MinInterval:              time.Minute,
		MaxConcurrent:            1,
		InlineRequestLimitBytes:  200,
		InlineResponseLimitBytes: 1024,
	})

	req := baseRequest()
	req.Limits = map[string]json.RawMessage{"large": json.RawMessage(`"` + string(bytesOf('x', 512)) + `"`)}

	responses, err := exec.Execute(context.Background(), req)
	if err != nil {
		t.Fatalf("execute: %v", err)
	}
	if len(responses) != 1 {
		t.Fatalf("got %d responses, want 1", len(responses))
	}
}

func TestExecutorRetryableErrorSplitsInterval(t *testing.T) {
	invoker := &flakyInvoker{failures: 1}
	exec := New(invoker, nil, Options{
		MaxInterval:              time.Hour,
		MinInterval:              10 * time.Minute,
		MaxConcurrent:            1,
		InlineRequestLimitBytes:  1024,
		InlineResponseLimitBytes: 1024,
	})

	req := baseRequest()
	req.EndUnixNanos = time.Unix(60*60, 0).UnixNano()

	responses, err := exec.Execute(context.Background(), req)
	if err != nil {
		t.Fatalf("execute: %v", err)
	}
	if len(responses) != 2 {
		t.Fatalf("got %d responses, want 2", len(responses))
	}
	if invoker.calls != 3 {
		t.Fatalf("calls = %d, want 3", invoker.calls)
	}
}

func baseRequest() protocol.ServerlessQueryRequest {
	req := protocol.ServerlessQueryRequest{
		LokiVersion:    "v3.4.0",
		TenantID:       "tenant-a",
		Query:          `{app="api"}`,
		StartUnixNanos: time.Unix(0, 0).UnixNano(),
		EndUnixNanos:   time.Unix(10*60, 0).UnixNano(),
	}
	req.SetDefaults()
	return req
}

type retryableTransportError struct{}

func (retryableTransportError) Error() string     { return "retryable transport" }
func (retryableTransportError) IsRetryable() bool { return true }

type flakyInvoker struct {
	calls    int
	failures int
}

func (i *flakyInvoker) Invoke(context.Context, []byte) ([]byte, error) {
	i.calls++
	if i.failures > 0 {
		i.failures--
		return nil, retryableTransportError{}
	}
	return json.Marshal(protocol.OKInline("v3.4.0", json.RawMessage(`{"ok":true}`)))
}

var _ error = retryableTransportError{}

func bytesOf(ch byte, n int) []byte {
	out := make([]byte, n)
	for i := range out {
		out[i] = ch
	}
	return out
}

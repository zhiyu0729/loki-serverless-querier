package interval

import (
	"testing"
	"time"

	"github.com/grafana/loki/v3/pkg/serverless/protocol"
)

func TestSplitRequest(t *testing.T) {
	req := protocol.ServerlessQueryRequest{
		LokiVersion:    "v3.4.0",
		TenantID:       "tenant-a",
		Query:          `{app="api"}`,
		StartUnixNanos: time.Unix(0, 0).UnixNano(),
		EndUnixNanos:   time.Unix(30*60, 0).UnixNano(),
	}

	parts, err := SplitRequest(req, 10*time.Minute)
	if err != nil {
		t.Fatalf("split request: %v", err)
	}
	if len(parts) != 3 {
		t.Fatalf("got %d parts, want 3", len(parts))
	}
	if parts[1].StartTime() != time.Unix(10*60, 0).UTC() {
		t.Fatalf("second part start = %s", parts[1].StartTime())
	}
}

func TestInstantQueryDoesNotSplit(t *testing.T) {
	req := protocol.ServerlessQueryRequest{
		LokiVersion:    "v3.4.0",
		TenantID:       "tenant-a",
		Query:          `sum(rate({app="api"}[5m]))`,
		QueryType:      protocol.QueryTypeInstant,
		StartUnixNanos: time.Unix(0, 0).UnixNano(),
		EndUnixNanos:   time.Unix(60*60, 0).UnixNano(),
	}

	parts, err := SplitRequest(req, 10*time.Minute)
	if err != nil {
		t.Fatalf("split request: %v", err)
	}
	if len(parts) != 1 {
		t.Fatalf("got %d parts, want 1", len(parts))
	}
}

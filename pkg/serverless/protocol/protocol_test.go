package protocol

import (
	"encoding/json"
	"testing"
	"time"
)

func TestRequestDefaultsAndValidate(t *testing.T) {
	req := ServerlessQueryRequest{
		LokiVersion:    "v3.4.0",
		TenantID:       "tenant-a",
		Query:          `{app="api"}`,
		StartUnixNanos: time.Unix(10, 0).UnixNano(),
		EndUnixNanos:   time.Unix(20, 0).UnixNano(),
	}
	req.SetDefaults()
	if err := req.Validate(); err != nil {
		t.Fatalf("validate request: %v", err)
	}
	if req.ProtocolVersion != ProtocolVersion {
		t.Fatalf("protocol default = %q", req.ProtocolVersion)
	}
	if req.Direction != DirectionBackward {
		t.Fatalf("direction default = %q", req.Direction)
	}
}

func TestResponseValidationRequiresSinglePayload(t *testing.T) {
	ref := &ObjectRef{Bucket: "b", Key: "k"}
	resp := OKRef("v3.4.0", ref)
	resp.InlineResponse = json.RawMessage(`{"data":[]}`)
	if err := resp.Validate(); err == nil {
		t.Fatal("expected validation error for inline response and result ref")
	}
}

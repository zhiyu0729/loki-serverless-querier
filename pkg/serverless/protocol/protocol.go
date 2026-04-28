package protocol

import (
	"encoding/json"
	"errors"
	"fmt"
	"time"
)

const (
	ProtocolVersion = "loki-serverless-query/v1"

	StatusOK       = "ok"
	StatusError    = "error"
	StatusCanceled = "canceled"

	QueryTypeInstant = "instant"
	QueryTypeRange   = "range"

	OperationSelectLogs    = "select_logs"
	OperationSelectSamples = "select_samples"

	LokiRequestEncodingProtoBase64 = "proto-base64"

	DirectionForward  = "forward"
	DirectionBackward = "backward"
)

type ObjectRef struct {
	URI         string            `json:"uri,omitempty"`
	Bucket      string            `json:"bucket,omitempty"`
	Key         string            `json:"key,omitempty"`
	VersionID   string            `json:"version_id,omitempty"`
	Region      string            `json:"region,omitempty"`
	ContentType string            `json:"content_type,omitempty"`
	Compression string            `json:"compression,omitempty"`
	SizeBytes   int64             `json:"size_bytes,omitempty"`
	Metadata    map[string]string `json:"metadata,omitempty"`
}

func (r ObjectRef) Validate() error {
	if r.URI != "" {
		return nil
	}
	if r.Bucket == "" || r.Key == "" {
		return errors.New("object ref requires either uri or bucket/key")
	}
	return nil
}

type ServerlessQueryRequest struct {
	ProtocolVersion     string                     `json:"protocol_version"`
	LokiVersion         string                     `json:"loki_version"`
	OverlayVersion      string                     `json:"overlay_version,omitempty"`
	TenantID            string                     `json:"tenant_id"`
	Query               string                     `json:"query"`
	QueryType           string                     `json:"query_type"`
	Operation           string                     `json:"operation,omitempty"`
	LokiRequest         string                     `json:"loki_request,omitempty"`
	LokiRequestEncoding string                     `json:"loki_request_encoding,omitempty"`
	StartUnixNanos      int64                      `json:"start_unix_nanos"`
	EndUnixNanos        int64                      `json:"end_unix_nanos"`
	StepNanos           int64                      `json:"step_nanos,omitempty"`
	Direction           string                     `json:"direction,omitempty"`
	Limit               uint32                     `json:"limit,omitempty"`
	StatsEnabled        bool                       `json:"stats_enabled,omitempty"`
	Limits              map[string]json.RawMessage `json:"limits,omitempty"`
	TraceID             string                     `json:"trace_id,omitempty"`
	CancelID            string                     `json:"cancel_id,omitempty"`
	DeadlineUnixNano    int64                      `json:"deadline_unix_nano,omitempty"`
}

func (r *ServerlessQueryRequest) SetDefaults() {
	if r.ProtocolVersion == "" {
		r.ProtocolVersion = ProtocolVersion
	}
	if r.QueryType == "" {
		r.QueryType = QueryTypeRange
	}
	if r.Direction == "" {
		r.Direction = DirectionBackward
	}
}

func (r ServerlessQueryRequest) Validate() error {
	if r.ProtocolVersion != ProtocolVersion {
		return fmt.Errorf("unsupported protocol version %q", r.ProtocolVersion)
	}
	if r.LokiVersion == "" {
		return errors.New("loki_version is required")
	}
	if r.TenantID == "" {
		return errors.New("tenant_id is required")
	}
	if r.Query == "" {
		return errors.New("query is required")
	}
	if r.QueryType != QueryTypeInstant && r.QueryType != QueryTypeRange {
		return fmt.Errorf("unsupported query_type %q", r.QueryType)
	}
	if r.Direction != DirectionForward && r.Direction != DirectionBackward {
		return fmt.Errorf("unsupported direction %q", r.Direction)
	}
	if r.EndUnixNanos < r.StartUnixNanos {
		return errors.New("end_unix_nanos must be greater than or equal to start_unix_nanos")
	}
	if r.QueryType == QueryTypeRange && r.EndUnixNanos == r.StartUnixNanos {
		return errors.New("range query requires a non-empty time interval")
	}
	if r.StepNanos < 0 {
		return errors.New("step_nanos must not be negative")
	}
	if r.Operation != "" {
		if r.Operation != OperationSelectLogs && r.Operation != OperationSelectSamples {
			return fmt.Errorf("unsupported operation %q", r.Operation)
		}
		if r.LokiRequest == "" {
			return errors.New("loki_request is required when operation is set")
		}
		if r.LokiRequestEncoding != LokiRequestEncodingProtoBase64 {
			return fmt.Errorf("unsupported loki_request_encoding %q", r.LokiRequestEncoding)
		}
	}
	return nil
}

func (r ServerlessQueryRequest) StartTime() time.Time {
	return time.Unix(0, r.StartUnixNanos).UTC()
}

func (r ServerlessQueryRequest) EndTime() time.Time {
	return time.Unix(0, r.EndUnixNanos).UTC()
}

func (r ServerlessQueryRequest) Duration() time.Duration {
	return time.Duration(r.EndUnixNanos - r.StartUnixNanos)
}

func (r ServerlessQueryRequest) WithInterval(start, end time.Time) *ServerlessQueryRequest {
	next := r
	next.StartUnixNanos = start.UnixNano()
	next.EndUnixNanos = end.UnixNano()
	return &next
}

type InvokeEnvelope struct {
	ProtocolVersion          string                  `json:"protocol_version"`
	LokiVersion              string                  `json:"loki_version"`
	Request                  *ServerlessQueryRequest `json:"request,omitempty"`
	RequestRef               *ObjectRef              `json:"request_ref,omitempty"`
	InlineResponseLimitBytes int64                   `json:"inline_response_limit_bytes,omitempty"`
}

func (e *InvokeEnvelope) SetDefaults() {
	if e.ProtocolVersion == "" {
		e.ProtocolVersion = ProtocolVersion
	}
	if e.Request != nil {
		e.Request.SetDefaults()
		if e.LokiVersion == "" {
			e.LokiVersion = e.Request.LokiVersion
		}
	}
}

func (e InvokeEnvelope) Validate() error {
	if e.ProtocolVersion != ProtocolVersion {
		return fmt.Errorf("unsupported protocol version %q", e.ProtocolVersion)
	}
	if (e.Request == nil) == (e.RequestRef == nil) {
		return errors.New("exactly one of request or request_ref is required")
	}
	if e.Request != nil {
		req := *e.Request
		req.SetDefaults()
		if err := req.Validate(); err != nil {
			return err
		}
	}
	if e.RequestRef != nil {
		if err := e.RequestRef.Validate(); err != nil {
			return fmt.Errorf("invalid request_ref: %w", err)
		}
	}
	return nil
}

type ServerlessQueryResponse struct {
	ProtocolVersion string          `json:"protocol_version"`
	LokiVersion     string          `json:"loki_version"`
	Status          string          `json:"status"`
	InlineResponse  json.RawMessage `json:"inline_response,omitempty"`
	ResultRef       *ObjectRef      `json:"result_ref,omitempty"`
	Stats           json.RawMessage `json:"stats,omitempty"`
	Warnings        []string        `json:"warnings,omitempty"`
	Error           *QueryError     `json:"error,omitempty"`
}

type QueryError struct {
	Code      string `json:"code"`
	Message   string `json:"message"`
	Retryable bool   `json:"retryable,omitempty"`
}

func OKInline(lokiVersion string, body json.RawMessage) *ServerlessQueryResponse {
	return &ServerlessQueryResponse{
		ProtocolVersion: ProtocolVersion,
		LokiVersion:     lokiVersion,
		Status:          StatusOK,
		InlineResponse:  body,
	}
}

func OKRef(lokiVersion string, ref *ObjectRef) *ServerlessQueryResponse {
	return &ServerlessQueryResponse{
		ProtocolVersion: ProtocolVersion,
		LokiVersion:     lokiVersion,
		Status:          StatusOK,
		ResultRef:       ref,
	}
}

func ErrorResponse(lokiVersion, code, message string, retryable bool) *ServerlessQueryResponse {
	return &ServerlessQueryResponse{
		ProtocolVersion: ProtocolVersion,
		LokiVersion:     lokiVersion,
		Status:          StatusError,
		Error: &QueryError{
			Code:      code,
			Message:   message,
			Retryable: retryable,
		},
	}
}

func CanceledResponse(lokiVersion, message string) *ServerlessQueryResponse {
	return &ServerlessQueryResponse{
		ProtocolVersion: ProtocolVersion,
		LokiVersion:     lokiVersion,
		Status:          StatusCanceled,
		Error: &QueryError{
			Code:    "canceled",
			Message: message,
		},
	}
}

func (r ServerlessQueryResponse) Validate() error {
	if r.ProtocolVersion != ProtocolVersion {
		return fmt.Errorf("unsupported protocol version %q", r.ProtocolVersion)
	}
	if r.LokiVersion == "" {
		return errors.New("loki_version is required")
	}
	switch r.Status {
	case StatusOK:
		if len(r.InlineResponse) == 0 && r.ResultRef == nil {
			return errors.New("ok response requires inline_response or result_ref")
		}
		if len(r.InlineResponse) > 0 && r.ResultRef != nil {
			return errors.New("ok response cannot contain both inline_response and result_ref")
		}
		if r.ResultRef != nil {
			if err := r.ResultRef.Validate(); err != nil {
				return fmt.Errorf("invalid result_ref: %w", err)
			}
		}
	case StatusError, StatusCanceled:
		if r.Error == nil {
			return fmt.Errorf("%s response requires error", r.Status)
		}
	default:
		return fmt.Errorf("unsupported status %q", r.Status)
	}
	return nil
}

func (r ServerlessQueryResponse) AsError() error {
	if r.Error == nil {
		return nil
	}
	return &ResponseError{Status: r.Status, QueryError: *r.Error}
}

type ResponseError struct {
	Status string
	QueryError
}

func (e *ResponseError) Error() string {
	if e == nil {
		return ""
	}
	return fmt.Sprintf("%s: %s", e.Code, e.Message)
}

func (e *ResponseError) IsRetryable() bool {
	return e != nil && e.Retryable
}

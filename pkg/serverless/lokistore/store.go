//go:build loki_serverless

package lokistore

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"

	"github.com/gogo/protobuf/proto"
	"github.com/grafana/dskit/tenant"

	"github.com/grafana/loki/v3/pkg/iter"
	"github.com/grafana/loki/v3/pkg/logproto"
	"github.com/grafana/loki/v3/pkg/logql"
	"github.com/grafana/loki/v3/pkg/querier"
	"github.com/grafana/loki/v3/pkg/serverless/executor"
	"github.com/grafana/loki/v3/pkg/serverless/protocol"
	"github.com/grafana/loki/v3/pkg/storage"
)

type Store struct {
	storage.Store
	executor          *executor.ServerlessStoreExecutor
	lokiVersion       string
	overlayVersion    string
	fallbackOnFailure bool
}

func Wrap(next storage.Store, exec *executor.ServerlessStoreExecutor, lokiVersion, overlayVersion string, fallbackOnFailure bool) *Store {
	return &Store{
		Store:             next,
		executor:          exec,
		lokiVersion:       lokiVersion,
		overlayVersion:    overlayVersion,
		fallbackOnFailure: fallbackOnFailure,
	}
}

func (s *Store) SelectLogs(ctx context.Context, params logql.SelectLogParams) (iter.EntryIterator, error) {
	if s.executor == nil || params.QueryRequest == nil {
		return s.Store.SelectLogs(ctx, params)
	}

	req, err := s.requestFromLogParams(ctx, params)
	if err != nil {
		return nil, err
	}
	responses, err := s.executor.Execute(ctx, *req)
	if err != nil {
		if s.fallbackOnFailure {
			return s.Store.SelectLogs(ctx, params)
		}
		return nil, err
	}

	var iters []iter.EntryIterator
	for _, response := range responses {
		var qr logproto.QueryResponse
		if err := json.Unmarshal(response.InlineResponse, &qr); err != nil {
			return nil, fmt.Errorf("decode serverless log response: %w", err)
		}
		iters = append(iters, iter.NewQueryResponseIterator(&qr, params.Direction))
	}
	if len(iters) == 0 {
		return iter.NoopEntryIterator, nil
	}
	if len(iters) == 1 {
		return iters[0], nil
	}
	return iter.NewMergeEntryIterator(ctx, iters, params.Direction), nil
}

func (s *Store) SelectSamples(ctx context.Context, params logql.SelectSampleParams) (iter.SampleIterator, error) {
	if s.executor == nil || params.SampleQueryRequest == nil {
		return s.Store.SelectSamples(ctx, params)
	}

	req, err := s.requestFromSampleParams(ctx, params)
	if err != nil {
		return nil, err
	}
	responses, err := s.executor.Execute(ctx, *req)
	if err != nil {
		if s.fallbackOnFailure {
			return s.Store.SelectSamples(ctx, params)
		}
		return nil, err
	}

	var iters []iter.SampleIterator
	for _, response := range responses {
		var qr logproto.SampleQueryResponse
		if err := json.Unmarshal(response.InlineResponse, &qr); err != nil {
			return nil, fmt.Errorf("decode serverless sample response: %w", err)
		}
		iters = append(iters, iter.NewSampleQueryResponseIterator(&qr))
	}
	if len(iters) == 0 {
		return iter.NoopSampleIterator, nil
	}
	if len(iters) == 1 {
		return iters[0], nil
	}
	return iter.NewMergeSampleIterator(ctx, iters), nil
}

func (s *Store) requestFromLogParams(ctx context.Context, params logql.SelectLogParams) (*protocol.ServerlessQueryRequest, error) {
	userID, err := tenant.TenantID(ctx)
	if err != nil {
		return nil, err
	}
	body, err := proto.Marshal(params.QueryRequest)
	if err != nil {
		return nil, err
	}
	return &protocol.ServerlessQueryRequest{
		ProtocolVersion:     protocol.ProtocolVersion,
		LokiVersion:         s.lokiVersion,
		OverlayVersion:      s.overlayVersion,
		TenantID:            userID,
		Query:               params.Selector,
		QueryType:           protocol.QueryTypeRange,
		Operation:           protocol.OperationSelectLogs,
		LokiRequest:         base64.StdEncoding.EncodeToString(body),
		LokiRequestEncoding: protocol.LokiRequestEncodingProtoBase64,
		StartUnixNanos:      params.Start.UnixNano(),
		EndUnixNanos:        params.End.UnixNano(),
		Direction:           directionToProtocol(params.Direction),
		Limit:               params.Limit,
		DeadlineUnixNano:    deadlineUnixNano(ctx),
	}, nil
}

func (s *Store) requestFromSampleParams(ctx context.Context, params logql.SelectSampleParams) (*protocol.ServerlessQueryRequest, error) {
	userID, err := tenant.TenantID(ctx)
	if err != nil {
		return nil, err
	}
	body, err := proto.Marshal(params.SampleQueryRequest)
	if err != nil {
		return nil, err
	}
	return &protocol.ServerlessQueryRequest{
		ProtocolVersion:     protocol.ProtocolVersion,
		LokiVersion:         s.lokiVersion,
		OverlayVersion:      s.overlayVersion,
		TenantID:            userID,
		Query:               params.Selector,
		QueryType:           protocol.QueryTypeRange,
		Operation:           protocol.OperationSelectSamples,
		LokiRequest:         base64.StdEncoding.EncodeToString(body),
		LokiRequestEncoding: protocol.LokiRequestEncodingProtoBase64,
		StartUnixNanos:      params.Start.UnixNano(),
		EndUnixNanos:        params.End.UnixNano(),
		DeadlineUnixNano:    deadlineUnixNano(ctx),
	}, nil
}

func directionToProtocol(direction logproto.Direction) string {
	if direction == logproto.FORWARD {
		return protocol.DirectionForward
	}
	return protocol.DirectionBackward
}

func deadlineUnixNano(ctx context.Context) int64 {
	deadline, ok := ctx.Deadline()
	if !ok {
		return 0
	}
	return deadline.UnixNano()
}

var _ querier.Store = (*Store)(nil)
var _ storage.Store = (*Store)(nil)

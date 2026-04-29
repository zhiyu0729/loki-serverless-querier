//go:build loki_serverless

package lokirunner

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"

	"github.com/gogo/protobuf/proto"
	"github.com/grafana/dskit/user"

	"github.com/grafana/loki/pkg/push"
	"github.com/grafana/loki/v3/pkg/iter"
	"github.com/grafana/loki/v3/pkg/logproto"
	"github.com/grafana/loki/v3/pkg/logql"
	"github.com/grafana/loki/v3/pkg/querier"
	"github.com/grafana/loki/v3/pkg/serverless/protocol"
)

type Runner struct {
	store querier.Store
}

func New(store querier.Store) *Runner {
	return &Runner{store: store}
}

func (r *Runner) RunStoreOnly(ctx context.Context, req protocol.ServerlessQueryRequest) (json.RawMessage, error) {
	if r.store == nil {
		return nil, fmt.Errorf("store is not configured")
	}
	if req.TenantID != "" {
		ctx = user.InjectOrgID(ctx, req.TenantID)
	}

	switch req.Operation {
	case protocol.OperationSelectLogs:
		return r.runSelectLogs(ctx, req)
	case protocol.OperationSelectSamples:
		return r.runSelectSamples(ctx, req)
	default:
		return nil, fmt.Errorf("unsupported serverless operation %q", req.Operation)
	}
}

func (r *Runner) runSelectLogs(ctx context.Context, req protocol.ServerlessQueryRequest) (json.RawMessage, error) {
	var queryReq logproto.QueryRequest
	if err := decodeLokiRequest(req, &queryReq); err != nil {
		return nil, err
	}
	queryReq.Start = req.StartTime()
	queryReq.End = req.EndTime()
	clearEmptyStoreChunks(&queryReq.StoreChunks)
	it, err := r.store.SelectLogs(ctx, logql.SelectLogParams{QueryRequest: &queryReq})
	if err != nil {
		return nil, err
	}
	defer it.Close()

	resp, err := collectLogs(it)
	if err != nil {
		return nil, err
	}
	return json.Marshal(resp)
}

func (r *Runner) runSelectSamples(ctx context.Context, req protocol.ServerlessQueryRequest) (json.RawMessage, error) {
	var sampleReq logproto.SampleQueryRequest
	if err := decodeLokiRequest(req, &sampleReq); err != nil {
		return nil, err
	}
	sampleReq.Start = req.StartTime()
	sampleReq.End = req.EndTime()
	clearEmptyStoreChunks(&sampleReq.StoreChunks)
	it, err := r.store.SelectSamples(ctx, logql.SelectSampleParams{SampleQueryRequest: &sampleReq})
	if err != nil {
		return nil, err
	}
	defer it.Close()

	resp, err := collectSamples(it)
	if err != nil {
		return nil, err
	}
	return json.Marshal(resp)
}

func decodeLokiRequest(req protocol.ServerlessQueryRequest, msg proto.Message) error {
	if req.LokiRequestEncoding != protocol.LokiRequestEncodingProtoBase64 {
		return fmt.Errorf("unsupported loki request encoding %q", req.LokiRequestEncoding)
	}
	body, err := base64.StdEncoding.DecodeString(req.LokiRequest)
	if err != nil {
		return fmt.Errorf("decode loki request: %w", err)
	}
	if err := proto.Unmarshal(body, msg); err != nil {
		return fmt.Errorf("unmarshal loki request: %w", err)
	}
	return nil
}

func clearEmptyStoreChunks(storeChunks **logproto.ChunkRefGroup) {
	if *storeChunks != nil && len((*storeChunks).Refs) == 0 {
		*storeChunks = nil
	}
}

func collectLogs(it iter.EntryIterator) (*logproto.QueryResponse, error) {
	streams := make([]push.Stream, 0)
	streamByLabels := map[string]int{}
	for it.Next() {
		labels := it.Labels()
		idx, ok := streamByLabels[labels]
		if !ok {
			idx = len(streams)
			streamByLabels[labels] = idx
			streams = append(streams, push.Stream{Labels: labels})
		}
		streams[idx].Entries = append(streams[idx].Entries, it.At())
	}
	if err := it.Err(); err != nil {
		return nil, err
	}
	return &logproto.QueryResponse{Streams: streams}, nil
}

func collectSamples(it iter.SampleIterator) (*logproto.SampleQueryResponse, error) {
	series := make([]logproto.Series, 0)
	seriesByLabels := map[string]int{}
	for it.Next() {
		labels := it.Labels()
		idx, ok := seriesByLabels[labels]
		if !ok {
			idx = len(series)
			seriesByLabels[labels] = idx
			series = append(series, logproto.Series{
				Labels:     labels,
				StreamHash: it.StreamHash(),
			})
		}
		series[idx].Samples = append(series[idx].Samples, it.At())
	}
	if err := it.Err(); err != nil {
		return nil, err
	}
	return &logproto.SampleQueryResponse{Series: series}, nil
}

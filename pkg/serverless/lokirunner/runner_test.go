//go:build loki_serverless

package lokirunner

import (
	"context"
	"encoding/base64"
	"testing"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"

	"github.com/grafana/loki/v3/pkg/iter"
	"github.com/grafana/loki/v3/pkg/logproto"
	"github.com/grafana/loki/v3/pkg/logql"
	"github.com/grafana/loki/v3/pkg/serverless/protocol"
	"github.com/grafana/loki/v3/pkg/storage/chunk"
	"github.com/grafana/loki/v3/pkg/storage/stores/index/stats"
)

func TestRunnerUsesSplitIntervalForLogs(t *testing.T) {
	store := &captureStore{}
	runner := New(store)
	start := time.Unix(100, 0).UTC()
	end := time.Unix(200, 0).UTC()

	req := serverlessRequest(t, protocol.OperationSelectLogs, &logproto.QueryRequest{
		Start:     time.Unix(1, 0).UTC(),
		End:       time.Unix(2, 0).UTC(),
		Selector:  `{app="api"}`,
		Direction: logproto.FORWARD,
		Limit:     10,
	}, start, end)

	if _, err := runner.RunStoreOnly(context.Background(), req); err != nil {
		t.Fatalf("RunStoreOnly returned error: %v", err)
	}
	if !store.logStart.Equal(start) || !store.logEnd.Equal(end) {
		t.Fatalf("log request interval = %s/%s, want %s/%s", store.logStart, store.logEnd, start, end)
	}
}

func TestRunnerUsesSplitIntervalForSamples(t *testing.T) {
	store := &captureStore{}
	runner := New(store)
	start := time.Unix(300, 0).UTC()
	end := time.Unix(400, 0).UTC()

	req := serverlessRequest(t, protocol.OperationSelectSamples, &logproto.SampleQueryRequest{
		Start:    time.Unix(1, 0).UTC(),
		End:      time.Unix(2, 0).UTC(),
		Selector: `rate({app="api"}[1m])`,
	}, start, end)

	if _, err := runner.RunStoreOnly(context.Background(), req); err != nil {
		t.Fatalf("RunStoreOnly returned error: %v", err)
	}
	if !store.sampleStart.Equal(start) || !store.sampleEnd.Equal(end) {
		t.Fatalf("sample request interval = %s/%s, want %s/%s", store.sampleStart, store.sampleEnd, start, end)
	}
}

func TestRunnerClearsEmptyStoreChunkOverrideForLogs(t *testing.T) {
	store := &captureStore{}
	runner := New(store)
	start := time.Unix(100, 0).UTC()
	end := time.Unix(200, 0).UTC()

	req := serverlessRequest(t, protocol.OperationSelectLogs, &logproto.QueryRequest{
		Selector:    `{app="api"}`,
		Direction:   logproto.FORWARD,
		Limit:       10,
		StoreChunks: &logproto.ChunkRefGroup{},
	}, start, end)

	if _, err := runner.RunStoreOnly(context.Background(), req); err != nil {
		t.Fatalf("RunStoreOnly returned error: %v", err)
	}
	if store.logStoreChunks != nil {
		t.Fatalf("log store chunk override = %#v, want nil", store.logStoreChunks)
	}
}

func TestRunnerPreservesNonEmptyStoreChunkOverrideForLogs(t *testing.T) {
	store := &captureStore{}
	runner := New(store)
	start := time.Unix(100, 0).UTC()
	end := time.Unix(200, 0).UTC()
	storeChunks := &logproto.ChunkRefGroup{
		Refs: []*logproto.ChunkRef{{
			Fingerprint: 1,
			UserID:      "tenant-a",
			From:        model.TimeFromUnix(90),
			Through:     model.TimeFromUnix(210),
		}},
	}

	req := serverlessRequest(t, protocol.OperationSelectLogs, &logproto.QueryRequest{
		Selector:    `{app="api"}`,
		Direction:   logproto.FORWARD,
		Limit:       10,
		StoreChunks: storeChunks,
	}, start, end)

	if _, err := runner.RunStoreOnly(context.Background(), req); err != nil {
		t.Fatalf("RunStoreOnly returned error: %v", err)
	}
	if store.logStoreChunks != storeChunks {
		t.Fatalf("log store chunk override = %#v, want %#v", store.logStoreChunks, storeChunks)
	}
}

func TestRunnerClearsEmptyStoreChunkOverrideForSamples(t *testing.T) {
	store := &captureStore{}
	runner := New(store)
	start := time.Unix(300, 0).UTC()
	end := time.Unix(400, 0).UTC()

	req := serverlessRequest(t, protocol.OperationSelectSamples, &logproto.SampleQueryRequest{
		Selector:    `rate({app="api"}[1m])`,
		StoreChunks: &logproto.ChunkRefGroup{},
	}, start, end)

	if _, err := runner.RunStoreOnly(context.Background(), req); err != nil {
		t.Fatalf("RunStoreOnly returned error: %v", err)
	}
	if store.sampleStoreChunks != nil {
		t.Fatalf("sample store chunk override = %#v, want nil", store.sampleStoreChunks)
	}
}

func serverlessRequest(t *testing.T, operation string, msg proto.Message, start, end time.Time) protocol.ServerlessQueryRequest {
	t.Helper()
	body, err := proto.Marshal(msg)
	if err != nil {
		t.Fatalf("marshal request: %v", err)
	}
	return protocol.ServerlessQueryRequest{
		ProtocolVersion:     protocol.ProtocolVersion,
		LokiVersion:         "v3.7.1",
		TenantID:            "tenant-a",
		Query:               `{app="api"}`,
		QueryType:           protocol.QueryTypeRange,
		Operation:           operation,
		LokiRequest:         base64.StdEncoding.EncodeToString(body),
		LokiRequestEncoding: protocol.LokiRequestEncodingProtoBase64,
		StartUnixNanos:      start.UnixNano(),
		EndUnixNanos:        end.UnixNano(),
	}
}

type captureStore struct {
	logStart    time.Time
	logEnd      time.Time
	sampleStart time.Time
	sampleEnd   time.Time

	logStoreChunks    *logproto.ChunkRefGroup
	sampleStoreChunks *logproto.ChunkRefGroup
}

func (s *captureStore) SelectLogs(_ context.Context, req logql.SelectLogParams) (iter.EntryIterator, error) {
	s.logStart = req.Start
	s.logEnd = req.End
	s.logStoreChunks = req.StoreChunks
	return iter.NoopEntryIterator, nil
}

func (s *captureStore) SelectSamples(_ context.Context, req logql.SelectSampleParams) (iter.SampleIterator, error) {
	s.sampleStart = req.Start
	s.sampleEnd = req.End
	s.sampleStoreChunks = req.StoreChunks
	return iter.NoopSampleIterator, nil
}

func (s *captureStore) SelectSeries(context.Context, logql.SelectLogParams) ([]logproto.SeriesIdentifier, error) {
	return nil, nil
}

func (s *captureStore) LabelValuesForMetricName(context.Context, string, model.Time, model.Time, string, string, ...*labels.Matcher) ([]string, error) {
	return nil, nil
}

func (s *captureStore) LabelNamesForMetricName(context.Context, string, model.Time, model.Time, string, ...*labels.Matcher) ([]string, error) {
	return nil, nil
}

func (s *captureStore) Stats(context.Context, string, model.Time, model.Time, ...*labels.Matcher) (*stats.Stats, error) {
	return &stats.Stats{}, nil
}

func (s *captureStore) Volume(context.Context, string, model.Time, model.Time, int32, []string, string, ...*labels.Matcher) (*logproto.VolumeResponse, error) {
	return &logproto.VolumeResponse{}, nil
}

func (s *captureStore) GetShards(context.Context, string, model.Time, model.Time, uint64, chunk.Predicate) (*logproto.ShardsResponse, error) {
	return &logproto.ShardsResponse{}, nil
}

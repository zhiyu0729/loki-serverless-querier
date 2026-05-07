//go:build loki_serverless

package lokistore

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"testing"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/grafana/dskit/user"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"

	logql_log "github.com/grafana/loki/v3/pkg/logql/log"

	"github.com/grafana/loki/v3/pkg/iter"
	"github.com/grafana/loki/v3/pkg/logproto"
	"github.com/grafana/loki/v3/pkg/logql"
	"github.com/grafana/loki/v3/pkg/serverless/executor"
	"github.com/grafana/loki/v3/pkg/serverless/protocol"
	"github.com/grafana/loki/v3/pkg/storage/chunk"
	"github.com/grafana/loki/v3/pkg/storage/chunk/fetcher"
	"github.com/grafana/loki/v3/pkg/storage/config"
	"github.com/grafana/loki/v3/pkg/storage/stores/index/stats"
	"github.com/grafana/loki/v3/pkg/storage/stores/shipper/indexshipper/tsdb/sharding"
)

func TestSelectSamplesUsesLocalStoreWithinIngesterWindow(t *testing.T) {
	local := &captureStore{}
	invoker := &captureInvoker{}
	store := Wrap(local, executor.NewServerlessStoreExecutor(invoker, nil, executor.Options{MaxInterval: time.Hour}), "v3.7.1", "test", false, time.Hour)

	ctx := user.InjectOrgID(context.Background(), "tenant-a")
	_, err := store.SelectSamples(ctx, logql.SelectSampleParams{
		SampleQueryRequest: &logproto.SampleQueryRequest{
			Start:    time.Now().Add(-20 * time.Minute),
			End:      time.Now().Add(-10 * time.Minute),
			Selector: `rate({app="api"}[1m])`,
		},
	})
	if err != nil {
		t.Fatalf("SelectSamples returned error: %v", err)
	}
	if local.sampleCalls != 1 {
		t.Fatalf("local sample calls = %d, want 1", local.sampleCalls)
	}
	if invoker.calls != 0 {
		t.Fatalf("remote invocations = %d, want 0", invoker.calls)
	}
}

func TestSelectSamplesOffloadsOutsideIngesterWindow(t *testing.T) {
	local := &captureStore{}
	invoker := &captureInvoker{}
	store := Wrap(local, executor.NewServerlessStoreExecutor(invoker, nil, executor.Options{MaxInterval: time.Hour}), "v3.7.1", "test", false, time.Hour)

	ctx := user.InjectOrgID(context.Background(), "tenant-a")
	_, err := store.SelectSamples(ctx, logql.SelectSampleParams{
		SampleQueryRequest: &logproto.SampleQueryRequest{
			Start:    time.Now().Add(-3 * time.Hour),
			End:      time.Now().Add(-2 * time.Hour),
			Selector: `rate({app="api"}[1m])`,
		},
	})
	if err != nil {
		t.Fatalf("SelectSamples returned error: %v", err)
	}
	if local.sampleCalls != 0 {
		t.Fatalf("local sample calls = %d, want 0", local.sampleCalls)
	}
	if invoker.calls != 1 {
		t.Fatalf("remote invocations = %d, want 1", invoker.calls)
	}
}

func TestSelectSamplesSplitsAcrossIngesterWindow(t *testing.T) {
	local := &captureStore{}
	invoker := &captureInvoker{}
	store := Wrap(local, executor.NewServerlessStoreExecutor(invoker, nil, executor.Options{MaxInterval: time.Hour}), "v3.7.1", "test", false, time.Hour)

	ctx := user.InjectOrgID(context.Background(), "tenant-a")
	_, err := store.SelectSamples(ctx, logql.SelectSampleParams{
		SampleQueryRequest: &logproto.SampleQueryRequest{
			Start:    time.Now().Add(-2 * time.Hour),
			End:      time.Now().Add(-30 * time.Minute),
			Selector: `rate({app="api"}[1m])`,
		},
	})
	if err != nil {
		t.Fatalf("SelectSamples returned error: %v", err)
	}
	if local.sampleCalls != 1 {
		t.Fatalf("local sample calls = %d, want 1", local.sampleCalls)
	}
	if invoker.calls != 1 {
		t.Fatalf("remote invocations = %d, want 1", invoker.calls)
	}
	if !invoker.sampleEnd.Equal(local.sampleStart) {
		t.Fatalf("remote/local split is out of order: remote end=%s local start=%s", invoker.sampleEnd, local.sampleStart)
	}
}

func TestSelectSamplesWithZeroIngesterWindowUsesLocalStore(t *testing.T) {
	local := &captureStore{}
	invoker := &captureInvoker{}
	store := Wrap(local, executor.NewServerlessStoreExecutor(invoker, nil, executor.Options{MaxInterval: time.Hour}), "v3.7.1", "test", false, 0)

	ctx := user.InjectOrgID(context.Background(), "tenant-a")
	_, err := store.SelectSamples(ctx, logql.SelectSampleParams{
		SampleQueryRequest: &logproto.SampleQueryRequest{
			Start:    time.Now().Add(-24 * time.Hour),
			End:      time.Now().Add(-23 * time.Hour),
			Selector: `rate({app="api"}[1m])`,
		},
	})
	if err != nil {
		t.Fatalf("SelectSamples returned error: %v", err)
	}
	if local.sampleCalls != 1 {
		t.Fatalf("local sample calls = %d, want 1", local.sampleCalls)
	}
	if invoker.calls != 0 {
		t.Fatalf("remote invocations = %d, want 0", invoker.calls)
	}
}

type captureInvoker struct {
	calls       int
	sampleStart time.Time
	sampleEnd   time.Time
}

func (i *captureInvoker) Invoke(_ context.Context, payload []byte) ([]byte, error) {
	i.calls++
	var envelope protocol.InvokeEnvelope
	if err := json.Unmarshal(payload, &envelope); err != nil {
		return nil, err
	}
	var sampleReq logproto.SampleQueryRequest
	if err := decodeLokiRequest(*envelope.Request, &sampleReq); err != nil {
		return nil, err
	}
	i.sampleStart = sampleReq.Start
	i.sampleEnd = sampleReq.End
	body, err := json.Marshal(&logproto.SampleQueryResponse{})
	if err != nil {
		return nil, err
	}
	return json.Marshal(&protocol.ServerlessQueryResponse{
		ProtocolVersion: protocol.ProtocolVersion,
		LokiVersion:     envelope.LokiVersion,
		Status:          protocol.StatusOK,
		InlineResponse:  body,
	})
}

func decodeLokiRequest(req protocol.ServerlessQueryRequest, msg proto.Message) error {
	body, err := base64.StdEncoding.DecodeString(req.LokiRequest)
	if err != nil {
		return err
	}
	return proto.Unmarshal(body, msg)
}

type captureStore struct {
	sampleCalls int
	logCalls    int
	sampleStart time.Time
	sampleEnd   time.Time
}

func (s *captureStore) SelectLogs(context.Context, logql.SelectLogParams) (iter.EntryIterator, error) {
	s.logCalls++
	return iter.NoopEntryIterator, nil
}

func (s *captureStore) SelectSamples(_ context.Context, req logql.SelectSampleParams) (iter.SampleIterator, error) {
	s.sampleCalls++
	s.sampleStart = req.Start
	s.sampleEnd = req.End
	return iter.NoopSampleIterator, nil
}

func (s *captureStore) SelectSeries(context.Context, logql.SelectLogParams) ([]logproto.SeriesIdentifier, error) {
	return nil, nil
}

func (s *captureStore) GetSchemaConfigs() []config.PeriodConfig {
	return nil
}

func (s *captureStore) SetExtractorWrapper(logql_log.SampleExtractorWrapper) {}

func (s *captureStore) SetPipelineWrapper(logql_log.PipelineWrapper) {}

func (s *captureStore) GetSeries(context.Context, string, model.Time, model.Time, ...*labels.Matcher) ([]labels.Labels, error) {
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

func (s *captureStore) HasForSeries(model.Time, model.Time) (sharding.ForSeries, bool) {
	return nil, false
}

func (s *captureStore) HasChunkSizingInfo(model.Time, model.Time) bool {
	return false
}

func (s *captureStore) GetChunkRefsWithSizingInfo(context.Context, string, model.Time, model.Time, chunk.Predicate) ([]logproto.ChunkRefWithSizingInfo, error) {
	return nil, nil
}

func (s *captureStore) SetChunkFilterer(chunk.RequestChunkFilterer) {}

func (s *captureStore) Put(context.Context, []chunk.Chunk) error {
	return nil
}

func (s *captureStore) PutOne(context.Context, model.Time, model.Time, chunk.Chunk) error {
	return nil
}

func (s *captureStore) GetChunks(context.Context, string, model.Time, model.Time, chunk.Predicate, *logproto.ChunkRefGroup) ([][]chunk.Chunk, []*fetcher.Fetcher, error) {
	return nil, nil, nil
}

func (s *captureStore) GetChunkFetcher(model.Time) *fetcher.Fetcher {
	return nil
}

func (s *captureStore) Stop() {}

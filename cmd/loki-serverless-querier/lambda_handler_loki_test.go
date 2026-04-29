//go:build loki_serverless

package main

import (
	"testing"
	"time"

	"github.com/grafana/loki/v3/pkg/loki"
	storageconfig "github.com/grafana/loki/v3/pkg/storage/config"
	"github.com/grafana/loki/v3/pkg/storage/types"
)

func TestPrepareLambdaLokiConfigDefaultsObjectStoreIndexPathPrefix(t *testing.T) {
	wrapper := &loki.ConfigWrapper{}
	wrapper.Config.SchemaConfig.Configs = []storageconfig.PeriodConfig{
		{
			IndexType: types.TSDBType,
			Schema:    "v13",
			IndexTables: storageconfig.IndexPeriodicTableConfig{
				PeriodicTableConfig: storageconfig.PeriodicTableConfig{
					Period: 24 * time.Hour,
				},
			},
		},
	}

	if err := prepareLambdaLokiConfig(wrapper); err != nil {
		t.Fatalf("prepare config: %v", err)
	}

	if got := wrapper.Config.SchemaConfig.Configs[0].IndexTables.PathPrefix; got != "index/" {
		t.Fatalf("TSDB path prefix = %q, want index/", got)
	}
}

func TestPrepareLambdaLokiConfigPreservesObjectStoreIndexPathPrefix(t *testing.T) {
	wrapper := &loki.ConfigWrapper{}
	wrapper.Config.SchemaConfig.Configs = []storageconfig.PeriodConfig{
		{
			IndexType: types.TSDBType,
			Schema:    "v13",
			IndexTables: storageconfig.IndexPeriodicTableConfig{
				PathPrefix: "custom/",
				PeriodicTableConfig: storageconfig.PeriodicTableConfig{
					Period: 24 * time.Hour,
				},
			},
		},
	}

	if err := prepareLambdaLokiConfig(wrapper); err != nil {
		t.Fatalf("prepare config: %v", err)
	}

	if got := wrapper.Config.SchemaConfig.Configs[0].IndexTables.PathPrefix; got != "custom/" {
		t.Fatalf("custom path prefix = %q, want custom/", got)
	}
}

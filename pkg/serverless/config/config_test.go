package config

import (
	"testing"
	"time"
)

func TestStoreConfigDefaultsAndValidate(t *testing.T) {
	cfg := StoreConfig{
		Enabled: true,
		AWS: AWSConfig{
			LambdaFunctionName: "loki-store-query",
		},
		ObjectStore: ObjectStoreConfig{
			Bucket: "loki-serverless-querier-results",
		},
	}
	cfg.SetDefaults()
	if cfg.MaxInterval != DefaultMaxInterval {
		t.Fatalf("max interval = %s", cfg.MaxInterval)
	}
	if err := cfg.Validate(); err != nil {
		t.Fatalf("validate config: %v", err)
	}
}

func TestStoreConfigAllowsEmptyObjectStorePrefix(t *testing.T) {
	cfg := StoreConfig{
		Enabled: true,
		AWS: AWSConfig{
			LambdaFunctionName: "loki-store-query",
		},
		ObjectStore: ObjectStoreConfig{
			Bucket: "loki-serverless-querier-results",
			Prefix: "",
		},
	}
	cfg.SetDefaults()
	if cfg.ObjectStore.Prefix != "" {
		t.Fatalf("prefix = %q", cfg.ObjectStore.Prefix)
	}
	if err := cfg.Validate(); err != nil {
		t.Fatalf("validate config: %v", err)
	}
}

func TestStoreConfigRejectsInvalidIntervals(t *testing.T) {
	cfg := StoreConfig{
		Enabled: true,
		AWS: AWSConfig{
			LambdaFunctionName: "loki-store-query",
		},
		ObjectStore: ObjectStoreConfig{
			Bucket: "loki-serverless-querier-results",
		},
		MaxInterval:   time.Minute,
		MinInterval:   2 * time.Minute,
		MaxConcurrent: 1,
	}
	cfg.SetDefaults()
	if err := cfg.Validate(); err == nil {
		t.Fatal("expected validation error")
	}
}

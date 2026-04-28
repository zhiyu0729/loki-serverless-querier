//go:build loki_serverless

package backend

import (
	"context"
	"fmt"

	"github.com/grafana/loki/v3/pkg/serverless/awsruntime"
	"github.com/grafana/loki/v3/pkg/serverless/config"
	"github.com/grafana/loki/v3/pkg/serverless/executor"
	"github.com/grafana/loki/v3/pkg/serverless/objectstore"
)

type Capabilities struct {
	MaxInvokePayloadBytes int
}

type Backend interface {
	Provider() string
	Invoker() executor.Invoker
	ObjectStore() objectstore.Store
	Capabilities() Capabilities
}

type staticBackend struct {
	provider     string
	invoker      executor.Invoker
	objectStore  objectstore.Store
	capabilities Capabilities
}

func (b staticBackend) Provider() string               { return b.provider }
func (b staticBackend) Invoker() executor.Invoker      { return b.invoker }
func (b staticBackend) ObjectStore() objectstore.Store { return b.objectStore }
func (b staticBackend) Capabilities() Capabilities     { return b.capabilities }

func New(ctx context.Context, cfg config.StoreConfig) (Backend, error) {
	cfg.SetDefaults()
	if err := cfg.Validate(); err != nil {
		return nil, err
	}

	switch cfg.Provider {
	case config.ProviderAWSLambda:
		return newAWSLambdaBackend(ctx, cfg)
	default:
		return nil, fmt.Errorf("unsupported serverless store provider %q", cfg.Provider)
	}
}

func NewObjectStore(_ context.Context, cfg config.StoreConfig) (objectstore.Store, error) {
	cfg.SetDefaults()
	switch cfg.ObjectStore.Type {
	case config.ObjectStoreS3:
		region := cfg.ObjectStore.Region
		if region == "" {
			region = cfg.AWS.Region
		}
		return awsruntime.NewS3Store(region, cfg.ObjectStore.Bucket, cfg.ObjectStore.Prefix)
	default:
		return nil, fmt.Errorf("unsupported serverless object store %q", cfg.ObjectStore.Type)
	}
}

func newAWSLambdaBackend(ctx context.Context, cfg config.StoreConfig) (Backend, error) {
	invoker, err := awsruntime.NewLambdaInvoker(cfg.AWS.Region, cfg.AWS.LambdaFunctionName)
	if err != nil {
		return nil, err
	}
	store, err := NewObjectStore(ctx, cfg)
	if err != nil {
		return nil, err
	}
	return staticBackend{
		provider:    cfg.Provider,
		invoker:     invoker,
		objectStore: store,
		capabilities: Capabilities{
			MaxInvokePayloadBytes: config.DefaultMaxInvokePayloadBytes,
		},
	}, nil
}

package lambdaexec

import (
	"context"
	"encoding/json"
	"errors"

	"github.com/grafana/loki/v3/pkg/serverless/protocol"
)

type UnwiredRunner struct{}

func (UnwiredRunner) RunStoreOnly(context.Context, protocol.ServerlessQueryRequest) (json.RawMessage, error) {
	return nil, errors.New("lambda executor is not wired to Loki's store-only query runner for this Loki version")
}

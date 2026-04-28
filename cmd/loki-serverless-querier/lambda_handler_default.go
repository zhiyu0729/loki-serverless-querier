//go:build !loki_serverless

package main

import (
	"context"

	"github.com/grafana/loki/v3/pkg/serverless/lambdaexec"
)

func newLambdaHandler(_ context.Context, inlineLimit int64, lokiVersion, overlayVersion string, _ []string) (*lambdaexec.Handler, error) {
	return lambdaexec.NewHandler(lambdaexec.UnwiredRunner{}, nil, nil, inlineLimit, lokiVersion, overlayVersion), nil
}

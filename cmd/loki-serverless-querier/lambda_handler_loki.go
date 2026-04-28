//go:build loki_serverless

package main

import (
	"context"
	"flag"
	"os"
	"strings"

	"github.com/go-kit/log/level"

	"github.com/grafana/loki/v3/pkg/loki"
	loki_runtime "github.com/grafana/loki/v3/pkg/runtime"
	serverlessbackend "github.com/grafana/loki/v3/pkg/serverless/backend"
	"github.com/grafana/loki/v3/pkg/serverless/lambdaexec"
	"github.com/grafana/loki/v3/pkg/serverless/lokirunner"
	"github.com/grafana/loki/v3/pkg/util/cfg"
	util_log "github.com/grafana/loki/v3/pkg/util/log"
	"github.com/grafana/loki/v3/pkg/validation"
)

func newLambdaHandler(ctx context.Context, inlineLimit int64, lokiVersion, overlayVersion string) (*lambdaexec.Handler, error) {
	configArgs := lokiConfigArgs()
	var wrapper loki.ConfigWrapper
	if err := cfg.DynamicUnmarshal(&wrapper, configArgs, flag.NewFlagSet("lambda-executor-loki-config", flag.ContinueOnError)); err != nil {
		return nil, err
	}

	validation.SetDefaultLimitsForYAMLUnmarshalling(wrapper.LimitsConfig)
	loki_runtime.SetDefaultLimitsForYAMLUnmarshalling(wrapper.OperationalConfig)

	storeCfg := wrapper.Config.ServerlessStore
	storeCfg.SetDefaults()
	// Lambda executor must run Loki's local store directly. If it reused a
	// querier config with serverless_store enabled, the Lambda would call itself.
	wrapper.Config.ServerlessStore.Enabled = false

	t, err := loki.New(wrapper.Config)
	if err != nil {
		return nil, err
	}
	if _, err := t.ModuleManager.InitModuleServices(loki.Store); err != nil {
		return nil, err
	}

	resultStore, err := serverlessbackend.NewObjectStore(ctx, storeCfg)
	if err != nil {
		level.Warn(util_log.Logger).Log("msg", "serverless result store is not configured; large lambda responses will fail", "err", err)
		resultStore = nil
	}

	return lambdaexec.NewHandler(lokirunner.New(t.Store), resultStore, resultStore, inlineLimit, lokiVersion, overlayVersion), nil
}

func lokiConfigArgs() []string {
	if raw := firstEnv("LOKI_SERVERLESS_QUERIER_CONFIG_ARGS", "SERVERLESS_LOKI_CONFIG_ARGS"); raw != "" {
		return strings.Fields(raw)
	}
	if file := firstEnv("LOKI_SERVERLESS_QUERIER_CONFIG_FILE", "SERVERLESS_LOKI_CONFIG_FILE"); file != "" {
		return []string{"-config.file=" + file}
	}
	return os.Args[1:]
}

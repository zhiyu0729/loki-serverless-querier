//go:build loki_serverless

package main

import (
	"context"
	"encoding/base64"
	"flag"
	"fmt"
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

func newLambdaHandler(ctx context.Context, inlineLimit int64, lokiVersion, overlayVersion string, fallbackArgs []string) (*lambdaexec.Handler, error) {
	configArgs, err := lokiConfigArgs(fallbackArgs)
	if err != nil {
		return nil, err
	}
	var wrapper loki.ConfigWrapper
	if err := cfg.DynamicUnmarshal(&wrapper, configArgs, flag.NewFlagSet("lambda-executor-loki-config", flag.ContinueOnError)); err != nil {
		return nil, err
	}

	validation.SetDefaultLimitsForYAMLUnmarshalling(wrapper.LimitsConfig)
	loki_runtime.SetDefaultLimitsForYAMLUnmarshalling(wrapper.OperationalConfig)
	if err := prepareLambdaLokiConfig(&wrapper); err != nil {
		return nil, err
	}

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

func prepareLambdaLokiConfig(wrapper *loki.ConfigWrapper) error {
	const loopbackAddr = "127.0.0.1"
	if wrapper.Config.Common.InstanceAddr == "" {
		wrapper.Config.Common.InstanceAddr = loopbackAddr
	}
	if wrapper.Config.Common.Ring.InstanceAddr == "" {
		wrapper.Config.Common.Ring.InstanceAddr = wrapper.Config.Common.InstanceAddr
	}
	if wrapper.Config.MemberlistKV.AdvertiseAddr == "" {
		wrapper.Config.MemberlistKV.AdvertiseAddr = wrapper.Config.Common.InstanceAddr
	}
	if err := wrapper.Config.SchemaConfig.Validate(); err != nil {
		return fmt.Errorf("validate schema config: %w", err)
	}
	return nil
}

func lokiConfigArgs(fallbackArgs []string) ([]string, error) {
	if raw := firstEnv("LOKI_SERVERLESS_QUERIER_CONFIG_B64", "SERVERLESS_LOKI_CONFIG_B64"); raw != "" {
		file, err := writeBase64Config(raw)
		if err != nil {
			return nil, err
		}
		return []string{"-config.file=" + file}, nil
	}
	if raw := firstEnv("LOKI_SERVERLESS_QUERIER_CONFIG_ARGS", "SERVERLESS_LOKI_CONFIG_ARGS"); raw != "" {
		return strings.Fields(raw), nil
	}
	if file := firstEnv("LOKI_SERVERLESS_QUERIER_CONFIG_FILE", "SERVERLESS_LOKI_CONFIG_FILE"); file != "" {
		return []string{"-config.file=" + file}, nil
	}
	return fallbackArgs, nil
}

func writeBase64Config(raw string) (string, error) {
	body, err := base64.StdEncoding.DecodeString(strings.TrimSpace(raw))
	if err != nil {
		return "", fmt.Errorf("decode base64 Loki config: %w", err)
	}
	file, err := os.CreateTemp("", "loki-serverless-config-*.yaml")
	if err != nil {
		return "", fmt.Errorf("create temporary Loki config file: %w", err)
	}
	if _, err := file.Write(body); err != nil {
		_ = file.Close()
		_ = os.Remove(file.Name())
		return "", fmt.Errorf("write temporary Loki config file: %w", err)
	}
	if err := file.Close(); err != nil {
		_ = os.Remove(file.Name())
		return "", fmt.Errorf("close temporary Loki config file: %w", err)
	}
	return file.Name(), nil
}

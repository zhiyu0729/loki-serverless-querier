package main

import (
	"context"
	"fmt"
	"io"
	"log"
	"os"
	"strconv"
	"strings"
	"syscall"
	"time"

	serverlessruntime "github.com/grafana/loki/v3/pkg/serverless/runtime"
)

var (
	lokiVersion    = "unknown"
	overlayVersion = "dev"
	gitSHA         = "unknown"
)

type cliConfig struct {
	mode           string
	once           bool
	inlineLimit    int64
	runtimeTimeout time.Duration
	lokiArgs       []string
}

func main() {
	cfg, err := parseCLI(os.Args[1:])
	if err != nil {
		log.Fatal(err)
	}
	if cfg.mode == "" {
		cfg.mode = firstEnv("LOKI_SERVERLESS_QUERIER_MODE", "SERVERLESS_LOKI_MODE")
	}
	if cfg.mode == "" {
		cfg.mode = defaultMode()
	}

	log.Printf("starting loki-serverless-querier mode=%s loki_version=%s overlay_version=%s git_sha=%s", cfg.mode, lokiVersion, overlayVersion, gitSHA)

	ctx := context.Background()
	if cfg.runtimeTimeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, cfg.runtimeTimeout)
		defer cancel()
	}

	switch cfg.mode {
	case "serverless-querier":
		runServerlessQuerier(cfg.lokiArgs)
	case "lambda-executor":
		runLambdaExecutor(ctx, cfg.once, cfg.inlineLimit, cfg.lokiArgs)
	case "version":
		fmt.Printf("loki_version=%s overlay_version=%s git_sha=%s\n", lokiVersion, overlayVersion, gitSHA)
	default:
		log.Fatalf("unknown mode %q; expected serverless-querier, lambda-executor, or version", cfg.mode)
	}
}

func defaultMode() string {
	if os.Getenv("AWS_LAMBDA_RUNTIME_API") != "" {
		return "lambda-executor"
	}
	return "serverless-querier"
}

func parseCLI(args []string) (cliConfig, error) {
	cfg := cliConfig{inlineLimit: 4 * 1024 * 1024}
	for i := 0; i < len(args); i++ {
		arg := args[i]
		if arg == "--" {
			cfg.lokiArgs = append(cfg.lokiArgs, args[i+1:]...)
			break
		}
		switch {
		case arg == "-mode" || arg == "--mode":
			i++
			if i >= len(args) {
				return cfg, fmt.Errorf("%s requires a value", arg)
			}
			cfg.mode = args[i]
		case strings.HasPrefix(arg, "-mode="):
			cfg.mode = strings.TrimPrefix(arg, "-mode=")
		case strings.HasPrefix(arg, "--mode="):
			cfg.mode = strings.TrimPrefix(arg, "--mode=")
		case arg == "-once" || arg == "--once":
			cfg.once = true
		case strings.HasPrefix(arg, "-once="):
			value := strings.TrimPrefix(arg, "-once=")
			parsed, err := strconv.ParseBool(value)
			if err != nil {
				return cfg, err
			}
			cfg.once = parsed
		case strings.HasPrefix(arg, "--once="):
			value := strings.TrimPrefix(arg, "--once=")
			parsed, err := strconv.ParseBool(value)
			if err != nil {
				return cfg, err
			}
			cfg.once = parsed
		case arg == "-inline-response-limit" || arg == "--inline-response-limit":
			i++
			if i >= len(args) {
				return cfg, fmt.Errorf("%s requires a value", arg)
			}
			parsed, err := strconv.ParseInt(args[i], 10, 64)
			if err != nil {
				return cfg, err
			}
			cfg.inlineLimit = parsed
		case strings.HasPrefix(arg, "-inline-response-limit="):
			parsed, err := strconv.ParseInt(strings.TrimPrefix(arg, "-inline-response-limit="), 10, 64)
			if err != nil {
				return cfg, err
			}
			cfg.inlineLimit = parsed
		case strings.HasPrefix(arg, "--inline-response-limit="):
			parsed, err := strconv.ParseInt(strings.TrimPrefix(arg, "--inline-response-limit="), 10, 64)
			if err != nil {
				return cfg, err
			}
			cfg.inlineLimit = parsed
		case arg == "-runtime-timeout" || arg == "--runtime-timeout":
			i++
			if i >= len(args) {
				return cfg, fmt.Errorf("%s requires a value", arg)
			}
			parsed, err := time.ParseDuration(args[i])
			if err != nil {
				return cfg, err
			}
			cfg.runtimeTimeout = parsed
		case strings.HasPrefix(arg, "-runtime-timeout="):
			parsed, err := time.ParseDuration(strings.TrimPrefix(arg, "-runtime-timeout="))
			if err != nil {
				return cfg, err
			}
			cfg.runtimeTimeout = parsed
		case strings.HasPrefix(arg, "--runtime-timeout="):
			parsed, err := time.ParseDuration(strings.TrimPrefix(arg, "--runtime-timeout="))
			if err != nil {
				return cfg, err
			}
			cfg.runtimeTimeout = parsed
		default:
			cfg.lokiArgs = append(cfg.lokiArgs, arg)
		}
	}
	return cfg, nil
}

func runServerlessQuerier(args []string) {
	lokiPath := firstEnv("LOKI_SERVERLESS_QUERIER_EXEC", "SERVERLESS_LOKI_EXEC")
	if lokiPath == "" {
		lokiPath = "/usr/bin/loki"
	}

	execArgs := []string{lokiPath}
	if !hasTargetArg(args) {
		execArgs = append(execArgs, "-target=querier")
	}
	execArgs = append(execArgs, args...)

	if err := syscall.Exec(lokiPath, execArgs, os.Environ()); err != nil {
		log.Fatalf("exec loki: %v", err)
	}
}

func hasTargetArg(args []string) bool {
	for i, arg := range args {
		if arg == "-target" || arg == "--target" {
			return i+1 < len(args)
		}
		if strings.HasPrefix(arg, "-target=") || strings.HasPrefix(arg, "--target=") {
			return true
		}
	}
	return false
}

func runLambdaExecutor(ctx context.Context, once bool, inlineLimit int64, lokiArgs []string) {
	handler, err := newLambdaHandler(ctx, inlineLimit, lokiVersion, overlayVersion, lokiArgs)
	if err != nil {
		log.Fatalf("create lambda handler: %v", err)
	}

	if once {
		payload, err := io.ReadAll(os.Stdin)
		if err != nil {
			log.Fatalf("read invoke payload: %v", err)
		}
		resp, err := handler.HandleRaw(ctx, payload)
		if err != nil {
			log.Fatalf("handle invoke payload: %v", err)
		}
		_, _ = os.Stdout.Write(resp)
		_, _ = os.Stdout.Write([]byte("\n"))
		return
	}

	if os.Getenv("AWS_LAMBDA_RUNTIME_API") == "" {
		log.Fatalf("lambda-executor mode requires AWS_LAMBDA_RUNTIME_API, or pass -once=true for local stdin/stdout execution")
	}

	clientTimeout := 0 * time.Second
	if v := firstEnv("LOKI_SERVERLESS_QUERIER_RUNTIME_HTTP_TIMEOUT_SECONDS", "SERVERLESS_RUNTIME_HTTP_TIMEOUT_SECONDS"); v != "" {
		seconds, err := strconv.Atoi(v)
		if err != nil {
			log.Fatalf("invalid runtime HTTP timeout seconds: %v", err)
		}
		clientTimeout = time.Duration(seconds) * time.Second
	}

	if err := serverlessruntime.Serve(ctx, handler.HandleRaw, clientTimeout); err != nil {
		log.Fatalf("lambda runtime exited: %v", err)
	}
}

func firstEnv(names ...string) string {
	for _, name := range names {
		if value := os.Getenv(name); value != "" {
			return value
		}
	}
	return ""
}

package main

import "testing"

func TestParseCLIPreservesLokiArgs(t *testing.T) {
	cfg, err := parseCLI([]string{
		"-mode=serverless-querier",
		"-config.file=/etc/loki/config.yaml",
		"-querier.frontend-address=dns:///query-frontend:9095",
	})
	if err != nil {
		t.Fatalf("parse cli: %v", err)
	}
	if cfg.mode != "serverless-querier" {
		t.Fatalf("mode = %q", cfg.mode)
	}
	if len(cfg.lokiArgs) != 2 {
		t.Fatalf("loki args = %#v", cfg.lokiArgs)
	}
}

func TestDefaultModeUsesLambdaExecutorInsideLambda(t *testing.T) {
	t.Setenv("AWS_LAMBDA_RUNTIME_API", "127.0.0.1:9001")
	if mode := defaultMode(); mode != "lambda-executor" {
		t.Fatalf("mode = %q", mode)
	}
}

func TestDefaultModeUsesServerlessQuerierOutsideLambda(t *testing.T) {
	t.Setenv("AWS_LAMBDA_RUNTIME_API", "")
	if mode := defaultMode(); mode != "serverless-querier" {
		t.Fatalf("mode = %q", mode)
	}
}

func TestHasTargetArg(t *testing.T) {
	if !hasTargetArg([]string{"-target=read"}) {
		t.Fatal("expected inline target to be detected")
	}
	if !hasTargetArg([]string{"-target", "querier"}) {
		t.Fatal("expected split target to be detected")
	}
	if hasTargetArg([]string{"-config.file=/etc/loki/config.yaml"}) {
		t.Fatal("did not expect target")
	}
}

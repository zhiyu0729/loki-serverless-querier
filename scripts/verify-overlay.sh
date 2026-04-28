#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

required=(
  "$ROOT_DIR/cmd/loki-serverless-querier/main.go"
  "$ROOT_DIR/pkg/serverless/protocol/protocol.go"
  "$ROOT_DIR/pkg/serverless/executor/executor.go"
  "$ROOT_DIR/pkg/serverless/lambdaexec/handler.go"
  "$ROOT_DIR/docker/Dockerfile.serverless"
  "$ROOT_DIR/patches/README.md"
)

for path in "${required[@]}"; do
  if [ ! -e "$path" ]; then
    echo "missing required overlay file: $path" >&2
    exit 1
  fi
done

go test ./...

echo "overlay verification passed"

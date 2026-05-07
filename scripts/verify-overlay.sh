#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
LOKI_VERSION="${LOKI_VERSION:-v3.7.1}"
WORK_DIR="${WORK_DIR:-$ROOT_DIR/build/loki-$LOKI_VERSION}"
SKIP_FETCH="${SKIP_FETCH:-0}"

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

mkdir -p "$(dirname "$WORK_DIR")"

if [ ! -d "$WORK_DIR/.git" ]; then
  git clone --depth 1 --branch "$LOKI_VERSION" "https://github.com/grafana/loki.git" "$WORK_DIR"
else
  if [ "$SKIP_FETCH" != "1" ]; then
    git -C "$WORK_DIR" fetch --depth 1 origin "refs/tags/$LOKI_VERSION:refs/tags/$LOKI_VERSION"
  fi
  git -C "$WORK_DIR" checkout --force "$LOKI_VERSION"
fi

git -C "$WORK_DIR" clean -fd -- cmd/serverless-loki cmd/loki-serverless-querier pkg/serverless dist >/dev/null

mkdir -p "$WORK_DIR/cmd/loki-serverless-querier"
mkdir -p "$WORK_DIR/pkg/serverless"

cp -R "$ROOT_DIR/cmd/loki-serverless-querier/." "$WORK_DIR/cmd/loki-serverless-querier/"
cp -R "$ROOT_DIR/pkg/serverless/." "$WORK_DIR/pkg/serverless/"

apply_patch_dir() {
  local patch_dir="$1"
  if [ ! -d "$patch_dir" ]; then
    return 0
  fi
  find "$patch_dir" -type f -name '*.patch' | sort | while read -r patch_file; do
    git -C "$WORK_DIR" apply "$patch_file"
  done
}

apply_patch_dir "$ROOT_DIR/patches/common"
apply_patch_dir "$ROOT_DIR/patches/$LOKI_VERSION"

(
  cd "$WORK_DIR"
  go test -tags loki_serverless ./cmd/loki-serverless-querier ./pkg/serverless/...
)

echo "overlay verification passed"

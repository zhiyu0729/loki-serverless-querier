#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
LOKI_VERSION="${LOKI_VERSION:-v3.7.1}"
LOKI_REPO="${LOKI_REPO:-https://github.com/grafana/loki.git}"
WORK_DIR="${WORK_DIR:-$ROOT_DIR/build/loki-$LOKI_VERSION}"
IMAGE="${IMAGE:-loki-serverless-querier:$LOKI_VERSION}"
OVERLAY_VERSION="${OVERLAY_VERSION:-dev}"
GO_VERSION="${GO_VERSION:-1.25.7}"
GO_BUILD_TAGS="${GO_BUILD_TAGS:-loki_serverless}"
BUILD_STRATEGY="${BUILD_STRATEGY:-docker}"
TARGETOS="${TARGETOS:-linux}"
TARGETARCH="${TARGETARCH:-$(go env GOARCH)}"
SKIP_FETCH="${SKIP_FETCH:-0}"
LAMBDA_ZIP="${LAMBDA_ZIP:-$WORK_DIR/dist/loki-serverless-querier-lambda-$TARGETARCH.zip}"

if ! command -v git >/dev/null 2>&1; then
  echo "git is required" >&2
  exit 1
fi

if [ "$BUILD_STRATEGY" != "lambda-zip" ] && ! command -v docker >/dev/null 2>&1; then
  echo "docker is required" >&2
  exit 1
fi

if [ "$BUILD_STRATEGY" = "lambda-zip" ] && ! command -v zip >/dev/null 2>&1; then
  echo "zip is required" >&2
  exit 1
fi

mkdir -p "$(dirname "$WORK_DIR")"

if [ ! -d "$WORK_DIR/.git" ]; then
  git clone --depth 1 --branch "$LOKI_VERSION" "$LOKI_REPO" "$WORK_DIR"
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
    echo "applying patch $patch_file"
    git -C "$WORK_DIR" apply "$patch_file"
  done
}

apply_patch_dir "$ROOT_DIR/patches/common"
apply_patch_dir "$ROOT_DIR/patches/$LOKI_VERSION"

go_build() {
  local tags="$1"
  local output="$2"
  local ldflags="$3"
  local package="$4"

  CGO_ENABLED=0 GOOS="$TARGETOS" GOARCH="$TARGETARCH" go build -tags "$tags" \
    -trimpath \
    -ldflags "$ldflags" \
    -o "$output" \
    "$package"
}

go_build_with_fallback() {
  local output="$1"
  local ldflags="$2"
  local package="$3"

  if go_build "$GO_BUILD_TAGS" "$output" "$ldflags" "$package"; then
    return 0
  fi

  if [ "$GO_BUILD_TAGS" = "loki_serverless" ] && [ "$TARGETARCH" = "amd64" ]; then
    echo "retrying build with loki_serverless noasm nosimd tags for amd64" >&2
    go_build "loki_serverless noasm nosimd" "$output" "$ldflags" "$package"
    return
  fi

  return 1
}

if [ "$BUILD_STRATEGY" = "lambda-zip" ]; then
  mkdir -p "$WORK_DIR/dist"
  mkdir -p "$(dirname "$LAMBDA_ZIP")"
  GIT_SHA="$(git -C "$WORK_DIR" rev-parse --short HEAD)"
  (
    cd "$WORK_DIR"
    go_build_with_fallback \
      "$WORK_DIR/dist/bootstrap" \
      "-s -w -X main.lokiVersion=$LOKI_VERSION -X main.overlayVersion=$OVERLAY_VERSION -X main.gitSHA=$GIT_SHA -X github.com/grafana/loki/v3/pkg/serverless/buildinfo.LokiVersion=$LOKI_VERSION -X github.com/grafana/loki/v3/pkg/serverless/buildinfo.OverlayVersion=$OVERLAY_VERSION -X github.com/grafana/loki/v3/pkg/serverless/buildinfo.GitSHA=$GIT_SHA" \
      ./cmd/loki-serverless-querier
    chmod 0755 "$WORK_DIR/dist/bootstrap"
    rm -f "$WORK_DIR/dist/loki-serverless-querier-lambda.zip"
    (cd "$WORK_DIR/dist" && zip -q loki-serverless-querier-lambda.zip bootstrap)
  )
  mv "$WORK_DIR/dist/loki-serverless-querier-lambda.zip" "$LAMBDA_ZIP"
  echo "built Lambda zip $LAMBDA_ZIP from Loki $LOKI_VERSION with overlay $OVERLAY_VERSION for $TARGETOS/$TARGETARCH"
  exit 0
fi

if [ "$BUILD_STRATEGY" = "local" ]; then
  mkdir -p "$WORK_DIR/dist"
  GIT_SHA="$(git -C "$WORK_DIR" rev-parse --short HEAD)"
  (
    cd "$WORK_DIR"
    go_build_with_fallback \
      "$WORK_DIR/dist/loki-serverless-querier" \
      "-s -w -X main.lokiVersion=$LOKI_VERSION -X main.overlayVersion=$OVERLAY_VERSION -X main.gitSHA=$GIT_SHA -X github.com/grafana/loki/v3/pkg/serverless/buildinfo.LokiVersion=$LOKI_VERSION -X github.com/grafana/loki/v3/pkg/serverless/buildinfo.OverlayVersion=$OVERLAY_VERSION -X github.com/grafana/loki/v3/pkg/serverless/buildinfo.GitSHA=$GIT_SHA" \
      ./cmd/loki-serverless-querier
    go_build_with_fallback \
      "$WORK_DIR/dist/loki" \
      "-s -w -X github.com/grafana/loki/v3/pkg/serverless/buildinfo.LokiVersion=$LOKI_VERSION -X github.com/grafana/loki/v3/pkg/serverless/buildinfo.OverlayVersion=$OVERLAY_VERSION -X github.com/grafana/loki/v3/pkg/serverless/buildinfo.GitSHA=$GIT_SHA" \
      ./cmd/loki
  )
  docker build \
    --platform "$TARGETOS/$TARGETARCH" \
    --file "$ROOT_DIR/docker/Dockerfile.serverless-prebuilt" \
    --build-arg "LOKI_VERSION=$LOKI_VERSION" \
    --build-arg "OVERLAY_VERSION=$OVERLAY_VERSION" \
    --build-arg "GIT_SHA=$GIT_SHA" \
    --label "org.opencontainers.image.version=$LOKI_VERSION" \
    --label "dev.loki-serverless-querier.overlay-version=$OVERLAY_VERSION" \
    --tag "$IMAGE" \
    "$WORK_DIR"
  echo "built $IMAGE from Loki $LOKI_VERSION with overlay $OVERLAY_VERSION using local Go for $TARGETOS/$TARGETARCH"
  exit 0
fi

docker build \
  --file "$ROOT_DIR/docker/Dockerfile.serverless" \
  --build-arg "LOKI_VERSION=$LOKI_VERSION" \
  --build-arg "OVERLAY_VERSION=$OVERLAY_VERSION" \
  --build-arg "GIT_SHA=$(git -C "$WORK_DIR" rev-parse --short HEAD)" \
  --build-arg "GO_VERSION=$GO_VERSION" \
  --build-arg "GO_BUILD_TAGS=$GO_BUILD_TAGS" \
  --label "org.opencontainers.image.version=$LOKI_VERSION" \
  --label "dev.loki-serverless-querier.overlay-version=$OVERLAY_VERSION" \
  --tag "$IMAGE" \
  "$WORK_DIR"

echo "built $IMAGE from Loki $LOKI_VERSION with overlay $OVERLAY_VERSION"

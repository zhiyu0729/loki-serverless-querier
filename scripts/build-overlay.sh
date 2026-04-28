#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
LOKI_VERSION="${LOKI_VERSION:-v3.7.1}"
LOKI_REPO="${LOKI_REPO:-https://github.com/grafana/loki.git}"
WORK_DIR="${WORK_DIR:-$ROOT_DIR/build/loki-$LOKI_VERSION}"
IMAGE="${IMAGE:-loki-serverless-querier:$LOKI_VERSION}"
OVERLAY_VERSION="${OVERLAY_VERSION:-dev}"
GO_VERSION="${GO_VERSION:-1.25.7}"
BUILD_STRATEGY="${BUILD_STRATEGY:-docker}"
TARGETOS="${TARGETOS:-linux}"
TARGETARCH="${TARGETARCH:-$(go env GOARCH)}"
SKIP_FETCH="${SKIP_FETCH:-0}"

if ! command -v git >/dev/null 2>&1; then
  echo "git is required" >&2
  exit 1
fi

if ! command -v docker >/dev/null 2>&1; then
  echo "docker is required" >&2
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

if [ "$BUILD_STRATEGY" = "local" ]; then
  mkdir -p "$WORK_DIR/dist"
  GIT_SHA="$(git -C "$WORK_DIR" rev-parse --short HEAD)"
  (
    cd "$WORK_DIR"
    CGO_ENABLED=0 GOOS="$TARGETOS" GOARCH="$TARGETARCH" go build -tags loki_serverless \
      -trimpath \
      -ldflags "-s -w -X main.lokiVersion=$LOKI_VERSION -X main.overlayVersion=$OVERLAY_VERSION -X main.gitSHA=$GIT_SHA -X github.com/grafana/loki/v3/pkg/serverless/buildinfo.LokiVersion=$LOKI_VERSION -X github.com/grafana/loki/v3/pkg/serverless/buildinfo.OverlayVersion=$OVERLAY_VERSION -X github.com/grafana/loki/v3/pkg/serverless/buildinfo.GitSHA=$GIT_SHA" \
      -o "$WORK_DIR/dist/loki-serverless-querier" \
      ./cmd/loki-serverless-querier
    CGO_ENABLED=0 GOOS="$TARGETOS" GOARCH="$TARGETARCH" go build -tags loki_serverless \
      -trimpath \
      -ldflags "-s -w -X github.com/grafana/loki/v3/pkg/serverless/buildinfo.LokiVersion=$LOKI_VERSION -X github.com/grafana/loki/v3/pkg/serverless/buildinfo.OverlayVersion=$OVERLAY_VERSION -X github.com/grafana/loki/v3/pkg/serverless/buildinfo.GitSHA=$GIT_SHA" \
      -o "$WORK_DIR/dist/loki" \
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
  --label "org.opencontainers.image.version=$LOKI_VERSION" \
  --label "dev.loki-serverless-querier.overlay-version=$OVERLAY_VERSION" \
  --tag "$IMAGE" \
  "$WORK_DIR"

echo "built $IMAGE from Loki $LOKI_VERSION with overlay $OVERLAY_VERSION"

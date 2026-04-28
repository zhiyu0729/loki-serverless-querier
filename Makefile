LOKI_VERSION ?= v3.7.1
IMAGE ?= loki-serverless-querier:$(LOKI_VERSION)
OVERLAY_VERSION ?= dev
GO_VERSION ?= 1.25.7

.PHONY: test verify build-overlay build-overlay-local build-lambda-zip

test:
	go test ./...

verify: test
	./scripts/verify-overlay.sh

build-overlay:
	LOKI_VERSION=$(LOKI_VERSION) IMAGE=$(IMAGE) OVERLAY_VERSION=$(OVERLAY_VERSION) GO_VERSION=$(GO_VERSION) ./scripts/build-overlay.sh

build-overlay-local:
	LOKI_VERSION=$(LOKI_VERSION) IMAGE=$(IMAGE) OVERLAY_VERSION=$(OVERLAY_VERSION) GO_VERSION=$(GO_VERSION) BUILD_STRATEGY=local ./scripts/build-overlay.sh

build-lambda-zip:
	LOKI_VERSION=$(LOKI_VERSION) OVERLAY_VERSION=$(OVERLAY_VERSION) GO_VERSION=$(GO_VERSION) BUILD_STRATEGY=lambda-zip ./scripts/build-overlay.sh

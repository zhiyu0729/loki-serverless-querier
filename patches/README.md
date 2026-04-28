# Patch Policy

The overlay should be implemented with new files whenever possible. Patches are
reserved for narrow Loki-version-specific hook points, especially:

- registering the `serverless-querier` startup target
- wiring `pkg/serverless/executor.Executor` into the querier store execution path
- wiring `pkg/serverless/lambdaexec.Handler` to a real store-only Loki query runner

Patch directories are applied in this order:

1. `patches/common/*.patch`
2. `patches/$LOKI_VERSION/*.patch`

Keep patches small enough that a failed upgrade points directly at the changed
Loki API surface.

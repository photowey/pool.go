# Changelog

All notable changes to this project are documented in this file.

## [v1.0.0] - 2026-06-29

### Added

- Added a single public facade package: `pkg/pool`.
- Added bounded fixed-size execution with `pool.ExecuteRequest`, explicit
  queue sizing, and reject policies.
- Added typed `pool.Future[T]`, producer-owned `pool.Promise[T]`, and ready
  future helpers.
- Added future composition helpers: `All`, `AllOf`, `Any`, `AnyOf`,
  `ThenApply`, `ThenCompose`, and `Exceptionally`.
- Added executor metrics and panic handler contracts.
- Added private implementation packages under root `internal/`.
- Added package examples, a runnable example, benchmarks, and release-readiness
  Makefile targets.
- Added design, API guide, and architecture documentation.

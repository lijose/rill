# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.1.2] - 2026-11-10

- Support DuckDB 2.0 while retaining 1.5.4 compatibility; add pinned test profiles and CI.
- Correct upsert timestamp alignment, chained SQL visibility, alert retries, retention, buffer limits, and engine cleanup.
- Isolate dashboard connection state and reconnect subscriptions; escape external data and validate complete SQL statements.
- Serve pinned chart assets locally; default to localhost with token authentication for remote access and restricted webhook destinations.
- Simplify the dashboard overview, query controls, and responsive layout.
- Require Python 3.10+ and document changed lifecycle/retention contracts in `docs/review-changes.md`.
- Remove the dashboard theme picker; use the default theme consistently (dashboard 1.0.1).

## [0.1.1] - 2026-07-31
### Added
- Added `query_metrics()` to `RillEngine` and `OutputAPI` to formalize metrics-DB access.

### Changed
- Private dictionary `engine._tables` is now used to track internal tables, replacing `engine.tables`.

### Removed
- Removed business metrics evaluation.
- Removed `OutputAPI.subscribe_table()`.
- Removed `restore_checkpoints()` method.

## [0.1.0] - 2026-07-30
### Added
- Initial release of the `rills` (formerly `rill`) streaming engine.
- High-performance PyArrow and DuckDB micro-batching integration.
- Built-in multi-stream join tasks and scheduled SQL tasks.
- In-memory, JSON, file, Kafka, and WebSocket streaming connectors.
- System metrics and business metrics logging via Quack dashboard integration.
- Type marker (`py.typed`) to support fully typed development.

### Changed
- Renamed PyPI package and module from `rill` to `rills`.
- SQL identifiers in metrics and SQL tasks are now properly quoted for defense-in-depth against SQL injection.
- Optimized image assets for the Quack dashboard (`logo.png`).

### Removed
- Dead `checkpoint_dir` and `checkpoint_interval_seconds` arguments from `RillEngine` to finalize the API for `v0.1.0`.

# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]
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

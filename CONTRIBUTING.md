# Contributing to Rill Streaming Engine

First off, thank you for considering contributing to **Rill**! We welcome bug reports, feature requests, documentation improvements, and pull requests from everyone.

## Local Development Setup

1. **Clone the repository**:
   ```bash
   git clone https://github.com/your-username/rill.git
   cd rill
   ```

2. **Create a virtual environment**:
   ```bash
   python3 -m venv .venv
   source .venv/bin/activate
   ```

3. **Install in editable mode with development dependencies**:
   ```bash
   pip install --upgrade pip
   pip install -e .[dev,connectors]
   ```

4. **Run the Test Suite**:
   We maintain strict test coverage across all features (Vectorized Upserts, Backpressure, Checkpointing, DuckDB SQL, and Multi-Stream Joins). Ensure all tests pass before committing:
   ```bash
   pytest tests/ -v
   ```

5. **Run the Live Demo**:
   Verify end-to-end functionality by running our demo script:
   ```bash
   python3 examples/multi_stream_join_demo.py
   ```

## Architecture & Code Guidelines

- **Zero-Copy C++ Memory**: Whenever adding or modifying table computations, always prefer PyArrow compute kernels (`pyarrow.compute`) or zero-copy DuckDB SQL queries over Python loops. Bypassing Python dictionaries and row-level iterations is foundational to Rill's high-throughput architecture.
- **System Metadata**: Always preserve `z_insert_ts` and `z_update_ts` system columns when writing compute transformations.
- **Mandatory TTL**: If implementing a new append-only mechanism (`mode="append"`), always ensure that a `RetentionPolicy` is enforced so memory stays bounded.

## Pull Request Process

1. Fork the repo and create your feature branch from `main` (`git checkout -b feature/amazing-feature`).
2. Add automated tests for any new behavior in `tests/`.
3. Verify that `pytest tests/ -v` succeeds (`27+ tests passing`).
4. Commit your changes (`git commit -m 'feat: add amazing feature'`) and push to your branch (`git push origin feature/amazing-feature`).
5. Open a Pull Request on GitHub describing what your change accomplishes and why.

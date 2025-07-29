# Polars Python vs Rust Benchmark Suite

A comprehensive benchmarking package comparing Polars performance between Python and Rust implementations using the public Coiled timeseries dataset.

## Overview

This package provides automated benchmarks for common data processing operations using Polars in both Python and Rust, allowing for direct performance comparisons on a standardized 662M row dataset. Built with **uv** ⚡ for fast Python package management and **Cargo** 🦀 for Rust builds.

## Features

- 🚀 Automated benchmarking of Python and Rust Polars implementations
- ⚡ **Ultra-fast setup** with uv (10-100x faster than pip)
- 🦀 **Optimized Rust builds** with Cargo release profiles
- 📊 Comprehensive performance metrics (execution time, memory usage)
- 🔍 Multiple operation types: filtering, aggregations, groupby, sorting
- 📈 Automated report generation with visualizations
- 🌐 Uses public Coiled dataset (662M rows, 16.7GB) for consistent benchmarking
- 🔒 **Reproducible builds** with lockfiles (uv.lock, Cargo.lock)
- 📦 Easy installation and setup

## Dataset

The benchmarks use the public Coiled timeseries dataset:
- **Location**: `s3://coiled-datasets/timeseries/20-years/parquet`
- **Size**: 662,256,000 rows (16.7GB compressed, 58.2GB uncompressed)
- **Schema**: 
  - `id` (int64): Unique identifier
  - `name` (object): String category
  - `x` (float64): Numeric value
  - `y` (float64): Numeric value
- **Time Range**: 2000-2021 with one row per second
- **Access**: Anonymous S3 access (no credentials required)

## Installation

### Prerequisites
- Python 3.8+
- Rust 1.70+
- Git
- Internet connection for S3 access

### Setup
```bash
git clone https://github.com/yourusername/polars-benchmark.git
cd polars-benchmark

# Install uv (fast Python package manager)
curl -LsSf https://astral.sh/uv/install.sh | sh

# Install Python dependencies with uv
uv sync

# Build Rust benchmarks
cd rust-benchmarks
cargo build --release
cd ..
```

### Alternative: Using pip
```bash
# If you prefer pip over uv
pip install -e .
```

## Why uv + Cargo?

This benchmark suite is optimized for performance at every level:

- **⚡ uv**: 10-100x faster dependency resolution and installation than pip
- **🔒 Lockfiles**: `uv.lock` and `Cargo.lock` ensure reproducible builds across environments  
- **🚀 Release Builds**: Cargo's `--release` flag enables maximum Rust optimization
- **📦 Zero Config**: Both tools work out of the box with minimal setup
- **🌐 Network Efficiency**: uv's parallel downloads and Cargo's incremental compilation save time

Perfect for benchmarking where setup speed and reproducibility matter!

## Performance Tips

### System Optimization
```bash
# For best benchmark results, consider:

# 1. Close other applications to reduce noise
# 2. Use release builds for Rust
cargo run --release

# 3. Increase system limits for large datasets
ulimit -n 65536

# 4. Use SSD storage for better I/O performance
# 5. Ensure stable internet connection for S3 access
```

### Benchmark Reliability
- Run benchmarks multiple times and average results
- Use `--limit-rows` for consistent testing during development
- Monitor system resources during benchmarks
- Consider CPU temperature and throttling for longer runs

## Usage

### Quick Start
```bash
# Run all benchmarks (automatically uses S3 dataset)
uv run python scripts/run_all_benchmarks.py

# Generate comparison report
uv run python scripts/generate_report.py
```

### Individual Benchmarks

#### Python Benchmarks
```bash
cd python-benchmarks
uv run python runner.py  # Uses S3 dataset by default
```

#### Rust Benchmarks
```bash
cd rust-benchmarks
cargo run --release  # Uses S3 dataset by default

# Alternative build profiles
cargo run --profile ci      # Faster compile for development
cargo run                   # Debug build (much slower runtime)
```

### Performance Testing with Different Data Sizes
```bash
# Test with smaller subset for development
uv run python scripts/run_all_benchmarks.py --limit-rows 100000

# Test with medium dataset
uv run python scripts/run_all_benchmarks.py --limit-rows 10000000

# Full dataset benchmark (662M rows)
uv run python scripts/run_all_benchmarks.py
```

### Using Custom Data
```bash
# Use your own parquet file instead
uv run python scripts/run_all_benchmarks.py --data-path /path/to/your/data.parquet
```

### Development Commands
```bash
# Install development dependencies
uv sync --dev

# Run tests
uv run pytest

# Format code
uv run black .
uv run isort .

# Type checking
uv run mypy python-benchmarks/
```

## Benchmark Operations

The suite tests the following operations on the Coiled timeseries dataset:
- **Read Performance**: Parquet file loading from S3
- **Filtering**: Numeric filters on `x` column (`x > 0.5`)
- **Aggregations**: Sum, mean, count on `x` and `y` columns
- **Group By**: Grouping by `name` column with aggregations
- **Sorting**: Sorting by `x` column (descending)
- **Complex Query**: Combined filter + group by + aggregation + sort
- **Memory Usage**: Peak memory consumption for each operation

## Results

Results are saved in the `results/` directory:
- `python_results.json`: Python benchmark results
- `rust_results.json`: Rust benchmark results  
- `comparison_report.html`: Visual comparison report

## Contributing

1. Fork the repository
2. Create a feature branch
3. Add your benchmarks or improvements
4. Run the test suite
5. Submit a pull request

## License

MIT License - see LICENSE file for details.

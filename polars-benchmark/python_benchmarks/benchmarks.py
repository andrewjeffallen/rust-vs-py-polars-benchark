"""Python Polars benchmark implementations using Coiled timeseries dataset."""

import json
import platform
import time
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional

import polars as pl
import psutil

DEFAULT_S3_DATASET = "s3://coiled-datasets/timeseries/20-years/parquet"


class BenchmarkResult:
    """Container for individual benchmark results."""
    
    def __init__(self, operation: str, duration_ms: int, memory_mb: int, rows_processed: Optional[int] = None):
        self.operation = operation
        self.duration_ms = duration_ms
        self.memory_mb = memory_mb
        self.rows_processed = rows_processed
    
    def to_dict(self) -> Dict:
        return {
            "operation": self.operation,
            "duration_ms": self.duration_ms,
            "memory_mb": self.memory_mb,
            "rows_processed": self.rows_processed,
        }


class PythonPolarsBenchmark:
    """Python Polars benchmark suite using Coiled timeseries dataset."""
    
    def __init__(self, data_path: str = DEFAULT_S3_DATASET, limit_rows: Optional[int] = None):
        self.data_path = data_path
        self.limit_rows = limit_rows
        self.results: List[BenchmarkResult] = []
        self.process = psutil.Process()
        
        # S3 storage options for anonymous access
        self.storage_options = {"anon": True, "use_ssl": True} if data_path.startswith("s3://") else None
    
    def _measure_memory(self) -> int:
        """Get current memory usage in MB."""
        return self.process.memory_info().rss // 1024 // 1024
    
    def benchmark_read(self) -> BenchmarkResult:
        """Benchmark parquet file reading from S3 or local file."""
        print("🔄 Running read benchmark...")
        
        initial_memory = self._measure_memory()
        start_time = time.perf_counter()
        
        if self.storage_options:
            # S3 read with storage options
            df = pl.read_parquet(
                self.data_path,
                storage_options=self.storage_options,
                n_rows=self.limit_rows
            )
        else:
            # Local file read
            df = pl.read_parquet(self.data_path, n_rows=self.limit_rows)
        
        end_time = time.perf_counter()
        final_memory = self._measure_memory()
        
        duration_ms = int((end_time - start_time) * 1000)
        memory_used = max(0, final_memory - initial_memory)
        
        result = BenchmarkResult(
            operation="read_parquet",
            duration_ms=duration_ms,
            memory_mb=memory_used,
            rows_processed=df.height
        )
        
        # Store df for later benchmarks
        self.df = df
        return result
    
    def benchmark_filter(self) -> BenchmarkResult:
        """Benchmark filtering operations on numeric column x."""
        print("🔄 Running filter benchmark...")
        
        initial_memory = self._measure_memory()
        start_time = time.perf_counter()
        
        # Filter for x > 0.5 (known numeric column from schema)
        filtered_df = self.df.filter(pl.col("x") > 0.5)
        
        end_time = time.perf_counter()
        final_memory = self._measure_memory()
        
        duration_ms = int((end_time - start_time) * 1000)
        memory_used = max(0, final_memory - initial_memory)
        
        return BenchmarkResult(
            operation="filter",
            duration_ms=duration_ms,
            memory_mb=memory_used,
            rows_processed=filtered_df.height
        )
    
    def benchmark_aggregation(self) -> BenchmarkResult:
        """Benchmark aggregation operations on numeric columns x and y."""
        print("🔄 Running aggregation benchmark...")
        
        initial_memory = self._measure_memory()
        start_time = time.perf_counter()
        
        # Aggregate x and y columns (known numeric columns)
        agg_df = self.df.select([
            pl.col("x").sum().alias("x_sum"),
            pl.col("y").sum().alias("y_sum"), 
            pl.col("x").mean().alias("x_mean"),
            pl.col("y").mean().alias("y_mean"),
            pl.col("id").count().alias("count"),
        ])
        
        end_time = time.perf_counter()
        final_memory = self._measure_memory()
        
        duration_ms = int((end_time - start_time) * 1000)
        memory_used = max(0, final_memory - initial_memory)
        
        return BenchmarkResult(
            operation="aggregation",
            duration_ms=duration_ms,
            memory_mb=memory_used,
            rows_processed=agg_df.height
        )
    
    def benchmark_group_by(self) -> BenchmarkResult:
        """Benchmark group by operations using name column."""
        print("🔄 Running group by benchmark...")
        
        initial_memory = self._measure_memory()
        start_time = time.perf_counter()
        
        # Group by name column and aggregate x, y
        grouped_df = self.df.group_by("name").agg([
            pl.col("x").sum().alias("x_sum"),
            pl.col("y").mean().alias("y_mean"),
            pl.col("id").count().alias("count")
        ])
        
        end_time = time.perf_counter()
        final_memory = self._measure_memory()
        
        duration_ms = int((end_time - start_time) * 1000)
        memory_used = max(0, final_memory - initial_memory)
        
        return BenchmarkResult(
            operation="group_by",
            duration_ms=duration_ms,
            memory_mb=memory_used,
            rows_processed=grouped_df.height
        )
    
    def benchmark_sort(self) -> BenchmarkResult:
        """Benchmark sorting operations on numeric column x."""
        print("🔄 Running sort benchmark...")
        
        initial_memory = self._measure_memory()
        start_time = time.perf_counter()
        
        # Sort by x column descending
        sorted_df = self.df.sort("x", descending=True)
        
        end_time = time.perf_counter()
        final_memory = self._measure_memory()
        
        duration_ms = int((end_time - start_time) * 1000)
        memory_used = max(0, final_memory - initial_memory)
        
        return BenchmarkResult(
            operation="sort",
            duration_ms=duration_ms,
            memory_mb=memory_used,
            rows_processed=sorted_df.height
        )
    
    def benchmark_complex_query(self) -> BenchmarkResult:
        """Benchmark complex query combining multiple operations."""
        print("🔄 Running complex query benchmark...")
        
        initial_memory = self._measure_memory()
        start_time = time.perf_counter()
        
        # Complex query: filter, group by, aggregate, and sort
        result_df = (
            self.df
            .filter((pl.col("x") > 0.0) & (pl.col("y") < 1.0))
            .group_by("name")
            .agg([
                pl.col("x").sum().alias("x_sum"),
                pl.col("y").mean().alias("y_mean"),
                (pl.col("x") * pl.col("y")).sum().alias("xy_sum")
            ])
            .sort("x_sum", descending=True)
        )
        
        end_time = time.perf_counter()
        final_memory = self._measure_memory()
        
        duration_ms = int((end_time - start_time) * 1000)
        memory_used = max(0, final_memory - initial_memory)
        
        return BenchmarkResult(
            operation="complex_query",
            duration_ms=duration_ms,
            memory_mb=memory_used,
            rows_processed=result_df.height
        )
    
    def run_all_benchmarks(self) -> List[BenchmarkResult]:
        """Run all benchmarks and return results."""
        print("🐍 Starting Python Polars benchmarks...")
        print(f"📁 Data source: {self.data_path}")
        if self.limit_rows:
            print(f"📊 Row limit: {self.limit_rows}")
        
        benchmarks = [
            self.benchmark_read,
            self.benchmark_filter,
            self.benchmark_aggregation,
            self.benchmark_group_by,
            self.benchmark_sort,
            self.benchmark_complex_query,
        ]
        
        results = []
        for benchmark in benchmarks:
            try:
                result = benchmark()
                results.append(result)
                print(f"✅ {result.operation}: {result.duration_ms}ms, {result.memory_mb}MB")
            except Exception as e:
                print(f"❌ Error in {benchmark.__name__}: {e}")
                # Continue with other benchmarks even if one fails
        
        self.results = results
        return results
    
    def save_results(self, output_path: Path):
        """Save benchmark results to JSON file."""
        system_info = {
            "os": platform.system(),
            "cpu_count": psutil.cpu_count(),
            "total_memory_gb": psutil.virtual_memory().total // 1024 // 1024 // 1024,
        }
        
        dataset_info = {
            "source": self.data_path,
            "rows_limit": self.limit_rows,
        }
        
        benchmark_suite = {
            "timestamp": datetime.utcnow().isoformat() + "Z",
            "results": [result.to_dict() for result in self.results],
            "system_info": system_info,
            "dataset_info": dataset_info,
        }
        
        output_path.parent.mkdir(parents=True, exist_ok=True)
        with open(output_path, 'w') as f:
            json.dump(benchmark_suite, f, indent=2)
        
        print(f"📁 Results saved to: {output_path}")
        
        # Print summary
        print("\n📊 Results Summary:")
        for result in self.results:
            print(f"  • {result.operation}: {result.duration_ms}ms")

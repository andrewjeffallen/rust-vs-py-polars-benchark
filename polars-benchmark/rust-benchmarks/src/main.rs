use clap::Parser;
use polars::prelude::*;
use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use std::time::Instant;
use sysinfo::{System, SystemExt};

const DEFAULT_S3_DATASET: &str = "s3://coiled-datasets/timeseries/20-years/parquet";

#[derive(Parser)]
#[command(name = "polars-rust-benchmark")]
#[command(about = "Rust benchmarks for Polars using Coiled timeseries dataset")]
struct Args {
    #[arg(default_value = DEFAULT_S3_DATASET)]
    data_path: String,
    
    #[arg(short, long, default_value = "../results/rust_results.json")]
    output: PathBuf,
    
    #[arg(long)]
    limit_rows: Option<usize>,
}

#[derive(Serialize, Deserialize, Debug)]
struct BenchmarkResult {
    operation: String,
    duration_ms: u64,
    memory_mb: u64,
    rows_processed: Option<usize>,
}

#[derive(Serialize, Deserialize, Debug)]
struct BenchmarkSuite {
    timestamp: String,
    results: Vec<BenchmarkResult>,
    system_info: SystemInfo,
    dataset_info: DatasetInfo,
}

#[derive(Serialize, Deserialize, Debug)]
struct SystemInfo {
    os: String,
    cpu_count: usize,
    total_memory_gb: u64,
}

#[derive(Serialize, Deserialize, Debug)]
struct DatasetInfo {
    source: String,
    rows_limit: Option<usize>,
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();
    
    println!("🦀 Starting Rust Polars benchmarks...");
    println!("📁 Data source: {}", args.data_path);
    if let Some(limit) = args.limit_rows {
        println!("📊 Row limit: {}", limit);
    }
    
    let mut system = System::new_all();
    system.refresh_all();
    
    let system_info = SystemInfo {
        os: system.name().unwrap_or_else(|| "Unknown".to_string()),
        cpu_count: system.cpus().len(),
        total_memory_gb: system.total_memory() / 1_024 / 1_024 / 1_024,
    };
    
    let dataset_info = DatasetInfo {
        source: args.data_path.clone(),
        rows_limit: args.limit_rows,
    };
    
    let mut results = Vec::new();
    
    let scan_args = ScanArgsParquet {
        n_rows: args.limit_rows,
        cache: true,
        parallel: ParallelStrategy::Auto,
        rechunk: false,
        row_count: None,
        low_memory: false,
        cloud_options: None,
        use_statistics: true,
    };
    
    println!("🔄 Running read benchmark...");
    let result = benchmark_read(&args.data_path, &scan_args)?;
    results.push(result);
    
    let df = LazyFrame::scan_parquet(&args.data_path, scan_args.clone())?;
    
    println!("🔄 Running filter benchmark...");
    let result = benchmark_filter(&df)?;
    results.push(result);
    
    println!("🔄 Running aggregation benchmark...");
    let result = benchmark_aggregation(&df)?;
    results.push(result);
    
    println!("🔄 Running group by benchmark...");
    let result = benchmark_group_by(&df)?;
    results.push(result);
    
    println!("🔄 Running sort benchmark...");
    let result = benchmark_sort(&df)?;
    results.push(result);
    
    println!("🔄 Running complex query benchmark...");
    let result = benchmark_complex_query(&df)?;
    results.push(result);
    
    let benchmark_suite = BenchmarkSuite {
        timestamp: chrono::Utc::now().to_rfc3339(),
        results,
        system_info,
        dataset_info,
    };
    
    std::fs::create_dir_all(args.output.parent().unwrap())?;
    let json = serde_json::to_string_pretty(&benchmark_suite)?;
    std::fs::write(&args.output, json)?;
    
    println!("✅ Benchmarks completed! Results saved to: {}", args.output.display());
    println!("\n📊 Results Summary:");
    for result in &benchmark_suite.results {
        println!("  • {}: {}ms ({}MB)", result.operation, result.duration_ms, result.memory_mb);
    }
    
    Ok(())
}

fn benchmark_read(path: &str, scan_args: &ScanArgsParquet) -> PolarsResult<BenchmarkResult> {
    let mut system = System::new_all();
    system.refresh_memory();
    let initial_memory = system.used_memory();
    
    let start = Instant::now();
    let df = LazyFrame::scan_parquet(path, scan_args.clone())?.collect()?;
    let duration = start.elapsed();
    
    system.refresh_memory();
    let final_memory = system.used_memory();
    let memory_used = ((final_memory.saturating_sub(initial_memory)) / 1_024 / 1_024) as u64;
    
    Ok(BenchmarkResult {
        operation: "read_parquet".to_string(),
        duration_ms: duration.as_millis() as u64,
        memory_mb: memory_used,
        rows_processed: Some(df.height()),
    })
}

fn benchmark_filter(df: &LazyFrame) -> PolarsResult<BenchmarkResult> {
    let mut system = System::new_all();
    system.refresh_memory();
    let initial_memory = system.used_memory();
    
    let start = Instant::now();
    let result = df
        .clone()
        .filter(col("x").gt(lit(0.5)))
        .collect()?;
    let duration = start.elapsed();
    
    system.refresh_memory();
    let final_memory = system.used_memory();
    let memory_used = ((final_memory.saturating_sub(initial_memory)) / 1_024 / 1_024) as u64;
    
    Ok(BenchmarkResult {
        operation: "filter".to_string(),
        duration_ms: duration.as_millis() as u64,
        memory_mb: memory_used,
        rows_processed: Some(result.height()),
    })
}

fn benchmark_aggregation(df: &LazyFrame) -> PolarsResult<BenchmarkResult> {
    let mut system = System::new_all();
    system.refresh_memory();
    let initial_memory = system.used_memory();
    
    let start = Instant::now();
    let result = df
        .clone()
        .select([
            col("x").sum().alias("x_sum"),
            col("y").sum().alias("y_sum"),
            col("x").mean().alias("x_mean"),
            col("y").mean().alias("y_mean"),
            col("id").count().alias("count"),
        ])
        .collect()?;
    let duration = start.elapsed();
    
    system.refresh_memory();
    let final_memory = system.used_memory();
    let memory_used = ((final_memory.saturating_sub(initial_memory)) / 1_024 / 1_024) as u64;
    
    Ok(BenchmarkResult {
        operation: "aggregation".to_string(),
        duration_ms: duration.as_millis() as u64,
        memory_mb: memory_used,
        rows_processed: Some(result.height()),
    })
}

fn benchmark_group_by(df: &LazyFrame) -> PolarsResult<BenchmarkResult> {
    let mut system = System::new_all();
    system.refresh_memory();
    let initial_memory = system.used_memory();
    
    let start = Instant::now();
    let result = df
        .clone()
        .groupby([col("name")])
        .agg([
            col("x").sum().alias("x_sum"),
            col("y").mean().alias("y_mean"),
            col("id").count().alias("count")
        ])
        .collect()?;
    let duration = start.elapsed();
    
    system.refresh_memory();
    let final_memory = system.used_memory();
    let memory_used = ((final_memory.saturating_sub(initial_memory)) / 1_024 / 1_024) as u64;
    
    Ok(BenchmarkResult {
        operation: "group_by".to_string(),
        duration_ms: duration.as_millis() as u64,
        memory_mb: memory_used,
        rows_processed: Some(result.height()),
    })
}

fn benchmark_sort(df: &LazyFrame) -> PolarsResult<BenchmarkResult> {
    let mut system = System::new_all();
    system.refresh_memory();
    let initial_memory = system.used_memory();
    
    let start = Instant::now();
    // Sort by x column descending - use correct API for Polars 0.32
    let result = df
        .clone()
        .sort("x", SortOptions {
            descending: true,
            nulls_last: false,
            multithreaded: true,
            maintain_order: false,
        })
        .collect()?;
    let duration = start.elapsed();
    
    system.refresh_memory();
    let final_memory = system.used_memory();
    let memory_used = ((final_memory.saturating_sub(initial_memory)) / 1_024 / 1_024) as u64;
    
    Ok(BenchmarkResult {
        operation: "sort".to_string(),
        duration_ms: duration.as_millis() as u64,
        memory_mb: memory_used,
        rows_processed: Some(result.height()),
    })
}

fn benchmark_complex_query(df: &LazyFrame) -> PolarsResult<BenchmarkResult> {
    let mut system = System::new_all();
    system.refresh_memory();
    let initial_memory = system.used_memory();
    
    let start = Instant::now();
    // Complex query: filter, group by, aggregate, and sort
    let result = df
        .clone()
        .filter(col("x").gt(lit(0.0)).and(col("y").lt(lit(1.0))))
        .groupby([col("name")])
        .agg([
            col("x").sum().alias("x_sum"),
            col("y").mean().alias("y_mean"),
            (col("x") * col("y")).sum().alias("xy_sum")
        ])
        .sort("x_sum", SortOptions {
            descending: true,
            nulls_last: false,
            multithreaded: true,
            maintain_order: false,
        })
        .collect()?;
    let duration = start.elapsed();
    
    system.refresh_memory();
    let final_memory = system.used_memory();
    let memory_used = ((final_memory.saturating_sub(initial_memory)) / 1_024 / 1_024) as u64;
    
    Ok(BenchmarkResult {
        operation: "complex_query".to_string(),
        duration_ms: duration.as_millis() as u64,
        memory_mb: memory_used,
        rows_processed: Some(result.height()),
    })
}
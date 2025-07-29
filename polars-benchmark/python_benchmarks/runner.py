"""Runner script for Python Polars benchmarks using Coiled timeseries dataset."""

from pathlib import Path

import click
from rich.console import Console

from .benchmarks import PythonPolarsBenchmark, DEFAULT_S3_DATASET

console = Console()


@click.command()
@click.option(
    '--data-path',
    type=str,
    default=DEFAULT_S3_DATASET,
    help='Path to the parquet file (defaults to S3 Coiled dataset)'
)
@click.option(
    '--output',
    type=click.Path(path_type=Path),
    default=Path('../results/python_results.json'),
    help='Output file for results'
)
@click.option(
    '--limit-rows',
    type=int,
    default=None,
    help='Limit number of rows to process (useful for testing)'
)
def main(data_path: str, output: Path, limit_rows: int):
    """Run Python Polars benchmarks on Coiled timeseries dataset."""
    console.print("🐍 [bold blue]Python Polars Benchmark Suite[/bold blue]")
    console.print(f"📁 Data source: {data_path}")
    console.print(f"📄 Output: {output}")
    if limit_rows:
        console.print(f"📊 Row limit: {limit_rows:,}")
    
    try:
        benchmark = PythonPolarsBenchmark(data_path, limit_rows)
        results = benchmark.run_all_benchmarks()
        benchmark.save_results(output)
        
        console.print("\n✅ [bold green]All benchmarks completed successfully![/bold green]")
        
        # Summary table
        console.print("\n📊 [bold]Performance Summary:[/bold]")
        total_time = 0
        for result in results:
            console.print(f"  • {result.operation.replace('_', ' ').title()}: {result.duration_ms:,}ms ({result.memory_mb}MB)")
            total_time += result.duration_ms
        
        console.print(f"\n⏱️  [bold]Total time: {total_time:,}ms[/bold]")
        
        if data_path == DEFAULT_S3_DATASET:
            console.print(f"\n🌐 [dim]Dataset: Coiled timeseries (662M rows from S3)[/dim]")
            
    except Exception as e:
        console.print(f"\n❌ [bold red]Error:[/bold red] {e}")
        raise


if __name__ == '__main__':
    main()

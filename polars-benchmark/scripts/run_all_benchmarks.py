#!/usr/bin/env python3
"""Run all benchmarks (Python and Rust) and generate comparison."""

import subprocess
import sys
from pathlib import Path

import click
from rich.console import Console

console = Console()

DEFAULT_S3_DATASET = "s3://coiled-datasets/timeseries/20-years/parquet"


@click.command()
@click.option(
    '--data-path',
    type=str,
    default=DEFAULT_S3_DATASET,
    help='Path to parquet data (defaults to Coiled S3 dataset)'
)
@click.option(
    '--limit-rows',
    type=int,
    default=None,
    help='Limit number of rows to process (useful for testing)'
)
@click.option(
    '--skip-rust',
    is_flag=True,
    help='Skip Rust benchmarks'
)
@click.option(
    '--skip-python',
    is_flag=True,
    help='Skip Python benchmarks'
)
def main(data_path: str, limit_rows: int, skip_rust: bool, skip_python: bool):
    """Run all Polars benchmarks and generate comparison report."""
    console.print("🚀 [bold blue]Running All Polars Benchmarks[/bold blue]")
    console.print(f"📁 Data source: {data_path}")
    if limit_rows:
        console.print(f"📊 Row limit: {limit_rows:,}")
    
    project_root = Path(__file__).parent.parent
    
    if not skip_python:
        # Run Python benchmarks with uv
        console.print("\n🐍 [bold]Running Python benchmarks with uv...[/bold]")
        try:
            cmd = [
                "uv", "run", "python", "-m", "python_benchmarks.runner",
                "--data-path", data_path
            ]
            if limit_rows:
                cmd.extend(["--limit-rows", str(limit_rows)])
                
            result = subprocess.run(cmd, cwd=project_root, check=True, capture_output=True, text=True)
            console.print("✅ Python benchmarks completed")
            if result.stdout:
                console.print(f"[dim]{result.stdout}[/dim]")
        except subprocess.CalledProcessError as e:
            console.print(f"❌ Python benchmarks failed: {e}")
            console.print(f"stdout: {e.stdout}")
            console.print(f"stderr: {e.stderr}")
            sys.exit(1)
        except FileNotFoundError:
            console.print("❌ uv not found. Please install uv or use --skip-python")
            console.print("Install uv: curl -LsSf https://astral.sh/uv/install.sh | sh")
            sys.exit(1)
    
    if not skip_rust:
        # Run Rust benchmarks with cargo
        console.print("\n🦀 [bold]Running Rust benchmarks with cargo...[/bold]")
        rust_dir = project_root / "rust-benchmarks"
        try:
            cmd = ["cargo", "run", "--release", "--", data_path]
            if limit_rows:
                cmd.extend(["--limit-rows", str(limit_rows)])
                
            result = subprocess.run(cmd, cwd=rust_dir, check=True, capture_output=True, text=True)
            console.print("✅ Rust benchmarks completed")
            if result.stdout:
                console.print(f"[dim]{result.stdout}[/dim]")
        except subprocess.CalledProcessError as e:
            console.print(f"❌ Rust benchmarks failed: {e}")
            console.print(f"stdout: {e.stdout}")
            console.print(f"stderr: {e.stderr}")
            sys.exit(1)
        except FileNotFoundError:
            console.print("❌ cargo not found. Please install Rust or use --skip-rust")
            console.print("Install Rust: https://rustup.rs/")
            sys.exit(1)
    
    # Generate comparison report
    if not skip_python and not skip_rust:
        console.print("\n📊 [bold]Generating comparison report...[/bold]")
        try:
            subprocess.run([
                "uv", "run", "python", str(project_root / "scripts" / "generate_report.py")
            ], check=True)
            console.print("✅ Report generated successfully!")
            console.print(f"📄 View report: {project_root / 'results' / 'comparison_report.html'}")
        except subprocess.CalledProcessError as e:
            console.print(f"❌ Report generation failed: {e}")
            sys.exit(1)
    
    # Show quick comparison
    if not skip_python and not skip_rust:
        console.print("\n📈 [bold]Quick comparison:[/bold]")
        try:
            subprocess.run([
                "uv", "run", "python", str(project_root / "scripts" / "compare_results.py")
            ], check=True)
        except subprocess.CalledProcessError as e:
            console.print(f"❌ Comparison failed: {e}")


if __name__ == "__main__":
    main()

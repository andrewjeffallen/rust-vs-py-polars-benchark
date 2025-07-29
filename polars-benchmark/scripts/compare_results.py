#!/usr/bin/env python3
"""Compare Python and Rust benchmark results."""

import json
from pathlib import Path
from typing import Dict, List

import pandas as pd
from rich.console import Console
from rich.table import Table

console = Console()


def load_results(results_path: Path) -> Dict:
    """Load benchmark results from JSON file."""
    with open(results_path) as f:
        return json.load(f)


def compare_results():
    """Compare Python and Rust benchmark results."""
    project_root = Path(__file__).parent.parent
    python_results_path = project_root / "results" / "python_results.json"
    rust_results_path = project_root / "results" / "rust_results.json"
    
    if not python_results_path.exists():
        console.print(f"❌ Python results not found: {python_results_path}")
        return
    
    if not rust_results_path.exists():
        console.print(f"❌ Rust results not found: {rust_results_path}")
        return
    
    python_data = load_results(python_results_path)
    rust_data = load_results(rust_results_path)
    
    console.print("📊 [bold blue]Benchmark Comparison: Python vs Rust[/bold blue]\n")
    
    # Create comparison table
    table = Table(title="Performance Comparison")
    table.add_column("Operation", style="cyan", no_wrap=True)
    table.add_column("Python (ms)", style="green")
    table.add_column("Rust (ms)", style="red")
    table.add_column("Speedup", style="yellow")
    table.add_column("Python RAM (MB)", style="green")
    table.add_column("Rust RAM (MB)", style="red")
    
    # Create lookup for rust results
    rust_lookup = {result["operation"]: result for result in rust_data["results"]}
    
    for py_result in python_data["results"]:
        operation = py_result["operation"]
        rust_result = rust_lookup.get(operation)
        
        if rust_result:
            py_time = py_result["duration_ms"]
            rust_time = rust_result["duration_ms"]
            speedup = f"{py_time / rust_time:.2f}x" if rust_time > 0 else "N/A"
            
            table.add_row(
                operation,
                str(py_time),
                str(rust_time),
                speedup,
                str(py_result["memory_mb"]),
                str(rust_result["memory_mb"])
            )
    
    console.print(table)
    
    # Summary statistics
    console.print("\n📈 [bold]Summary Statistics:[/bold]")
    
    py_total = sum(r["duration_ms"] for r in python_data["results"])
    rust_total = sum(r["duration_ms"] for r in rust_data["results"])
    overall_speedup = py_total / rust_total if rust_total > 0 else 0
    
    console.print(f"  • Total Python time: {py_total}ms")
    console.print(f"  • Total Rust time: {rust_total}ms")
    console.print(f"  • Overall speedup: {overall_speedup:.2f}x")
    
    py_memory = sum(r["memory_mb"] for r in python_data["results"])
    rust_memory = sum(r["memory_mb"] for r in rust_data["results"])
    
    console.print(f"  • Total Python memory: {py_memory}MB")
    console.print(f"  • Total Rust memory: {rust_memory}MB")


if __name__ == "__main__":
    compare_results()

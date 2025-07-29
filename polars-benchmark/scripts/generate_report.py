#!/usr/bin/env python3
"""Generate HTML comparison report with visualizations."""

import json
from pathlib import Path
from typing import Dict

import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns
from jinja2 import Template

# Set style for better-looking plots
plt.style.use('seaborn-v0_8')
sns.set_palette("husl")


def load_results(results_path: Path) -> Dict:
    """Load benchmark results from JSON file."""
    with open(results_path) as f:
        return json.load(f)


def create_comparison_plots(python_data: Dict, rust_data: Dict, output_dir: Path):
    """Create comparison plots and save them."""
    # Prepare data
    operations = []
    python_times = []
    rust_times = []
    python_memory = []
    rust_memory = []
    
    rust_lookup = {result["operation"]: result for result in rust_data["results"]}
    
    for py_result in python_data["results"]:
        operation = py_result["operation"]
        rust_result = rust_lookup.get(operation)
        
        if rust_result:
            operations.append(operation.replace('_', ' ').title())
            python_times.append(py_result["duration_ms"])
            rust_times.append(rust_result["duration_ms"])
            python_memory.append(py_result["memory_mb"])
            rust_memory.append(rust_result["memory_mb"])
    
    # Time comparison plot
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(15, 6))
    
    x = range(len(operations))
    width = 0.35
    
    ax1.bar([i - width/2 for i in x], python_times, width, label='Python', alpha=0.8)
    ax1.bar([i + width/2 for i in x], rust_times, width, label='Rust', alpha=0.8)
    ax1.set_xlabel('Operations')
    ax1.set_ylabel('Time (ms)')
    ax1.set_title('Execution Time Comparison')
    ax1.set_xticks(x)
    ax1.set_xticklabels(operations, rotation=45, ha='right')
    ax1.legend()
    ax1.grid(True, alpha=0.3)
    
    # Memory comparison plot
    ax2.bar([i - width/2 for i in x], python_memory, width, label='Python', alpha=0.8)
    ax2.bar([i + width/2 for i in x], rust_memory, width, label='Rust', alpha=0.8)
    ax2.set_xlabel('Operations')
    ax2.set_ylabel('Memory (MB)')
    ax2.set_title('Memory Usage Comparison')
    ax2.set_xticks(x)
    ax2.set_xticklabels(operations, rotation=45, ha='right')
    ax2.legend()
    ax2.grid(True, alpha=0.3)
    
    plt.tight_layout()
    plt.savefig(output_dir / 'comparison_chart.png', dpi=300, bbox_inches='tight')
    plt.close()
    
    # Speedup chart
    speedups = [py_time / rust_time if rust_time > 0 else 0 
                for py_time, rust_time in zip(python_times, rust_times)]
    
    fig, ax = plt.subplots(figsize=(10, 6))
    bars = ax.bar(operations, speedups, alpha=0.8, color='coral')
    ax.axhline(y=1, color='red', linestyle='--', alpha=0.7, label='1x (No speedup)')
    ax.set_xlabel('Operations')
    ax.set_ylabel('Speedup Factor (Python time / Rust time)')
    ax.set_title('Rust Speedup Over Python')
    ax.set_xticklabels(operations, rotation=45, ha='right')
    ax.legend()
    ax.grid(True, alpha=0.3)
    
    # Add value labels on bars
    for bar, speedup in zip(bars, speedups):
        ax.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 0.05,
                f'{speedup:.2f}x', ha='center', va='bottom', fontweight='bold')
    
    plt.tight_layout()
    plt.savefig(output_dir / 'speedup_chart.png', dpi=300, bbox_inches='tight')
    plt.close()


def generate_html_report(python_data: Dict, rust_data: Dict, output_path: Path):
    """Generate HTML report with results and visualizations."""
    
    # Calculate comparison data
    comparisons = []
    rust_lookup = {result["operation"]: result for result in rust_data["results"]}
    
    for py_result in python_data["results"]:
        operation = py_result["operation"]
        rust_result = rust_lookup.get(operation)
        
        if rust_result:
            speedup = py_result["duration_ms"] / rust_result["duration_ms"] if rust_result["duration_ms"] > 0 else 0
            comparisons.append({
                "operation": operation.replace('_', ' ').title(),
                "python_time": py_result["duration_ms"],
                "rust_time": rust_result["duration_ms"],
                "speedup": speedup,
                "python_memory": py_result["memory_mb"],
                "rust_memory": rust_result["memory_mb"],
                "python_rows": py_result.get("rows_processed"),
                "rust_rows": rust_result.get("rows_processed"),
            })
    
    # Calculate summary stats
    total_python_time = sum(c["python_time"] for c in comparisons)
    total_rust_time = sum(c["rust_time"] for c in comparisons)
    overall_speedup = total_python_time / total_rust_time if total_rust_time > 0 else 0
    
    html_template = """
<!DOCTYPE html>
<html>
<head>
    <title>Polars Python vs Rust Benchmark Report</title>
    <style>
        body { font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif; margin: 40px; background-color: #f5f5f5; }
        .container { max-width: 1200px; margin: 0 auto; background: white; padding: 30px; border-radius: 10px; box-shadow: 0 0 20px rgba(0,0,0,0.1); }
        h1 { color: #2c3e50; border-bottom: 3px solid #3498db; padding-bottom: 10px; }
        h2 { color: #34495e; margin-top: 30px; }
        .summary { background: #ecf0f1; padding: 20px; border-radius: 5px; margin: 20px 0; }
        .summary-stat { display: inline-block; margin: 10px 20px; }
        .summary-stat .value { font-size: 1.5em; font-weight: bold; color: #2980b9; }
        .summary-stat .label { color: #7f8c8d; }
        table { width: 100%; border-collapse: collapse; margin: 20px 0; }
        th, td { padding: 12px; text-align: left; border-bottom: 1px solid #ddd; }
        th { background-color: #3498db; color: white; }
        tr:nth-child(even) { background-color: #f2f2f2; }
        .speedup { font-weight: bold; }
        .speedup.good { color: #27ae60; }
        .speedup.bad { color: #e74c3c; }
        .chart-container { text-align: center; margin: 30px 0; }
        .chart-container img { max-width: 100%; height: auto; border: 1px solid #ddd; border-radius: 5px; }
        .metadata { background: #f8f9fa; padding: 15px; border-radius: 5px; margin: 20px 0; font-size: 0.9em; }
        .timestamp { color: #6c757d; font-style: italic; }
    </style>
</head>
<body>
    <div class="container">
        <h1>🚀 Polars Python vs Rust Benchmark Report</h1>
        
        <div class="metadata">
            <p><strong>Python Report:</strong> <span class="timestamp">{{ python_data.timestamp }}</span></p>
            <p><strong>Rust Report:</strong> <span class="timestamp">{{ rust_data.timestamp }}</span></p>
            <p><strong>System:</strong> {{ python_data.system_info.os }} ({{ python_data.system_info.cpu_count }} CPUs, {{ python_data.system_info.total_memory_gb }}GB RAM)</p>
        </div>
        
        <div class="summary">
            <h2>📊 Summary Statistics</h2>
            <div class="summary-stat">
                <div class="value">{{ "%.2f"|format(overall_speedup) }}x</div>
                <div class="label">Overall Speedup</div>
            </div>
            <div class="summary-stat">
                <div class="value">{{ total_python_time }}ms</div>
                <div class="label">Total Python Time</div>
            </div>
            <div class="summary-stat">
                <div class="value">{{ total_rust_time }}ms</div>
                <div class="label">Total Rust Time</div>
            </div>
        </div>
        
        <h2>📈 Performance Visualizations</h2>
        <div class="chart-container">
            <img src="comparison_chart.png" alt="Performance Comparison Chart">
        </div>
        <div class="chart-container">
            <img src="speedup_chart.png" alt="Speedup Chart">
        </div>
        
        <h2>📋 Detailed Results</h2>
        <table>
            <thead>
                <tr>
                    <th>Operation</th>
                    <th>Python Time (ms)</th>
                    <th>Rust Time (ms)</th>
                    <th>Speedup</th>
                    <th>Python Memory (MB)</th>
                    <th>Rust Memory (MB)</th>
                    <th>Rows Processed</th>
                </tr>
            </thead>
            <tbody>
                {% for comp in comparisons %}
                <tr>
                    <td>{{ comp.operation }}</td>
                    <td>{{ comp.python_time }}</td>
                    <td>{{ comp.rust_time }}</td>
                    <td class="speedup {% if comp.speedup > 1 %}good{% else %}bad{% endif %}">
                        {{ "%.2f"|format(comp.speedup) }}x
                    </td>
                    <td>{{ comp.python_memory }}</td>
                    <td>{{ comp.rust_memory }}</td>
                    <td>{{ comp.python_rows or 'N/A' }}</td>
                </tr>
                {% endfor %}
            </tbody>
        </table>
        
        <div class="metadata">
            <p><em>Generated on {{ generation_time }}</em></p>
        </div>
    </div>
</body>
</html>
    """
    
    template = Template(html_template)
    html_content = template.render(
        python_data=python_data,
        rust_data=rust_data,
        comparisons=comparisons,
        total_python_time=total_python_time,
        total_rust_time=total_rust_time,
        overall_speedup=overall_speedup,
        generation_time=pd.Timestamp.now().strftime("%Y-%m-%d %H:%M:%S UTC")
    )
    
    with open(output_path, 'w') as f:
        f.write(html_content)


def main():
    """Generate the complete benchmark report."""
    project_root = Path(__file__).parent.parent
    results_dir = project_root / "results"
    
    python_results_path = results_dir / "python_results.json"
    rust_results_path = results_dir / "rust_results.json"
    
    if not python_results_path.exists():
        print(f"❌ Python results not found: {python_results_path}")
        return
    
    if not rust_results_path.exists():
        print(f"❌ Rust results not found: {rust_results_path}")
        return
    
    print("📊 Loading benchmark results...")
    python_data = load_results(python_results_path)
    rust_data = load_results(rust_results_path)
    
    print("📈 Creating visualization charts...")
    create_comparison_plots(python_data, rust_data, results_dir)
    
    print("📄 Generating HTML report...")
    report_path = results_dir / "comparison_report.html"
    generate_html_report(python_data, rust_data, report_path)
    
    print(f"✅ Report generated successfully: {report_path}")
    print(f"🌐 Open in browser: file://{report_path.absolute()}")


if __name__ == "__main__":
    main()

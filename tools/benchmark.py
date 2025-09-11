"""
Minimal Python API for CompressionBenchmark
"""

import subprocess
from pathlib import Path


def run_benchmark(duckdb_path: str, output_csv: str, executable_path: str = None):
    """
    Run compression benchmark.
    
    Args:
        duckdb_path: Path to DuckDB database file
        output_csv: Path to output CSV file
        executable_path: Optional path to CompressionBenchmarkCLI executable
    """
    if executable_path is None:
        # Find executable relative to this file
        current_dir = Path(__file__).parent.parent
        executable_path = current_dir / "CompressionBenchmarkCLI"
        
        if not executable_path.exists():
            raise FileNotFoundError("CompressionBenchmarkCLI executable not found")
    
    subprocess.run([str(executable_path), duckdb_path, output_csv], check=True)
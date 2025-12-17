"""Simple benchmark comparing runtime and memory usage.

This script benchmarks the convert_dose_units_for_continuous_meds function,
with DuckDB cache cleaning between iterations to ensure fair comparison.

Usage:
    python benchmark_simple.py              # Full cohort (2000 IDs)
    python benchmark_simple.py -n 100       # Random sample of 100 IDs
    python benchmark_simple.py -memory      # Run with memray memory profiling
    python benchmark_simple.py -n 100 -memory  # Sample + memory profiling
"""
import time
import tracemalloc
import shutil
import tempfile
import random
import sys
import pandas as pd
from pathlib import Path
from clifpy.clif_orchestrator import ClifOrchestrator


def clean_duckdb_cache():
    """Remove DuckDB temporary files to ensure cold cache.

    This prevents subsequent runs from benefiting from disk-spilled
    intermediate results from previous runs.
    """
    temp_dir = Path(tempfile.gettempdir())
    for path in temp_dir.glob("duckdb_temp*"):
        try:
            if path.is_dir():
                shutil.rmtree(path)
            else:
                path.unlink()
        except Exception:
            pass  # Ignore errors if files are in use


def load_hospitalization_ids(csv_path: str, n: int = None, random_seed: int = 42) -> list:
    """Load hospitalization IDs from CSV file.

    Args:
        csv_path: Path to CSV file with hospitalization_id column
        n: Optional number of IDs to randomly sample. If None, returns all IDs.
        random_seed: Random seed for reproducible sampling

    Returns:
        List of hospitalization ID strings
    """
    df = pd.read_csv(csv_path)
    hosp_ids = df['hospitalization_id'].astype(str).tolist()

    if n is not None:
        if n > len(hosp_ids):
            print(f"⚠️  Requested n={n} but only {len(hosp_ids)} IDs available. Using all IDs.")
            return hosp_ids
        # Random sampling with fixed seed for reproducibility
        random.seed(random_seed)
        hosp_ids = random.sample(hosp_ids, n)

    return hosp_ids


def benchmark_convert_dose_units(hospitalization_ids: list = None, n: int = None, num_iterations: int = 5, random_seed: int = 42):
    """Benchmark the convert_dose_units_for_continuous_meds function.

    Args:
        hospitalization_ids: List of hospitalization IDs to process.
                            If None, loads from .dev/cohort_hosp_ids.csv
        n: Number of IDs to randomly sample from CSV. If None, uses all IDs.
           Ignored if hospitalization_ids is provided.
        num_iterations: Number of benchmark iterations to run
        random_seed: Random seed for reproducible sampling

    Runs iterations with cache cleaning between each iteration.
    Reports both first run (cold cache) and average across all runs.
    """
    # Load hospitalization IDs if not provided
    if hospitalization_ids is None:
        csv_path = Path(__file__).parent.parent.parent / '.dev' / 'cohort_hosp_ids.csv'
        hospitalization_ids = load_hospitalization_ids(str(csv_path), n=n, random_seed=random_seed)
        if n is not None:
            print(f"📊 Randomly sampled {len(hospitalization_ids)} hospitalizations from cohort (seed={random_seed})")
        else:
            print(f"📊 Using full cohort from CSV")

    cohort_size = len(hospitalization_ids)
    print(f"Benchmarking with {cohort_size} hospitalization(s)")

    # Estimate runtime
    if cohort_size > 100:
        est_time = cohort_size * 0.2  # Rough estimate: ~0.2s per hospitalization
        print(f"⚠️  Large cohort detected. Estimated time per iteration: ~{est_time:.1f}s (~{est_time/60:.1f} minutes)")

    # Setup
    config_path = Path(__file__).parent.parent.parent / "config" / "config.yaml"
    co = ClifOrchestrator(config_path=str(config_path))
    preferred_units = {
        "propofol": "mcg/min",
        "fentanyl": "mcg/hr",
        "insulin": "u/hr",
        "midazolam": "mg/hr",
        "heparin": "u/min"
    }

    # Run iterations with timing and memory tracking
    times = []
    memory_peaks = []

    print(f"Running {num_iterations} iterations with DuckDB cache cleaning...")
    print("(Each iteration starts with a clean cache)\n")

    for i in range(num_iterations):
        # Clean DuckDB cache before each run
        clean_duckdb_cache()

        # Start memory tracking
        tracemalloc.start()

        # Time the function
        start = time.perf_counter()
        co.convert_dose_units_for_continuous_meds(
            preferred_units=preferred_units,
            override=True,
            hospitalization_ids=hospitalization_ids
        )
        elapsed = time.perf_counter() - start

        # Get peak memory
        _, peak = tracemalloc.get_traced_memory()
        tracemalloc.stop()

        times.append(elapsed)
        memory_peaks.append(peak / 1024 / 1024)  # Convert to MB
        print(f"  Iteration {i+1}: {elapsed:.3f}s, {peak/1024/1024:.1f} MB peak memory")

    # Calculate statistics
    avg_time = sum(times) / len(times)
    min_time = min(times)
    max_time = max(times)
    avg_memory = sum(memory_peaks) / len(memory_peaks)
    min_memory = min(memory_peaks)
    max_memory = max(memory_peaks)

    print(f"\n{'='*60}")
    print("RESULTS")
    print(f"{'='*60}")
    print(f"Cohort size: {cohort_size} hospitalizations")
    print(f"\nFirst run (cold cache):")
    print(f"  Time:   {times[0]:.3f}s")
    print(f"  Memory: {memory_peaks[0]:.1f} MB")
    print(f"\nAcross all {num_iterations} runs:")
    print(f"  Average time:   {avg_time:.3f}s (min: {min_time:.3f}s, max: {max_time:.3f}s)")
    print(f"  Average memory: {avg_memory:.1f} MB (min: {min_memory:.1f} MB, max: {max_memory:.1f} MB)")
    print(f"{'='*60}")

    return {
        'cohort_size': cohort_size,
        'first_time': times[0],
        'first_memory': memory_peaks[0],
        'avg_time': avg_time,
        'min_time': min_time,
        'max_time': max_time,
        'avg_memory': avg_memory,
        'min_memory': min_memory,
        'max_memory': max_memory,
        'all_times': times,
        'all_memories': memory_peaks
    }


if __name__ == "__main__":
    # Parse command-line arguments
    n_sample = None
    use_memray = False

    i = 1
    while i < len(sys.argv):
        arg = sys.argv[i]

        if arg == '-n':
            # Next argument should be the sample size
            if i + 1 >= len(sys.argv):
                print("Error: -n requires a numeric argument")
                sys.exit(1)
            try:
                n_sample = int(sys.argv[i + 1])
                i += 2
            except ValueError:
                print(f"Error: -n requires a numeric argument, got '{sys.argv[i + 1]}'")
                sys.exit(1)
        elif arg == '-memory':
            use_memray = True
            i += 1
        else:
            print(f"Error: Unknown argument '{arg}'")
            print("\nUsage:")
            print("  python benchmark_simple.py              # Full cohort (2000 IDs)")
            print("  python benchmark_simple.py -n 100       # Random sample of 100 IDs")
            print("  python benchmark_simple.py -memory      # Run with memray memory profiling")
            print("  python benchmark_simple.py -n 100 -memory  # Sample + memory profiling")
            sys.exit(1)

    # Run with memray if requested
    if use_memray:
        print("⚠️  Memory profiling requested but -memory flag doesn't trigger memray")
        print("    Use: memray run -o profile.bin python benchmark_simple.py")
        print("    Then: memray flamegraph profile.bin")
        print("\nProceeding with standard benchmark (tracemalloc only)...\n")

    # Run benchmark
    benchmark_convert_dose_units(n=n_sample)

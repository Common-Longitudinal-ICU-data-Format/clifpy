"""SOFA-2 benchmark with per-subscore timing and progressive scaling.

Measures wall-clock time, peak memory, and per-subscore breakdown using
the perf_profile=True parameter on calculate_sofa2().

Usage:
    python benchmark_sofa2.py -n 100              # Single run with 100 IDs
    python benchmark_sofa2.py --scale              # Progressive: 100, 500, 1000, 2000
    python benchmark_sofa2.py --scale --max 5000   # Scale up to 5000 IDs
    python benchmark_sofa2.py -n 100 --iters 5     # 5 iterations for stability
    python benchmark_sofa2.py -n 100 --daily       # Use calculate_sofa2_daily

Requires a cohort CSV at .dev/cohort_hosp_ids.csv (or pass --cohort-csv).
Requires CLIF config at config/config.yaml (or pass --config).
"""
import sys
import time
import tracemalloc
import argparse
from pathlib import Path

import pandas as pd

# Reuse existing infrastructure
sys.path.insert(0, str(Path(__file__).parent))
from benchmark_simple import clean_duckdb_cache, load_hospitalization_ids


PROJECT_ROOT = Path(__file__).parent.parent.parent


def build_cohort_from_adt(
    hospitalization_ids: list[str],
    config_path: str,
) -> pd.DataFrame:
    """Build single-window cohort from ADT data for benchmarking.

    Creates one 24h window per hospitalization_id (from earliest in_dttm).
    """
    import duckdb
    from clifpy import load_data

    adt = load_data('adt', config_path=config_path, return_rel=True)
    hosp_ids_str = ", ".join(f"'{h}'" for h in hospitalization_ids)
    cohort = duckdb.sql(f"""
        FROM adt
        SELECT
            hospitalization_id
            , MIN(in_dttm) AS start_dttm
            , MIN(in_dttm) + INTERVAL '24 hours' AS end_dttm
        WHERE hospitalization_id IN ({hosp_ids_str})
        GROUP BY hospitalization_id
    """).df()
    return cohort


def build_cohort_for_daily(
    hospitalization_ids: list[str],
    config_path: str,
) -> pd.DataFrame:
    """Build multi-day cohort from ADT data for daily scoring benchmark.

    Creates one window per hospitalization spanning full stay duration.
    """
    import duckdb
    from clifpy import load_data

    adt = load_data('adt', config_path=config_path, return_rel=True)
    hosp_ids_str = ", ".join(f"'{h}'" for h in hospitalization_ids)
    cohort = duckdb.sql(f"""
        FROM adt
        SELECT
            hospitalization_id
            , MIN(in_dttm) AS start_dttm
            , MAX(out_dttm) AS end_dttm
        WHERE hospitalization_id IN ({hosp_ids_str})
        GROUP BY hospitalization_id
    """).df()
    return cohort


def benchmark_single(
    n: int,
    config_path: str,
    cohort_csv: str,
    num_iterations: int = 3,
    daily: bool = False,
) -> dict:
    """Run SOFA-2 benchmark for a single cohort size.

    Returns dict with timing, memory, and per-subscore breakdown.
    """
    from clifpy.utils.sofa2 import calculate_sofa2, calculate_sofa2_daily

    hosp_ids = load_hospitalization_ids(cohort_csv, n=n)

    if daily:
        cohort_df = build_cohort_for_daily(hosp_ids, config_path)
    else:
        cohort_df = build_cohort_from_adt(hosp_ids, config_path)

    actual_n = len(cohort_df)
    print(f"  Cohort: {actual_n} rows ({len(hosp_ids)} IDs)")

    times = []
    memory_peaks = []
    subscore_timers = []
    cv_timers = []

    for i in range(num_iterations):
        clean_duckdb_cache()
        tracemalloc.start()

        start = time.perf_counter()
        if daily:
            result, outer_timer, inner_timer, inner_cv_timer = calculate_sofa2_daily(
                cohort_df,
                clif_config_path=config_path,
                perf_profile=True,
            )
            subscore_timers.append(inner_timer)
            cv_timers.append(inner_cv_timer)
        else:
            result, subscore_timer, cv_timer = calculate_sofa2(
                cohort_df,
                clif_config_path=config_path,
                perf_profile=True,
            )
            subscore_timers.append(subscore_timer)
            cv_timers.append(cv_timer)
        elapsed = time.perf_counter() - start

        _, peak = tracemalloc.get_traced_memory()
        tracemalloc.stop()

        times.append(elapsed)
        memory_peaks.append(peak / 1024 / 1024)
        print(f"    Iteration {i+1}: {elapsed:.3f}s, {peak/1024/1024:.1f} MB, {len(result)} output rows")

    return {
        'n': actual_n,
        'num_ids': len(hosp_ids),
        'avg_time': sum(times) / len(times),
        'min_time': min(times),
        'max_time': max(times),
        'avg_memory': sum(memory_peaks) / len(memory_peaks),
        'time_per_id': sum(times) / len(times) / actual_n * 1000,
        'all_times': times,
        'all_memories': memory_peaks,
        'subscore_timers': subscore_timers,
        'cv_timers': cv_timers,
    }


def print_breakdown(results: dict):
    """Print per-subscore and per-CV-step breakdown from last iteration."""
    if results.get('subscore_timers'):
        last_timer = results['subscore_timers'][-1]
        print(f"\n  Per-subscore breakdown (last iteration):")
        print("  " + last_timer.report(cohort_size=results['n']).replace("\n", "\n  "))

    if results.get('cv_timers') and results['cv_timers'][-1]:
        last_cv = results['cv_timers'][-1]
        if last_cv.results:
            print(f"\n  CV internal breakdown (last iteration):")
            print("  " + last_cv.report().replace("\n", "\n  "))


def run_scaling_test(
    sample_sizes: list[int],
    config_path: str,
    cohort_csv: str,
    num_iterations: int = 3,
    daily: bool = False,
):
    """Run progressive scaling test."""
    all_results = []

    for n in sample_sizes:
        print(f"\n{'='*70}")
        print(f"Testing with n={n}")
        print(f"{'='*70}")

        results = benchmark_single(n, config_path, cohort_csv, num_iterations, daily)
        all_results.append(results)
        print_breakdown(results)

    # Summary table
    print(f"\n{'='*70}")
    print("SCALING SUMMARY")
    print(f"{'='*70}")
    print(f"{'N':<10} {'IDs':<10} {'Avg Time':<12} {'Time/ID (ms)':<15} {'Memory (MB)':<15}")
    print("-" * 62)
    for r in all_results:
        print(f"{r['n']:<10} {r['num_ids']:<10} {r['avg_time']:<12.3f} {r['time_per_id']:<15.2f} {r['avg_memory']:<15.1f}")

    # Scaling analysis
    if len(all_results) >= 2:
        first, last = all_results[0], all_results[-1]
        time_ratio = last['avg_time'] / first['avg_time']
        n_ratio = last['n'] / first['n']
        efficiency = time_ratio / n_ratio * 100

        print(f"\n{'='*70}")
        print("SCALING ANALYSIS")
        print(f"{'='*70}")
        print(f"Cohort size increased: {first['n']} -> {last['n']} ({n_ratio:.1f}x)")
        print(f"Time increased: {first['avg_time']:.3f}s -> {last['avg_time']:.3f}s ({time_ratio:.1f}x)")
        print(f"Scaling efficiency: {efficiency:.1f}%")
        if efficiency < 120:
            print("  -> Excellent linear scaling")
        elif efficiency < 150:
            print("  -> Good scaling with slight overhead")
        else:
            print("  -> Sub-linear scaling detected - investigate bottlenecks")


def main():
    parser = argparse.ArgumentParser(
        description="SOFA-2 Performance Benchmark",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument('-n', type=int, default=100, help='Number of hospitalization IDs to sample (default: 100)')
    parser.add_argument('--scale', action='store_true', help='Run progressive scaling test')
    parser.add_argument('--max', type=int, default=2000, help='Max cohort size for scaling (default: 2000)')
    parser.add_argument('--iters', type=int, default=3, help='Iterations per cohort size (default: 3)')
    parser.add_argument('--daily', action='store_true', help='Use calculate_sofa2_daily (multi-day windows)')
    parser.add_argument('--config', type=str, default=None, help='Path to CLIF config.yaml')
    parser.add_argument('--cohort-csv', type=str, default=None, help='Path to cohort hospitalization IDs CSV')
    args = parser.parse_args()

    config_path = args.config or str(PROJECT_ROOT / "config" / "config.yaml")
    cohort_csv = args.cohort_csv or str(PROJECT_ROOT / '.dev' / 'cohort_hosp_ids.csv')

    mode = "daily" if args.daily else "single-window"
    print(f"SOFA-2 Benchmark ({mode} mode)")
    print(f"Config: {config_path}")
    print(f"Cohort CSV: {cohort_csv}")

    if args.scale:
        sizes = [100, 500, 1000]
        if args.max > 1000:
            sizes.append(args.max)
        run_scaling_test(sizes, config_path, cohort_csv, args.iters, args.daily)
    else:
        print(f"\n{'='*70}")
        print(f"Single run with n={args.n}")
        print(f"{'='*70}")
        results = benchmark_single(args.n, config_path, cohort_csv, args.iters, args.daily)
        print_breakdown(results)

        # Final summary
        print(f"\n{'='*70}")
        print("RESULTS")
        print(f"{'='*70}")
        print(f"Cohort size: {results['n']} rows ({results['num_ids']} IDs)")
        print(f"Average time: {results['avg_time']:.3f}s (min: {results['min_time']:.3f}s, max: {results['max_time']:.3f}s)")
        print(f"Average memory: {results['avg_memory']:.1f} MB")
        print(f"Time per ID: {results['time_per_id']:.2f} ms")


if __name__ == "__main__":
    main()

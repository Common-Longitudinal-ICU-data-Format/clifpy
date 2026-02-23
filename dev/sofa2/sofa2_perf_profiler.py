"""SOFA-2 performance profiler with per-subscore timing and scaling analysis.

Profiles where time is spent in the SOFA-2 pipeline using the
perf_profile=True parameter on calculate_sofa2().

Usage:
    python sofa2_perf_profiler.py -n 100                  # Single run, 100 ICU stays
    python sofa2_perf_profiler.py -n 100 1000 5000        # Custom scaling test
    python sofa2_perf_profiler.py -n 100 --daily           # Daily mode
    python sofa2_perf_profiler.py --site-test mimic-iv     # One-click site profiling
    python sofa2_perf_profiler.py --cohort-csv ids.csv     # Use external cohort IDs

Requires CLIF config at config/config.yaml (or pass --config).
"""
import io
import sys
import time
import tracemalloc
import shutil
import tempfile
import random
import argparse
from datetime import datetime
from pathlib import Path

import pandas as pd

from clifpy.utils.sofa2._perf import _fmt_time


PROJECT_ROOT = Path(__file__).parent.parent.parent

BOX_WIDTH = 58


# ---------------------------------------------------------------------------
# Output formatting
# ---------------------------------------------------------------------------

def _box_double(text: str) -> str:
    """Mode-level header with double-line box: ╔═╗ ║ ║ ╚═╝"""
    inner = f"  {text}"
    pad = BOX_WIDTH - len(inner)
    return (
        f"╔{'═' * BOX_WIDTH}╗\n"
        f"║{inner}{' ' * max(pad, 0)}║\n"
        f"╚{'═' * BOX_WIDTH}╝"
    )


def _box_single(text: str) -> str:
    """Sub-section header with single-line box: ┌─┐ │ │ └─┘"""
    inner = f"  {text}"
    pad = BOX_WIDTH - len(inner)
    return (
        f"┌{'─' * BOX_WIDTH}┐\n"
        f"│{inner}{' ' * max(pad, 0)}│\n"
        f"└{'─' * BOX_WIDTH}┘"
    )


class TeeOutput:
    """Captures print() output to both stdout and a string buffer."""

    def __init__(self, original_stdout):
        self.original = original_stdout
        self.buffer = io.StringIO()

    def write(self, text):
        self.original.write(text)
        self.buffer.write(text)

    def flush(self):
        self.original.flush()

    def getvalue(self) -> str:
        return self.buffer.getvalue()


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def clean_duckdb_cache():
    """Remove DuckDB temporary files to ensure cold cache."""
    temp_dir = Path(tempfile.gettempdir())
    for path in temp_dir.glob("duckdb_temp*"):
        try:
            if path.is_dir():
                shutil.rmtree(path)
            else:
                path.unlink()
        except Exception:
            pass


def load_hospitalization_ids(csv_path: str, n: int = None, random_seed: int = 42) -> list:
    """Load hospitalization IDs from CSV file with optional random sampling."""
    df = pd.read_csv(csv_path)
    hosp_ids = df['hospitalization_id'].astype(str).tolist()

    if n is not None:
        if n > len(hosp_ids):
            print(f"  Requested n={n} but only {len(hosp_ids)} IDs available. Using all IDs.")
            return hosp_ids
        random.seed(random_seed)
        hosp_ids = random.sample(hosp_ids, n)

    return hosp_ids


def auto_detect_max_icu_stays(config_path: str) -> int:
    """Query ADT to find total number of distinct ICU hospitalization_ids."""
    import duckdb
    from clifpy import load_data

    adt = load_data('adt', config_path=config_path, return_rel=True)
    result = duckdb.sql("""
        FROM adt
        SELECT COUNT(DISTINCT hospitalization_id) AS n
        WHERE location_category = 'icu'
    """).fetchone()
    return result[0]


def generate_power_of_10_sizes(max_n: int) -> list[int]:
    """Generate [100, 1000, 10000, ...] up to max_n.

    Appends max_n if it isn't already a power of 10 in the list.
    """
    sizes = []
    power = 2  # Start at 10^2 = 100
    while 10 ** power <= max_n:
        sizes.append(10 ** power)
        power += 1
    # Append actual max if it isn't already in the list
    if not sizes or sizes[-1] != max_n:
        sizes.append(max_n)
    return sizes


def save_report(
    content: str,
    report_dir: Path,
    prefix: str = "sofa2_profiling",
    site_name: str | None = None,
) -> Path:
    """Save report content to timestamped .md file in report_dir."""
    report_dir.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now().strftime("%y%m%d_%H%M%S")
    name_part = f"_{site_name}" if site_name else ""
    filepath = report_dir / f"{prefix}{name_part}_{timestamp}.md"
    filepath.write_text(content)
    return filepath


# ---------------------------------------------------------------------------
# Cohort builders
# ---------------------------------------------------------------------------

def build_cohort_from_adt_limit(
    n: int,
    config_path: str,
    daily: bool = False,
) -> pd.DataFrame:
    """Default cohort: query ADT directly with LIMIT (no external CSV needed).

    Mirrors the pattern from sofa2_dev.py — filters to ICU stays and limits to n rows.
    """
    import duckdb
    from clifpy import load_data

    adt = load_data('adt', config_path=config_path, return_rel=True)
    end_expr = "MAX(out_dttm)" if daily else "MIN(in_dttm) + INTERVAL '24 hours'"
    return duckdb.sql(f"""
        FROM adt
        SELECT
            hospitalization_id
            , MIN(in_dttm) AS start_dttm
            , {end_expr} AS end_dttm
        WHERE location_category = 'icu'
        GROUP BY hospitalization_id
        LIMIT {n}
    """).df()


def build_cohort_from_adt(
    hospitalization_ids: list[str],
    config_path: str,
) -> pd.DataFrame:
    """Build single-window cohort from ADT data.

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
    """Build multi-day cohort from ADT data.

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


# ---------------------------------------------------------------------------
# Profiling
# ---------------------------------------------------------------------------

def profile_single(
    n: int,
    config_path: str,
    num_iterations: int = 1,
    daily: bool = False,
    cohort_csv: str | None = None,
) -> dict:
    """Run SOFA-2 profiler for a single cohort size.

    Returns dict with timing, memory, and per-subscore breakdown.
    """
    from clifpy.utils.sofa2 import calculate_sofa2, calculate_sofa2_daily

    if cohort_csv is not None:
        hosp_ids = load_hospitalization_ids(cohort_csv, n=n)
        if daily:
            cohort_df = build_cohort_for_daily(hosp_ids, config_path)
        else:
            cohort_df = build_cohort_from_adt(hosp_ids, config_path)
        print(f"  Cohort: {len(cohort_df)} rows ({len(hosp_ids)} IDs from external CSV)")
    else:
        cohort_df = build_cohort_from_adt_limit(n, config_path, daily=daily)
        print(f"  Cohort: {len(cohort_df)} rows (ADT LIMIT {n})")

    actual_n = len(cohort_df)

    times = []
    memory_peaks = []
    subscore_timers = []
    cv_timers = []
    last_output_rows = 0

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

        last_output_rows = len(result)
        times.append(elapsed)
        memory_peaks.append(peak / 1024 / 1024)
        print(f"    Iteration {i+1}: {_fmt_time(elapsed)}, {peak/1024/1024:.1f} MB, {last_output_rows} output rows")
        if daily:
            print(f"    -> Expanded to {last_output_rows} 24-hr windows ({last_output_rows/actual_n:.1f}x expansion from {actual_n} input rows)")

    return {
        'n': actual_n,
        'avg_time': sum(times) / len(times),
        'min_time': min(times),
        'max_time': max(times),
        'avg_memory': sum(memory_peaks) / len(memory_peaks),
        'output_rows': last_output_rows,
        'all_times': times,
        'all_memories': memory_peaks,
        'subscore_timers': subscore_timers,
        'cv_timers': cv_timers,
    }


def print_breakdown(results: dict):
    """Print per-subscore and per-CV-step breakdown from last iteration."""
    if results.get('subscore_timers'):
        last_timer = results['subscore_timers'][-1]
        print(f"\n  Per-subscore breakdown:")
        print("  " + last_timer.report().replace("\n", "\n  "))

    if results.get('cv_timers') and results['cv_timers'][-1]:
        last_cv = results['cv_timers'][-1]
        if last_cv.results:
            print(f"\n  CV internal breakdown:")
            print("  " + last_cv.report().replace("\n", "\n  "))


def _print_scaling_summary(
    all_results: list[dict],
    title: str = "Summary",
):
    """Print scaling summary table and efficiency analysis."""
    print(f"\n{_box_single(title)}")

    # Show Expanded N column when any result has output_rows != n (daily mode)
    has_expansion = any(r.get('output_rows', r['n']) != r['n'] for r in all_results)

    if has_expansion:
        print(f"  {'N':<10} {'Expanded N':<13} {'Avg Time':<15} {'Memory (MB)':<15}")
        print(f"  {'-' * 53}")
        for r in all_results:
            expanded = r.get('output_rows', r['n'])
            print(f"  {r['n']:<10} {expanded:<13} {_fmt_time(r['avg_time']):<15} {r['avg_memory']:<15.1f}")
    else:
        print(f"  {'N':<10} {'Avg Time':<15} {'Memory (MB)':<15}")
        print(f"  {'-' * 40}")
        for r in all_results:
            print(f"  {r['n']:<10} {_fmt_time(r['avg_time']):<15} {r['avg_memory']:<15.1f}")

    if len(all_results) >= 2:
        first, last = all_results[0], all_results[-1]
        time_ratio = last['avg_time'] / first['avg_time']
        n_ratio = last['n'] / first['n']
        efficiency = time_ratio / n_ratio * 100

        print(f"\n  Scaling: {first['n']} -> {last['n']} ({n_ratio:.1f}x)")
        print(f"  Time: {_fmt_time(first['avg_time'])} -> {_fmt_time(last['avg_time'])} ({time_ratio:.1f}x)")
        print(f"  Efficiency: {efficiency:.1f}%")
        if efficiency < 120:
            print("    -> Excellent linear scaling")
        elif efficiency < 150:
            print("    -> Good scaling with slight overhead")
        else:
            print("    -> Sub-linear scaling detected - investigate bottlenecks")


def _print_skipped(skipped: list[int]):
    """Print skipped sizes in summary."""
    for s in skipped:
        print(f"  {s:<10} {'SKIPPED (timeout)':<15}")


def run_scaling_test(
    sample_sizes: list[int],
    config_path: str,
    num_iterations: int = 1,
    daily: bool = False,
    cohort_csv: str | None = None,
    step_timeout_mins: float = 0,
) -> list[dict]:
    """Run progressive scaling test.

    If step_timeout_mins > 0, skip remaining sizes when any step exceeds the timeout.
    Returns list of result dicts (one per completed size).
    """
    all_results = []
    skipped = []

    for i, n in enumerate(sample_sizes):
        is_probe = (i == 0)
        label = f"n={n} (probe)" if is_probe else f"n={n}"
        print(f"\n{_box_single(label)}")

        start = time.perf_counter()
        results = profile_single(
            n, config_path,
            num_iterations=num_iterations, daily=daily,
            cohort_csv=cohort_csv,
        )
        elapsed = time.perf_counter() - start
        all_results.append(results)

        print_breakdown(results)

        # After probe, show estimates for remaining sizes
        if is_probe and len(sample_sizes) > 1:
            probe_n = results['n']
            probe_time = results['avg_time']
            print(f"\n  Estimated times for remaining sizes:")
            for future_n in sample_sizes[1:]:
                est_s = probe_time * (future_n / probe_n)
                print(f"    n={future_n}:   ~{_fmt_time(est_s)}")

        # Timeout check (only if enabled)
        if step_timeout_mins > 0 and elapsed > step_timeout_mins * 60:
            remaining = sample_sizes[i + 1:]
            if remaining:
                print(f"\n  TIMEOUT: n={n} took {_fmt_time(elapsed)} (>{step_timeout_mins:.0f} min limit)")
                print(f"  Skipping remaining sizes: {remaining}")
                skipped = remaining
            break

    return all_results


def run_site_test(
    config_path: str,
    site_name: str | None = None,
    max_override: int | None = None,
    cohort_csv: str | None = None,
    step_timeout_mins: float = 10.0,
):
    """One-click site performance profiling test.

    Runs both generic and daily modes with auto-detected scaling.
    """
    # Auto-detect max
    detected_max = auto_detect_max_icu_stays(config_path)
    print(f"Detected {detected_max} ICU hospitalizations in ADT")
    effective_max = max_override if max_override is not None else detected_max
    if max_override is not None:
        print(f"Using --max override: {effective_max}")

    sizes = generate_power_of_10_sizes(effective_max)
    print(f"Scaling sizes: {sizes}")

    # --- Generic mode ---
    print(f"\n{_box_double('GENERIC MODE')}")
    generic_results = run_scaling_test(
        sizes, config_path,
        daily=False, cohort_csv=cohort_csv,
        step_timeout_mins=step_timeout_mins,
    )
    _print_scaling_summary(generic_results, title="Generic Summary")

    # --- Daily mode ---
    print(f"\n{_box_double('DAILY MODE')}")
    daily_results = run_scaling_test(
        sizes, config_path,
        daily=True, cohort_csv=cohort_csv,
        step_timeout_mins=step_timeout_mins,
    )
    _print_scaling_summary(daily_results, title="Daily Summary")

    # --- Final ---
    generic_max_n = generic_results[-1]['n'] if generic_results else 0
    daily_max_n = daily_results[-1]['n'] if daily_results else 0
    print(f"\n{_box_double('SITE PROFILING COMPLETE')}")
    print(f"Total ICU hospitalizations: {detected_max}")
    print(f"Generic mode: tested up to n={generic_max_n}")
    print(f"Daily mode: tested up to n={daily_max_n}")


def main():
    from clifpy.utils.logging_config import setup_logging
    setup_logging()

    parser = argparse.ArgumentParser(
        description="SOFA-2 Performance Profiler",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument(
        '-n', nargs='+', type=int, default=[100],
        help='Cohort size(s). Single value = single run; multiple = scaling test (default: 100)',
    )
    parser.add_argument(
        '--site-test', nargs='?', const=True, default=False, metavar='SITE_NAME',
        help='One-click site profiling (optional: site name for report filename)',
    )
    parser.add_argument('--max', type=int, default=None, help='Override max cohort size for --site-test')
    parser.add_argument('--iters', type=int, default=1, help='Iterations per cohort size (default: 1)')
    parser.add_argument('--daily', action='store_true', help='Use calculate_sofa2_daily (multi-day windows)')
    parser.add_argument('--config', type=str, default=None, help='Path to CLIF config.yaml')
    parser.add_argument('--cohort-csv', type=str, default=None, help='Path to external CSV with hospitalization_ids')
    args = parser.parse_args()

    config_path = args.config or str(PROJECT_ROOT / "config" / "config.yaml")
    report_dir = PROJECT_ROOT / "output" / "perf"

    # Install tee for report capture
    tee = TeeOutput(sys.stdout)
    sys.stdout = tee

    if args.site_test:
        site_name = args.site_test if isinstance(args.site_test, str) else None
        label = f" ({site_name})" if site_name else ""
        print(f"SOFA-2 Site Performance Profile{label}")
        print(f"Config: {config_path}")

        run_site_test(
            config_path,
            site_name=site_name,
            max_override=args.max,
            cohort_csv=args.cohort_csv,
            step_timeout_mins=10.0,
        )

        # Save report
        sys.stdout = tee.original
        filepath = save_report(tee.getvalue(), report_dir, site_name=site_name)
        print(f"\nReport saved to: {filepath}")

    elif len(args.n) > 1:
        # Custom scaling test with user-specified sizes
        mode = "daily" if args.daily else "generic"
        print(f"SOFA-2 Scaling Test ({mode} mode)")
        print(f"Config: {config_path}")
        print(f"Sizes: {args.n}")

        print(f"\n{_box_double(f'{mode.upper()} MODE')}")
        results = run_scaling_test(
            args.n, config_path,
            num_iterations=args.iters, daily=args.daily,
            cohort_csv=args.cohort_csv,
        )
        _print_scaling_summary(results, title=f"{mode.capitalize()} Summary")

        # Save report
        sys.stdout = tee.original
        filepath = save_report(tee.getvalue(), report_dir, prefix="sofa2_scaling")
        print(f"\nReport saved to: {filepath}")

    else:
        # Single run
        n = args.n[0]
        mode = "daily" if args.daily else "generic"
        cohort_source = f"external CSV ({args.cohort_csv})" if args.cohort_csv else "ADT LIMIT"
        print(f"SOFA-2 Profiler ({mode} mode)")
        print(f"Config: {config_path}")
        print(f"Cohort source: {cohort_source}")

        print(f"\n{_box_single(f'n={n}')}")
        results = profile_single(
            n, config_path,
            num_iterations=args.iters, daily=args.daily,
            cohort_csv=args.cohort_csv,
        )
        print_breakdown(results)

        # Final summary
        print(f"\n{_box_single('Results')}")
        print(f"  Cohort size: {results['n']} rows")
        print(f"  Average time: {_fmt_time(results['avg_time'])}")
        print(f"  Average memory: {results['avg_memory']:.1f} MB")

        # Restore stdout (no report saved for single runs)
        sys.stdout = tee.original


if __name__ == "__main__":
    main()

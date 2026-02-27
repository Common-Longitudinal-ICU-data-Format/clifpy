"""SOFA-2 performance profiler with per-subscore timing and scaling analysis.

Profiles where time is spent in the SOFA-2 pipeline using the
perf_profile=True parameter on calculate_sofa2().

Usage:
    python sofa2_perf_profiler.py -n 100                           # Single run, generic mode
    python sofa2_perf_profiler.py -n max --mode daily              # All data, daily mode
    python sofa2_perf_profiler.py -n 100 1000 5000 --mode daily    # Custom scaling, daily
    python sofa2_perf_profiler.py --site-test --site ucmc          # Full site profiling (both modes)
    python sofa2_perf_profiler.py -n max --site ucmc --mode daily  # Max stress test, daily
    python sofa2_perf_profiler.py -n 100 --stitch                  # Stitched encounters (default 6h)
    python sofa2_perf_profiler.py -n 100 --stitch 12               # Stitched with 12h window
    python sofa2_perf_profiler.py -n 100 --mem-profile             # With memray memory profiling
    python sofa2_perf_profiler.py --site-test --site ucmc --mem-profile  # Site test + memray
    python sofa2_perf_profiler.py -n 100 --mem-limit 4GB           # With DuckDB memory limit
    python sofa2_perf_profiler.py -n 100 --batch-size 50            # With cohort batching
    python sofa2_perf_profiler.py -n 100 --mem-limit 8GB --temp-dir /tmp/sofa2 --max-temp-size 10GB

Requires CLIF config at config/config.yaml (or pass --config).
"""
import io
import sys
import time
import subprocess
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


def auto_detect_max_icu_stays(config_path: str) -> tuple[int, int]:
    """Query ADT to find ICU stay counts.

    Returns (total_icu_rows, distinct_hospitalization_ids).
    Generic mode uses total rows (each ADT row = one scoring window).
    Daily mode uses distinct IDs (one row per hospitalization).
    """
    import duckdb
    from clifpy import load_data

    adt = load_data('adt', config_path=config_path, return_rel=True)
    result = duckdb.sql("""
        FROM adt
        SELECT
            COUNT(*) AS total_rows
            , COUNT(DISTINCT hospitalization_id) AS distinct_ids
        WHERE location_category = 'icu'
    """).fetchone()
    return result[0], result[1]


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

    Generic mode: each ICU ADT row is a separate scoring window, capped at 24h.
    Daily mode: one row per hospitalization spanning full stay (GROUP BY).
    """
    import duckdb
    from clifpy import load_data

    adt = load_data('adt', config_path=config_path, return_rel=True)
    if daily:
        return duckdb.sql(f"""
            FROM adt
            SELECT
                hospitalization_id
                , MIN(in_dttm) AS start_dttm
                , MAX(out_dttm) AS end_dttm
            WHERE location_category = 'icu'
            GROUP BY hospitalization_id
            LIMIT {n}
        """).df()
    else:
        # Each ICU ADT row = one scoring window; capped at 24h
        # Non-overlapping guaranteed by ADT's physical location model
        return duckdb.sql(f"""
            FROM adt
            SELECT
                hospitalization_id
                , in_dttm AS start_dttm
                , LEAST(out_dttm, in_dttm + INTERVAL '24 hours') AS end_dttm
            WHERE location_category = 'icu'
            LIMIT {n}
        """).df()


def build_cohort_from_adt(
    hospitalization_ids: list[str],
    config_path: str,
) -> pd.DataFrame:
    """Build generic-mode cohort from ADT data for external CSV IDs.

    Each ICU ADT row is a separate scoring window, capped at 24h.
    hospitalization_id can repeat (multiple ICU stays per hospitalization).
    """
    import duckdb
    from clifpy import load_data

    adt = load_data('adt', config_path=config_path, return_rel=True)
    hosp_ids_str = ", ".join(f"'{h}'" for h in hospitalization_ids)
    cohort = duckdb.sql(f"""
        FROM adt
        SELECT
            hospitalization_id
            , in_dttm AS start_dttm
            , LEAST(out_dttm, in_dttm + INTERVAL '24 hours') AS end_dttm
        WHERE hospitalization_id IN ({hosp_ids_str})
          AND location_category = 'icu'
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


def build_cohort_stitched(
    n: int,
    config_path: str,
    time_interval: int = 6,
    daily: bool = False,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Build cohort using encounter_block from stitch_encounters().

    Loads hospitalization and ADT tables, runs encounter stitching, and builds
    a SOFA-2 cohort keyed by encounter_block with LIMIT n.

    Returns (cohort_df, encounter_mapping).
    """
    import duckdb
    from clifpy import load_data
    from clifpy.utils.stitching_encounters import stitch_encounters

    hospitalization = load_data('hospitalization', config_path=config_path)
    adt_data = load_data('adt', config_path=config_path)
    hosp_stitched, adt_stitched, encounter_mapping = stitch_encounters(
        hospitalization, adt_data, time_interval=time_interval,
    )

    # Log stitching impact stats
    total_hosp = len(encounter_mapping)
    total_encounters = encounter_mapping['encounter_block'].nunique()
    stitched_groups = encounter_mapping.groupby('encounter_block').size()
    multi_hosp = (stitched_groups > 1).sum()
    hosp_in_multi = stitched_groups[stitched_groups > 1].sum()
    print(f"  Stitching stats (time_interval={time_interval}h):")
    print(f"    {total_hosp} hospitalizations -> {total_encounters} encounter_blocks")
    print(f"    {multi_hosp} blocks have >1 hospitalization ({hosp_in_multi} hosp_ids remapped)")

    if daily:
        cohort = duckdb.sql(f"""
            FROM adt_stitched
            SELECT
                encounter_block
                , MIN(in_dttm) AS start_dttm
                , MAX(out_dttm) AS end_dttm
            WHERE location_category = 'icu'
            GROUP BY encounter_block
            LIMIT {n}
        """).df()
    else:
        # Each ICU ADT row = one scoring window; capped at 24h
        cohort = duckdb.sql(f"""
            FROM adt_stitched
            SELECT
                encounter_block
                , in_dttm AS start_dttm
                , LEAST(out_dttm, in_dttm + INTERVAL '24 hours') AS end_dttm
            WHERE location_category = 'icu'
            LIMIT {n}
        """).df()

    return cohort, encounter_mapping


# ---------------------------------------------------------------------------
# Profiling
# ---------------------------------------------------------------------------

def profile_single(
    n: int,
    config_path: str,
    num_iterations: int = 1,
    daily: bool = False,
    cohort_csv: str | None = None,
    stitch_interval: int | None = None,
    mem_profile_dir: Path | None = None,
    duckdb_config=None,
) -> dict:
    """Run SOFA-2 profiler for a single cohort size.

    Returns dict with timing and per-subscore breakdown.
    When mem_profile_dir is set, wraps scoring in memray.Tracker for
    native memory capture (DuckDB C++ allocations included).
    """
    from clifpy.utils.sofa2 import calculate_sofa2, calculate_sofa2_daily

    # Build cohort (stitched, external CSV, or default ADT-limit)
    encounter_mapping = None
    if stitch_interval is not None:
        cohort_df, encounter_mapping = build_cohort_stitched(
            n, config_path, time_interval=stitch_interval, daily=daily,
        )
        if daily:
            print(f"  Cohort: {len(cohort_df)} encounter_blocks (ADT LIMIT {n})")
        else:
            print(f"  Cohort: {len(cohort_df)} ICU stays as encounter_blocks (ADT LIMIT {n})")
    elif cohort_csv is not None:
        hosp_ids = load_hospitalization_ids(cohort_csv, n=n)
        if daily:
            cohort_df = build_cohort_for_daily(hosp_ids, config_path)
            print(f"  Cohort: {len(cohort_df)} unique hospitalizations ({len(hosp_ids)} IDs from external CSV)")
        else:
            cohort_df = build_cohort_from_adt(hosp_ids, config_path)
            print(f"  Cohort: {len(cohort_df)} ICU stays ({len(hosp_ids)} IDs from external CSV)")
    else:
        cohort_df = build_cohort_from_adt_limit(n, config_path, daily=daily)
        if daily:
            print(f"  Cohort: {len(cohort_df)} unique hospitalizations (ADT LIMIT {n})")
        else:
            print(f"  Cohort: {len(cohort_df)} ICU stays (ADT LIMIT {n})")

    actual_n = len(cohort_df)

    # Build SOFA-2 kwargs (add id_name/id_mapping when stitching)
    sofa_kwargs = {'clif_config_path': config_path, 'perf_profile': True}
    if duckdb_config is not None:
        sofa_kwargs['duckdb_config'] = duckdb_config
    if stitch_interval is not None:
        sofa_kwargs['id_name'] = 'encounter_block'
        sofa_kwargs['id_mapping'] = encounter_mapping

    times = []
    subscore_timers = []
    cv_timers = []
    last_output_rows = 0

    # Set up memray capture if requested
    capture_file = None
    tracker = None
    if mem_profile_dir:
        try:
            import memray
            mem_profile_dir.mkdir(parents=True, exist_ok=True)
            mode_tag = "daily" if daily else "generic"
            capture_file = mem_profile_dir / f"sofa2_mem_{mode_tag}_n{n}.bin"
            tracker = memray.Tracker(str(capture_file), native_traces=True)
            tracker.__enter__()
            print(f"  Memray: capturing native allocations to {capture_file.name}")
        except ImportError:
            print("  WARNING: memray not installed. Skipping memory profiling.")
            print("  Install with: uv sync --group dev")
            tracker = None

    try:
        for i in range(num_iterations):
            clean_duckdb_cache()
            start = time.perf_counter()
            if daily:
                result, outer_timer, inner_timer, inner_cv_timer = calculate_sofa2_daily(
                    cohort_df, **sofa_kwargs,
                )
                subscore_timers.append(inner_timer)
                cv_timers.append(inner_cv_timer)
            else:
                result, subscore_timer, cv_timer = calculate_sofa2(
                    cohort_df, **sofa_kwargs,
                )
                subscore_timers.append(subscore_timer)
                cv_timers.append(cv_timer)
            elapsed = time.perf_counter() - start

            last_output_rows = len(result)
            times.append(elapsed)
            print(f"    Iteration {i+1}: {_fmt_time(elapsed)}, {last_output_rows} output rows")
            if daily:
                print(f"    -> Expanded to {last_output_rows} 24-hr windows ({last_output_rows/actual_n:.1f}x expansion from {actual_n} input rows)")
    finally:
        if tracker is not None:
            tracker.__exit__(None, None, None)

    # Auto-generate flamegraph from memray capture
    if capture_file is not None and capture_file.exists():
        flamegraph_file = capture_file.with_suffix('.html')
        gen_result = subprocess.run(
            [sys.executable, "-m", "memray", "flamegraph", str(capture_file), "-o", str(flamegraph_file), "--force"],
            capture_output=True, text=True,
        )
        if gen_result.returncode == 0:
            print(f"  Memray: flamegraph -> {flamegraph_file.name}")
        else:
            print(f"  Memray: flamegraph generation failed: {gen_result.stderr.strip()}")

    return {
        'n': actual_n,
        'avg_time': sum(times) / len(times),
        'min_time': min(times),
        'max_time': max(times),
        'output_rows': last_output_rows,
        'all_times': times,
        'subscore_timers': subscore_timers,
        'cv_timers': cv_timers,
        'mem_capture': capture_file,
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
        print(f"  {'N':<10} {'Expanded N':<13} {'Avg Time':<15}")
        print(f"  {'-' * 38}")
        for r in all_results:
            expanded = r.get('output_rows', r['n'])
            print(f"  {r['n']:<10} {expanded:<13} {_fmt_time(r['avg_time']):<15}")
    else:
        print(f"  {'N':<10} {'Avg Time':<15}")
        print(f"  {'-' * 25}")
        for r in all_results:
            print(f"  {r['n']:<10} {_fmt_time(r['avg_time']):<15}")

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


def run_scaling_test(
    sample_sizes: list[int],
    config_path: str,
    num_iterations: int = 1,
    daily: bool = False,
    cohort_csv: str | None = None,
    step_timeout_mins: float = 0,
    stitch_interval: int | None = None,
    mem_profile_dir: Path | None = None,
    duckdb_config=None,
) -> list[dict]:
    """Run progressive scaling test.

    If step_timeout_mins > 0, skip remaining sizes when any step exceeds the timeout.
    Returns list of result dicts (one per completed size).
    """
    all_results = []

    for i, n in enumerate(sample_sizes):
        is_probe = (i == 0)
        label = f"n={n} (probe)" if is_probe else f"n={n}"
        print(f"\n{_box_single(label)}")

        start = time.perf_counter()
        try:
            results = profile_single(
                n, config_path,
                num_iterations=num_iterations, daily=daily,
                cohort_csv=cohort_csv,
                stitch_interval=stitch_interval,
                mem_profile_dir=mem_profile_dir,
                duckdb_config=duckdb_config,
            )
        except Exception as exc:
            print(f"\n  ERROR at n={n}: {type(exc).__name__}: {exc}")
            remaining = sample_sizes[i + 1:]
            if remaining:
                print(f"  Skipping remaining sizes: {remaining}")
            break

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
            break

    return all_results


def _parse_cohort_size(value: str) -> int | str:
    """Parse cohort size: integer or 'max' for auto-detect."""
    if value.lower() == 'max':
        return 'max'
    return int(value)


def main():
    from clifpy.utils.logging_config import setup_logging
    setup_logging()

    parser = argparse.ArgumentParser(
        description="SOFA-2 Performance Profiler",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument(
        '-n', nargs='+', type=_parse_cohort_size, default=[100],
        help='Cohort size(s). Use "max" for auto-detected max (default: 100)',
    )
    parser.add_argument(
        '--site-test', action='store_true',
        help='Power-of-10 scaling with probe (overrides -n). Defaults --mode to "both"',
    )
    parser.add_argument('--site', type=str, default=None, help='Site name for report filename (e.g., ucmc, mimic)')
    parser.add_argument('--mode', choices=['generic', 'daily', 'both'], default=None,
                        help='Scoring mode (default: generic, or both with --site-test)')
    parser.add_argument('--max', type=int, default=None, help='Cap auto-detected max cohort size')
    parser.add_argument('--iters', type=int, default=1, help='Iterations per cohort size (default: 1)')
    parser.add_argument('--config', type=str, default=None, help='Path to CLIF config.yaml')
    parser.add_argument('--cohort-csv', type=str, default=None, help='Path to external CSV with hospitalization_ids')
    parser.add_argument(
        '--stitch', nargs='?', type=int, const=6, default=None, metavar='HOURS',
        help='Use encounter_block via stitch_encounters(time_interval=HOURS). Default: 6 hours',
    )
    parser.add_argument(
        '--mem-profile', nargs='?', const='memray', default=None,
        choices=['memray'],
        metavar='MODE',
        help='Enable memory profiling (dev only). Default: memray. Captures native C/C++ allocations.',
    )
    parser.add_argument(
        '--mem-limit', type=str, default=None, metavar='SIZE',
        help="DuckDB memory limit (e.g., '4GB', '8GB'). Forces spill-to-disk when exceeded.",
    )
    parser.add_argument(
        '--temp-dir', type=str, default=None, metavar='PATH',
        help="Directory for DuckDB spill files (default: .tmp in CWD).",
    )
    parser.add_argument(
        '--max-temp-size', type=str, default=None, metavar='SIZE',
        help="Max disk for spill files (e.g., '10GB'). Prevents filling disk.",
    )
    parser.add_argument(
        '--batch-size', type=int, default=None, metavar='N',
        help="Process cohort in batches of N rows. Reduces peak memory.",
    )
    args = parser.parse_args()

    from clifpy.utils._duckdb_config import DuckDBResourceConfig

    config_path = args.config or str(PROJECT_ROOT / "config" / "config.yaml")
    report_dir = PROJECT_ROOT / "output" / "perf"
    mem_profile_dir = report_dir / "mem" if args.mem_profile else None

    # Build DuckDB resource config from CLI flags
    has_duckdb_flags = any([args.mem_limit, args.temp_dir, args.max_temp_size, args.batch_size])
    duckdb_cfg = DuckDBResourceConfig(
        memory_limit=args.mem_limit,
        temp_directory=args.temp_dir,
        max_temp_directory_size=args.max_temp_size,
        batch_size=args.batch_size,
    ) if has_duckdb_flags else None

    # ── Resolve mode ──────────────────────────────────────────────────────
    mode = args.mode or ('both' if args.site_test else 'generic')
    modes = ['generic', 'daily'] if mode == 'both' else [mode]

    # ── Auto-detect if needed ─────────────────────────────────────────────
    needs_auto_detect = args.site_test or 'max' in args.n
    max_map = {}
    if needs_auto_detect:
        total_icu_rows, distinct_ids = auto_detect_max_icu_stays(config_path)
        max_map = {'generic': total_icu_rows, 'daily': distinct_ids}

    # ── Resolve sizes per mode ────────────────────────────────────────────
    sizes_map: dict[str, list[int]] = {}
    if args.site_test:
        for m in modes:
            cap = min(max_map[m], args.max) if args.max else max_map[m]
            sizes_map[m] = generate_power_of_10_sizes(cap)
    else:
        for m in modes:
            resolved = []
            for x in args.n:
                if x == 'max':
                    cap = min(max_map[m], args.max) if args.max else max_map[m]
                    resolved.append(cap)
                else:
                    resolved.append(x)
            sizes_map[m] = resolved

    # ── Install tee for report capture ────────────────────────────────────
    tee = TeeOutput(sys.stdout)
    sys.stdout = tee

    # Determine if report should be saved (pre-compute so finally block can use it)
    has_multi_sizes = any(len(sizes_map[m]) > 1 for m in modes)
    should_save = args.site or args.site_test or has_multi_sizes

    try:
        # ── Header ────────────────────────────────────────────────────────
        site_label = f" ({args.site})" if args.site else ""
        if args.site_test:
            print(f"SOFA-2 Site Performance Profile{site_label}")
        else:
            print(f"SOFA-2 Performance Profile{site_label}")
        print(f"Config: {config_path}")
        if needs_auto_detect:
            print(f"Detected {max_map.get('generic', '?')} ICU stays ({max_map.get('daily', '?')} unique hospitalizations) in ADT")
        if args.stitch is not None:
            print(f"Identity: encounter_block (stitch_encounters, time_interval={args.stitch}h)")
        if args.mem_limit:
            print(f"Memory limit: {args.mem_limit}")
        if args.temp_dir:
            print(f"Temp directory: {args.temp_dir}")
        if args.max_temp_size:
            print(f"Max temp size: {args.max_temp_size}")
        if args.batch_size:
            print(f"Batch size: {args.batch_size}")
        if args.mem_profile:
            print(f"Memory profiling: {args.mem_profile} (native C/C++ allocations)")
        if args.max:
            print(f"Max override: {args.max}")
        for m in modes:
            print(f"{'  ' if len(modes) > 1 else ''}{m.capitalize()} scaling sizes: {sizes_map[m]}")

        # ── Run each mode ─────────────────────────────────────────────────
        step_timeout = 10.0 if args.site_test else 0
        for m in modes:
            sizes = sizes_map[m]
            is_daily = (m == 'daily')

            print(f"\n{_box_double(f'{m.upper()} MODE')}")

            if len(sizes) > 1:
                results = run_scaling_test(
                    sizes, config_path,
                    num_iterations=args.iters, daily=is_daily,
                    cohort_csv=args.cohort_csv,
                    step_timeout_mins=step_timeout,
                    stitch_interval=args.stitch,
                    mem_profile_dir=mem_profile_dir,
                    duckdb_config=duckdb_cfg,
                )
                _print_scaling_summary(results, title=f"{m.capitalize()} Summary")
            else:
                print(f"\n{_box_single(f'n={sizes[0]}')}")
                try:
                    results = profile_single(
                        sizes[0], config_path,
                        num_iterations=args.iters, daily=is_daily,
                        cohort_csv=args.cohort_csv,
                        stitch_interval=args.stitch,
                        mem_profile_dir=mem_profile_dir,
                        duckdb_config=duckdb_cfg,
                    )
                    print_breakdown(results)

                    print(f"\n{_box_single('Results')}")
                    print(f"  Cohort size: {results['n']} rows")
                    print(f"  Average time: {_fmt_time(results['avg_time'])}")
                except Exception as exc:
                    print(f"\n  ERROR: {type(exc).__name__}: {exc}")

        # ── Final summary (site-test) ─────────────────────────────────────
        if args.site_test:
            print(f"\n{_box_double('SITE PROFILING COMPLETE')}")
            if needs_auto_detect:
                print(f"Total ICU stays: {max_map.get('generic', '?')} ({max_map.get('daily', '?')} unique hospitalizations)")
            for m in modes:
                tested_n = sizes_map[m][-1]
                print(f"{m.capitalize()} mode: tested up to n={tested_n}")

    finally:
        # ── Always save report (even on crash) ────────────────────────────
        sys.stdout = tee.original
        if should_save:
            prefix = "sofa2_profiling" if args.site_test else "sofa2_scaling"
            filepath = save_report(tee.getvalue(), report_dir, prefix=prefix, site_name=args.site)
            print(f"\nReport saved to: {filepath}")


if __name__ == "__main__":
    main()

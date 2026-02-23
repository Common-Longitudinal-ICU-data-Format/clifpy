# SOFA-2 Performance Profiler

The SOFA-2 profiler tells you **where time is spent** in the `calculate_sofa2()` pipeline. It breaks down wall-clock time per subscore (brain, resp, cv, liver, kidney, hemo, assembly) and further into the 4 internal phases of the CV subscore. This helps identify bottlenecks when scaling to large cohorts (1K+ ICU stays).

## Site Testing Guide

For sites running performance profiling before rollout, use `--site-test` with `--site`:

```bash
python dev/sofa2/sofa2_perf_profiler.py --site-test --site ucmc --config config/config.yaml
```

Replace `ucmc` with your site name — it gets embedded in the report filename.

This automatically:

1. Detects the number of ICU stays and unique hospitalizations in your ADT table

2. Runs **both** generic and daily modes back-to-back (with separate scaling sizes — generic uses total ICU rows, daily uses distinct hospitalization count)

3. For each mode: probes at n=100, estimates remaining sizes, then scales up at powers of 10 (100, 1000, 10000, ...) up to your full dataset

4. Applies a **10-minute timeout** per step — if any step exceeds it, remaining sizes are skipped for that mode

5. Saves a full report to `output/perf/sofa2_profiling_<site>_YYMMDD_HHMMSS.md`

**Share the report file with the CLIF team for cross-site comparison.**

To cap the maximum test size (e.g., only test up to 1000 even if you have more):

```bash
python dev/sofa2/sofa2_perf_profiler.py --site-test --site ucmc --max 1000
```

To run only daily mode with all available data (max stress test):

```bash
python dev/sofa2/sofa2_perf_profiler.py -n max --site ucmc --mode daily --config config/config.yaml
```

## Quick Start

```bash
# One-click site profiling (recommended for sites — runs both modes)
python dev/sofa2/sofa2_perf_profiler.py --site-test --site ucmc

# Max stress test — all data, daily mode
python dev/sofa2/sofa2_perf_profiler.py -n max --mode daily

# Single run, 100 ICU stays, generic mode
python dev/sofa2/sofa2_perf_profiler.py -n 100

# Custom scaling test with specific sizes
python dev/sofa2/sofa2_perf_profiler.py -n 100 1000 5000 --mode daily

# Both modes, single size
python dev/sofa2/sofa2_perf_profiler.py -n 500 --mode both
```

## CLI Reference

| Flag | Default | Description |
|------|---------|-------------|
| `-n N [N ...]` | 100 | Cohort size(s). Use `max` for auto-detected max from ADT |
| `--site-test` | off | Power-of-10 scaling with probe. Defaults `--mode` to `both` |
| `--site NAME` | off | Site name for report filename (e.g., `ucmc`, `mimic`) |
| `--mode MODE` | `generic` | Scoring mode: `generic`, `daily`, or `both` |
| `--max N` | auto-detected | Cap auto-detected max cohort size |
| `--iters N` | 1 | Iterations per cohort size (higher = more stable averages) |
| `--config PATH` | `config/config.yaml` | Path to CLIF config |
| `--cohort-csv PATH` | off | Path to external CSV with `hospitalization_id` column |

### Cohort source

By default, cohort is built by querying the ADT table directly with `WHERE location_category = 'icu'` and `LIMIT {n}`. This requires no external CSV.

**Generic vs daily cohort semantics:**

- **Generic mode**: each ADT row is a separate scoring window (capped at 24h). A single `hospitalization_id` can appear multiple times if it has multiple ICU stays. Auto-detected max = total ICU ADT rows.

- **Daily mode**: one row per unique `hospitalization_id` spanning the full ICU stay duration (the daily function expands this internally into 24h chunks). Auto-detected max = distinct ICU `hospitalization_id` count.

To use a specific reproducible set of IDs (e.g., for comparing across branches), pass `--cohort-csv path/to/ids.csv`. The CSV must have a `hospitalization_id` column.

## Programmatic Usage

The profiler is powered by a `perf_profile` keyword-only parameter on `calculate_sofa2()` and `calculate_sofa2_daily()`. When enabled, it returns extra `StepTimer` objects alongside the result.

### Generic scoring

```python
from clifpy.utils.sofa2 import calculate_sofa2

result, timer, cv_timer = calculate_sofa2(
    cohort_df,
    clif_config_path="config/config.yaml",
    perf_profile=True,
)

# Per-subscore breakdown
print(timer.report())

# CV internal breakdown (4 phases)
print(cv_timer.report())
```

### Daily scoring

```python
from clifpy.utils.sofa2 import calculate_sofa2_daily

result, outer_timer, inner_timer, inner_cv_timer = calculate_sofa2_daily(
    cohort_df,
    clif_config_path="config/config.yaml",
    perf_profile=True,
)

# outer_timer  — per-day-window timing
# inner_timer  — per-subscore timing (last window's calculate_sofa2 call)
# inner_cv_timer — CV internal steps (last window)
```

### StepTimer API

```python
from clifpy.utils.sofa2._perf import StepTimer

timer = StepTimer()

# Context manager style (used in _core.py)
with timer.step("my_step"):
    do_something()

# Inline style (used in _cv.py for flat code)
timer.start("another_step")
do_something_else()
timer.stop("another_step")

# Inspect
timer.results     # list of {'step': str, 'elapsed_s': float}
timer.total       # sum of all elapsed times
timer.report()    # formatted table string
```

## How It Works

### Architecture

```
calculate_sofa2(perf_profile=True)
  │
  ├── timer = StepTimer()                    # subscore-level timing
  │   ├── timer.step("load_tables")
  │   ├── timer.step("brain")
  │   ├── timer.step("resp")
  │   ├── timer.step("cv")  ─────────────── cv_timer = StepTimer()
  │   │                                       ├── cv.map_agg_and_asof
  │   │                                       ├── cv.dedup_and_unit_conversion
  │   │                                       ├── cv.pivot_and_episodes
  │   │                                       └── cv.aggregation_and_scoring
  │   ├── timer.step("liver")
  │   ├── timer.step("kidney")
  │   ├── timer.step("hemo")
  │   └── timer.step("assembly")
  │
  └── return result, timer, cv_timer
```

### Key files

- **`clifpy/utils/sofa2/_perf.py`** — `StepTimer`, `NoOpTimer`, temp table registry

- **`clifpy/utils/sofa2/_core.py`** — wraps each subscore call with `timer.step()`, passes `_timer` to CV

- **`clifpy/utils/sofa2/_cv.py`** — uses `timer.start()`/`timer.stop()` for the 4 CV phases

### Zero overhead when off

When `perf_profile=False` (the default), a `NoOpTimer` is used instead of `StepTimer`. Its `step()` context manager and `start()`/`stop()` methods are empty no-ops, so there is zero overhead in production code.

## Reading the Output

### Visual hierarchy

The profiler uses two levels of box-drawing for clear section nesting:

- **`╔═══╗` double-line boxes**: Top-level mode headers (GENERIC MODE, DAILY MODE, SITE PROFILING COMPLETE)

- **`┌───┐` single-line boxes**: Sub-sections within each mode (n=100 probe, n=1000, Summary)

### Subscore breakdown

```
Step                                Time            % Total
------------------------------------------------------------
load_tables                         0.2s            3.0
brain                               0.1s            1.1
resp                                0.5s            5.8
cv                                  4.1s            52.2
liver                               0.0s            0.6
kidney                              0.1s            0.8
hemo                                0.0s            0.4
assembly                            2.8s            36.0
------------------------------------------------------------
TOTAL                               7.9s
```

Times are shown as seconds (`7.9s`) or minutes-seconds (`1m6s`, `10m24s`).

**What to look for:** which subscore dominates `% Total`. CV is typically the largest due to its multi-step vasopressor pipeline (ASOF joins, unit conversion, episode detection, duration validation).

### CV internal breakdown

```
Step                                Time            % Total
------------------------------------------------------------
cv.map_agg_and_asof                 1.2s            29.9
cv.dedup_and_unit_conversion        0.9s            21.6
cv.pivot_and_episodes               1.5s            35.3
cv.aggregation_and_scoring          0.5s            13.2
------------------------------------------------------------
TOTAL                               4.1s
```

**What to look for:** `cv.pivot_and_episodes` includes forward-fill, episode detection, and 60-min duration validation — this is the most compute-intensive phase.

### Daily mode — expansion ratio

When using `--mode daily`, the profiler shows how many 24-hr windows the input cohort expands into:

```
  Cohort: 100 rows (ADT LIMIT 100)
    Iteration 1: 11.2s, 98.7 MB, 347 output rows
    -> Expanded to 347 24-hr windows (3.5x expansion from 100 input rows)
```

This is useful because a 100-row daily cohort with average 3.5-day stays produces 347 scoring windows — the actual workload is 3.5x what the input row count suggests.

In daily mode, the scaling summary table automatically includes an **Expanded N** column showing the actual number of 24-hr windows scored:

```
  N          Expanded N    Avg Time        Memory (MB)
  -----------------------------------------------------
  100        347           11.2s           98.7
  1000       3412          10m38s          456.2
```

### Scaling analysis

When running multiple sizes (via `-n 100 1000 5000` or `--site-test`), the profiler reports:

- **Scaling efficiency** — `(time_ratio / n_ratio) * 100`:

  - `< 120%` — excellent linear scaling

  - `120-150%` — good with slight overhead

  - `> 150%` — sub-linear, investigate bottlenecks

### Multiple iterations for stability

```bash
python dev/sofa2/sofa2_perf_profiler.py -n 100 1000 --iters 3
```

Running multiple iterations averages out noise from system load, DuckDB JIT warmup, and OS-level caching.

## Report Files

The profiler automatically saves console output to a timestamped `.md` file when there are multiple sizes, `--site-test` is used, or `--site` is provided:

- **`--site-test`**: `output/perf/sofa2_profiling_<site>_YYMMDD_HHMMSS.md`

- **`--site` or multi-size `-n`**: `output/perf/sofa2_scaling_<site>_YYMMDD_HHMMSS.md`

- **Single runs without `--site`**: output goes to stdout only (no file saved)

The `output/` directory is gitignored, so reports are local-only. Share the `.md` file with the CLIF team for cross-site comparison.

Pipeline logs (from `clifpy.utils.logging_config`) are saved separately to `output/logs/clifpy_all.log` and `output/logs/clifpy_errors.log`.

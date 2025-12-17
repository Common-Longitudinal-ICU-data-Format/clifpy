# Performance Benchmark for Medication Unit Converter

This directory contains benchmarking tools for measuring the performance of the `convert_dose_units_for_continuous_meds()` function in the pyCLIF package.

## Quick Start

### Run with full cohort (2000 hospitalizations from CSV)

```bash
cd dev/perf-benchmark
python benchmark_simple.py
```

**Expected runtime:** ~6-10 minutes per iteration with 2000 hospitalizations

### Run with random sample of N hospitalizations

Use the `-n` flag to specify sample size:

```bash
python benchmark_simple.py -n 100  # Random sample of 100 IDs
python benchmark_simple.py -n 10   # Quick test with 10 IDs
```

**Expected runtime:** ~3-5 seconds per iteration with 10 hospitalizations

### Memory Profiling Note

The `-memory` flag is informational only (the script always uses `tracemalloc`). For detailed memory profiling with memray:

```bash
memray run -o profile.bin python benchmark_simple.py -n 100
memray flamegraph profile.bin
```

## Files in This Directory

- **`benchmark_simple.py`** - Main benchmark script with DuckDB cache cleaning
- **`*_results.txt`** - Benchmark output from different runs
- **`*_memory_*.bin`** - Memray memory profiles (binary format)
- **`*_memory_*.html`** - Interactive memory flame graphs
- **`BENCHMARK_COMPARISON_REPORT.md`** - Detailed comparison analysis between versions

## Features

### DuckDB Cache Cleaning

The benchmark cleans DuckDB temporary files before each iteration to ensure:

- No warm cache benefits from previous runs
- Fair comparison between branches
- Consistent "cold cache" performance measurements

### Memory Profiling

The script tracks:

- Peak memory usage via `tracemalloc`
- Detailed allocation patterns via memray (when used)
- Memory usage across iterations

### Flexible Cohort Size

- **Default:** Loads all hospitalization IDs from `.dev/cohort_hosp_ids.csv` (2000 IDs)
- **Random Sampling:** Use `n` parameter to randomly sample N IDs from the cohort
- **Reproducible:** Uses fixed random seed (default: 42) for consistent sampling
- **Custom:** Can pass a custom list of hospitalization IDs

## Performance Expectations

### Small Cohort (10 hospitalizations)

- **First run (cold cache):** ~2-3 seconds
- **Subsequent runs:** ~1-2 seconds
- **Peak memory:** ~5-10 MB

### Medium Cohort (100 hospitalizations)

- **First run (cold cache):** ~15-25 seconds
- **Subsequent runs:** ~10-15 seconds
- **Peak memory:** ~15-30 MB

### Full Cohort (2000 hospitalizations)

- **First run (cold cache):** ~8-12 minutes
- **Subsequent runs:** ~6-10 minutes
- **Peak memory:** ~50-200 MB

*Note: Times are approximate and depend on system performance*

## Running with Memray Memory Profiler

For detailed memory analysis:

```bash
cd dev/perf-benchmark

# Run with memray
memray run -o perf_profile.bin python benchmark_simple.py

# Generate interactive flame graph
memray flamegraph perf_profile.bin

# Open the generated HTML file
open memray-flamegraph-perf_profile.html
```

## Comparing Branches

To compare performance between branches:

1. **Run on current branch:**
   ```bash
   python benchmark_simple.py > current_results.txt
   ```

2. **Switch to comparison branch:**
   ```bash
   git stash
   git checkout main
   ```

3. **Run on comparison branch:**
   ```bash
   python benchmark_simple.py > main_results.txt
   ```

4. **Switch back:**
   ```bash
   git checkout -
   git stash pop
   ```

5. **Compare results manually** or use the provided comparison report

## Understanding the Output

The benchmark reports:

```
============================================================
RESULTS
============================================================
Cohort size: 2000 hospitalizations

First run (cold cache):
  Time:   450.123s
  Memory: 85.3 MB

Across all 5 runs:
  Average time:   350.456s (min: 320.123s, max: 450.123s)
  Average memory: 72.5 MB (min: 65.2 MB, max: 85.3 MB)
============================================================
```

**Key Metrics:**

- **First run:** Cold cache performance (no DuckDB disk spilling benefits)
- **Average:** Typical performance across multiple runs
- **Min/Max:** Performance variance

## Tips for Accurate Benchmarking

1. **Close other applications** to reduce system load
2. **Run multiple times** to account for variance
3. **Start with small cohorts** to validate changes
4. **Scale gradually:** 10 → 100 → 500 → 2000 IDs
5. **Monitor memory** to avoid swapping to disk

## Troubleshooting

### "Config file not found" error

Make sure you're running from the `dev/perf-benchmark/` directory, or the script can find `config/config.yaml` at the project root.

### Very slow first run

This is normal! The first iteration includes:

- Loading parquet files from disk
- DuckDB query compilation
- Python object initialization

Subsequent runs are faster due to in-memory caching (not DuckDB disk spilling, which we clean).

### Out of memory

If benchmarking with 2000+ IDs causes memory issues:

- Reduce the cohort size using the `limit` parameter
- Reduce the number of iterations (change `num_iterations=5` to `num_iterations=3`)
- Close other applications

## Advanced Usage

### Random Sample with Custom Seed

```python
from benchmark_simple import benchmark_convert_dose_units

# Random sample of 50 IDs with custom seed for different sample
results = benchmark_convert_dose_units(n=50, random_seed=123, num_iterations=3)
print(f"Average time: {results['avg_time']:.3f}s")
```

### Custom Cohort

```python
from benchmark_simple import benchmark_convert_dose_units

# Use specific hospitalization IDs
my_ids = ['21674796', '21676306', '22760915']
results = benchmark_convert_dose_units(hospitalization_ids=my_ids, num_iterations=3)
print(f"Average time: {results['avg_time']:.3f}s")
```

### Progressive Scaling Test

```python
from benchmark_simple import benchmark_convert_dose_units

# Test with increasing sample sizes
for n in [10, 50, 100, 500, 1000]:
    print(f"\n=== Testing with n={n} ===")
    results = benchmark_convert_dose_units(n=n, num_iterations=3)
    print(f"Average time: {results['avg_time']:.3f}s")
    print(f"Time per ID: {results['avg_time']/n*1000:.2f}ms")
```

### Integration with CI/CD

```bash
# Run benchmark and fail if regression detected
python benchmark_simple.py > results.txt
if grep -q "Average time.*[5-9][0-9][0-9]\." results.txt; then
    echo "Performance regression detected!"
    exit 1
fi
```

## Related Documentation

- **Main benchmark report:** `BENCHMARK_COMPARISON_REPORT.md`
- **Unit converter code:** `clifpy/utils/unit_converter.py`
- **Orchestrator code:** `clifpy/clif_orchestrator.py`

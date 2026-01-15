"""
SOFA Polars Windows Test Suite
==============================
Comprehensive testing for SOFA polars implementation on Windows.

Test Cases:
- Test1: Cohort size comparison - Memory scaling
- Test2: Full hospitalization SOFA (admission to discharge)
- Test3: Batch processing (configurable batch size)
- Test4: Pandas conversion & timezone verification

Usage:
    python test_sofa_suite.py                                     # Run all tests with defaults
    python test_sofa_suite.py --test 1                            # Run Test1 only
    python test_sofa_suite.py --test 1 --sizes 1000 10000         # Custom sizes
    python test_sofa_suite.py --test 1 --ed-only                  # ED patients only
    python test_sofa_suite.py --test 1 --profile                  # With profiling
"""

import polars as pl
import pandas as pd
import time
import os
import sys
import argparse
import logging
import json
import platform
import gc
from datetime import datetime
from typing import List, Dict, Any, Optional
from pathlib import Path

# ============================================================================
# CONFIGURATION - UPDATE THESE PATHS
# ============================================================================
DATA_DIR = r"G:\Medicine\Pulmonary\Research\Bhavani Lab\CLIF-Data\Post-EPIC\Version-1"
TIMEZONE = "US/Eastern"
LOG_DIR = "./sofa_test_logs"

# DEFAULT TEST CONFIGURATION
DEFAULT_CONFIG = {
    "tests": [1, 2, 3, 4],
    "sizes": [10000, 100000],
    "batch_size": 10000,
    "hours": 24,
    "filter_type": "ed",  # Default to ed patients
    "profile": True        # Default profiling ON
}
# ============================================================================


def setup_logging(log_dir: str, test_name: str = None) -> logging.Logger:
    """Set up logging to both console and file."""
    Path(log_dir).mkdir(parents=True, exist_ok=True)
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    test_name = test_name or "sofa_test"
    log_file = Path(log_dir) / f"{test_name}_{timestamp}.log"
    
    logger = logging.getLogger("sofa_test")
    logger.setLevel(logging.INFO)
    logger.handlers = []
    
    file_handler = logging.FileHandler(log_file, encoding='utf-8')
    file_handler.setLevel(logging.INFO)
    file_format = logging.Formatter('%(asctime)s | %(levelname)s | %(message)s', 
                                     datefmt='%Y-%m-%d %H:%M:%S')
    file_handler.setFormatter(file_format)
    
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.INFO)
    console_format = logging.Formatter('%(message)s')
    console_handler.setFormatter(console_format)
    
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)
    
    logger.info(f"Log file: {log_file}")
    
    return logger, log_file


def get_system_info() -> Dict[str, str]:
    """Get system information for reproducibility."""
    info = {
        "platform": platform.platform(),
        "python_version": platform.python_version(),
        "processor": platform.processor(),
        "machine": platform.machine(),
    }
    
    try:
        import psutil
        info["cpu_count"] = psutil.cpu_count()
        info["total_memory_gb"] = round(psutil.virtual_memory().total / (1024**3), 1)
        info["available_memory_gb"] = round(psutil.virtual_memory().available / (1024**3), 1)
    except ImportError:
        pass
    
    try:
        info["polars_version"] = pl.__version__
    except:
        pass
    
    try:
        import clifpy
        info["clifpy_version"] = getattr(clifpy, '__version__', 'unknown')
    except:
        pass
    
    return info


def get_memory_mb() -> float:
    """Get current memory usage in MB."""
    try:
        import psutil
        return psutil.Process(os.getpid()).memory_info().rss / (1024 * 1024)
    except ImportError:
        return 0


def load_hospitalizations(filter_type: str = "all", logger=None) -> pl.DataFrame:
    """Load hospitalization data with optional filtering."""
    log = logger.info if logger else print
    
    hosp_path = os.path.join(DATA_DIR, "clif_hospitalization.parquet")
    hosp_df = pl.read_parquet(hosp_path)
    log(f"Loaded {len(hosp_df):,} total hospitalizations")
    
    if filter_type in ["icu", "ed"]:
        adt_path = os.path.join(DATA_DIR, "clif_adt.parquet")
        adt_df = pl.read_parquet(adt_path)
        
        filtered_ids = adt_df.filter(
            pl.col("location_category") == filter_type
        ).select("hospitalization_id").unique()
        
        hosp_df = hosp_df.join(filtered_ids, on="hospitalization_id", how="inner")
        log(f"Filtered to {filter_type.upper()} patients: {len(hosp_df):,} hospitalizations")
    
    return hosp_df


# ============================================================================
# TEST 1: Cohort Size Comparison
# ============================================================================
def test1_cohort_size_comparison(
    sizes: List[int],
    filter_type: str,
    profile: bool,
    hours: int,
    logger=None
) -> Dict:
    """Test1: Compare performance across different cohort sizes."""
    log = logger.info if logger else print
    from clifpy import compute_sofa_polars
    
    log("\n" + "="*70)
    log("TEST 1: Cohort Size Comparison (Memory Scaling)")
    log("="*70)
    log(f"Goal:     Identify if memory scales linearly with cohort size")
    log(f"Sizes:    {sizes}")
    log(f"Filter:   {filter_type.upper()}")
    log(f"Window:   {hours}h")
    log(f"Profile:  {'ENABLED' if profile else 'disabled'}")
    
    hosp_df = load_hospitalizations(filter_type=filter_type, logger=logger)
    
    results = []
    
    for n_patients in sizes:
        log(f"\n--- Testing {n_patients:,} patients ---")
        
        if n_patients < len(hosp_df):
            sample_df = hosp_df.sample(n=n_patients, seed=42)
        else:
            sample_df = hosp_df
            n_patients = len(hosp_df)
            log(f"  (Using all {len(hosp_df):,} available)")
        
        cohort = sample_df.select([
            pl.col("hospitalization_id"),
            pl.col("admission_dttm").alias("start_dttm"),
            (pl.col("admission_dttm") + pl.duration(hours=hours)).alias("end_dttm")
        ])
        
        log(f"Cohort: {len(cohort):,} hospitalizations, {hours}h window")
        
        start_mem = get_memory_mb()
        start_time = time.perf_counter()
        
        result = compute_sofa_polars(
            data_directory=DATA_DIR,
            cohort_df=cohort,
            filetype="parquet",
            timezone=TIMEZONE,
            profile=profile
        )
        
        elapsed = time.perf_counter() - start_time
        end_mem = get_memory_mb()
        
        log(f"✓ Completed in {elapsed:.2f}s")
        log(f"  Memory: {start_mem:.0f} → {end_mem:.0f} MB (Δ {end_mem - start_mem:.0f} MB)")
        log(f"  Results: {len(result):,} rows")
        log(f"  Columns: {result.columns}")
        
        results.append({
            "test": "test1_cohort_size",
            "n_patients": n_patients,
            "filter_type": filter_type,
            "time_window_hours": hours,
            "time_seconds": round(elapsed, 2),
            "memory_start_mb": round(start_mem, 0),
            "memory_end_mb": round(end_mem, 0),
            "memory_delta_mb": round(end_mem - start_mem, 0),
            "result_rows": len(result),
            "result_columns": result.columns
        })
    
    # Summary
    log("\n--- Test 1 Summary ---")
    log(f"{'Patients':>12} | {'Time (s)':>10} | {'Mem Δ (MB)':>12} | {'Results':>10}")
    log("-" * 52)
    for r in results:
        log(f"{r['n_patients']:>12,} | {r['time_seconds']:>10.2f} | {r['memory_delta_mb']:>12.0f} | {r['result_rows']:>10,}")
    
    if len(results) >= 2:
        r1, r2 = results[0], results[-1]
        patient_ratio = r2['n_patients'] / r1['n_patients']
        time_ratio = r2['time_seconds'] / max(r1['time_seconds'], 0.01)
        mem_ratio = r2['memory_delta_mb'] / max(r1['memory_delta_mb'], 1)
        
        log("-" * 52)
        log(f"Patient scaling:  {patient_ratio:.1f}x")
        log(f"Time scaling:     {time_ratio:.1f}x")
        log(f"Memory scaling:   {mem_ratio:.1f}x")
        
        if mem_ratio > patient_ratio * 1.5:
            log("⚠ WARNING: Memory scaling is super-linear!")
        else:
            log("✓ Memory scaling looks linear")
    
    return {"test1": results}


# ============================================================================
# TEST 2: Full Hospitalization SOFA
# ============================================================================
def test2_full_hospitalization(
    sizes: List[int],
    filter_type: str,
    profile: bool,
    logger=None
) -> Dict:
    """Test2: Whole hospitalization SOFA (admission to discharge)."""
    log = logger.info if logger else print
    from clifpy import compute_sofa_polars
    
    log("\n" + "="*70)
    log("TEST 2: Full Hospitalization SOFA")
    log("="*70)
    log(f"Goal:    Test SOFA over entire hospitalization")
    log(f"Filter:  {filter_type.upper()}")
    log(f"Profile: {'ENABLED' if profile else 'disabled'}")
    
    hosp_df = load_hospitalizations(filter_type=filter_type, logger=logger)
    
    # Use provided sizes, add full dataset as last test
    test_sizes = sizes.copy()
    if len(hosp_df) not in test_sizes:
        test_sizes.append(len(hosp_df))
    
    log(f"Sizes:   {test_sizes}")
    
    results = []
    
    for n_patients in test_sizes:
        log(f"\n--- Testing {n_patients:,} patients (full stay) ---")
        
        if n_patients < len(hosp_df):
            sample_df = hosp_df.sample(n=n_patients, seed=42)
        else:
            sample_df = hosp_df
            n_patients = len(hosp_df)
        
        cohort = sample_df.select([
            pl.col("hospitalization_id"),
            pl.col("admission_dttm").alias("start_dttm"),
            pl.col("discharge_dttm").alias("end_dttm")
        ]).drop_nulls()
        
        los_hours = sample_df.select(
            ((pl.col("discharge_dttm") - pl.col("admission_dttm")).dt.total_hours())
        ).drop_nulls().mean().item()
        
        log(f"Cohort: {len(cohort):,} hospitalizations")
        log(f"Avg LOS: {los_hours:.1f} hours ({los_hours/24:.1f} days)")
        
        start_mem = get_memory_mb()
        start_time = time.perf_counter()
        
        try:
            result = compute_sofa_polars(
                data_directory=DATA_DIR,
                cohort_df=cohort,
                filetype="parquet",
                timezone=TIMEZONE,
                profile=profile
            )
            
            elapsed = time.perf_counter() - start_time
            end_mem = get_memory_mb()
            
            log(f"✓ Completed in {elapsed:.2f}s")
            log(f"  Memory: {start_mem:.0f} → {end_mem:.0f} MB (Δ {end_mem - start_mem:.0f} MB)")
            log(f"  Results: {len(result):,} rows")
            
            results.append({
                "test": "test2_full_hospitalization",
                "n_patients": n_patients,
                "filter_type": filter_type,
                "avg_los_hours": round(los_hours, 1),
                "time_seconds": round(elapsed, 2),
                "memory_delta_mb": round(end_mem - start_mem, 0),
                "result_rows": len(result),
                "status": "success"
            })
            
        except Exception as e:
            log(f"✗ Failed: {e}")
            import traceback
            log(traceback.format_exc())
            results.append({
                "test": "test2_full_hospitalization",
                "n_patients": n_patients,
                "status": "failed",
                "error": str(e)
            })
    
    return {"test2": results}


# ============================================================================
# TEST 3: Batch Processing
# ============================================================================
def test3_batch_processing(
    batch_size: int,
    filter_type: str,
    profile: bool,
    hours: int,
    logger=None
) -> Dict:
    """Test3: Process in batches and compare memory usage."""
    log = logger.info if logger else print
    from clifpy import compute_sofa_polars
    import gc  # Add this
    
    log("\n" + "="*70)
    log("TEST 3: Batch Processing")
    log("="*70)
    log(f"Goal:       Process full dataset in batches")
    log(f"Batch size: {batch_size:,}")
    log(f"Filter:     {filter_type.upper()}")
    log(f"Window:     {hours}h")
    log(f"Profile:    {'ENABLED' if profile else 'disabled'}")
    
    # Force garbage collection before starting
    gc.collect()
    
    hosp_df = load_hospitalizations(filter_type=filter_type, logger=logger)
    
    full_cohort = hosp_df.select([
        pl.col("hospitalization_id"),
        pl.col("admission_dttm").alias("start_dttm"),
        (pl.col("admission_dttm") + pl.duration(hours=hours)).alias("end_dttm")
    ])
    
    total_patients = len(full_cohort)
    n_batches = (total_patients + batch_size - 1) // batch_size
    
    log(f"Total patients: {total_patients:,}")
    log(f"Number of batches: {n_batches}")
    
    batch_results = []
    all_result_rows = 0  # Don't store all results, just count
    
    start_total_time = time.perf_counter()
    peak_memory = 0
    
    for batch_idx in range(n_batches):
        # Force garbage collection between batches
        gc.collect()
        
        start_idx = batch_idx * batch_size
        end_idx = min(start_idx + batch_size, total_patients)
        
        batch_cohort = full_cohort.slice(start_idx, end_idx - start_idx)
        
        log(f"\n--- Batch {batch_idx + 1}/{n_batches} ({len(batch_cohort):,} patients) ---")
        
        start_mem = get_memory_mb()
        start_time = time.perf_counter()
        
        try:
            result = compute_sofa_polars(
                data_directory=DATA_DIR,
                cohort_df=batch_cohort,
                filetype="parquet",
                timezone=TIMEZONE,
                profile=profile
            )
            
            elapsed = time.perf_counter() - start_time
            end_mem = get_memory_mb()
            peak_memory = max(peak_memory, end_mem)
            
            result_rows = len(result)
            all_result_rows += result_rows
            
            log(f"✓ Batch {batch_idx + 1}: {elapsed:.2f}s, {result_rows:,} rows, {end_mem:.0f} MB")
            
            # Don't keep results in memory - just store count
            del result
            gc.collect()
            
            batch_results.append({
                "batch": batch_idx + 1,
                "n_patients": len(batch_cohort),
                "time_seconds": round(elapsed, 2),
                "memory_mb": round(end_mem, 0),
                "result_rows": result_rows,
                "status": "success"
            })
            
        except Exception as e:
            log(f"✗ Batch {batch_idx + 1} failed: {e}")
            batch_results.append({
                "batch": batch_idx + 1,
                "status": "failed",
                "error": str(e)
            })
            
            # Try to recover memory
            gc.collect()
    
    total_time = time.perf_counter() - start_total_time
    
    log(f"\n--- Batch Processing Summary ---")
    log(f"Total time: {total_time:.2f}s")
    log(f"Peak memory: {peak_memory:.0f} MB")
    log(f"Total results: {all_result_rows:,} rows")
    log(f"Successful batches: {sum(1 for b in batch_results if b.get('status') == 'success')}/{n_batches}")
    log(f"Avg time per batch: {total_time/n_batches:.2f}s")
    
    return {
        "test3": {
            "batch_size": batch_size,
            "filter_type": filter_type,
            "time_window_hours": hours,
            "n_batches": n_batches,
            "total_patients": total_patients,
            "total_time_seconds": round(total_time, 2),
            "peak_memory_mb": round(peak_memory, 0),
            "total_result_rows": all_result_rows,
            "successful_batches": sum(1 for b in batch_results if b.get('status') == 'success'),
            "avg_time_per_batch": round(total_time/n_batches, 2),
            "batches": batch_results
        }
    }

# ============================================================================
# TEST 4: Pandas Conversion & Timezone Verification
# ============================================================================
def test4_pandas_timezone(
    filter_type: str,
    profile: bool,
    logger=None
) -> Dict:
    """Test4: Convert output to pandas and verify timezone."""
    log = logger.info if logger else print
    from clifpy import compute_sofa_polars
    
    log("\n" + "="*70)
    log("TEST 4: Pandas Conversion & Timezone Verification")
    log("="*70)
    log(f"Goal:    Verify timezone matches config: {TIMEZONE}")
    log(f"Filter:  {filter_type.upper()}")
    
    hosp_df = load_hospitalizations(filter_type=filter_type, logger=logger)
    sample_df = hosp_df.sample(n=min(10000, len(hosp_df)), seed=42)
    
    cohort = sample_df.select([
        pl.col("hospitalization_id"),
        pl.col("admission_dttm").alias("start_dttm"),
        (pl.col("admission_dttm") + pl.duration(hours=24)).alias("end_dttm")
    ])
    
    log(f"Test cohort: {len(cohort)} patients")
    
    result_polars = compute_sofa_polars(
        data_directory=DATA_DIR,
        cohort_df=cohort,
        filetype="parquet",
        timezone=TIMEZONE,
        profile=profile
    )
    
    log(f"Polars result: {len(result_polars):,} rows")
    
    # Convert to pandas
    log("\n--- Converting to Pandas ---")
    start_time = time.perf_counter()
    result_pandas = result_polars.to_pandas()
    conversion_time = time.perf_counter() - start_time
    
    log(f"Conversion time: {conversion_time:.3f}s")
    log(f"Pandas shape: {result_pandas.shape}")
    
    # Check timezone
    log("\n--- Timezone Verification ---")
    datetime_cols = [col for col in result_pandas.columns 
                     if 'dttm' in col.lower() or 'time' in col.lower()]
    
    timezone_results = {}
    mismatches = []
    
    for col in datetime_cols:
        if col in result_pandas.columns:
            sample_val = result_pandas[col].dropna().iloc[0] if not result_pandas[col].dropna().empty else None
            
            if sample_val is not None and hasattr(sample_val, 'tzinfo'):
                tz = str(sample_val.tzinfo) if sample_val.tzinfo else "None (naive)"
            else:
                tz = "N/A"
            
            timezone_results[col] = {
                "timezone": tz,
                "sample": str(sample_val)
            }
            
            log(f"  {col}: {tz}")
            
            if tz not in ['None (naive)', 'N/A', 'UTC'] and TIMEZONE not in tz:
                mismatches.append(col)
    
    if not mismatches:
        log("✓ All datetime columns have consistent timezone")
    else:
        log(f"⚠ Timezone mismatches: {mismatches}")
    
    # Data integrity
    log("\n--- Data Integrity ---")
    log(f"Polars rows: {len(result_polars)}")
    log(f"Pandas rows: {len(result_pandas)}")
    log(f"Match: {'✓' if len(result_polars) == len(result_pandas) else '✗'}")
    
    return {
        "test4": {
            "config_timezone": TIMEZONE,
            "filter_type": filter_type,
            "conversion_time_seconds": round(conversion_time, 3),
            "polars_rows": len(result_polars),
            "pandas_rows": len(result_pandas),
            "datetime_columns": timezone_results,
            "timezone_mismatches": mismatches,
            "status": "pass" if not mismatches else "warning"
        }
    }


# ============================================================================
# SAVE RESULTS
# ============================================================================
def save_results(results: Dict, config: Dict, system_info: Dict, 
                 log_dir: str, logger=None):
    """Save results to JSON file."""
    log = logger.info if logger else print
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    results_file = Path(log_dir) / f"sofa_test_results_{timestamp}.json"
    
    output = {
        "test_run": {
            "timestamp": datetime.now().isoformat(),
            "config": config,
            "system_info": system_info
        },
        "results": results
    }
    
    with open(results_file, 'w') as f:
        json.dump(output, f, indent=2, default=str)
    
    log(f"\nResults saved to: {results_file}")
    return results_file


# ============================================================================
# MAIN
# ============================================================================
def main():
    parser = argparse.ArgumentParser(
        description="SOFA Polars Windows Test Suite",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Test Cases:
  1 - Cohort size comparison (memory scaling)
  2 - Full hospitalization SOFA (admission to discharge)
  3 - Batch processing
  4 - Pandas conversion & timezone verification

Examples:
  python test_sofa_suite.py                                   # Run ALL with defaults
  python test_sofa_suite.py --test 1                          # Run Test1 only
  python test_sofa_suite.py --test 1 --sizes 1000 5000 10000  # Custom sizes
  python test_sofa_suite.py --test 1 --ed-only                # ED patients
  python test_sofa_suite.py --test 1 --icu-only               # ICU patients
  python test_sofa_suite.py --test 3 --batch 5000             # Custom batch size
  python test_sofa_suite.py --no-profile                      # Disable profiling

Default Configuration:
  Tests:      All (1, 2, 3, 4)
  Sizes:      1000, 10000
  Batch:      10000
  Filter:     ICU patients
  Profiling:  ENABLED
        """
    )
    parser.add_argument("--test", nargs="+", default=None,
                       help="Test(s) to run: 1, 2, 3, 4 (default: all)")
    parser.add_argument("--sizes", type=int, nargs="+", default=None,
                       help="Patient counts (default: 1000 10000)")
    parser.add_argument("--batch", type=int, default=None,
                       help="Batch size for Test3 (default: 10000)")
    parser.add_argument("--hours", type=int, default=None,
                       help="Time window in hours (default: 24)")
    parser.add_argument("--ed-only", action="store_true",
                       help="Filter to ED patients only")
    parser.add_argument("--icu-only", action="store_true",
                       help="Filter to ICU patients only (default)")
    parser.add_argument("--all-patients", action="store_true",
                       help="Use all patients (no filter)")
    parser.add_argument("--profile", action="store_true", dest="profile", default=None,
                       help="Enable profiling (default: enabled)")
    parser.add_argument("--no-profile", action="store_false", dest="profile",
                       help="Disable profiling")
    parser.add_argument("--data-dir", type=str, default=None,
                       help="Override data directory")
    parser.add_argument("--log-dir", type=str, default=LOG_DIR,
                       help="Directory for log files")
    parser.add_argument("--no-log", action="store_true",
                       help="Disable file logging")
    
    args = parser.parse_args()
    
    # Update data directory
    global DATA_DIR
    if args.data_dir:
        DATA_DIR = args.data_dir
    
    # Apply defaults
    tests_to_run = [int(t) for t in args.test] if args.test else DEFAULT_CONFIG["tests"]
    sizes = args.sizes or DEFAULT_CONFIG["sizes"]
    batch_size = args.batch or DEFAULT_CONFIG["batch_size"]
    hours = args.hours or DEFAULT_CONFIG["hours"]
    profile = args.profile if args.profile is not None else DEFAULT_CONFIG["profile"]
    
    # Determine filter type
    if args.ed_only:
        filter_type = "ed"
    elif args.all_patients:
        filter_type = "all"
    elif args.icu_only:
        filter_type = "icu"
    else:
        filter_type = DEFAULT_CONFIG["filter_type"]
    
    # Setup logging
    if args.no_log:
        logger = None
        log_file = None
        log = print
    else:
        logger, log_file = setup_logging(args.log_dir, "sofa_suite")
        log = logger.info
    
    # Config and system info
    config = {
        "data_dir": DATA_DIR,
        "timezone": TIMEZONE,
        "tests": tests_to_run,
        "sizes": sizes,
        "batch_size": batch_size,
        "hours": hours,
        "filter_type": filter_type,
        "profile": profile
    }
    system_info = get_system_info()
    
    # Header
    log("=" * 70)
    log("SOFA Polars Windows Test Suite")
    log("=" * 70)
    log(f"Timestamp:  {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    log(f"Data:       {DATA_DIR}")
    log(f"Tests:      {tests_to_run}")
    log(f"Sizes:      {sizes}")
    log(f"Batch:      {batch_size:,}")
    log(f"Window:     {hours}h")
    log(f"Filter:     {filter_type.upper()}")
    log(f"Profiling:  {'ENABLED' if profile else 'disabled'}")
    
    log("\n--- System Info ---")
    for key, value in system_info.items():
        log(f"  {key}: {value}")
    
    # Run tests
    all_results = {}
    
    if 1 in tests_to_run:
        all_results.update(test1_cohort_size_comparison(
            sizes=sizes,
            filter_type=filter_type,
            profile=profile,
            hours=hours,
            logger=logger
        ))
        gc.collect()
    
    if 2 in tests_to_run:
        all_results.update(test2_full_hospitalization(
            sizes=sizes,
            filter_type=filter_type,
            profile=profile,
            logger=logger
        ))
        gc.collect()
    
    if 3 in tests_to_run:
        all_results.update(test3_batch_processing(
            batch_size=batch_size,
            filter_type=filter_type,
            profile=profile,
            hours=hours,
            logger=logger
        ))
        gc.collect()
    
    if 4 in tests_to_run:
        all_results.update(test4_pandas_timezone(
            filter_type=filter_type,
            profile=profile,
            logger=logger
        ))
        gc.collect()
    
    # Save results
    if not args.no_log:
        save_results(all_results, config, system_info, args.log_dir, logger=logger)
    
    # Final summary
    log("\n" + "=" * 70)
    log("TEST SUITE COMPLETED")
    log("=" * 70)
    if log_file:
        log(f"Full log: {log_file}")


if __name__ == "__main__":
    main()
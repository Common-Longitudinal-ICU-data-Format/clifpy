"""
DuckDB-Optimized Continuous SOFA Calculator 

This module provides a standalone, highly optimized implementation of SOFA
(Sequential Organ Failure Assessment) score calculation using Polars for
maximum performance. It loads raw data files directly and performs all
computations including unit conversion without relying on other clifpy methods.
"""

import polars as pl
import pandas as pd
import duckdb
from typing import Optional, Tuple, Union, Dict, List
from pathlib import Path
import logging
from datetime import datetime, timedelta
import gc

# =============================================================================
# CONSTANTS
# =============================================================================

# Unit conversion patterns for medication doses
UNIT_NAMING_VARIANTS = {
    '/hr': r'/h(r|our)?$',
    '/min': r'/m(in|inute)?$',
    'u': r'u(nits|nit)?',
    'm': r'milli-?',
    "l": r'l(iters|itres|itre|iter)?',
    'mcg': r'^(u|µ|μ)g',
    'g': r'^g(rams|ram)?',
}

# Required data categories
REQUIRED_LABS = ['creatinine', 'platelet_count', 'po2_arterial', 'bilirubin_total']
REQUIRED_VITALS = ['map', 'spo2', 'weight_kg']
REQUIRED_ASSESSMENTS = ['gcs_total']
REQUIRED_MEDS = ['norepinephrine', 'epinephrine', 'dopamine', 'dobutamine']

# Device ranking for respiratory SOFA (lower rank = worse)
DEVICE_RANK_DICT = {
    'IMV': 1,
    'NIPPV': 2,
    'CPAP': 3,
    'High Flow NC': 4,
    'Face Mask': 5,
    'Trach Collar': 6,
    'Nasal Cannula': 7,
    'Other': 8,
    'Room Air': 9
}

# Physiologic outlier bounds
OUTLIER_BOUNDS = {
    'po2_arterial': (0, 700),      # mmHg
    'spo2': (50, 100),              # %
    'fio2_set': (0.21, 1.0),        # fraction
    'creatinine': (0, 30),          # mg/dL
    'bilirubin_total': (0, 50),     # mg/dL
    'platelet_count': (0, 2000),    # thousands/µL
    'map': (0, 200),                # mmHg
    'gcs_total': (3, 15),           # score
}

logger = logging.getLogger(__name__)


# =============================================================================
# TIMEZONE HELPER FUNCTIONS
# =============================================================================

def _ensure_timezone_pandas(df: pd.DataFrame, dt_col: str, tz: str) -> pd.DataFrame:
    """Ensure datetime column in pandas DataFrame has proper timezone."""
    if dt_col not in df.columns:
        return df
    
    if pd.api.types.is_datetime64_any_dtype(df[dt_col]):
        if df[dt_col].dt.tz is None:
            df[dt_col] = df[dt_col].dt.tz_localize(tz)
        elif str(df[dt_col].dt.tz) != tz:
            df[dt_col] = df[dt_col].dt.tz_convert(tz)
    return df


def _ensure_timezone_polars(df: pl.DataFrame, dt_col: str, tz: str) -> pl.DataFrame:
    """Ensure datetime column in Polars DataFrame has proper timezone."""
    if dt_col not in df.columns:
        return df
    
    if df[dt_col].dtype in [pl.Datetime, pl.Datetime('ms'), pl.Datetime('us'), pl.Datetime('ns')]:
        if df[dt_col].dtype.time_zone is None:
            df = df.with_columns([
                pl.col(dt_col).dt.replace_time_zone(tz).alias(dt_col)
            ])
        elif df[dt_col].dtype.time_zone != tz:
            df = df.with_columns([
                pl.col(dt_col).dt.convert_time_zone(tz).alias(dt_col)
            ])
    
    return df


def _get_timezone_conversion_sql(
    con: duckdb.DuckDBPyConnection,
    temp_table: str,
    dt_col: str,
    local_tz: str
) -> str:
    """
    Get proper SQL for timezone conversion based on actual data type.
    
    Strategy:
    1. Check if column has timezone info (TIMESTAMP vs TIMESTAMPTZ)
    2. If naive: Assume data is already in local_tz, just add timezone
    3. If aware: Convert to local_tz
    """
    try:
        dtype_query = f"SELECT typeof({dt_col}) as dt_type FROM {temp_table} LIMIT 1"
        result = con.execute(dtype_query).fetchone()
        
        if result is None:
            logger.warning(f"  ⚠️  No data to check timezone for {dt_col}")
            return dt_col
        
        dtype = result[0]
        logger.info(f"  ℹ️  Detected type for {dt_col}: {dtype}")
        
        if 'TIME ZONE' in dtype or 'TIMESTAMPTZ' in dtype:
            logger.info(f"  ✓ Converting timezone-aware column to {local_tz}")
            return f"timezone('{local_tz}', {dt_col})"
        else:
            logger.info(f"  ✓ Treating naive timestamp as {local_tz} and adding timezone")
            return f"{dt_col} AT TIME ZONE '{local_tz}'"
            
    except Exception as e:
        logger.warning(f"  ⚠️  Error checking timezone for {dt_col}: {e}")
        logger.warning(f"  ⚠️  Falling back to naive timestamp assumption")
        return f"{dt_col} AT TIME ZONE '{local_tz}'"


# =============================================================================
# TIME BUCKET CREATION
# =============================================================================

def create_continuous_time_buckets(
    cohort_df: pd.DataFrame,
    time_bucket_hours: float = 8.0,
    bucket_id_col: str = 'hospitalization_id',
    time_window_cols: Tuple[str, str] = ('start_dttm', 'end_dttm'),
    include_partial_buckets: bool = True
) -> pd.DataFrame:
    """
    Create continuous time buckets within time windows.
    
    Parameters
    ----------
    cohort_df : pd.DataFrame
        Cohort with time window columns
    time_bucket_hours : float
        Duration of each time bucket in hours
    bucket_id_col : str
        Column name to group by when creating buckets
    time_window_cols : Tuple[str, str]
        Column names for time window boundaries
    include_partial_buckets : bool
        If True, include final bucket even if shorter than time_bucket_hours
        
    Returns
    -------
    pd.DataFrame
        Expanded dataframe with one row per time bucket
    """
    start_col, end_col = time_window_cols
    
    if start_col not in cohort_df.columns or end_col not in cohort_df.columns:
        raise ValueError(f"Time window columns {time_window_cols} not found")
    
    if bucket_id_col not in cohort_df.columns:
        raise ValueError(f"bucket_id_col '{bucket_id_col}' not found in cohort")
    
    original_tz = None
    if pd.api.types.is_datetime64_any_dtype(cohort_df[start_col]):
        if hasattr(cohort_df[start_col].dtype, 'tz') and cohort_df[start_col].dtype.tz:
            original_tz = str(cohort_df[start_col].dtype.tz)
    
    logger.info(f"📅 Creating time buckets ({time_bucket_hours}h intervals, grouped by {bucket_id_col})...")
    
    bucket_timedelta = pd.Timedelta(hours=time_bucket_hours)
    expanded_rows = []
    
    for idx, row in cohort_df.iterrows():
        start_time = row[start_col]
        end_time = row[end_col]
        
        if pd.isna(start_time) or pd.isna(end_time):
            logger.warning(f"⚠️  Row {idx}: Missing time window, skipping")
            continue
            
        if start_time >= end_time:
            logger.warning(f"⚠️  Row {idx}: Invalid time window, skipping")
            continue
        
        current_start = start_time
        bucket_num = 1
        
        while current_start < end_time:
            bucket_end = min(current_start + bucket_timedelta, end_time)
            bucket_duration = (bucket_end - current_start).total_seconds() / 3600
            
            if not include_partial_buckets and bucket_duration < time_bucket_hours:
                break
            
            bucket_row = row.to_dict()
            bucket_row['bucket_start_dttm'] = current_start
            bucket_row['bucket_end_dttm'] = bucket_end
            bucket_row['bucket_number'] = bucket_num
            bucket_row['bucket_duration_hours'] = bucket_duration
            
            expanded_rows.append(bucket_row)
            
            current_start = bucket_end
            bucket_num += 1
    
    if not expanded_rows:
        logger.warning("⚠️  No valid time buckets created")
        return pd.DataFrame()
    
    result_df = pd.DataFrame(expanded_rows)
    
    if original_tz:
        for col in ['bucket_start_dttm', 'bucket_end_dttm']:
            if col in result_df.columns and result_df[col].dt.tz is None:
                result_df[col] = result_df[col].dt.tz_localize(original_tz)
    
    logger.info(f"  ✓ Created {len(result_df):,} buckets from {len(cohort_df):,} {bucket_id_col} groups")
    
    return result_df


# =============================================================================
# UNIT CONVERSION HELPER
# =============================================================================

def _clean_dose_unit_polars(unit_col: pl.Expr) -> pl.Expr:
    """Clean and standardize medication dose unit strings."""
    cleaned = unit_col.str.replace_all(r'\s+', '').str.to_lowercase()
    
    for replacement, pattern in UNIT_NAMING_VARIANTS.items():
        cleaned = cleaned.str.replace_all(pattern, replacement)
    
    return cleaned

# =============================================================================
# OUTLIER REMOVAL HELPER
# =============================================================================
def _apply_outlier_filter(
    view_name: str,
    value_column: str,
    category_column: Optional[str],
    bounds_dict: Dict[str, Tuple[float, float]],
    con: duckdb.DuckDBPyConnection
) -> None:
    """
    Apply outlier filters to excludes physiologically implausible values based on OUTLIER_BOUNDS.
    """

    # Snapshot table to prevent recursion issues
    snapshot_table = f"{view_name}_snapshot"

    # Create snapshot only once
    try:
        con.execute(f"SELECT 1 FROM {snapshot_table} LIMIT 1")
        snapshot_exists = True
    except duckdb.CatalogException:
        snapshot_exists = False

    if not snapshot_exists:
        con.execute(f"""
            CREATE TABLE {snapshot_table} AS
            SELECT * FROM {view_name}
        """)
        logger.info(f"📌 Created snapshot table: {snapshot_table}")

    # Build filtering conditions
    if category_column is None:

        # Single global bound
        if len(bounds_dict) != 1:
            raise ValueError("bounds_dict must contain exactly 1 entry when category_column is None.")

        category, (min_val, max_val) = next(iter(bounds_dict.items()))

        filter_sql = f"""
        CREATE OR REPLACE VIEW {view_name} AS
        SELECT *
        FROM {snapshot_table}
        WHERE {value_column} BETWEEN {min_val} AND {max_val}
        """

    else:
        # Category-specific filters
        conditions = []

        for cat, (min_val, max_val) in bounds_dict.items():
            conditions.append(
                f"({category_column} = '{cat}' AND {value_column} BETWEEN {min_val} AND {max_val})"
            )

        # If category not in bounds_dict → keep unchanged
        categories = "', '".join(bounds_dict.keys())
        conditions.append(f"({category_column} NOT IN ('{categories}'))")

        where_clause = " OR ".join(conditions)

        filter_sql = f"""
        CREATE OR REPLACE VIEW {view_name} AS
        SELECT *
        FROM {snapshot_table}
        WHERE {where_clause}
        """

    # Apply filter
    con.execute(filter_sql)
    logger.info(f"✨ Applied outlier filter to {view_name}")



# =============================================================================
# DATA LOADING FUNCTIONS (WITH FILTERING AND TIMEZONE CHECKING)
# =============================================================================

def _load_labs(
    con: duckdb.DuckDBPyConnection,
    data_dir: Path,
    hospitalization_ids: Optional[List[str]] = None,
    local_tz: Optional[str] = None,
    filetype: str = 'parquet',
    remove_outliers: bool = True
) -> str:
    """Load labs data with filtering and timezone checking."""
    labs_path = data_dir / f"clif_labs.{filetype}"
    if not labs_path.exists():
        logger.warning(f"Labs file not found: {labs_path}")
        return None
    
    logger.info("📥 Loading labs data...")
    
    where_clauses = [f"lab_category IN {tuple(REQUIRED_LABS)}"]
    if hospitalization_ids:
        ids_str = "', '".join(str(id) for id in hospitalization_ids)
        where_clauses.append(f"hospitalization_id IN ('{ids_str}')")
    
    where_sql = " AND ".join(where_clauses)
    
    temp_query = f"""
        CREATE OR REPLACE VIEW labs_raw_temp AS
        SELECT 
            hospitalization_id,
            lab_result_dttm,
            lab_category,
            lab_value_numeric
        FROM read_parquet('{labs_path}')
        WHERE {where_sql}
    """
    con.execute(temp_query)
    
    if local_tz:
        tz_conversion = _get_timezone_conversion_sql(con, 'labs_raw_temp', 'lab_result_dttm', local_tz)
        
        final_query = f"""
            CREATE OR REPLACE VIEW labs_raw AS
            SELECT 
                hospitalization_id,
                {tz_conversion} as lab_result_dttm,
                lab_category,
                lab_value_numeric
            FROM labs_raw_temp
        """
        con.execute(final_query)
    else:
        con.execute("CREATE OR REPLACE VIEW labs_raw AS SELECT * FROM labs_raw_temp")
    
    # Apply outlier filters if requested
    if remove_outliers:
        lab_bounds = {
            'po2_arterial': OUTLIER_BOUNDS['po2_arterial'],
            'creatinine': OUTLIER_BOUNDS['creatinine'],
            'bilirubin_total': OUTLIER_BOUNDS['bilirubin_total'],
            'platelet_count': OUTLIER_BOUNDS['platelet_count']
        }
        _apply_outlier_filter('labs_raw', 'lab_value_numeric', 'lab_category', lab_bounds, con)
        logger.info("  ✓ Applied outlier filters to labs")
    
    if hospitalization_ids:
        logger.info(f"  ✓ Filtered to {len(hospitalization_ids)} hospitalizations")
    
    return 'labs_raw'


def _load_vitals(
    con: duckdb.DuckDBPyConnection,
    data_dir: Path,
    hospitalization_ids: Optional[List[str]] = None,
    local_tz: Optional[str] = None,
    filetype: str = 'parquet',
    remove_outliers: bool = True
) -> str:
    """Load vitals data with filtering and timezone checking."""
    vitals_path = data_dir / f"clif_vitals.{filetype}"
    if not vitals_path.exists():
        logger.warning(f"Vitals file not found: {vitals_path}")
        return None
    
    logger.info("📥 Loading vitals data...")
    
    where_clauses = [f"vital_category IN {tuple(REQUIRED_VITALS)}"]
    if hospitalization_ids:
        ids_str = "', '".join(str(id) for id in hospitalization_ids)
        where_clauses.append(f"hospitalization_id IN ('{ids_str}')")
    
    where_sql = " AND ".join(where_clauses)
    
    temp_query = f"""
        CREATE OR REPLACE VIEW vitals_raw_temp AS
        SELECT 
            hospitalization_id,
            recorded_dttm,
            vital_category,
            vital_value
        FROM read_parquet('{vitals_path}')
        WHERE {where_sql}
    """
    con.execute(temp_query)
    
    if local_tz:
        tz_conversion = _get_timezone_conversion_sql(con, 'vitals_raw_temp', 'recorded_dttm', local_tz)
        
        final_query = f"""
            CREATE OR REPLACE VIEW vitals_raw AS
            SELECT 
                hospitalization_id,
                {tz_conversion} as recorded_dttm,
                vital_category,
                vital_value
            FROM vitals_raw_temp
        """
        con.execute(final_query)
    else:
        con.execute("CREATE OR REPLACE VIEW vitals_raw AS SELECT * FROM vitals_raw_temp")
    
    # Apply outlier filters if requested
    if remove_outliers:
        vital_bounds = {
            'map': OUTLIER_BOUNDS['map'],
            'spo2': OUTLIER_BOUNDS['spo2']
        }
        _apply_outlier_filter('vitals_raw', 'vital_value', 'vital_category', vital_bounds, con)
        logger.info("  ✓ Applied outlier filters to vitals")
    
    if hospitalization_ids:
        logger.info(f"  ✓ Filtered to {len(hospitalization_ids)} hospitalizations")
    
    return 'vitals_raw'


def _load_assessments(
    con: duckdb.DuckDBPyConnection,
    data_dir: Path,
    hospitalization_ids: Optional[List[str]] = None,
    local_tz: Optional[str] = None,
    filetype: str = 'parquet',
    remove_outliers: bool = True
) -> str:
    """Load patient assessments with filtering and timezone checking."""
    assess_path = data_dir / f"clif_patient_assessments.{filetype}"
    if not assess_path.exists():
        logger.warning(f"Assessments file not found: {assess_path}")
        return None
    
    logger.info("📥 Loading GCS assessments...")
    
    where_clauses = [f"assessment_category IN {tuple(REQUIRED_ASSESSMENTS)}"]
    if hospitalization_ids:
        ids_str = "', '".join(str(id) for id in hospitalization_ids)
        where_clauses.append(f"hospitalization_id IN ('{ids_str}')")
    
    where_sql = " AND ".join(where_clauses)
    
    temp_query = f"""
        CREATE OR REPLACE VIEW assessments_raw_temp AS
        SELECT 
            hospitalization_id,
            recorded_dttm,
            assessment_category,
            numerical_value
        FROM read_parquet('{assess_path}')
        WHERE {where_sql}
    """
    con.execute(temp_query)
    
    if local_tz:
        tz_conversion = _get_timezone_conversion_sql(con, 'assessments_raw_temp', 'recorded_dttm', local_tz)
        
        final_query = f"""
            CREATE OR REPLACE VIEW assessments_raw AS
            SELECT 
                hospitalization_id,
                {tz_conversion} as recorded_dttm,
                assessment_category,
                numerical_value
            FROM assessments_raw_temp
        """
        con.execute(final_query)
    else:
        con.execute("CREATE OR REPLACE VIEW assessments_raw AS SELECT * FROM assessments_raw_temp")
    
    # Apply outlier filters if requested
    if remove_outliers:
        assessments_bounds = {
            'gcs_total': OUTLIER_BOUNDS['gcs_total']
        }
        _apply_outlier_filter('assessments_raw', 'numerical_value', 'assessment_category', assessments_bounds, con)
        logger.info("  ✓ Applied outlier filters to patient_assessments")
    
    if hospitalization_ids:
        logger.info(f"  ✓ Filtered to {len(hospitalization_ids)} hospitalizations")
    
    return 'assessments_raw'


def _load_respiratory_support(
    con: duckdb.DuckDBPyConnection,
    data_dir: Path,
    hospitalization_ids: Optional[List[str]] = None,
    local_tz: Optional[str] = None,
    filetype: str = 'parquet'
) -> str:
    """Load respiratory support with filtering and timezone checking."""
    resp_path = data_dir / f"clif_respiratory_support.{filetype}"
    if not resp_path.exists():
        logger.warning(f"Respiratory support file not found: {resp_path}")
        return None
    
    logger.info("📥 Loading respiratory support...")
    
    where_clauses = []
    if hospitalization_ids:
        ids_str = "', '".join(str(id) for id in hospitalization_ids)
        where_clauses.append(f"hospitalization_id IN ('{ids_str}')")
    
    where_sql = f"WHERE {' AND '.join(where_clauses)}" if where_clauses else ""
    
    temp_query = f"""
        CREATE OR REPLACE VIEW resp_support_raw_temp AS
        SELECT 
            hospitalization_id,
            recorded_dttm,
            device_category,
            mode_category,
            fio2_set,
            lpm_set
        FROM read_parquet('{resp_path}')
        {where_sql}
    """
    con.execute(temp_query)
    
    if local_tz:
        tz_conversion = _get_timezone_conversion_sql(con, 'resp_support_raw_temp', 'recorded_dttm', local_tz)
        
        final_query = f"""
            CREATE OR REPLACE VIEW resp_support_raw AS
            SELECT 
                hospitalization_id,
                {tz_conversion} as recorded_dttm,
                device_category,
                mode_category,
                fio2_set,
                lpm_set
            FROM resp_support_raw_temp
        """
        con.execute(final_query)
    else:
        con.execute("CREATE OR REPLACE VIEW resp_support_raw AS SELECT * FROM resp_support_raw_temp")
    
    if hospitalization_ids:
        logger.info(f"  ✓ Filtered to {len(hospitalization_ids)} hospitalizations")
    
    return 'resp_support_raw'


def _load_medications(
    con: duckdb.DuckDBPyConnection,
    data_dir: Path,
    hospitalization_ids: Optional[List[str]] = None,
    local_tz: Optional[str] = None,
    filetype: str = 'parquet'
) -> str:
    """Load medications with filtering and timezone checking."""
    meds_path = data_dir / f"clif_medication_admin_continuous.{filetype}"
    if not meds_path.exists():
        logger.warning(f"Medications file not found: {meds_path}")
        return None
    
    logger.info("📥 Loading vasopressor medications...")
    
    where_clauses = [f"med_category IN {tuple(REQUIRED_MEDS)}"]
    if hospitalization_ids:
        ids_str = "', '".join(str(id) for id in hospitalization_ids)
        where_clauses.append(f"hospitalization_id IN ('{ids_str}')")
    
    where_sql = " AND ".join(where_clauses)
    
    temp_query = f"""
        CREATE OR REPLACE VIEW medications_raw_temp AS
        SELECT 
            hospitalization_id,
            admin_dttm,
            med_category,
            med_dose,
            med_dose_unit
        FROM read_parquet('{meds_path}')
        WHERE {where_sql}
    """
    con.execute(temp_query)
    
    if local_tz:
        tz_conversion = _get_timezone_conversion_sql(con, 'medications_raw_temp', 'admin_dttm', local_tz)
        
        final_query = f"""
            CREATE OR REPLACE VIEW medications_raw AS
            SELECT 
                hospitalization_id,
                {tz_conversion} as admin_dttm,
                med_category,
                med_dose,
                med_dose_unit
            FROM medications_raw_temp
        """
        con.execute(final_query)
    else:
        con.execute("CREATE OR REPLACE VIEW medications_raw AS SELECT * FROM medications_raw_temp")
    
    if hospitalization_ids:
        logger.info(f"  ✓ Filtered to {len(hospitalization_ids)} hospitalizations")
    
    return 'medications_raw'


# =============================================================================
# RESPIRATORY EPISODE CREATION (WITH ALL HEURISTICS)
# =============================================================================

def _create_resp_support_episodes(
    resp_df: pl.DataFrame,
    id_col: str = 'hospitalization_id'
) -> pl.DataFrame:
    """
    Create respiratory support episode IDs for waterfall forward-filling.
    
    Implements waterfall heuristics including:
    - IMV detection from mode_category patterns (assist control, SIMV, pressure control)
    - NIPPV detection from mode_category patterns (pressure support, not CPAP)
    - Room air FiO2 defaults (0.21)
    - FiO2 imputation from nasal cannula LPM (1L→24%, 2L→28%, ..., 10L→60%)
    - Hierarchical episode tracking (device_cat_id, mode_cat_id)
    
    Parameters
    ----------
    resp_df : pl.DataFrame
        Respiratory support data with device_category, mode_category, lpm_set, recorded_dttm
    id_col : str
        Patient identifier column
        
    Returns
    -------
    pl.DataFrame
        Respiratory data with episode IDs added (device_cat_id, mode_cat_id)
    """
    if len(resp_df) == 0:
        return resp_df
    
    # Sort by patient and time
    resp_df = resp_df.sort([id_col, 'recorded_dttm'])
    
    # === HEURISTIC 1: IMV detection from mode_category ===
    # Fill in missing device_category if mode_category suggests IMV
    # Patterns: assist control-volume control, SIMV, pressure control
    resp_df = resp_df.with_columns([
        pl.when(
            pl.col('device_category').is_null() &
            pl.col('mode_category').is_not_null() &
            pl.col('mode_category').str.to_lowercase().str.contains(
                r"(?:assist control-volume control|simv|pressure control)"
            )
        )
        .then(pl.lit('IMV'))
        .otherwise(pl.col('device_category'))
        .alias('device_category')
    ])
    
    # === HEURISTIC 2: NIPPV detection from mode_category ===
    # Pattern: pressure support (but not CPAP)
    resp_df = resp_df.with_columns([
        pl.when(
            pl.col('device_category').is_null() &
            pl.col('mode_category').is_not_null() &
            pl.col('mode_category').str.to_lowercase().str.contains(r"pressure support") &
            ~pl.col('mode_category').str.to_lowercase().str.contains(r"cpap")
        )
        .then(pl.lit('NIPPV'))
        .otherwise(pl.col('device_category'))
        .alias('device_category')
    ])
    
    # === HEURISTIC 3: Room air FiO2 default ===
    # Set FiO2 = 0.21 for room air when missing
    resp_df = resp_df.with_columns([
        pl.when(
            (pl.col('device_category').str.to_lowercase() == 'room air') &
            pl.col('fio2_set').is_null()
        )
        .then(pl.lit(0.21))
        .otherwise(pl.col('fio2_set'))
        .alias('fio2_set')
    ])
    
    # === HEURISTIC 4: FiO2 imputation from nasal cannula flow ===
    # Impute FiO2 based on LPM for nasal cannula using clinical conversion table
    # Standard conversion: 1L → 24%, 2L → 28%, 3L → 32%, 4L → 36%, 5L → 40%,
    #                      6L → 44%, 7L → 48%, 8L → 52%, 9L → 56%, 10L → 60%
    
    if 'lpm_set' in resp_df.columns:
        resp_df = resp_df.with_columns([
            pl.col('lpm_set').round(0).cast(pl.Int32).alias('_lpm_rounded')
        ])
        
        fio2_from_lpm = (
            pl.when(pl.col('_lpm_rounded') == 1).then(pl.lit(0.24))
            .when(pl.col('_lpm_rounded') == 2).then(pl.lit(0.28))
            .when(pl.col('_lpm_rounded') == 3).then(pl.lit(0.32))
            .when(pl.col('_lpm_rounded') == 4).then(pl.lit(0.36))
            .when(pl.col('_lpm_rounded') == 5).then(pl.lit(0.40))
            .when(pl.col('_lpm_rounded') == 6).then(pl.lit(0.44))
            .when(pl.col('_lpm_rounded') == 7).then(pl.lit(0.48))
            .when(pl.col('_lpm_rounded') == 8).then(pl.lit(0.52))
            .when(pl.col('_lpm_rounded') == 9).then(pl.lit(0.56))
            .when(pl.col('_lpm_rounded') == 10).then(pl.lit(0.60))
            .otherwise(None)
        )
        
        resp_df = resp_df.with_columns([
            pl.when(
                (pl.col('device_category').str.to_lowercase() == 'nasal cannula') &
                pl.col('fio2_set').is_null() &
                pl.col('lpm_set').is_not_null() &
                (pl.col('_lpm_rounded') >= 1) &
                (pl.col('_lpm_rounded') <= 10)
            )
            .then(fio2_from_lpm)
            .otherwise(pl.col('fio2_set'))
            .alias('fio2_set')
        ])
        
        nasal_cannula_imputed = (
            (resp_df['device_category'].str.to_lowercase() == 'nasal cannula') &
            (resp_df['fio2_set'].is_not_null()) &
            (resp_df['_lpm_rounded'].is_not_null()) &
            (resp_df['_lpm_rounded'] >= 1) &
            (resp_df['_lpm_rounded'] <= 10)
        ).sum()
        
        if nasal_cannula_imputed > 0:
            logger.info(f"  ℹ️  Imputed FiO2 for {nasal_cannula_imputed:,} nasal cannula rows using LPM")
        
        resp_df = resp_df.drop('_lpm_rounded')
    
    # === Forward-fill device_category and mode_category ===
    resp_df = resp_df.with_columns([
        pl.col('device_category').forward_fill().over(id_col).alias('device_category'),
        pl.col('mode_category').forward_fill().over(id_col).alias('mode_category')
    ])
    
    # === Create hierarchical episode IDs ===
    
    # Level 1: device_cat_id - changes when device_category changes
    resp_df = resp_df.with_columns([
        pl.when(
            (pl.col('device_category') != pl.col('device_category').shift(1).over(id_col)) |
            (pl.col(id_col) != pl.col(id_col).shift(1))
        )
        .then(1)
        .otherwise(0)
        .alias('_device_cat_change')
    ])
    
    resp_df = resp_df.with_columns([
        pl.col('_device_cat_change').cum_sum().over(id_col).alias('device_cat_id')
    ])
    
    # Level 2: mode_cat_id - changes when mode_category changes (nested within device_cat_id)
    resp_df = resp_df.with_columns([
        pl.when(
            (pl.col('mode_category') != pl.col('mode_category').shift(1).over(id_col)) |
            (pl.col('device_cat_id') != pl.col('device_cat_id').shift(1).over(id_col)) |
            (pl.col(id_col) != pl.col(id_col).shift(1))
        )
        .then(1)
        .otherwise(0)
        .alias('_mode_cat_change')
    ])
    
    resp_df = resp_df.with_columns([
        pl.col('_mode_cat_change').cum_sum().over(id_col).alias('mode_cat_id')
    ])
    
    # Clean up temporary columns
    resp_df = resp_df.drop(['_device_cat_change', '_mode_cat_change'])
    
    return resp_df


def _process_respiratory_with_episodes(
    con: duckdb.DuckDBPyConnection,
    local_tz: Optional[str] = None,
    lookback_hours: int = 24
) -> pl.DataFrame:
    """
    Process respiratory support data with episode creation and forward-filling.
    
    Steps:
    1. Load cohort bucket windows from DuckDB
    2. Add a lookback start time for each bucket (bucket_start - lookback_hours)
    3. Use the expanded cohort window to load respiratory data
       (includes earlier data needed for forward-filling)
    4. Convert the data to Polars once and apply timezone adjustments
    5. Create respiratory episodes (device/mode transitions)
    6. Forward-fill FiO2 values within episodes (mode_cat_id-level)
    7. Filter data back to the original bucket window
    8. Add device rank and return the final Polars DataFrame
    
    Parameters
    ----------
    con : duckdb.DuckDBPyConnection
        Active DuckDB connection
    local_tz : str, optional
        Local timezone
    lookback_hours : int
        Hours to look back before time bucket for forward-filling
        
    Returns
    -------
    pl.DataFrame
        Processed respiratory data (in Polars format)
    """
    logger.info("💨 Processing respiratory episodes (with lookback)...")
    
    # Get cohort time windows for lookback calculation
    cohort = con.execute("SELECT hospitalization_id, bucket_start_dttm, bucket_end_dttm FROM cohort").df()
    
    if len(cohort) == 0:
        logger.warning("  ⚠️  No cohort data")
        return pl.DataFrame(schema={
            'hospitalization_id': pl.Utf8,
            'recorded_dttm': pl.Datetime,
            'device_category': pl.Utf8,
            'device_rank': pl.Int64,
            'fio2_set': pl.Float64
        })
    
    # Calculate lookback period for each bucket
    cohort['start_dttm_lookback'] = cohort['bucket_start_dttm'] - pd.Timedelta(hours=lookback_hours)
    
    logger.info(f"  ℹ️  Loading data with {lookback_hours}h lookback for forward-filling...")
    
    # Register expanded cohort with lookback
    con.register('cohort_expanded', cohort)
    
    # Load respiratory data WITH LOOKBACK PERIOD
    resp_with_lookback_pd = con.execute("""
    SELECT 
        r.hospitalization_id,
        r.recorded_dttm,
        r.device_category,
        r.mode_category,
        r.fio2_set,
        r.lpm_set,
        c.bucket_start_dttm,
        c.bucket_end_dttm
    FROM resp_support_raw r
    INNER JOIN cohort_expanded c
        ON r.hospitalization_id = c.hospitalization_id
        AND r.recorded_dttm >= c.start_dttm_lookback
        AND r.recorded_dttm <= c.bucket_end_dttm
    """).df()
    
    if len(resp_with_lookback_pd) == 0:
        logger.warning("  ⚠️  No respiratory data in lookback period")
        return pl.DataFrame(schema={
            'hospitalization_id': pl.Utf8,
            'recorded_dttm': pl.Datetime,
            'device_category': pl.Utf8,
            'device_rank': pl.Int64,
            'fio2_set': pl.Float64
        })
    
    # Convert to Polars ONCE
    resp_pl = pl.from_pandas(resp_with_lookback_pd)
    
    if local_tz:
        resp_pl = _ensure_timezone_polars(resp_pl, 'recorded_dttm', local_tz)
        resp_pl = _ensure_timezone_polars(resp_pl, 'bucket_start_dttm', local_tz)
        resp_pl = _ensure_timezone_polars(resp_pl, 'bucket_end_dttm', local_tz)
    
    # Forward-fill FiO2 within mode_cat_id episodes (most granular level)
    # device_category and mode_category are already forward-filled in _create_resp_support_episodes
    resp_with_episodes = _create_resp_support_episodes(resp_pl, id_col='hospitalization_id')
    
    resp_with_episodes = resp_with_episodes.sort(['hospitalization_id', 'recorded_dttm'])
    
    resp_with_episodes = resp_with_episodes.with_columns([
        pl.col('fio2_set').forward_fill().over(['hospitalization_id', 'mode_cat_id']).alias('fio2_set')
    ])
    
    logger.info("  ✓ Created episodes and forward-filled FiO2 (including lookback data)")
    
    # Filter back to original window
    resp_filtered = resp_with_episodes.filter(
        (pl.col('recorded_dttm') >= pl.col('bucket_start_dttm')) &
        (pl.col('recorded_dttm') <= pl.col('bucket_end_dttm'))
    )
    
    # Drop window columns
    resp_filtered = resp_filtered.drop(['bucket_start_dttm', 'bucket_end_dttm'])
    
    logger.info(f"  ✓ Filtered to original windows (kept {len(resp_filtered)} rows with forward-filled values)")
    
    # Add device rank
    resp_filtered = resp_filtered.with_columns([
        pl.col('device_category').replace(DEVICE_RANK_DICT, default=9).alias('device_rank')
    ])
    
    return resp_filtered


# =============================================================================
# MEDICATION CONVERSION
# =============================================================================

def _convert_medications_comprehensive(
    con: duckdb.DuckDBPyConnection,
    local_tz: Optional[str] = None
) -> pl.DataFrame:
    """
    Convert all medication doses to mcg/kg/min using comprehensive logic.
    
    Steps:
    1. Load raw medication data from DuckDB
    2. Clean and normalize medication dose units
    3. Load weight data from vitals_raw
    4. Sort medication and weight data for joining and perform an as-of join to attach the most recent weight
    5. Normalize med dose unit to mcg/kg/min
    
    Returns
    -------
    pl.DataFrame
        Medications with dose_mcg_kg_min column (in Polars format)
    """
    logger.info("💉 Converting medication doses...")
    
    meds_raw_pd = con.execute("SELECT * FROM medications_raw").df()
    
    if len(meds_raw_pd) == 0:
        logger.warning("  ⚠️  No medication data to process")
        return pl.DataFrame(schema={
            'hospitalization_id': pl.Utf8,
            'admin_dttm': pl.Datetime,
            'med_category': pl.Utf8,
            'dose_mcg_kg_min': pl.Float64
        })
    
    # Convert to Polars ONCE
    meds_pl = pl.from_pandas(meds_raw_pd)
    
    if local_tz:
        meds_pl = _ensure_timezone_polars(meds_pl, 'admin_dttm', local_tz)
    
    # Clean dose units 
    meds_pl = meds_pl.with_columns([
        _clean_dose_unit_polars(pl.col('med_dose_unit')).alias('dose_unit_clean')
    ])
    
    logger.info("  ✓ Cleaned dose units")
    
    # Get weight data - use most recent weight before medication time
    weight_query = """
    SELECT 
        hospitalization_id,
        recorded_dttm,
        vital_value as weight_kg
    FROM vitals_raw
    WHERE vital_category = 'weight_kg'
    ORDER BY hospitalization_id, recorded_dttm
    """
    weight_data_pd = con.execute(weight_query).df()
    weight_pl = pl.from_pandas(weight_data_pd)
    
    if local_tz:
        weight_pl = _ensure_timezone_polars(weight_pl, 'recorded_dttm', local_tz)
    
    # Sort for asof join (in Polars)
    meds_pl = meds_pl.sort(['hospitalization_id', 'admin_dttm'])
    weight_pl = weight_pl.sort(['hospitalization_id', 'recorded_dttm'])
    
    # Asof join (in Polars)
    meds_with_weight = meds_pl.join_asof(
        weight_pl,
        left_on='admin_dttm',
        right_on='recorded_dttm',
        by='hospitalization_id',
        strategy='backward'
    )
    
    logger.info("  ✓ Joined with weight data")
    
    # Convert doses to mcg/kg/min
    # Apply mass conversions
    meds_converted = meds_with_weight.with_columns([
        pl.when(pl.col('dose_unit_clean').str.contains(r'^mg'))
        .then(pl.col('med_dose') * 1000)
        .when(pl.col('dose_unit_clean').str.contains(r'^g/'))
        .then(pl.col('med_dose') * 1000000)
        .when(pl.col('dose_unit_clean').str.contains(r'^ng'))
        .then(pl.col('med_dose') / 1000)
        .otherwise(pl.col('med_dose'))
        .alias('dose_converted')
    ])
    
    # Apply time conversions (/hr to /min)
    meds_converted = meds_converted.with_columns([
        pl.when(pl.col('dose_unit_clean').str.contains(r'/h'))
        .then(pl.col('dose_converted') / 60)
        .otherwise(pl.col('dose_converted'))
        .alias('dose_converted')
    ])
    
    # Apply weight conversions
    # If unit already contains /kg or /lb, it's already weight-normalized - keep as-is
    # If unit does NOT contain weight, we need to normalize by dividing by weight
    # But since most vasopressors are already in /kg units, we just keep the value
    meds_converted = meds_converted.with_columns([
        pl.when(pl.col('dose_unit_clean').str.contains(r'/kg'))
        .then(pl.col('dose_converted'))
        .when(pl.col('dose_unit_clean').str.contains(r'/lb'))
        .then(pl.col('dose_converted') * 2.205)
        .when(pl.col('weight_kg').is_not_null())
        .then(pl.col('dose_converted') / pl.col('weight_kg'))
        .otherwise(None)
        .alias('dose_mcg_kg_min')
    ])
    
    logger.info("  ✓ Converted doses to mcg/kg/min")
    
    # Return Polars DataFrame
    return meds_converted


# =============================================================================
# DATA AGGREGATION 
# =============================================================================

def _aggregate_all_data(
    con: duckdb.DuckDBPyConnection,
    sofa_agg_id_col: str
) -> Dict[str, pd.DataFrame]:
    """
    Aggregate all clinical data sources per time bucket for SOFA scoring.

    Parameters
    ----------
    con : duckdb.DuckDBPyConnection
        Active DuckDB connection used to run aggregation queries.
    sofa_agg_id_col : str
        Name of the cohort-level aggregation identifier

    Steps
    -----
    1. Aggregate labs by selecting max creatinine, max bilirubin_total, min platelet_count, and min po2_arterial per time bucket.
    2. Aggregate vitals by selecting the min MAP and min SpO2 per time bucket.
    3. Aggregate GCS by selecting the min gcs_total per time bucket.
    4. Aggregate respiratory data by selecting the device with the lowest device_rank (and highest FiO2 if tied) per time bucket.
    5. Aggregate medications by selecting the max dose (mcg/kg/min) for each vasopressor category per time bucket.
    6. Return all aggregated tables as a dictionary of pandas DataFrames.

    Returns
    -------
    Dict[str, pd.DataFrame]
        A dictionary containing aggregated DataFrames for labs, vitals, GCS, respiratory, and medications.

    Notes
    -----
    - All aggregations are aligned to cohort-defined bucket_start_dttm and bucket_end_dttm.
    - Respiratory aggregation returns both device_rank and device_category for correct SOFA scoring.
    """
    logger.info("🔗 Aggregating all data with DuckDB...")

    # Handle case where sofa_agg_id_col is 'hospitalization_id' to avoid duplicate columns
    select_cols = [sofa_agg_id_col]
    if sofa_agg_id_col != 'hospitalization_id':
        select_cols.append('hospitalization_id')
    
    select_cols_str = ', '.join([f'c.{col}' for col in select_cols])
    group_by_cols_str = ', '.join(select_cols)
    
    # Labs
    logger.info("  📊 Labs...")
    labs_agg = con.execute(f"""
    SELECT 
        {select_cols_str},
        c.bucket_start_dttm,
        c.bucket_end_dttm,
        MAX(CASE WHEN d.lab_category = 'creatinine' THEN d.lab_value_numeric END) as creatinine,
        MAX(CASE WHEN d.lab_category = 'bilirubin_total' THEN d.lab_value_numeric END) as bilirubin_total,
        MIN(CASE WHEN d.lab_category = 'platelet_count' THEN d.lab_value_numeric END) as platelet_count,
        MIN(CASE WHEN d.lab_category = 'po2_arterial' THEN d.lab_value_numeric END) as po2_arterial
    FROM cohort c
    LEFT JOIN labs_raw d
        ON c.hospitalization_id = d.hospitalization_id
        AND d.lab_result_dttm >= c.bucket_start_dttm
        AND d.lab_result_dttm <= c.bucket_end_dttm
    GROUP BY {select_cols_str}, c.bucket_start_dttm, c.bucket_end_dttm
    """).df()
    
    # Vitals
    logger.info("  💓 Vitals...")
    vitals_agg = con.execute(f"""
    SELECT 
        {select_cols_str},
        c.bucket_start_dttm,
        c.bucket_end_dttm,
        MIN(CASE WHEN d.vital_category = 'map' THEN d.vital_value END) as map,
        MIN(CASE WHEN d.vital_category = 'spo2' THEN d.vital_value END) as spo2
    FROM cohort c
    LEFT JOIN vitals_raw d
        ON c.hospitalization_id = d.hospitalization_id
        AND d.recorded_dttm >= c.bucket_start_dttm
        AND d.recorded_dttm <= c.bucket_end_dttm
    GROUP BY {select_cols_str}, c.bucket_start_dttm, c.bucket_end_dttm
    """).df()
    
    # GCS
    logger.info("  🧠 GCS...")
    gcs_agg = con.execute(f"""
    SELECT 
        {select_cols_str},
        c.bucket_start_dttm,
        c.bucket_end_dttm,
        MIN(d.numerical_value) as gcs_total
    FROM cohort c
    LEFT JOIN assessments_raw d
        ON c.hospitalization_id = d.hospitalization_id
        AND d.recorded_dttm >= c.bucket_start_dttm
        AND d.recorded_dttm <= c.bucket_end_dttm
    GROUP BY {select_cols_str}, c.bucket_start_dttm, c.bucket_end_dttm
    """).df()
    
    # Respiratory (return BOTH device_rank AND device_category)
    logger.info("  💨 Respiratory...")
    resp_agg = con.execute(f"""
    WITH ranked_resp AS (
        SELECT 
            {select_cols_str},
            c.bucket_start_dttm,
            c.bucket_end_dttm,
            d.device_rank,
            d.device_category,
            d.fio2_set,
            ROW_NUMBER() OVER (
                PARTITION BY {select_cols_str}, c.bucket_start_dttm, c.bucket_end_dttm
                ORDER BY d.device_rank, d.fio2_set DESC NULLS LAST
            ) as rn
        FROM cohort c
        LEFT JOIN resp_data_processed d
            ON c.hospitalization_id = d.hospitalization_id
            AND d.recorded_dttm >= c.bucket_start_dttm
            AND d.recorded_dttm <= c.bucket_end_dttm
    )
    SELECT 
        {group_by_cols_str}, 
        bucket_start_dttm, 
        bucket_end_dttm, 
        device_rank, 
        device_category,
        fio2_set
    FROM ranked_resp WHERE rn = 1
    """).df()
    
    # Medications
    logger.info("  💉 Medications...")
    meds_agg = con.execute(f"""
    SELECT 
        {select_cols_str},
        c.bucket_start_dttm,
        c.bucket_end_dttm,
        MAX(CASE WHEN d.med_category = 'norepinephrine' THEN d.dose_mcg_kg_min END) as norepinephrine_mcg_kg_min,
        MAX(CASE WHEN d.med_category = 'epinephrine' THEN d.dose_mcg_kg_min END) as epinephrine_mcg_kg_min,
        MAX(CASE WHEN d.med_category = 'dopamine' THEN d.dose_mcg_kg_min END) as dopamine_mcg_kg_min,
        MAX(CASE WHEN d.med_category = 'dobutamine' THEN d.dose_mcg_kg_min END) as dobutamine_mcg_kg_min
    FROM cohort c
    LEFT JOIN medications_converted d
        ON c.hospitalization_id = d.hospitalization_id
        AND d.admin_dttm >= c.bucket_start_dttm
        AND d.admin_dttm <= c.bucket_end_dttm
    GROUP BY {select_cols_str}, c.bucket_start_dttm, c.bucket_end_dttm
    """).df()
    
    logger.info("  ✓ All aggregations complete")
    
    return {
        'labs': labs_agg,
        'vitals': vitals_agg,
        'gcs': gcs_agg,
        'resp': resp_agg,
        'meds': meds_agg
    }


# =============================================================================
# IMPUTATION
# =============================================================================

def _impute_pao2_from_spo2(df: Union[pd.DataFrame, pl.DataFrame]) -> pl.DataFrame:
    """
    Impute PaO2 from SpO2 using Severinghaus equation.
    
    Only imputes when SpO2 < 97% (oxygen dissociation curve too flat above this).
    Creates pao2_imputed column (doesn't overwrite po2_arterial).
    
    Parameters
    ----------
    df : pd.DataFrame or pl.DataFrame
        DataFrame containing spo2 and po2_arterial columns
        
    Returns
    -------
    pl.DataFrame
        DataFrame with pao2_imputed and updated po2_arterial (in Polars format)
    """
    logger.info("🔬 Imputing PaO2 from SpO2 (Severinghaus equation)...")
    
    # Convert to Polars if needed (only once)
    if isinstance(df, pd.DataFrame):
        df_pl = pl.from_pandas(df)
    else:
        df_pl = df
    
    # Severinghaus equation - only for SpO2 < 97% (in Polars)
    df_pl = df_pl.with_columns([
        pl.when(pl.col('spo2') < 97)
        .then(
            (
                (
                    (
                        (11700.0 / ((100.0 / pl.col('spo2')) - 1)) ** 2 + 50 ** 3
                    ) ** 0.5 +
                    (11700.0 / ((100.0 / pl.col('spo2')) - 1))
                ) ** (1.0/3.0)
            ) -
            (
                (
                    (
                        (11700.0 / ((100.0 / pl.col('spo2')) - 1)) ** 2 + 50 ** 3
                    ) ** 0.5 -
                    (11700.0 / ((100.0 / pl.col('spo2')) - 1))
                ) ** (1.0/3.0)
            )
        )
        .otherwise(None)  # Don't impute if SpO2 >= 97%
        .alias('pao2_imputed')
    ])
    
    
    n_imputed = df_pl.filter(pl.col('pao2_imputed').is_not_null()).height
    
    if n_imputed > 0:
        logger.info(f"  ✓ Imputed {n_imputed:,} PaO2 values (SpO2 < 97% only)")
    else:
        logger.info("  ✓ No imputation needed")

    return df_pl


def _calculate_concurrent_pf_ratios(
    con: duckdb.DuckDBPyConnection,
    sofa_agg_id_col: str,
    time_tolerance_minutes: int = 240
) -> pd.DataFrame:
    """
    Calculate P/F ratios from concurrent PO2 and FiO2 measurements.
    
    For SOFA-97 specification, P/F ratio must be calculated from PO2 and FiO2
    measured at the same time (or within a tolerance window). This function
    matches each PO2 measurement with the most recent FiO2 (lookback).
    
    Parameters
    ----------
    con : duckdb.DuckDBPyConnection
        Active DuckDB connection with labs_raw and resp_data_processed views
    sofa_agg_id_col : str
        Column to aggregate by (e.g., 'hospitalization_id')
    time_tolerance_minutes : int
        Maximum lookback time to find FiO2 before PO2 measurement (default: 240 = 4 hours)
        
    Returns
    -------
    pd.DataFrame
        DataFrame with concurrent P/F ratios per time bucket, including:
        - sofa_agg_id_col, hospitalization_id, bucket_start_dttm, bucket_end_dttm
        - p_f: worst concurrent P/F ratio
        - po2_arterial: PO2 at worst P/F
        - fio2_set: FiO2 at worst P/F
        - device_category: device at worst P/F
        - device_rank: rank of device
    """
    logger.info("💨 Calculating concurrent P/F ratios (SOFA-97 compliant)...")
    
    # Handle case where sofa_agg_id_col is 'hospitalization_id' to avoid duplicate columns
    select_cols = [sofa_agg_id_col]
    if sofa_agg_id_col != 'hospitalization_id':
        select_cols.append('hospitalization_id')
    
    group_by_cols_str = ', '.join(select_cols)

    concurrent_pf_query = f"""
    WITH po2_data AS (
        -- Get all individual PO2 measurements within time buckets
        SELECT 
            {', '.join([f'c.{col}' for col in select_cols])},
            c.bucket_start_dttm,
            c.bucket_end_dttm,
            d.lab_result_dttm,
            d.lab_value_numeric as po2_arterial
        FROM cohort c
        INNER JOIN labs_raw d
            ON c.hospitalization_id = d.hospitalization_id
            AND d.lab_category = 'po2_arterial'
            AND d.lab_result_dttm >= c.bucket_start_dttm
            AND d.lab_result_dttm <= c.bucket_end_dttm
            AND d.lab_value_numeric IS NOT NULL
    ),
    matched_fio2 AS (
        -- For each PO2, find the most recent FiO2 within tolerance window
        SELECT 
            {', '.join([f'p.{col}' for col in select_cols])},
            p.bucket_start_dttm,
            p.bucket_end_dttm,
            p.lab_result_dttm,
            p.po2_arterial,
            r.fio2_set,
            r.device_category,
            r.recorded_dttm as fio2_dttm,
            -- Rank by closest FiO2 before PO2 (within tolerance)
            ROW_NUMBER() OVER (
                PARTITION BY {', '.join([f'p.{col}' for col in select_cols])}, p.bucket_start_dttm, p.lab_result_dttm
                ORDER BY 
                    CASE 
                        WHEN r.recorded_dttm <= p.lab_result_dttm 
                        AND EXTRACT(EPOCH FROM (p.lab_result_dttm - r.recorded_dttm)) / 60 <= {time_tolerance_minutes}
                        THEN EXTRACT(EPOCH FROM (p.lab_result_dttm - r.recorded_dttm))
                        ELSE 9999999
                    END ASC
            ) as rn
        FROM po2_data p
        LEFT JOIN resp_data_processed r
            ON p.hospitalization_id = r.hospitalization_id
            AND r.recorded_dttm <= p.lab_result_dttm
            AND EXTRACT(EPOCH FROM (p.lab_result_dttm - r.recorded_dttm)) / 60 <= {time_tolerance_minutes}
    ),
    concurrent_pf_calcs AS (
        -- Calculate P/F from matched pairs
        SELECT 
            {group_by_cols_str},
            bucket_start_dttm,
            bucket_end_dttm,
            lab_result_dttm,
            po2_arterial,
            fio2_set,
            device_category,
            CASE 
                WHEN fio2_set IS NOT NULL AND fio2_set > 0 
                THEN po2_arterial / fio2_set
                ELSE NULL
            END as concurrent_pf
        FROM matched_fio2
        WHERE rn = 1 AND fio2_set IS NOT NULL
    )
    -- Aggregate to worst P/F per time bucket
    SELECT 
        {group_by_cols_str},
        bucket_start_dttm,
        bucket_end_dttm,
        MIN(concurrent_pf) as p_f,
        FIRST(po2_arterial ORDER BY concurrent_pf ASC) as po2_arterial,
        FIRST(fio2_set ORDER BY concurrent_pf ASC) as fio2_set,
        FIRST(device_category ORDER BY concurrent_pf ASC) as device_category
    FROM concurrent_pf_calcs
    WHERE concurrent_pf IS NOT NULL
    GROUP BY {group_by_cols_str}, bucket_start_dttm, bucket_end_dttm
    """
    
    try:
        concurrent_pf_df = con.execute(concurrent_pf_query).df()
        
        if len(concurrent_pf_df) > 0:
            # Add device_rank
            concurrent_pf_pl = pl.from_pandas(concurrent_pf_df)
            concurrent_pf_pl = concurrent_pf_pl.with_columns([
                pl.col('device_category').replace(DEVICE_RANK_DICT, default=9).alias('device_rank')
            ])
            concurrent_pf_df = concurrent_pf_pl.to_pandas()
            
            logger.info(f"  ✓ Calculated concurrent P/F for {len(concurrent_pf_df):,} time buckets")
        else:
            logger.warning("  ⚠️  No concurrent P/F ratios could be calculated")
            # Return empty DataFrame with expected schema
            expected_cols = select_cols + ['bucket_start_dttm', 'bucket_end_dttm',
                                           'p_f', 'po2_arterial', 'fio2_set', 'device_category', 'device_rank']
            concurrent_pf_df = pd.DataFrame(columns=expected_cols)
        
        return concurrent_pf_df
        
    except Exception as e:
        logger.error(f"  ✗ Error calculating concurrent P/F ratios: {e}")
        # Return empty DataFrame on error
        expected_cols = select_cols + ['bucket_start_dttm', 'bucket_end_dttm',
                                       'p_f', 'po2_arterial', 'fio2_set', 'device_category', 'device_rank']
        return pd.DataFrame(columns=expected_cols)


# =============================================================================
# SOFA SCORING
# =============================================================================

def _compute_sofa_scores(df: pl.DataFrame, fill_na_with_zero: bool = True) -> pl.DataFrame:
    """
    Calculate SOFA component scores from aggregated extremal values.
    
    CORRECTED:
    - Proper column validation
    - Correct CNS cut-offs (< 6 for score 4, not >= 6)
    - Correct CV logic (any dose checks)
    - Uses device_category (not device_rank)
    - Handles pao2_imputed
    """
    logger.info("🧮 Computing SOFA scores...")
    
    # Ensure all required columns exist
    required_cols = {
        'norepinephrine_mcg_kg_min': pl.Float64,
        'epinephrine_mcg_kg_min': pl.Float64,
        'dopamine_mcg_kg_min': pl.Float64,
        'dobutamine_mcg_kg_min': pl.Float64,
        'platelet_count': pl.Float64,
        'bilirubin_total': pl.Float64,
        'creatinine': pl.Float64,
        'po2_arterial': pl.Float64,
        'pao2_imputed': pl.Float64,
        'map': pl.Float64,
        'spo2': pl.Float64,
        'fio2_set': pl.Float64,
        'gcs_total': pl.Float64,
        'device_category': pl.Utf8
    }
    
    for col, dtype in required_cols.items():
        if col not in df.columns:
            df = df.with_columns([pl.lit(None).cast(dtype).alias(col)])
    
    # Calculate P/F ratios (only if not already calculated from concurrent measurements)
    if 'p_f' not in df.columns:
        # MDCalc logic: calculate from aggregated PO2/FiO2
        df = df.with_columns([
            (pl.col('po2_arterial') / pl.col('fio2_set')).alias('p_f'),
            (pl.col('pao2_imputed') / pl.col('fio2_set')).alias('p_f_imputed')
        ])
    else:
        # SOFA-97 logic: P/F already calculated from concurrent measurements
        df = df.with_columns([
            # Still calculate imputed P/F for reference
            (pl.col('pao2_imputed') / pl.col('fio2_set')).alias('p_f_imputed')
        ])

    # Map device rank back to device category for respiratory scoring (if needed)
    if 'device_category' not in df.columns:
        rank_to_device = {v: k for k, v in DEVICE_RANK_DICT.items()}
        df = df.with_columns([
            pl.col('device_rank').replace(rank_to_device, default='Other').alias('device_category')
        ])

    # Calculate SOFA scores ##################################################
    # Cardiovascular
    df = df.with_columns([
        pl.when(
            (pl.col('dopamine_mcg_kg_min') > 15) |
            (pl.col('epinephrine_mcg_kg_min') > 0.1) |
            (pl.col('norepinephrine_mcg_kg_min') > 0.1)
        ).then(4)
        .when(
            (pl.col('dopamine_mcg_kg_min') > 5) |
            (pl.col('epinephrine_mcg_kg_min') <= 0.1) |  
            (pl.col('norepinephrine_mcg_kg_min') <= 0.1)  
        ).then(3)
        .when(
            (pl.col('dopamine_mcg_kg_min') <= 5) |  
            (pl.col('dobutamine_mcg_kg_min') > 0)
        ).then(2)
        .when(pl.col('map') < 70).then(1)
        .when(pl.col('map') >= 70).then(0)
        .otherwise(None)
        .alias('sofa_cv_97')
    ])
    
    # Coagulation
    df = df.with_columns([
        pl.when(pl.col('platelet_count') < 20).then(4)
        .when(pl.col('platelet_count') < 50).then(3)
        .when(pl.col('platelet_count') < 100).then(2)
        .when(pl.col('platelet_count') < 150).then(1)
        .when(pl.col('platelet_count') >= 150).then(0)
        .otherwise(None)
        .alias('sofa_coag')
    ])
    
    # Liver
    df = df.with_columns([
        pl.when(pl.col('bilirubin_total') >= 12).then(4)
        .when(pl.col('bilirubin_total') >= 6).then(3)
        .when(pl.col('bilirubin_total') >= 2).then(2)
        .when(pl.col('bilirubin_total') >= 1.2).then(1)
        .when(pl.col('bilirubin_total') < 1.2).then(0)
        .otherwise(None)
        .alias('sofa_liver')
    ])
    
    # Respiratory
    df = df.with_columns([
        pl.when(
            (pl.col('p_f') < 100) &
            pl.col('device_category').is_in(['IMV', 'NIPPV', 'CPAP'])
        ).then(4)
        .when(
            (pl.col('p_f') >= 100) & (pl.col('p_f') < 200) &
            pl.col('device_category').is_in(['IMV', 'NIPPV', 'CPAP'])
        ).then(3)
        .when((pl.col('p_f') >= 200) & (pl.col('p_f') < 300)).then(2)
        .when((pl.col('p_f') >= 300) & (pl.col('p_f') < 400)).then(1)
        .when(pl.col('p_f') >= 400).then(0)
        .otherwise(None)
        .alias('sofa_resp')
    ])
    
    # CNS
    df = df.with_columns([
        pl.when(pl.col('gcs_total') < 6).then(4)
        .when((pl.col('gcs_total') >= 6) & (pl.col('gcs_total') <= 9)).then(3)
        .when((pl.col('gcs_total') >= 10) & (pl.col('gcs_total') <= 12)).then(2)
        .when((pl.col('gcs_total') >= 13) & (pl.col('gcs_total') <= 14)).then(1)
        .when(pl.col('gcs_total') == 15).then(0)
        .otherwise(None)
        .alias('sofa_cns')
    ])
    
    # Renal
    df = df.with_columns([
        pl.when(pl.col('creatinine') >= 5).then(4)
        .when(pl.col('creatinine') >= 3.5).then(3)
        .when(pl.col('creatinine') >= 2).then(2)
        .when(pl.col('creatinine') >= 1.2).then(1)
        .when(pl.col('creatinine') < 1.2).then(0)
        .otherwise(None)
        .alias('sofa_renal')
    ])
    
    # Fill NA if requested
    subscore_cols = ['sofa_cv_97', 'sofa_coag', 'sofa_liver', 'sofa_resp', 'sofa_cns', 'sofa_renal']
    if fill_na_with_zero:
        for col in subscore_cols:
            df = df.with_columns([pl.col(col).fill_null(0)])
    
    # Total
    df = df.with_columns([
        pl.sum_horizontal([pl.col(c) for c in subscore_cols]).alias('sofa_total')
    ])
    
    # Log stats
    total_count = df.height
    valid_count = df.filter(pl.col('sofa_total').is_not_null()).height
    mean_score = df.select(pl.col('sofa_total').mean()).item()
    logger.info(f"  ✓ Valid scores: {valid_count:,}/{total_count:,} ({100*valid_count/total_count:.1f}%)")
    if mean_score is not None:
        logger.info(f"  ✓ Mean SOFA: {mean_score:.2f}")
    
    return df


# =============================================================================
# MAIN FUNCTION
# =============================================================================

def compute_sofa_with_time_buckets(
    cohort_df: Union[pd.DataFrame, pl.DataFrame],
    data_dir: Union[str, Path],
    time_bucket_hours: Optional[float] = None,
    bucket_id_col: str = 'hospitalization_id',
    sofa_agg_id_col: str = 'hospitalization_id',
    time_window_cols: Tuple[str, str] = ('start_dttm', 'end_dttm'),
    bucket_cols: Optional[Tuple[str, str]] = None,
    local_tz: str = 'America/New_York',
    filetype: str = 'parquet',
    remove_outliers_flag: bool = True,
    fill_na_scores_with_zero: bool = True,
    include_partial_buckets: bool = True
) -> pd.DataFrame:
    """
    Calculate SOFA scores using DuckDB with full support for continuous or pre-created time buckets.

    Parameters
    ----------
    cohort_df : pd.DataFrame or pl.DataFrame
        Cohort table with hospitalization and time window info.
    data_dir : str or Path
        Directory containing clif tables
    time_bucket_hours : float, optional
        If provided, automatically split time windows into continuous buckets of this duration.
    bucket_id_col : str
        Column to group by when creating time buckets (default: 'hospitalization_id').
    sofa_agg_id_col : str
        Column used for SOFA score aggregation (default: 'hospitalization_id').
    time_window_cols : tuple of str
        Columns defining start and end of time windows (default: ('start_dttm', 'end_dttm')).
    bucket_cols : tuple of str, optional
        If cohort already has bucket start/end, pass column names here.
    local_tz : str
        Timezone for all datetime operations.
    filetype : str
        Data file format (default: 'parquet').
    remove_outliers : bool
        Remove physiologically implausible values (default: True).
    fill_na_scores_with_zero : bool
        Fill missing SOFA subscores with 0 (default: True).
    include_partial_buckets : bool
        Include partial final buckets when auto-creating (default: True).

    Returns
    -------
    pd.DataFrame
        Cohort table with calculated SOFA subscores and total scores.
    
    Examples
    --------
    >>> path_to_tables = "/path/to/data"
    >>> time_zone = "US/Central"

    # Case 1️⃣ Single time window
    >>> cohort = pl.DataFrame({
    ...     'hospitalization_id': ['H1', 'H2'],
    ...     'admission_dttm': [datetime(2024,1,1), datetime(2024,1,2)],
    ...     'admission_dttm_24h': [datetime(2024,1,2), datetime(2024,1,3)]
    ... })
    >>> sofa_scores = compute_sofa_with_time_buckets(
    ...     cohort_df=cohort,
    ...     data_dir=path_to_tables,
    ...     sofa_agg_id_col='hospitalization_id',
    ...     time_window_cols=('admission_dttm', 'admission_dttm_24h'),
    ...     local_tz=time_zone
    ... )

    # Case 2️⃣ Predefined buckets
    >>> cohort = pl.DataFrame({
    ...     'hospitalization_id': ['H1', 'H2', 'H3'],
    ...     'encounter_block': [1, 1, 2],
    ...     'bucket_start_dttm': [datetime(2024,1,1), datetime(2024,1,2), datetime(2024,1,3)],
    ...     'bucket_end_dttm': [datetime(2024,1,1,6), datetime(2024,1,2,6), datetime(2024,1,3,6)]
    ... })
    >>> sofa_scores_2 = compute_sofa_with_time_buckets(
    ...     cohort_df=cohort,
    ...     data_dir=path_to_tables,
    ...     sofa_agg_id_col='encounter_block',
    ...     bucket_cols=('bucket_start_dttm', 'bucket_end_dttm'),
    ...     local_tz=time_zone
    ... )

    # Case 3️⃣ Create and use continuous 6-hour buckets
    >>> cohort = pl.DataFrame({
    ...     'hospitalization_id': ['H1', 'H2'],
    ...     'encounter_block': [1, 2],
    ...     'admission_dttm': [datetime(2024,1,1), datetime(2024,1,2)],
    ...     'discharge_dttm': [datetime(2024,1,3), datetime(2024,1,4)]
    ... })
    >>> sofa_scores_3 = compute_sofa_with_time_buckets(
    ...     cohort_df=cohort,
    ...     data_dir=path_to_tables,
    ...     time_bucket_hours=6,
    ...     bucket_id_col='hospitalization_id',
    ...     sofa_agg_id_col='encounter_block',
    ...     time_window_cols=('admission_dttm', 'discharge_dttm'),
    ...     local_tz=time_zone
    ... )
    """
    logger.info("="*80)
    logger.info("🚀 CORRECTED Standalone DuckDB SOFA Calculator")
    logger.info("="*80)
    
    # Step 1: Prepare cohort
    logger.info("📋 Step 1/9: Preparing cohort...")
    
    if isinstance(cohort_df, pl.DataFrame):
        cohort_df = cohort_df.to_pandas()
    
    if 'hospitalization_id' not in cohort_df.columns:
        raise ValueError(f"cohort_df must contain 'hospitalization_id'")
    
    # Determine time bucket strategy
    if bucket_cols is not None:
        bucket_start_col, bucket_end_col = bucket_cols
        bucketed_cohort = cohort_df.copy()
        bucketed_cohort['bucket_start_dttm'] = bucketed_cohort[bucket_start_col]
        bucketed_cohort['bucket_end_dttm'] = bucketed_cohort[bucket_end_col]
    
        logger.info(f"  ✓ Using pre-created buckets")
    elif time_bucket_hours is not None:
        bucketed_cohort = create_continuous_time_buckets(
            cohort_df, time_bucket_hours, bucket_id_col, time_window_cols, include_partial_buckets
        )
        logger.info(f"  ✓ Created {time_bucket_hours}h buckets")
    else:
        start_col, end_col = time_window_cols
        bucketed_cohort = cohort_df.copy()
        bucketed_cohort['bucket_start_dttm'] = bucketed_cohort[start_col]
        bucketed_cohort['bucket_end_dttm'] = bucketed_cohort[end_col]
        logger.info(f"  ✓ Using single time windows")
    
    # Ensure timezone
    bucketed_cohort = _ensure_timezone_pandas(bucketed_cohort, 'bucket_start_dttm', local_tz)
    bucketed_cohort = _ensure_timezone_pandas(bucketed_cohort, 'bucket_end_dttm', local_tz)
    
    if sofa_agg_id_col not in bucketed_cohort.columns:
        raise ValueError(f"sofa_agg_id_col '{sofa_agg_id_col}' not found")
    
    logger.info(f"  ✓ {len(bucketed_cohort):,} time buckets")
    logger.info(f"  ✓ {bucketed_cohort[sofa_agg_id_col].nunique():,} unique {sofa_agg_id_col}")
    logger.info(f"  ✓ {bucketed_cohort['hospitalization_id'].nunique():,} unique hospitalization_ids")
    
    # Step 2: Extract hospitalization_ids for filtering
    logger.info("🎯 Step 2/9: Extracting hospitalization_ids for filtering...")
    
    hospitalization_ids = bucketed_cohort['hospitalization_id'].unique().tolist()
    logger.info(f"  ✓ Will filter data to {len(hospitalization_ids):,} hospitalizations")
    
    # Step 3: Initialize DuckDB and load data
    logger.info("📥 Step 3/9: Loading data (filtered by hospitalization_ids)...")
    
    con = duckdb.connect(database=':memory:')
    data_dir = Path(data_dir)
    
    try:
        con.register('cohort', bucketed_cohort)
        
        _load_labs(con, data_dir, hospitalization_ids, local_tz, filetype, remove_outliers=remove_outliers_flag)
        _load_vitals(con, data_dir, hospitalization_ids, local_tz, filetype, remove_outliers=remove_outliers_flag)
        _load_assessments(con, data_dir, hospitalization_ids, local_tz, filetype, remove_outliers=remove_outliers_flag)
        _load_respiratory_support(con, data_dir, hospitalization_ids, local_tz, filetype)
        _load_medications(con, data_dir, hospitalization_ids, local_tz, filetype)
        
        logger.info("  ✓ All data loaded (filtered)")
        
        # Step 4: Process respiratory with episodes
        logger.info("💨 Step 4/9: Processing respiratory episodes (with ALL heuristics)...")
        resp_processed_pl = _process_respiratory_with_episodes(con, local_tz)
        # Register Polars DataFrame directly to DuckDB
        con.register('resp_data_processed', resp_processed_pl.to_pandas())
        del resp_processed_pl
        gc.collect()
        
        # Step 5: Convert medications
        logger.info("💉 Step 5/9: Converting medication doses...")
        meds_converted_pl = _convert_medications_comprehensive(con, local_tz)
        # Register Polars DataFrame directly to DuckDB
        con.register('medications_converted', meds_converted_pl.to_pandas())
        del meds_converted_pl
        gc.collect()
        
        # Step 6: Aggregate non-respiratory data first
        logger.info("🔗 Step 6/10: Aggregating data (excluding respiratory for P/F matching)...")
        agg_results = _aggregate_all_data(con, sofa_agg_id_col)
        
        # Combine non-respiratory aggregations first
        combined = agg_results['labs']
        for key in ['vitals', 'gcs', 'meds']:
            combined = combined.merge(
                agg_results[key],
                on=[sofa_agg_id_col, 'hospitalization_id', 'bucket_start_dttm', 'bucket_end_dttm'],
                how='outer'
            )
        
        # Merge with original cohort
        combined = bucketed_cohort.merge(
            combined,
            on=[sofa_agg_id_col, 'hospitalization_id', 'bucket_start_dttm', 'bucket_end_dttm'],
            how='left'
        )
        
        logger.info(f"  ✓ Combined shape (before P/F): {combined.shape}")
        
        del agg_results
        gc.collect()
        
        # Step 7: Impute PaO2 from SpO2 (before P/F calculation)
        logger.info("🔬 Step 7/10: Imputing PaO2 from SpO2...")
        combined_pl = pl.from_pandas(combined)
        combined_pl = _impute_pao2_from_spo2(combined_pl)
        combined = combined_pl.to_pandas()
        del combined_pl
        gc.collect()
        
        # Step 8: Calculate CONCURRENT P/F ratios (SOFA-97 compliant)
        logger.info("💨 Step 8/10: Calculating concurrent P/F ratios...")
        concurrent_pf_df = _calculate_concurrent_pf_ratios(
            con, 
            sofa_agg_id_col,
            time_tolerance_minutes=240  # 4 hour lookback
        )
        
        # Merge concurrent P/F data with other aggregated values
        # Handle case where sofa_agg_id_col is 'hospitalization_id' to avoid duplicate columns
        select_cols = [sofa_agg_id_col]
        if sofa_agg_id_col != 'hospitalization_id':
            select_cols.append('hospitalization_id')
        
        combined = combined.merge(
            concurrent_pf_df,
            on= select_cols + ['bucket_start_dttm', 'bucket_end_dttm'],
            how='left'
        )
        
        logger.info(f"  ✓ Combined shape (after P/F): {combined.shape}")
        
        del concurrent_pf_df
        gc.collect()
        
        # Step 9: Convert to Polars for SOFA scoring
        logger.info("🔄 Step 9/10: Preparing data for SOFA scoring...")
        combined_pl = pl.from_pandas(combined)
        del combined
        gc.collect()
        
        # Step 10: Calculate SOFA scores
        logger.info("🧮 Step 10/10: Computing SOFA scores (SOFA-97 compliant)...")
        
        sofa_results = _compute_sofa_scores(combined_pl, fill_na_scores_with_zero)
        
        # Convert to pandas only at the very end
        sofa_results_pd = sofa_results.to_pandas()
        
        # Final step: keep only cohort columns + SOFA-related columns ---
        # Get cohort columns
        cohort_cols = cohort_df.columns.tolist()
        bucket_cols = ['bucket_start_dttm', 'bucket_end_dttm']

        # Identify SOFA-related columns (all non-cohort columns)
        sofa_cols = ['sofa_cv_97', 'sofa_coag', 'sofa_liver', 'sofa_resp', 'sofa_cns', 'sofa_renal', 'sofa_total']
        final_df = sofa_results_pd[cohort_cols + bucket_cols + sofa_cols]

        logger.info("="*80)
        logger.info("✅ SOFA Calculation Complete")
        logger.info(f"   {len(final_df):,} rows")
        logger.info("="*80)

        return final_df
        
    finally:
        con.close()
        gc.collect()


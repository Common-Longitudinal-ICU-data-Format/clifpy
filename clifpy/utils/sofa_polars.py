"""
Optimized SOFA score computation using Polars.

This module provides a highly optimized implementation of SOFA (Sequential Organ
Failure Assessment) score calculation using Polars for maximum performance.

It loads raw data files directly using the polars-based I/O utilities and performs
all computations including unit conversion without relying on the pandas-based
clifpy methods.

Example
-------
>>> import polars as pl
>>> from clifpy import compute_sofa_polars
>>>
>>> # Define cohort with time windows
>>> cohort_df = pl.DataFrame({
...     'hospitalization_id': ['H1', 'H2'],
...     'start_dttm': [datetime(2024,1,1), datetime(2024,1,2)],
...     'end_dttm': [datetime(2024,1,2), datetime(2024,1,3)]
... })
>>>
>>> sofa_scores = compute_sofa_polars(
...     data_directory='/path/to/clif/data',
...     cohort_df=cohort_df,
...     filetype='parquet',
...     timezone='US/Central'
... )
"""

import polars as pl
from typing import Optional, List
from pathlib import Path
import logging
import gc

from .datetime_polars import standardize_datetime_columns
from .io_polars import load_data_polars

# Set up logging
logger = logging.getLogger('clifpy.utils.sofa_polars')


# =============================================================================
# Constants
# =============================================================================

# SOFA required categories by table
REQUIRED_LABS = ['creatinine', 'platelet_count', 'po2_arterial', 'bilirubin_total']
REQUIRED_VITALS = ['map', 'spo2', 'weight_kg']
REQUIRED_ASSESSMENTS = ['gcs_total']
REQUIRED_MEDS = ['norepinephrine', 'epinephrine', 'dopamine', 'dobutamine']
REQUIRED_RESP_SUPPORT_COLS = ['device_category', 'mode_category', 'fio2_set']

# Device ranking for respiratory SOFA score (lower rank = worse)
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

# Unit conversion patterns for medication doses
UNIT_NAMING_VARIANTS = {
    # time
    '/hr': r'/h(r|our)?$',
    '/min': r'/m(in|inute)?$',
    # unit
    'u': r'u(nits|nit)?',
    # milli
    'm': r'milli-?',
    # volume
    "l": r'l(iters|itres|itre|iter)?',
    # mass
    'mcg': r'^(u|µ|μ)g',
    'g': r'^g(rams|ram)?',
}


# =============================================================================
# Private Helper Functions - Respiratory Support Processing
# =============================================================================

def _create_resp_support_episodes(
    resp_df: pl.DataFrame,
    id_col: str = 'hospitalization_id'
) -> pl.DataFrame:
    """
    Create respiratory support episode IDs for waterfall forward-filling.

    Implements waterfall heuristics including:
    - Room air FiO2 defaults (0.21)
    - FiO2 imputation from nasal cannula LPM (1L→24%, 2L→28%, ..., 10L→60%)
    - IMV detection from mode_category patterns
    - NIPPV detection from mode_category patterns
    - Hierarchical episode tracking (device_cat_id, mode_cat_id)

    Parameters
    ----------
    resp_df : pl.DataFrame
        Respiratory support data with device_category, mode_category, lpm_set, recorded_dttm
    id_col : str
        ID column for grouping (default: 'hospitalization_id')

    Returns
    -------
    pl.DataFrame
        Input DataFrame with added device_cat_id and mode_cat_id columns
    """
    # Sort by patient and time
    resp_df = resp_df.sort([id_col, 'recorded_dttm'])

    logger.debug("Waterfall: IMV detection from mode_category...")
    # === HEURISTIC 1: IMV detection from mode_category ===
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

    logger.debug("Waterfall: NIPPV detection from mode_category...")
    # === HEURISTIC 2: NIPPV detection from mode_category ===
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
    resp_df = resp_df.with_columns([
        pl.when(
            (pl.col('device_category').str.to_lowercase() == 'room air') &
            pl.col('fio2_set').is_null()
        )
        .then(pl.lit(0.21))
        .otherwise(pl.col('fio2_set'))
        .alias('fio2_set')
    ])

    logger.debug("Waterfall: FiO2 imputation from nasal cannula LPM...")
    # === HEURISTIC 4: FiO2 imputation from nasal cannula flow ===
    if 'lpm_set' in resp_df.columns:
        resp_df = resp_df.with_columns([
            pl.col('lpm_set').round(0).cast(pl.Int32).alias('_lpm_rounded')
        ])

        # Create mapping expression using when/then chains
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

        resp_df = resp_df.drop('_lpm_rounded')

    logger.debug("Waterfall: Forward-filling device and mode categories...")
    # === Forward-fill device_category and mode_category ===
    resp_df = resp_df.with_columns([
        pl.col('device_category').forward_fill().over(id_col).alias('device_category'),
        pl.col('mode_category').forward_fill().over(id_col).alias('mode_category')
    ])

    logger.debug("Waterfall: Creating hierarchical episode IDs...")
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

    # Level 2: mode_cat_id - changes when mode_category changes
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


# =============================================================================
# Private Helper Functions - Data Loading
# =============================================================================

def _load_labs(
    data_directory: str,
    filetype: str,
    hospitalization_ids: List[str],
    cohort_df: pl.DataFrame,
    timezone: Optional[str] = None
) -> pl.LazyFrame:
    """Load and filter labs data (returns LazyFrame for memory efficiency)."""
    file_path = Path(data_directory) / f"clif_labs.{filetype}"

    if not file_path.exists():
        logger.warning(f"Labs file not found: {file_path}")
        return pl.LazyFrame(schema={
            'hospitalization_id': pl.Utf8,
            'lab_result_dttm': pl.Datetime,
            'lab_category': pl.Utf8,
            'lab_value_numeric': pl.Float64
        })

    load_columns = ['hospitalization_id', 'lab_result_dttm', 'lab_category', 'lab_value', 'lab_value_numeric']

    if filetype == 'parquet':
        labs = pl.scan_parquet(str(file_path)).select(load_columns)
    else:
        labs = pl.scan_csv(str(file_path)).select(load_columns)

    # Normalize hospitalization_id to Utf8
    labs = labs.with_columns([
        pl.col('hospitalization_id').cast(pl.Utf8).alias('hospitalization_id')
    ])

    # Filter for required categories and hospitalization_ids
    labs = labs.filter(
        pl.col('lab_category').is_in(REQUIRED_LABS) &
        pl.col('hospitalization_id').is_in(hospitalization_ids)
    )

    # Standardize datetime columns
    if timezone:
        labs = standardize_datetime_columns(
            labs,
            target_timezone=timezone,
            target_time_unit='ns',
            datetime_columns=['lab_result_dttm']
        )

    # Join with cohort to apply time window filter
    labs = labs.join(
        cohort_df.lazy(),
        on='hospitalization_id',
        how='inner'
    ).filter(
        (pl.col('lab_result_dttm') >= pl.col('start_dttm')) &
        (pl.col('lab_result_dttm') <= pl.col('end_dttm'))
    )

    # Select relevant columns
    id_cols = [col for col in cohort_df.columns if col not in ['start_dttm', 'end_dttm']]

    labs = labs.select([
        *id_cols,
        'lab_result_dttm',
        'lab_category',
        'lab_value_numeric'
    ])

    return labs


def _load_vitals(
    data_directory: str,
    filetype: str,
    hospitalization_ids: List[str],
    cohort_df: pl.DataFrame,
    timezone: Optional[str] = None
) -> pl.LazyFrame:
    """Load and filter vitals data (returns LazyFrame for memory efficiency)."""
    file_path = Path(data_directory) / f"clif_vitals.{filetype}"

    if not file_path.exists():
        logger.warning(f"Vitals file not found: {file_path}")
        return pl.LazyFrame(schema={
            'hospitalization_id': pl.Utf8,
            'recorded_dttm': pl.Datetime,
            'vital_category': pl.Utf8,
            'vital_value': pl.Float64
        })

    load_columns = ['hospitalization_id', 'recorded_dttm', 'vital_category', 'vital_value']

    if filetype == 'parquet':
        vitals = pl.scan_parquet(str(file_path)).select(load_columns)
    else:
        vitals = pl.scan_csv(str(file_path)).select(load_columns)

    vitals = vitals.with_columns([
        pl.col('hospitalization_id').cast(pl.Utf8).alias('hospitalization_id')
    ])

    vitals = vitals.filter(
        pl.col('vital_category').is_in(REQUIRED_VITALS) &
        pl.col('hospitalization_id').is_in(hospitalization_ids)
    )

    if timezone:
        vitals = standardize_datetime_columns(
            vitals,
            target_timezone=timezone,
            target_time_unit='ns',
            datetime_columns=['recorded_dttm']
        )

    vitals = vitals.join(
        cohort_df.lazy(),
        on='hospitalization_id',
        how='inner'
    ).filter(
        (pl.col('recorded_dttm') >= pl.col('start_dttm')) &
        (pl.col('recorded_dttm') <= pl.col('end_dttm'))
    )

    id_cols = [col for col in cohort_df.columns if col not in ['start_dttm', 'end_dttm']]

    vitals = vitals.select([
        *id_cols,
        'recorded_dttm',
        'vital_category',
        'vital_value'
    ])

    return vitals


def _load_patient_assessments(
    data_directory: str,
    filetype: str,
    hospitalization_ids: List[str],
    cohort_df: pl.DataFrame,
    timezone: Optional[str] = None
) -> pl.LazyFrame:
    """Load and filter patient assessments data using pandas for stability."""
    import pandas as pd

    file_path = Path(data_directory) / f"clif_patient_assessments.{filetype}"

    if not file_path.exists():
        logger.warning(f"Patient assessments file not found: {file_path}")
        return pl.LazyFrame(schema={
            'hospitalization_id': pl.Utf8,
            'recorded_dttm': pl.Datetime,
            'assessment_category': pl.Utf8,
            'assessment_value': pl.Float64
        })

    load_columns = ['hospitalization_id', 'recorded_dttm', 'assessment_category',
                    'numerical_value', 'categorical_value']

    logger.debug("Loading patient assessments with pandas...")

    if filetype == 'parquet':
        assessments_pd = pd.read_parquet(file_path, columns=load_columns)
    else:
        assessments_pd = pd.read_csv(file_path, usecols=load_columns)

    logger.debug(f"Loaded {len(assessments_pd)} assessment rows")

    assessments_pd['hospitalization_id'] = assessments_pd['hospitalization_id'].astype(str)

    assessments_pd = assessments_pd[
        assessments_pd['assessment_category'].isin(REQUIRED_ASSESSMENTS) &
        assessments_pd['hospitalization_id'].isin(hospitalization_ids)
    ]

    if timezone:
        assessments_pd['recorded_dttm'] = pd.to_datetime(assessments_pd['recorded_dttm'])
        if assessments_pd['recorded_dttm'].dt.tz is None:
            assessments_pd['recorded_dttm'] = assessments_pd['recorded_dttm'].dt.tz_localize('UTC')
        assessments_pd['recorded_dttm'] = assessments_pd['recorded_dttm'].dt.tz_convert(timezone)

    cohort_pd = cohort_df.to_pandas()
    assessments_pd = assessments_pd.merge(
        cohort_pd,
        on='hospitalization_id',
        how='inner'
    )

    assessments_pd = assessments_pd[
        (assessments_pd['recorded_dttm'] >= assessments_pd['start_dttm']) &
        (assessments_pd['recorded_dttm'] <= assessments_pd['end_dttm'])
    ]

    # Coalesce numerical and categorical values
    assessments_pd['assessment_value'] = assessments_pd['numerical_value'].fillna(
        pd.to_numeric(assessments_pd['categorical_value'], errors='coerce')
    )

    id_cols = [col for col in cohort_df.columns if col not in ['start_dttm', 'end_dttm']]
    assessments_pd = assessments_pd[[*id_cols, 'recorded_dttm', 'assessment_category', 'assessment_value']]

    assessments = pl.from_pandas(assessments_pd)

    # Re-standardize datetime columns after conversion
    if timezone:
        assessments = standardize_datetime_columns(
            assessments,
            target_timezone=timezone,
            target_time_unit='ns',
            datetime_columns=['recorded_dttm']
        )

    return assessments.lazy()


def _load_respiratory_support(
    data_directory: str,
    filetype: str,
    hospitalization_ids: List[str],
    cohort_df: pl.DataFrame,
    lookback_hours: int = 24,
    timezone: Optional[str] = None
) -> pl.LazyFrame:
    """Load respiratory support data using pandas for stability."""
    import pandas as pd
    from datetime import timedelta

    file_path = Path(data_directory) / f"clif_respiratory_support.{filetype}"

    if not file_path.exists():
        logger.warning(f"Respiratory support file not found: {file_path}")
        return pl.LazyFrame(schema={
            'hospitalization_id': pl.Utf8,
            'recorded_dttm': pl.Datetime,
            'device_category': pl.Utf8,
            'mode_category': pl.Utf8,
            'fio2_set': pl.Float64,
            'device_rank': pl.Int64
        })

    load_columns = ['hospitalization_id', 'recorded_dttm', 'device_category', 'mode_category',
                    'fio2_set', 'lpm_set', 'tidal_volume_set', 'resp_rate_set']

    logger.debug("Loading respiratory support with pandas...")

    if filetype == 'parquet':
        resp_pd = pd.read_parquet(file_path, columns=load_columns)
    else:
        resp_pd = pd.read_csv(file_path, usecols=load_columns)

    resp_pd['hospitalization_id'] = resp_pd['hospitalization_id'].astype(str)
    resp_pd = resp_pd[resp_pd['hospitalization_id'].isin(hospitalization_ids)]

    if timezone:
        resp_pd['recorded_dttm'] = pd.to_datetime(resp_pd['recorded_dttm'])
        if resp_pd['recorded_dttm'].dt.tz is None:
            resp_pd['recorded_dttm'] = resp_pd['recorded_dttm'].dt.tz_localize('UTC')
        resp_pd['recorded_dttm'] = resp_pd['recorded_dttm'].dt.tz_convert(timezone)

    # Create expanded cohort with lookback
    lookback_delta = timedelta(hours=lookback_hours)
    cohort_pd = cohort_df.to_pandas()
    cohort_pd['start_dttm_lookback'] = cohort_pd['start_dttm'] - lookback_delta
    cohort_pd['end_dttm_original'] = cohort_pd['end_dttm']

    resp_pd = resp_pd.merge(
        cohort_pd,
        on='hospitalization_id',
        how='inner'
    )

    resp_pd = resp_pd[
        (resp_pd['recorded_dttm'] >= resp_pd['start_dttm_lookback']) &
        (resp_pd['recorded_dttm'] <= resp_pd['end_dttm_original'])
    ]

    id_cols = [col for col in cohort_df.columns if col not in ['start_dttm', 'end_dttm']]
    resp_pd = resp_pd[[*id_cols, 'recorded_dttm', 'device_category', 'mode_category',
                       'fio2_set', 'start_dttm', 'end_dttm']]

    # Also include lpm_set if available
    if 'lpm_set' in resp_pd.columns:
        resp_pd = resp_pd[[*id_cols, 'recorded_dttm', 'device_category', 'mode_category',
                           'fio2_set', 'lpm_set', 'start_dttm', 'end_dttm']]

    resp = pl.from_pandas(resp_pd)

    if timezone:
        resp = standardize_datetime_columns(
            resp,
            target_timezone=timezone,
            target_time_unit='ns',
            datetime_columns=['recorded_dttm']
        )

    # Create respiratory support episodes for forward-filling
    resp = _create_resp_support_episodes(resp, id_col='hospitalization_id')

    # Forward-fill FiO2 within mode_cat_id episodes
    resp = resp.sort(['hospitalization_id', 'recorded_dttm'])
    resp = resp.with_columns([
        pl.col('fio2_set').forward_fill().over(['hospitalization_id', 'mode_cat_id']).alias('fio2_set')
    ])

    # Filter to the original SOFA window
    resp = resp.filter(
        (pl.col('recorded_dttm') >= pl.col('start_dttm')) &
        (pl.col('recorded_dttm') <= pl.col('end_dttm'))
    )

    resp = resp.drop(['start_dttm', 'end_dttm'])

    # Add device rank
    resp = resp.with_columns([
        pl.col('device_category').replace(DEVICE_RANK_DICT, default=9).alias('device_rank')
    ])

    return resp.lazy()


def _clean_dose_unit(unit_series: pl.Expr) -> pl.Expr:
    """Clean and standardize dose unit strings using Polars expressions."""
    cleaned = unit_series.str.replace_all(r'\s+', '').str.to_lowercase()

    for replacement, pattern in UNIT_NAMING_VARIANTS.items():
        cleaned = cleaned.str.replace_all(pattern, replacement)

    return cleaned


def _load_and_convert_medications(
    data_directory: str,
    filetype: str,
    hospitalization_ids: List[str],
    cohort_df: pl.DataFrame,
    timezone: Optional[str] = None
) -> pl.LazyFrame:
    """Load medication data using pandas for stability."""
    import pandas as pd

    file_path = Path(data_directory) / f"clif_medication_admin_continuous.{filetype}"

    if not file_path.exists():
        logger.warning(f"Medication admin continuous file not found: {file_path}")
        return pl.LazyFrame(schema={
            'hospitalization_id': pl.Utf8,
            'admin_dttm': pl.Datetime,
            'med_category': pl.Utf8,
            'dose_mcg_kg_min': pl.Float64
        })

    load_columns = ['hospitalization_id', 'admin_dttm', 'med_category', 'med_dose', 'med_dose_unit']

    logger.debug("Loading medications with pandas...")

    if filetype == 'parquet':
        meds_pd = pd.read_parquet(file_path, columns=load_columns)
    else:
        meds_pd = pd.read_csv(file_path, usecols=load_columns)

    meds_pd['hospitalization_id'] = meds_pd['hospitalization_id'].astype(str)

    meds_pd = meds_pd[
        meds_pd['med_category'].isin(REQUIRED_MEDS) &
        meds_pd['hospitalization_id'].isin(hospitalization_ids)
    ]

    if timezone:
        meds_pd['admin_dttm'] = pd.to_datetime(meds_pd['admin_dttm'])
        if meds_pd['admin_dttm'].dt.tz is None:
            meds_pd['admin_dttm'] = meds_pd['admin_dttm'].dt.tz_localize('UTC')
        meds_pd['admin_dttm'] = meds_pd['admin_dttm'].dt.tz_convert(timezone)

    cohort_pd = cohort_df.to_pandas()
    meds_pd = meds_pd.merge(cohort_pd, on='hospitalization_id', how='inner')

    meds_pd = meds_pd[
        (meds_pd['admin_dttm'] >= meds_pd['start_dttm']) &
        (meds_pd['admin_dttm'] <= meds_pd['end_dttm'])
    ]

    meds = pl.from_pandas(meds_pd)

    if timezone:
        meds = standardize_datetime_columns(
            meds,
            target_timezone=timezone,
            target_time_unit='ns',
            datetime_columns=['admin_dttm']
        )

    # Clean dose units
    meds = meds.with_columns([
        _clean_dose_unit(pl.col('med_dose_unit')).alias('dose_unit_clean')
    ])

    # Load weight data
    logger.debug("Loading weight data...")
    weight_file = Path(data_directory) / f"clif_vitals.{filetype}"
    if weight_file.exists():
        if filetype == 'parquet':
            weight_pd = pd.read_parquet(weight_file)
        else:
            weight_pd = pd.read_csv(weight_file)

        weight_pd['hospitalization_id'] = weight_pd['hospitalization_id'].astype(str)
        weight_pd = weight_pd[
            weight_pd['hospitalization_id'].isin(hospitalization_ids) &
            (weight_pd['vital_category'] == 'weight_kg')
        ][['hospitalization_id', 'recorded_dttm', 'vital_value']].copy()

        weight_pd.rename(columns={'vital_value': 'weight_kg'}, inplace=True)
        weight_pd['recorded_dttm'] = pd.to_datetime(weight_pd['recorded_dttm'])

        if timezone:
            if weight_pd['recorded_dttm'].dt.tz is None:
                weight_pd['recorded_dttm'] = weight_pd['recorded_dttm'].dt.tz_localize('UTC')
            weight_pd['recorded_dttm'] = weight_pd['recorded_dttm'].dt.tz_convert(timezone)

        weight_data = pl.from_pandas(weight_pd)

        if timezone:
            weight_data = standardize_datetime_columns(
                weight_data,
                target_timezone=timezone,
                target_time_unit='ns',
                datetime_columns=['recorded_dttm']
            )
    else:
        logger.warning(f"Weight data file not found: {weight_file}")
        weight_data = pl.DataFrame({
            'hospitalization_id': pl.Series([], dtype=pl.Utf8),
            'recorded_dttm': pl.Series([], dtype=pl.Datetime),
            'weight_kg': pl.Series([], dtype=pl.Float64)
        })

    # Sort for join_asof
    meds = meds.sort(['hospitalization_id', 'admin_dttm'])
    weight_data = weight_data.sort(['hospitalization_id', 'recorded_dttm'])

    # Join with weight
    meds = meds.join_asof(
        weight_data,
        left_on='admin_dttm',
        right_on='recorded_dttm',
        by='hospitalization_id',
        strategy='backward'
    )

    # Convert doses to mcg/kg/min
    meds = meds.with_columns([
        pl.when(pl.col('dose_unit_clean').str.contains(r'^mg'))
        .then(pl.col('med_dose') * 1000)
        .when(pl.col('dose_unit_clean').str.contains(r'^g/'))
        .then(pl.col('med_dose') * 1000000)
        .when(pl.col('dose_unit_clean').str.contains(r'^ng'))
        .then(pl.col('med_dose') / 1000)
        .otherwise(pl.col('med_dose'))
        .alias('dose_converted')
    ])

    meds = meds.with_columns([
        pl.when(pl.col('dose_unit_clean').str.contains(r'/hr$'))
        .then(pl.col('dose_converted') / 60)
        .otherwise(pl.col('dose_converted'))
        .alias('dose_converted')
    ])

    meds = meds.with_columns([
        pl.when(pl.col('dose_unit_clean').str.contains(r'/kg'))
        .then(pl.col('dose_converted'))
        .when(pl.col('dose_unit_clean').str.contains(r'/lb'))
        .then(pl.col('dose_converted') * 2.20462)
        .otherwise(pl.col('dose_converted') / pl.col('weight_kg'))
        .alias('dose_mcg_kg_min')
    ])

    id_cols = [col for col in cohort_df.columns if col not in ['start_dttm', 'end_dttm']]
    meds_select = meds.select([
        *id_cols,
        'admin_dttm',
        'med_category',
        'dose_mcg_kg_min'
    ])

    return meds_select.lazy()


# =============================================================================
# Private Helper Functions - SOFA Computation
# =============================================================================

def _impute_pao2_from_spo2(df: pl.DataFrame) -> pl.DataFrame:
    """
    Impute PaO2 from SpO2 using Severinghaus equation.

    Only applies when SpO2 < 97% (above this, oxygen dissociation curve is too flat).
    """
    df = df.with_columns([
        pl.when(pl.col('spo2') < 97)
        .then(
            (
                (
                    (
                        (11700.0 / ((100.0 / pl.col('spo2')) - 1)) ** 2 + 50 ** 3
                    ) ** 0.5 +
                    (11700.0 / ((100.0 / pl.col('spo2')) - 1))
                ) ** (1.0 / 3.0)
            ) -
            (
                (
                    (
                        (11700.0 / ((100.0 / pl.col('spo2')) - 1)) ** 2 + 50 ** 3
                    ) ** 0.5 -
                    (11700.0 / ((100.0 / pl.col('spo2')) - 1))
                ) ** (1.0 / 3.0)
            )
        )
        .otherwise(None)
        .alias('pao2_imputed')
    ])

    return df


def _calculate_concurrent_pf_ratios(
    labs_df: pl.DataFrame,
    resp_df: pl.DataFrame,
    time_tolerance_minutes: int = 240,
    id_cols: List[str] = None
) -> pl.DataFrame:
    """
    Calculate P/F ratios from concurrent PO2 and FiO2 measurements.

    For SOFA-97 specification, P/F ratio must be calculated from PO2 and FiO2
    measured at the same time (or within a tolerance window).
    """
    if id_cols is None:
        id_cols = ['hospitalization_id']

    po2_df = labs_df.filter(pl.col('po2_arterial').is_not_null())

    resp_for_join = resp_df.select([
        *id_cols,
        'recorded_dttm',
        'fio2_set',
        'device_category'
    ])

    po2_df = po2_df.sort([*id_cols, 'lab_result_dttm'])
    resp_for_join = resp_for_join.sort([*id_cols, 'recorded_dttm'])

    po2_with_fio2 = po2_df.join_asof(
        resp_for_join,
        left_on='lab_result_dttm',
        right_on='recorded_dttm',
        by=id_cols,
        tolerance=f'{time_tolerance_minutes}m',
        strategy='backward'
    )

    po2_with_fio2 = po2_with_fio2.with_columns([
        pl.when(
            (pl.col('po2_arterial').is_not_null()) &
            (pl.col('fio2_set').is_not_null()) &
            (pl.col('fio2_set') > 0)
        )
        .then(pl.col('po2_arterial') / pl.col('fio2_set'))
        .otherwise(None)
        .alias('concurrent_pf')
    ])

    concurrent_pf_df = po2_with_fio2.filter(pl.col('concurrent_pf').is_not_null())

    logger.debug(f"Calculated {len(concurrent_pf_df)} concurrent P/F ratios from {len(po2_df)} PO2 measurements")

    return concurrent_pf_df


def _compute_sofa_scores(extremal_df: pl.DataFrame, id_name: str) -> pl.DataFrame:
    """Calculate SOFA component scores from aggregated extremal values."""
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
        'device_rank': pl.Float64
    }

    for col, dtype in required_cols.items():
        if col not in extremal_df.columns:
            extremal_df = extremal_df.with_columns([
                pl.lit(None).cast(dtype).alias(col)
            ])

    # Calculate P/F ratios if not already present
    if 'p_f' not in extremal_df.columns:
        df = extremal_df.with_columns([
            (pl.col('po2_arterial') / pl.col('fio2_set')).alias('p_f'),
            (pl.col('pao2_imputed') / pl.col('fio2_set')).alias('p_f_imputed')
        ])
    else:
        df = extremal_df.with_columns([
            (pl.col('pao2_imputed') / pl.col('fio2_set')).alias('p_f_imputed')
        ])

    # Map device rank back to device category
    if 'device_category' not in df.columns:
        rank_to_device = {v: k for k, v in DEVICE_RANK_DICT.items()}
        df = df.with_columns([
            pl.col('device_rank').replace(rank_to_device, default='Other').alias('device_category')
        ])

    # Calculate SOFA scores
    df = df.with_columns([
        # Cardiovascular
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
        .alias('sofa_cv_97'),

        # Coagulation
        pl.when(pl.col('platelet_count') < 20).then(4)
        .when(pl.col('platelet_count') < 50).then(3)
        .when(pl.col('platelet_count') < 100).then(2)
        .when(pl.col('platelet_count') < 150).then(1)
        .when(pl.col('platelet_count') >= 150).then(0)
        .otherwise(None)
        .alias('sofa_coag'),

        # Liver
        pl.when(pl.col('bilirubin_total') >= 12).then(4)
        .when(pl.col('bilirubin_total') >= 6).then(3)
        .when(pl.col('bilirubin_total') >= 2).then(2)
        .when(pl.col('bilirubin_total') >= 1.2).then(1)
        .when(pl.col('bilirubin_total') < 1.2).then(0)
        .otherwise(None)
        .alias('sofa_liver'),

        # Respiratory
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
        .alias('sofa_resp'),

        # CNS
        pl.when(pl.col('gcs_total') < 6).then(4)
        .when((pl.col('gcs_total') >= 6) & (pl.col('gcs_total') <= 9)).then(3)
        .when((pl.col('gcs_total') >= 10) & (pl.col('gcs_total') <= 12)).then(2)
        .when((pl.col('gcs_total') >= 13) & (pl.col('gcs_total') <= 14)).then(1)
        .when(pl.col('gcs_total') == 15).then(0)
        .otherwise(None)
        .alias('sofa_cns'),

        # Renal
        pl.when(pl.col('creatinine') >= 5).then(4)
        .when(pl.col('creatinine') >= 3.5).then(3)
        .when(pl.col('creatinine') >= 2).then(2)
        .when(pl.col('creatinine') >= 1.2).then(1)
        .when(pl.col('creatinine') < 1.2).then(0)
        .otherwise(None)
        .alias('sofa_renal')
    ])

    # Calculate total SOFA score
    subscore_cols = ['sofa_cv_97', 'sofa_coag', 'sofa_liver', 'sofa_resp', 'sofa_cns', 'sofa_renal']
    df = df.with_columns([
        pl.sum_horizontal([pl.col(c) for c in subscore_cols]).alias('sofa_total')
    ])

    return df


# =============================================================================
# Main Public Function
# =============================================================================

def compute_sofa_polars(
    data_directory: str,
    cohort_df: pl.DataFrame,
    filetype: str = 'parquet',
    id_name: str = 'hospitalization_id',
    extremal_type: str = 'worst',
    fill_na_scores_with_zero: bool = True,
    remove_outliers: bool = True,
    timezone: Optional[str] = None,
    time_unit: str = 'us'
) -> pl.DataFrame:
    """
    Compute SOFA scores using optimized Polars operations.

    This function loads raw data files directly and performs all computations
    including unit conversion without relying on other clifpy methods.

    Parameters
    ----------
    data_directory : str
        Path to directory containing CLIF data files
    cohort_df : pl.DataFrame
        Cohort definition with columns:
        - hospitalization_id (required)
        - start_dttm (required): Start of observation window
        - end_dttm (required): End of observation window
        - Other ID columns (optional, e.g., encounter_block)
    filetype : str, default='parquet'
        File type of data files ('parquet' or 'csv')
    id_name : str, default='hospitalization_id'
        Column name to use for grouping SOFA scores
        (e.g., 'hospitalization_id' or 'encounter_block')
    extremal_type : str, default='worst'
        Type of aggregation ('worst' for min/max values)
    fill_na_scores_with_zero : bool, default=True
        If True, fill missing component scores with 0
    remove_outliers : bool, default=True
        If True, remove physiologically implausible values
    timezone : Optional[str]
        Timezone for datetime parsing (e.g., 'US/Central', 'America/New_York')
    time_unit : str, default='us'
        Time unit for datetime columns ('ms', 'us', 'ns')

    Returns
    -------
    pl.DataFrame
        DataFrame with SOFA scores, one row per id_name
        Columns: id_name, sofa_cv_97, sofa_coag, sofa_liver, sofa_resp,
                sofa_cns, sofa_renal, sofa_total, plus intermediate values

    Examples
    --------
    >>> import polars as pl
    >>> from datetime import datetime
    >>> from clifpy import compute_sofa_polars
    >>>
    >>> cohort = pl.DataFrame({
    ...     'hospitalization_id': ['H1', 'H2'],
    ...     'start_dttm': [datetime(2024,1,1), datetime(2024,1,2)],
    ...     'end_dttm': [datetime(2024,1,5), datetime(2024,1,6)]
    ... })
    >>> sofa_df = compute_sofa_polars('/path/to/data', cohort, timezone='US/Central')

    >>> # With encounter blocks
    >>> cohort = pl.DataFrame({
    ...     'hospitalization_id': ['H1', 'H2', 'H3'],
    ...     'encounter_block': [1, 1, 2],
    ...     'start_dttm': [...],
    ...     'end_dttm': [...]
    ... })
    >>> sofa_df = compute_sofa_polars('/path/to/data', cohort, id_name='encounter_block')
    """
    logger.info("Starting SOFA score computation with Polars")
    logger.info(f"Data directory: {data_directory}")
    logger.info(f"Cohort size: {cohort_df.height} rows")
    logger.info(f"Grouping by: {id_name}")

    # Validate cohort_df
    required_cols = ['hospitalization_id', 'start_dttm', 'end_dttm']
    missing_cols = [col for col in required_cols if col not in cohort_df.columns]
    if missing_cols:
        raise ValueError(f"cohort_df must contain columns: {required_cols}. Missing: {missing_cols}")

    if id_name not in cohort_df.columns:
        raise ValueError(f"id_name '{id_name}' not found in cohort_df columns")

    # Standardize cohort datetime columns
    logger.info(f"Standardizing cohort datetime columns to {timezone} with nanosecond precision")
    cohort_df_local = standardize_datetime_columns(
        cohort_df.clone(),
        target_timezone=timezone,
        target_time_unit='ns',
        datetime_columns=['start_dttm', 'end_dttm']
    )

    # Normalize hospitalization_id to Utf8
    cohort_df_local = cohort_df_local.with_columns([
        pl.col('hospitalization_id').cast(pl.Utf8).alias('hospitalization_id')
    ])

    # Extract unique hospitalization_ids for filtering
    hospitalization_ids = cohort_df_local['hospitalization_id'].unique().to_list()
    logger.info(f"Loading data for {len(hospitalization_ids)} unique hospitalization(s)")

    # Load all required tables
    logger.info("Loading labs data...")
    labs_df = _load_labs(data_directory, filetype, hospitalization_ids, cohort_df_local, timezone)

    logger.info("Loading vitals data...")
    vitals_df = _load_vitals(data_directory, filetype, hospitalization_ids, cohort_df_local, timezone)

    logger.info("Loading patient assessments data...")
    assessments_df = _load_patient_assessments(data_directory, filetype, hospitalization_ids, cohort_df_local, timezone)

    logger.info("Loading respiratory support data...")
    resp_df = _load_respiratory_support(data_directory, filetype, hospitalization_ids, cohort_df_local, lookback_hours=24, timezone=timezone)

    logger.info("Loading and converting medication data...")
    meds_df = _load_and_convert_medications(data_directory, filetype, hospitalization_ids, cohort_df_local, timezone)

    # Collect each data source BEFORE combining
    logger.info("Collecting individual data sources before combining...")

    logger.info("Collecting labs...")
    labs_collected = labs_df.collect()
    logger.debug(f"Labs: {len(labs_collected)} rows")

    logger.info("Collecting vitals...")
    vitals_collected = vitals_df.collect()
    logger.debug(f"Vitals: {len(vitals_collected)} rows")

    logger.info("Collecting assessments...")
    assessments_collected = assessments_df.collect()
    logger.debug(f"Assessments: {len(assessments_collected)} rows")

    logger.info("Collecting respiratory...")
    resp_collected = resp_df.collect()
    logger.debug(f"Respiratory: {len(resp_collected)} rows")

    logger.info("Collecting medications...")
    meds_collected = meds_df.collect()
    logger.debug(f"Medications: {len(meds_collected)} rows")

    # Prepare collected data with renamed and standardized time columns
    logger.info("Preparing data for combination...")
    labs_collected = labs_collected.rename({'lab_result_dttm': 'event_time'}).with_columns([
        pl.col('event_time').dt.cast_time_unit(time_unit)
    ])

    vitals_collected = vitals_collected.rename({'recorded_dttm': 'event_time'}).with_columns([
        pl.col('event_time').dt.cast_time_unit(time_unit)
    ])

    assessments_collected = assessments_collected.rename({'recorded_dttm': 'event_time'}).with_columns([
        pl.col('event_time').dt.cast_time_unit(time_unit)
    ])

    resp_collected = resp_collected.rename({'recorded_dttm': 'event_time'}).with_columns([
        pl.col('event_time').dt.cast_time_unit(time_unit)
    ])

    meds_collected = meds_collected.rename({'admin_dttm': 'event_time'}).with_columns([
        pl.col('event_time').dt.cast_time_unit(time_unit)
    ])

    # Combine COLLECTED DataFrames
    logger.info("Combining collected data sources...")
    combined_collected = pl.concat([
        labs_collected,
        vitals_collected,
        assessments_collected,
        resp_collected,
        meds_collected
    ], how='diagonal')
    logger.debug(f"Combined data: {len(combined_collected)} rows")

    # Clean up individual collected frames
    del labs_collected, vitals_collected, assessments_collected, resp_collected, meds_collected

    # Apply outlier removal
    if remove_outliers:
        logger.info("Applying outlier removal...")
        combined_collected = combined_collected.with_columns([
            pl.when(
                (pl.col('lab_value_numeric').is_not_null()) &
                (pl.col('lab_category') == 'po2_arterial') &
                (pl.col('lab_value_numeric') >= 0) &
                (pl.col('lab_value_numeric') <= 700)
            )
            .then(pl.col('lab_value_numeric'))
            .when(
                (pl.col('lab_value_numeric').is_not_null()) &
                (pl.col('lab_category') == 'po2_arterial')
            )
            .then(None)
            .otherwise(pl.col('lab_value_numeric'))
            .alias('lab_value_numeric'),

            pl.when(
                (pl.col('fio2_set').is_not_null()) &
                (pl.col('fio2_set') >= 0.21) &
                (pl.col('fio2_set') <= 1)
            )
            .then(pl.col('fio2_set'))
            .when(pl.col('fio2_set').is_not_null())
            .then(None)
            .otherwise(pl.col('fio2_set'))
            .alias('fio2_set'),

            pl.when(
                (pl.col('vital_value').is_not_null()) &
                (pl.col('vital_category') == 'spo2') &
                (pl.col('vital_value') >= 50) &
                (pl.col('vital_value') <= 100)
            )
            .then(pl.col('vital_value'))
            .when(
                (pl.col('vital_value').is_not_null()) &
                (pl.col('vital_category') == 'spo2')
            )
            .then(None)
            .otherwise(pl.col('vital_value'))
            .alias('vital_value')
        ])

    # Aggregate extremal values in long format BEFORE pivoting
    logger.info("Aggregating extremal values in long format...")

    max_labs = ['creatinine', 'bilirubin_total']
    max_meds = ['norepinephrine', 'epinephrine', 'dopamine', 'dobutamine']
    min_labs = ['platelet_count', 'po2_arterial']

    labs_max_agg = combined_collected.filter(
        pl.col('lab_category').is_in(max_labs)
    ).group_by([id_name, 'lab_category']).agg([
        pl.col('lab_value_numeric').max().alias('value')
    ])

    labs_min_agg = combined_collected.filter(
        pl.col('lab_category').is_in(min_labs)
    ).group_by([id_name, 'lab_category']).agg([
        pl.col('lab_value_numeric').min().alias('value')
    ])

    labs_agg = pl.concat([labs_max_agg, labs_min_agg], how='vertical').with_columns([
        pl.lit('lab').alias('data_type')
    ])

    vitals_agg = combined_collected.filter(pl.col('vital_category').is_not_null()).group_by(
        [id_name, 'vital_category']
    ).agg([
        pl.col('vital_value').min().alias('value')
    ]).rename({'vital_category': 'lab_category'}).with_columns([
        pl.lit('vital').alias('data_type')
    ])

    meds_agg = combined_collected.filter(pl.col('med_category').is_not_null()).group_by(
        [id_name, 'med_category']
    ).agg([
        pl.col('dose_mcg_kg_min').max().alias('value')
    ]).rename({'med_category': 'lab_category'}).with_columns([
        pl.lit('med').alias('data_type')
    ])

    assess_agg = combined_collected.filter(pl.col('assessment_category').is_not_null()).group_by(
        [id_name, 'assessment_category']
    ).agg([
        pl.col('assessment_value').min().alias('value')
    ]).rename({'assessment_category': 'lab_category'}).with_columns([
        pl.lit('assessment').alias('data_type')
    ])

    # Concatenate all aggregated results
    aggregated_df = pl.concat([labs_agg, vitals_agg, meds_agg, assess_agg], how='vertical')
    logger.debug(f"Aggregated data shape: {aggregated_df.height:,} rows x {aggregated_df.width} columns")

    # Pivot to wide format
    logger.info("Pivoting aggregated data to wide format...")
    combined_df = aggregated_df.pivot(
        index=id_name,
        on='lab_category',
        values='value'
    )

    # Add _mcg_kg_min suffix to medication columns
    med_cols_to_rename = {col: f"{col}_mcg_kg_min"
                          for col in combined_df.columns
                          if col in max_meds}
    if med_cols_to_rename:
        combined_df = combined_df.rename(med_cols_to_rename)

    logger.debug(f"Pivoted data shape: {combined_df.height:,} rows x {combined_df.width} columns")

    # Impute PaO2 from SpO2
    logger.info("Imputing PaO2 from SpO2...")
    combined_df = _impute_pao2_from_spo2(combined_df)

    # Calculate concurrent P/F ratios (SOFA-97 specification)
    logger.info("Calculating concurrent P/F ratios...")
    labs_df_collected = _load_labs(data_directory, filetype, hospitalization_ids, cohort_df_local, timezone).collect()
    resp_df_collected = _load_respiratory_support(data_directory, filetype, hospitalization_ids, cohort_df_local, 24, timezone).collect()

    # Extract labs data with PO2
    labs_with_po2 = labs_df_collected.filter(
        (pl.col('lab_category') == 'po2_arterial') &
        (pl.col('lab_value_numeric').is_not_null())
    ).select([
        id_name,
        'lab_result_dttm',
        pl.col('lab_value_numeric').alias('po2_arterial')
    ] + [col for col in labs_df_collected.columns if col in cohort_df_local.columns and col not in [id_name, 'lab_result_dttm', 'lab_value_numeric', 'lab_category', 'start_dttm', 'end_dttm']])

    # Calculate concurrent P/F
    id_cols = [col for col in cohort_df_local.columns if col not in ['start_dttm', 'end_dttm']]
    concurrent_pf_df = _calculate_concurrent_pf_ratios(
        labs_with_po2,
        resp_df_collected,
        time_tolerance_minutes=240,
        id_cols=id_cols
    )

    # Aggregate concurrent P/F
    logger.info(f"Aggregating concurrent P/F ratios by {id_name}...")
    pf_agg = concurrent_pf_df.group_by(id_name).agg([
        pl.col('concurrent_pf').min().alias('p_f'),
        pl.col('po2_arterial').min().alias('po2_arterial'),
        pl.col('fio2_set').max().alias('fio2_set'),
        pl.col('device_category').sort_by('concurrent_pf').first().alias('device_category')
    ])

    pf_agg = pf_agg.with_columns([
        pl.col('device_category').replace(DEVICE_RANK_DICT, default=9).alias('device_rank')
    ])

    # Merge P/F data with other aggregated values
    combined_df = combined_df.join(pf_agg, on=id_name, how='left')

    # Free memory
    logger.info("Freeing memory from intermediate DataFrames...")
    del labs_df, vitals_df, assessments_df, resp_df, meds_df
    del labs_df_collected, resp_df_collected, labs_with_po2
    del aggregated_df
    gc.collect()

    # Compute SOFA scores
    logger.info("Computing SOFA scores...")
    sofa_df = _compute_sofa_scores(combined_df, id_name)

    # Fill NA scores with zero if requested
    if fill_na_scores_with_zero:
        logger.info("Filling missing scores with 0...")
        subscore_cols = ['sofa_cv_97', 'sofa_coag', 'sofa_liver', 'sofa_resp', 'sofa_cns', 'sofa_renal']
        sofa_df = sofa_df.with_columns([
            pl.col(c).fill_null(0) for c in subscore_cols
        ])
        # Recalculate total
        sofa_df = sofa_df.with_columns([
            pl.sum_horizontal([pl.col(c) for c in subscore_cols]).alias('sofa_total')
        ])

    logger.info(f"SOFA computation complete. Result shape: {sofa_df.height} rows x {sofa_df.width} columns")

    return sofa_df


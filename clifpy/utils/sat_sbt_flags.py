"""
SAT and SBT Flag Identification Module

This module provides functions to identify Spontaneous Awakening Trial (SAT)
and Spontaneous Breathing Trial (SBT) events from CLIF-formatted ICU data.

SAT identifies when sedation is interrupted/reduced based on objective
documentation from medication_admin_continuous and respiratory_support tables.

SBT identifies when patients are ready for extubation based on respiratory
and hemodynamic stability criteria.

Based on the implementation from:
https://github.com/Common-Longitudinal-ICU-data-Format/CLIF_rule_based_SAT_SBT_signature
"""

import pandas as pd
import numpy as np
from typing import Optional, Literal
from tqdm import tqdm
import logging

logger = logging.getLogger('clifpy.utils.sat_sbt_flags')


def identify_sat_events(
    cohort: pd.DataFrame,
    threshold_hours: float = 4.0,
    window_start_hour: int = 22,
    window_end_hour: int = 6,
    time_column: str = 'event_time',
    hospitalization_id_column: str = 'hospitalization_id',
    device_category_column: str = 'device_category',
    location_category_column: str = 'location_category',
    sedation_column: str = 'min_sedation_dose_2',
    paralytics_column: str = 'max_paralytics',
    show_progress: bool = True
) -> pd.DataFrame:
    """
    Identify Spontaneous Awakening Trial (SAT) events from ICU data.
    
    SAT events are identified when patients meet the following conditions for
    a cumulative threshold duration within a specified time window:
    - Patient is on invasive mechanical ventilation (IMV)
    - Patient has sedation > 0
    - Patient is in ICU
    - No paralytics administered
    
    Parameters
    ----------
    cohort : pd.DataFrame
        DataFrame containing merged patient data with respiratory support,
        medication administration, and location information. Must include
        columns specified in the other parameters.
    threshold_hours : float, default=4.0
        Minimum cumulative hours that conditions must be met within the
        time window to identify a SAT event
    window_start_hour : int, default=22
        Hour of day (0-23) when the evaluation window starts.
        Default is 22 (10 PM previous day)
    window_end_hour : int, default=6
        Hour of day (0-23) when the evaluation window ends.
        Default is 6 (6 AM current day)
    time_column : str, default='event_time'
        Name of the datetime column for event timestamps
    hospitalization_id_column : str, default='hospitalization_id'
        Name of the column containing hospitalization identifiers
    device_category_column : str, default='device_category'
        Name of the column containing respiratory device category
        (should include 'imv' for invasive mechanical ventilation)
    location_category_column : str, default='location_category'
        Name of the column containing patient location category
        (should include 'icu' for intensive care unit)
    sedation_column : str, default='min_sedation_dose_2'
        Name of the column containing sedation dose values
    paralytics_column : str, default='max_paralytics'
        Name of the column containing paralytic medication values
    show_progress : bool, default=True
        Whether to display progress bar during processing
        
    Returns
    -------
    pd.DataFrame
        DataFrame with identified SAT events containing:
        - hospitalization_id: Hospital encounter identifier
        - current_day_key: Date of the identified SAT event
        - event_time_at_threshold: Timestamp when threshold was reached
        
    Examples
    --------
    >>> sat_events = identify_sat_events(
    ...     cohort=merged_data,
    ...     threshold_hours=4.0
    ... )
    >>> print(f"Found {len(sat_events)} SAT events")
    
    Notes
    -----
    The default time window (10 PM to 6 AM) represents overnight periods
    when SAT protocols are commonly evaluated. The window spans from
    10 PM on the previous calendar day to 6 AM on the current day.
    
    The function identifies contiguous segments where all conditions are met
    and calculates cumulative duration. Once the threshold is reached in a
    segment, that hospitalization-day is flagged as having a SAT event.
    """
    # Validate required columns
    required_cols = [
        time_column, hospitalization_id_column, device_category_column,
        location_category_column, sedation_column, paralytics_column
    ]
    missing_cols = [col for col in required_cols if col not in cohort.columns]
    if missing_cols:
        raise ValueError(f"Missing required columns: {missing_cols}")
    
    # Create a copy to avoid modifying original data
    df = cohort.copy()
    
    # Preprocessing
    df = df.sort_values(by=[hospitalization_id_column, time_column]).reset_index(drop=True)
    df[f'{device_category_column}_ffill'] = df.groupby(hospitalization_id_column)[device_category_column].ffill()
    df[f'{location_category_column}_ffill'] = df.groupby(hospitalization_id_column)[location_category_column].ffill()
    df[time_column] = pd.to_datetime(df[time_column])
    
    # Precompute condition flags
    df['all_conditions_check'] = (
        (df[f'{device_category_column}_ffill'].str.lower() == 'imv') &
        (df[sedation_column] > 0) &
        (df[f'{location_category_column}_ffill'].str.lower() == 'icu') &
        (df[paralytics_column] <= 0)
    ).astype(int)
    
    result = []
    
    # Create day key for filtering ventilated days
    df['_day_key'] = df[time_column].dt.normalize()
    df['_hosp_day_key'] = df[hospitalization_id_column].astype(str) + '_' + df['_day_key'].astype(str)
    vented_days = df[df[device_category_column].str.lower() == 'imv']['_hosp_day_key'].unique()
    df_filtered = df[df['_hosp_day_key'].isin(vented_days)]
    
    # Build hospitalization groups dictionary (for accessing full hospitalization data)
    hosp_groups = {
        hosp_id: group_df.copy().sort_values(time_column)
        for hosp_id, group_df in df.groupby(hospitalization_id_column)
    }
    
    # Group by hospitalization and day
    hosp_grouped = df_filtered.groupby([hospitalization_id_column, df_filtered[time_column].dt.normalize()])
    
    # Time window offsets
    window_start_offset = pd.Timedelta(hours=window_start_hour) - pd.Timedelta(days=1)
    window_end_offset = pd.Timedelta(hours=window_end_hour)
    
    threshold_td = pd.Timedelta(hours=threshold_hours)
    
    iterator = tqdm(hosp_grouped, desc='Identifying SAT events') if show_progress else hosp_grouped
    
    for (hosp_id, date), group in iterator:
        start_time = date + window_start_offset
        end_time = date + window_end_offset
        
        # Use full hospitalization data so events before midnight can contribute
        hosp_df = hosp_groups[hosp_id]
        window_df = hosp_df[(hosp_df[time_column] >= start_time) & (hosp_df[time_column] <= end_time)].copy()
        
        if window_df.empty or not window_df['all_conditions_check'].any():
            continue
        
        # Identify contiguous segments where conditions are met
        window_df['condition_met_group'] = (
            window_df['all_conditions_check'] != window_df['all_conditions_check'].shift()
        ).cumsum()
        valid_segments = window_df[window_df['all_conditions_check'] == 1].groupby('condition_met_group')
        
        for _, segment in valid_segments:
            segment = segment.sort_values(time_column)
            segment['duration'] = segment[time_column].diff().fillna(pd.Timedelta(seconds=0))
            segment['cumulative_duration'] = segment['duration'].cumsum()
            
            if segment['cumulative_duration'].iloc[-1] >= threshold_td:
                # Get the event_time when duration crosses threshold
                event_time_at_threshold = segment[segment['cumulative_duration'] >= threshold_td].iloc[0][time_column]
                result.append({
                    hospitalization_id_column: hosp_id,
                    'current_day_key': date,
                    'event_time_at_threshold': event_time_at_threshold
                })
                break  # Move to next hospitalization-day
    
    return pd.DataFrame(result)


def identify_sbt_events(
    cohort: pd.DataFrame,
    stability_mode: Literal['Standard', 'Respiratory_Stability', 'Hemodynamic_Stability', 'Both_stabilities'] = 'Standard',
    threshold_hours: float = 6.0,
    window_start_hour: int = 22,
    window_end_hour: int = 6,
    time_column: str = 'event_time',
    hospitalization_id_column: str = 'hospitalization_id',
    device_category_column: str = 'device_category',
    location_category_column: str = 'location_category',
    paralytics_column: str = 'max_paralytics',
    respiratory_stability_column: Optional[str] = 'Respiratory_Stability',
    hemodynamic_stability_column: Optional[str] = 'Hemodynamic_Stability_by_NEE',
    show_progress: bool = True
) -> pd.DataFrame:
    """
    Identify Spontaneous Breathing Trial (SBT) eligible days from ICU data.
    
    SBT eligibility is determined when patients meet ventilation and stability
    conditions for a cumulative threshold duration within a specified time window.
    
    Parameters
    ----------
    cohort : pd.DataFrame
        DataFrame containing patient data with respiratory support, location,
        and stability information. Must include columns specified in other parameters.
    stability_mode : {'Standard', 'Respiratory_Stability', 'Hemodynamic_Stability', 'Both_stabilities'}
        Determines which stability criteria to apply:
        - 'Standard': Only requires IMV and ICU location
        - 'Respiratory_Stability': Requires respiratory stability flag
        - 'Hemodynamic_Stability': Requires hemodynamic stability flag
        - 'Both_stabilities': Requires both stability flags
    threshold_hours : float, default=6.0
        Minimum cumulative hours that IMV in controlled mode must be met
        within the time window
    window_start_hour : int, default=22
        Hour of day (0-23) when the evaluation window starts.
        Default is 22 (10 PM previous day)
    window_end_hour : int, default=6
        Hour of day (0-23) when the evaluation window ends.
        Default is 6 (6 AM current day)
    time_column : str, default='event_time'
        Name of the datetime column for event timestamps
    hospitalization_id_column : str, default='hospitalization_id'
        Name of the column containing hospitalization identifiers
    device_category_column : str, default='device_category'
        Name of the column containing respiratory device category
    location_category_column : str, default='location_category'
        Name of the column containing patient location category
    paralytics_column : str, default='max_paralytics'
        Name of the column containing paralytic medication values
    respiratory_stability_column : str, optional, default='Respiratory_Stability'
        Name of the column containing respiratory stability flag (0/1)
        Required when stability_mode includes respiratory stability
    hemodynamic_stability_column : str, optional, default='Hemodynamic_Stability_by_NEE'
        Name of the column containing hemodynamic stability flag (0/1)
        Required when stability_mode includes hemodynamic stability
    show_progress : bool, default=True
        Whether to display progress bar during processing
        
    Returns
    -------
    pd.DataFrame
        Input DataFrame with additional columns:
        - eligible_day: Binary flag (0/1) indicating SBT-eligible days
        - IMV_Controlled_met_time: Timestamp when threshold was reached
        - vent_day: Binary flag indicating ventilated days
        - vent_day_without_paralytics: Binary flag for vent days without paralytics
        
    Raises
    ------
    ValueError
        If stability_mode is not one of the valid options
        If required columns for the selected stability_mode are missing
        
    Examples
    --------
    >>> # Standard SBT identification (no stability requirements)
    >>> sbt_df = identify_sbt_events(
    ...     cohort=merged_data,
    ...     stability_mode='Standard'
    ... )
    >>> eligible_days = sbt_df[sbt_df['eligible_day'] == 1]
    
    >>> # SBT with both stability requirements
    >>> sbt_df = identify_sbt_events(
    ...     cohort=merged_data,
    ...     stability_mode='Both_stabilities',
    ...     respiratory_stability_column='resp_stable',
    ...     hemodynamic_stability_column='hemo_stable'
    ... )
    
    Notes
    -----
    The function marks entire hospitalization-days as eligible when the
    threshold is met during the evaluation window. The IMV_Controlled_met_time
    records the specific timestamp when the threshold was reached.
    
    Days with any paralytic use during the window are excluded from eligibility.
    """
    # Validate stability_mode
    valid_modes = ['Standard', 'Respiratory_Stability', 'Hemodynamic_Stability', 'Both_stabilities']
    if stability_mode not in valid_modes:
        raise ValueError(f"Invalid stability_mode: {stability_mode}. Must be one of {valid_modes}")
    
    # Validate required columns based on stability mode
    required_cols = [
        time_column, hospitalization_id_column, device_category_column,
        location_category_column, paralytics_column
    ]
    
    if stability_mode in ['Respiratory_Stability', 'Both_stabilities']:
        if respiratory_stability_column not in cohort.columns:
            raise ValueError(f"Column '{respiratory_stability_column}' required for stability_mode '{stability_mode}'")
        required_cols.append(respiratory_stability_column)
    
    if stability_mode in ['Hemodynamic_Stability', 'Both_stabilities']:
        if hemodynamic_stability_column not in cohort.columns:
            raise ValueError(f"Column '{hemodynamic_stability_column}' required for stability_mode '{stability_mode}'")
        required_cols.append(hemodynamic_stability_column)
    
    missing_cols = [col for col in required_cols if col not in cohort.columns]
    if missing_cols:
        raise ValueError(f"Missing required columns: {missing_cols}")
    
    # Create a copy to avoid modifying original data
    df = cohort.copy()
    
    # Ensure event_time is datetime and sort
    df[time_column] = pd.to_datetime(df[time_column])
    df = df.sort_values([hospitalization_id_column, time_column]).reset_index(drop=False)
    
    # Create IMV flag based on stability mode
    if stability_mode == 'Standard':
        df['IMV_flag'] = (
            (df[device_category_column].str.lower() == 'imv') &
            (df[location_category_column].str.lower() == 'icu')
        )
    elif stability_mode == 'Respiratory_Stability':
        df['IMV_flag'] = (
            (df[device_category_column].str.lower() == 'imv') &
            (df[location_category_column].str.lower() == 'icu') &
            (df[respiratory_stability_column] == 1)
        )
    elif stability_mode == 'Hemodynamic_Stability':
        df['IMV_flag'] = (
            (df[device_category_column].str.lower() == 'imv') &
            (df[location_category_column].str.lower() == 'icu') &
            (df[hemodynamic_stability_column] == 1)
        )
    elif stability_mode == 'Both_stabilities':
        df['IMV_flag'] = (
            (df[device_category_column].str.lower() == 'imv') &
            (df[location_category_column].str.lower() == 'icu') &
            (df[respiratory_stability_column] == 1) &
            (df[hemodynamic_stability_column] == 1)
        )
    
    logger.info(f'Analyzing SBT eligibility using: {stability_mode}')
    
    # Initialize result columns
    df['IMV_Controlled_met_time'] = pd.NaT
    df['eligible_day'] = 0
    df['vent_day'] = 0
    df['vent_day_without_paralytics'] = 0
    
    # For grouping by day
    df['current_day'] = df[time_column].dt.normalize()
    
    # Build hospitalization groups dictionary
    hosp_groups = {
        hosp_id: group_df.copy().sort_values(time_column)
        for hosp_id, group_df in df.groupby(hospitalization_id_column)
    }
    
    # Define thresholds and time windows
    threshold_td = pd.Timedelta(hours=threshold_hours)
    window_start_offset = pd.Timedelta(hours=window_start_hour) - pd.Timedelta(days=1)
    window_end_offset = pd.Timedelta(hours=window_end_hour)
    
    # Create day key for filtering ventilated days
    df['_hosp_day_key'] = df[hospitalization_id_column].astype(str) + '_' + df['current_day'].astype(str)
    vented_day = df[df[device_category_column].str.lower() == 'imv']['_hosp_day_key'].unique()
    
    # Group by hospitalization and current day
    groups = df[df['_hosp_day_key'].isin(vented_day)].groupby([hospitalization_id_column, 'current_day'])
    
    iterator = tqdm(groups, desc=f"Evaluating SBT eligibility ({stability_mode})") if show_progress else groups
    
    for (hosp_id, curr_day), day_group in iterator:
        df.loc[day_group.index, 'vent_day'] = 1
        
        # Define window based on current day
        window_start = curr_day + window_start_offset
        window_end = curr_day + window_end_offset
        
        # Use full hospitalization data so events before midnight can contribute
        hosp_df = hosp_groups[hosp_id]
        window_df = hosp_df[(hosp_df[time_column] >= window_start) & (hosp_df[time_column] <= window_end)].copy()
        
        # Check for paralytics
        if window_df[paralytics_column].max() > 0:
            continue
        
        df.loc[day_group.index, 'vent_day_without_paralytics'] = 1
        
        if window_df.empty:
            continue
        
        if not window_df['IMV_flag'].any():
            continue
        
        # Identify contiguous segments where IMV_flag is True
        window_df['seg'] = (window_df['IMV_flag'] != window_df['IMV_flag'].shift()).cumsum()
        valid_segs = window_df[window_df['IMV_flag']].groupby('seg')
        
        condition_met = False
        for seg_id, seg_df in valid_segs:
            seg_df = seg_df.sort_values(time_column)
            seg_df['duration'] = seg_df[time_column].diff().fillna(pd.Timedelta(seconds=0))
            seg_df['cum_duration'] = seg_df['duration'].cumsum()
            
            if seg_df['cum_duration'].iloc[-1] >= threshold_td:
                # Find the first row where cumulative duration reaches threshold
                flag_row = seg_df[seg_df['cum_duration'] >= threshold_td].iloc[0]
                flag_idx = flag_row.name
                flag_time = flag_row[time_column]
                df.loc[flag_idx, 'IMV_Controlled_met_time'] = flag_time
                condition_met = True
                break
        
        # Mark eligible day if condition was met
        if condition_met:
            df.loc[day_group.index, 'eligible_day'] = 1
    
    # Drop temporary columns
    df = df.drop(columns=['IMV_flag', 'current_day', '_hosp_day_key'], errors='ignore')
    
    return df


def calculate_respiratory_stability(
    cohort: pd.DataFrame,
    mode_category_column: str = 'mode_category',
    pressure_support_column: str = 'pressure_support_set',
    peep_column: str = 'peep_set',
    fio2_column: str = 'fio2_set',
    ps_threshold: float = 8.0,
    peep_threshold: float = 8.0,
    fio2_threshold: float = 0.5
) -> pd.DataFrame:
    """
    Calculate respiratory stability flag based on ventilator settings.
    
    Respiratory stability is defined as having low ventilator support settings:
    - Pressure support ≤ threshold (default 8 cmH2O)
    - PEEP ≤ threshold (default 8 cmH2O)
    - FiO2 ≤ threshold (default 0.5 or 50%)
    - Mode is pressure support or CPAP
    
    Parameters
    ----------
    cohort : pd.DataFrame
        DataFrame with respiratory support data
    mode_category_column : str, default='mode_category'
        Column name for ventilator mode category
    pressure_support_column : str, default='pressure_support_set'
        Column name for pressure support setting
    peep_column : str, default='peep_set'
        Column name for PEEP setting
    fio2_column : str, default='fio2_set'
        Column name for FiO2 setting
    ps_threshold : float, default=8.0
        Maximum pressure support for stability (cmH2O)
    peep_threshold : float, default=8.0
        Maximum PEEP for stability (cmH2O)
    fio2_threshold : float, default=0.5
        Maximum FiO2 for stability (0.0-1.0)
        
    Returns
    -------
    pd.DataFrame
        Input DataFrame with added 'Respiratory_Stability' column (0/1)
    """
    df = cohort.copy()
    
    mode_lower = df[mode_category_column].fillna('').str.lower()
    is_ps_cpap = mode_lower.str.contains('pressure support|cpap', regex=True, na=False)
    
    df['Respiratory_Stability'] = (
        is_ps_cpap &
        (df[pressure_support_column] <= ps_threshold) &
        (df[peep_column] <= peep_threshold) &
        (df[fio2_column] <= fio2_threshold)
    ).astype(int)
    
    return df


def calculate_hemodynamic_stability(
    cohort: pd.DataFrame,
    norepinephrine_column: str = 'norepinephrine',
    epinephrine_column: str = 'epinephrine',
    phenylephrine_column: str = 'phenylephrine',
    vasopressin_column: str = 'vasopressin',
    dopamine_column: str = 'dopamine',
    nee_threshold: float = 0.1
) -> pd.DataFrame:
    """
    Calculate hemodynamic stability flag based on vasopressor requirements.
    
    Hemodynamic stability is typically defined using norepinephrine equivalent
    (NEE) dose, which combines multiple vasopressors into a single metric.
    
    NEE = norepinephrine + epinephrine + phenylephrine/10 + vasopressin*2.5 + dopamine/100
    
    Stability is achieved when NEE ≤ threshold (default 0.1 mcg/kg/min)
    
    Parameters
    ----------
    cohort : pd.DataFrame
        DataFrame with medication administration data
    norepinephrine_column : str, default='norepinephrine'
        Column name for norepinephrine dose (mcg/kg/min)
    epinephrine_column : str, default='epinephrine'
        Column name for epinephrine dose (mcg/kg/min)
    phenylephrine_column : str, default='phenylephrine'
        Column name for phenylephrine dose (mcg/kg/min)
    vasopressin_column : str, default='vasopressin'
        Column name for vasopressin dose (units/min, converted to NEE)
    dopamine_column : str, default='dopamine'
        Column name for dopamine dose (mcg/kg/min)
    nee_threshold : float, default=0.1
        Maximum NEE for hemodynamic stability (mcg/kg/min)
        
    Returns
    -------
    pd.DataFrame
        Input DataFrame with added columns:
        - 'NEE': Norepinephrine equivalent dose
        - 'Hemodynamic_Stability_by_NEE': Binary flag (0/1)
        
    Notes
    -----
    The NEE calculation uses standard conversion factors from critical care
    literature. Missing values are treated as 0.
    """
    df = cohort.copy()
    
    # Calculate NEE, treating missing values as 0
    norepi = df[norepinephrine_column].fillna(0)
    epi = df[epinephrine_column].fillna(0)
    phenyl = df[phenylephrine_column].fillna(0)
    vaso = df[vasopressin_column].fillna(0)
    dopa = df[dopamine_column].fillna(0)
    
    df['NEE'] = norepi + epi + (phenyl / 10) + (vaso * 2.5) + (dopa / 100)
    df['Hemodynamic_Stability_by_NEE'] = (df['NEE'] <= nee_threshold).astype(int)
    
    return df

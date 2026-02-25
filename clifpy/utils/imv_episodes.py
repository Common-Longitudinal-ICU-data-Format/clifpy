"""
IMV Episode Detection for CLIF Respiratory Support Data.

This module provides functions to identify discrete invasive mechanical ventilation
(IMV) episodes from respiratory_support table data.
"""

import pandas as pd
import numpy as np
from typing import Optional, List


# Observed variables that indicate active ventilation (excluding resp_rate_obs)
IMV_OBS_COLUMNS = [
    "tidal_volume_obs",
    "plateau_pressure_obs",
    "peak_inspiratory_pressure_obs",
    "peep_obs",
    "minute_vent_obs",
    "mean_airway_pressure_obs",
]

# Non-IMV device categories
NON_IMV_DEVICE_CATEGORIES = [
    "NIPPV",
    "CPAP",
    "High Flow NC",
    "Face Mask",
    "Trach Collar",
    "Nasal Cannula",
    "Room Air",
    "Other",
]


def detect_imv_episodes(
    respiratory_support: pd.DataFrame,
    min_gap_hours: float = 1.0,
    include_tracheostomy: bool = True,
) -> pd.DataFrame:
    """
    Detect invasive mechanical ventilation (IMV) episodes from respiratory support data.
    
    This function identifies discrete IMV episodes by leveraging the fact that observed
    ventilator measurements (tidal volume, pressures, etc.) don't occur when the 
    ventilator is in standby or disconnected.
    
    Episode Logic:
    - IMV_start: First documented *_obs variable (OTHER than resp_rate_obs) 
      where device_category = 'IMV'
    - IMV_end: Last *_obs followed by ANY documentation of non-IMV respiratory 
      data (any other device_category OR lpm_set > 0)
    
    Parameters
    ----------
    respiratory_support : pd.DataFrame
        Respiratory support table with required columns:
        - hospitalization_id
        - recorded_dttm
        - device_category
        - lpm_set
        - Plus at least one *_obs column (tidal_volume_obs, peep_obs, etc.)
        
    min_gap_hours : float, default=1.0
        Minimum gap in hours between IMV observations to consider as separate episodes.
        Shorter gaps are treated as continuous ventilation.
        
    include_tracheostomy : bool, default=True
        If True, include tracheostomy status in output if available.
        
    Returns
    -------
    pd.DataFrame
        DataFrame with one row per IMV episode containing:
        - hospitalization_id : str
            Patient hospitalization identifier
        - imv_episode_id : int
            Sequential episode number within hospitalization (1, 2, 3, ...)
        - imv_start_dttm : datetime
            Start of IMV episode (first *_obs measurement)
        - imv_end_dttm : datetime
            End of IMV episode (last *_obs before non-IMV documentation)
        - duration_hours : float
            Episode duration in hours
        - has_tracheostomy : int (0/1), optional
            Whether tracheostomy was documented during episode
            
    Raises
    ------
    ValueError
        If required columns are missing from input DataFrame
        
    Examples
    --------
    >>> from clifpy.utils import detect_imv_episodes
    >>> episodes = detect_imv_episodes(respiratory_support_df)
    >>> print(episodes.head())
       hospitalization_id  imv_episode_id     imv_start_dttm       imv_end_dttm  duration_hours
    0              H00001               1  2024-01-01 08:00:00  2024-01-03 14:00:00           54.0
    1              H00001               2  2024-01-05 10:00:00  2024-01-07 16:00:00           54.0
    2              H00002               1  2024-01-02 12:00:00  2024-01-02 18:00:00            6.0
    
    Notes
    -----
    The logic excludes resp_rate_obs from IMV detection because respiratory rate
    can be documented even when the ventilator is on standby or the patient is
    breathing spontaneously.
    
    See Also
    --------
    stitch_encounters : For linking related hospitalizations
    create_wide_dataset : For creating hourly respiratory data
    """
    # Validate required columns
    required_cols = ["hospitalization_id", "recorded_dttm", "device_category", "lpm_set"]
    missing_cols = [col for col in required_cols if col not in respiratory_support.columns]
    if missing_cols:
        raise ValueError(f"Missing required columns: {missing_cols}")
    
    # Check for at least one *_obs column
    available_obs_cols = [col for col in IMV_OBS_COLUMNS if col in respiratory_support.columns]
    if not available_obs_cols:
        raise ValueError(
            f"At least one observation column required. Expected one of: {IMV_OBS_COLUMNS}"
        )
    
    # Work with a copy and ensure datetime
    df = respiratory_support.copy()
    df["recorded_dttm"] = pd.to_datetime(df["recorded_dttm"])
    
    # Sort by hospitalization and time
    df = df.sort_values(["hospitalization_id", "recorded_dttm"]).reset_index(drop=True)
    
    # Create flag for any *_obs value present (excluding resp_rate_obs)
    df["has_imv_obs"] = df[available_obs_cols].notna().any(axis=1)
    
    # Create flag for IMV device
    df["is_imv"] = df["device_category"].str.upper() == "IMV"
    
    # Create flag for non-IMV documentation (other device OR lpm > 0)
    df["is_non_imv"] = (
        df["device_category"].isin(NON_IMV_DEVICE_CATEGORIES) |
        (df["lpm_set"].notna() & (df["lpm_set"] > 0))
    )
    
    # Active IMV = IMV device with observed measurements
    df["active_imv"] = df["is_imv"] & df["has_imv_obs"]
    
    # Process each hospitalization
    episodes_list = []
    
    for hosp_id, hosp_df in df.groupby("hospitalization_id"):
        hosp_episodes = _detect_episodes_for_hospitalization(
            hosp_df, 
            hosp_id, 
            min_gap_hours=min_gap_hours,
            include_tracheostomy=include_tracheostomy
        )
        episodes_list.extend(hosp_episodes)
    
    # Create output DataFrame
    if not episodes_list:
        # Return empty DataFrame with correct schema
        cols = ["hospitalization_id", "imv_episode_id", "imv_start_dttm", 
                "imv_end_dttm", "duration_hours"]
        if include_tracheostomy and "tracheostomy" in respiratory_support.columns:
            cols.append("has_tracheostomy")
        return pd.DataFrame(columns=cols)
    
    episodes_df = pd.DataFrame(episodes_list)
    
    # Calculate duration
    episodes_df["duration_hours"] = (
        (episodes_df["imv_end_dttm"] - episodes_df["imv_start_dttm"])
        .dt.total_seconds() / 3600
    ).round(2)
    
    return episodes_df


def _detect_episodes_for_hospitalization(
    hosp_df: pd.DataFrame,
    hosp_id: str,
    min_gap_hours: float,
    include_tracheostomy: bool,
) -> List[dict]:
    """
    Detect IMV episodes within a single hospitalization.
    
    Parameters
    ----------
    hosp_df : pd.DataFrame
        Respiratory support data for one hospitalization, sorted by time.
    hosp_id : str
        Hospitalization identifier.
    min_gap_hours : float
        Minimum gap to consider separate episodes.
    include_tracheostomy : bool
        Whether to track tracheostomy status.
        
    Returns
    -------
    List[dict]
        List of episode dictionaries.
    """
    episodes = []
    
    # Get active IMV rows
    imv_rows = hosp_df[hosp_df["active_imv"]].copy()
    
    if imv_rows.empty:
        return episodes
    
    # Calculate time gaps between consecutive IMV observations
    imv_rows = imv_rows.sort_values("recorded_dttm").reset_index(drop=True)
    imv_rows["time_gap_hours"] = (
        imv_rows["recorded_dttm"].diff().dt.total_seconds() / 3600
    )
    
    # Also check if there's non-IMV documentation between IMV rows
    # by looking at the original dataframe
    all_times = hosp_df[["recorded_dttm", "is_non_imv", "active_imv"]].copy()
    
    # Identify episode boundaries
    # New episode starts when:
    # 1. First IMV observation, OR
    # 2. Gap > min_gap_hours, OR  
    # 3. Non-IMV documentation occurred since last IMV obs
    
    episode_id = 0
    current_episode_start = None
    current_episode_end = None
    current_has_trach = 0
    
    for idx, row in imv_rows.iterrows():
        start_new_episode = False
        
        if current_episode_start is None:
            # First IMV observation
            start_new_episode = True
        elif row["time_gap_hours"] > min_gap_hours:
            # Gap too large
            start_new_episode = True
        else:
            # Check for non-IMV documentation between last IMV and current
            between_mask = (
                (all_times["recorded_dttm"] > current_episode_end) &
                (all_times["recorded_dttm"] < row["recorded_dttm"]) &
                (all_times["is_non_imv"])
            )
            if between_mask.any():
                start_new_episode = True
        
        if start_new_episode:
            # Save previous episode if exists
            if current_episode_start is not None:
                episode_id += 1
                episode_dict = {
                    "hospitalization_id": hosp_id,
                    "imv_episode_id": episode_id,
                    "imv_start_dttm": current_episode_start,
                    "imv_end_dttm": current_episode_end,
                }
                if include_tracheostomy and "tracheostomy" in hosp_df.columns:
                    episode_dict["has_tracheostomy"] = current_has_trach
                episodes.append(episode_dict)
            
            # Start new episode
            current_episode_start = row["recorded_dttm"]
            current_episode_end = row["recorded_dttm"]
            current_has_trach = 0
        
        # Update current episode end
        current_episode_end = row["recorded_dttm"]
        
        # Track tracheostomy
        if include_tracheostomy and "tracheostomy" in hosp_df.columns:
            if pd.notna(row.get("tracheostomy")) and row.get("tracheostomy") == 1:
                current_has_trach = 1
    
    # Save final episode
    if current_episode_start is not None:
        episode_id += 1
        episode_dict = {
            "hospitalization_id": hosp_id,
            "imv_episode_id": episode_id,
            "imv_start_dttm": current_episode_start,
            "imv_end_dttm": current_episode_end,
        }
        if include_tracheostomy and "tracheostomy" in hosp_df.columns:
            episode_dict["has_tracheostomy"] = current_has_trach
        episodes.append(episode_dict)
    
    return episodes


def calculate_ventilator_free_days(
    imv_episodes: pd.DataFrame,
    hospitalization: pd.DataFrame,
    observation_window_days: int = 28,
    mortality_column: Optional[str] = "discharge_category",
    death_values: Optional[List[str]] = None,
) -> pd.DataFrame:
    """
    Calculate ventilator-free days (VFDs) from IMV episodes.
    
    Ventilator-free days are defined as days alive and free of mechanical
    ventilation during the observation window. Patients who die receive 0 VFDs.
    
    Parameters
    ----------
    imv_episodes : pd.DataFrame
        Output from detect_imv_episodes().
    hospitalization : pd.DataFrame
        Hospitalization table with admission_dttm and discharge info.
    observation_window_days : int, default=28
        Number of days for VFD calculation (typically 28).
    mortality_column : str, default="discharge_category"
        Column name containing mortality/discharge information.
    death_values : List[str], optional
        Values in mortality_column that indicate death.
        Default: ["Expired", "Dead", "Deceased", "Death"]
        
    Returns
    -------
    pd.DataFrame
        DataFrame with hospitalization_id and ventilator_free_days.
        
    Examples
    --------
    >>> episodes = detect_imv_episodes(resp_df)
    >>> vfds = calculate_ventilator_free_days(episodes, hosp_df)
    >>> print(vfds.head())
       hospitalization_id  ventilator_free_days  total_imv_days  died
    0              H00001                  22.5             5.5     0
    1              H00002                   0.0             3.2     1
    """
    if death_values is None:
        death_values = ["Expired", "Dead", "Deceased", "Death"]
    
    # Get hospitalization info
    hosp_info = hospitalization[["hospitalization_id", "admission_dttm"]].copy()
    if mortality_column in hospitalization.columns:
        hosp_info["died"] = hospitalization[mortality_column].isin(death_values).astype(int)
    else:
        hosp_info["died"] = 0
    
    hosp_info["admission_dttm"] = pd.to_datetime(hosp_info["admission_dttm"])
    hosp_info["window_end"] = hosp_info["admission_dttm"] + pd.Timedelta(days=observation_window_days)
    
    # Calculate total IMV days per hospitalization
    if imv_episodes.empty:
        hosp_info["total_imv_days"] = 0.0
    else:
        imv_by_hosp = imv_episodes.groupby("hospitalization_id")["duration_hours"].sum() / 24
        imv_by_hosp = imv_by_hosp.reset_index()
        imv_by_hosp.columns = ["hospitalization_id", "total_imv_days"]
        hosp_info = hosp_info.merge(imv_by_hosp, on="hospitalization_id", how="left")
        hosp_info["total_imv_days"] = hosp_info["total_imv_days"].fillna(0)
    
    # Cap IMV days at observation window
    hosp_info["total_imv_days"] = hosp_info["total_imv_days"].clip(upper=observation_window_days)
    
    # Calculate VFDs (0 if died)
    hosp_info["ventilator_free_days"] = np.where(
        hosp_info["died"] == 1,
        0,
        observation_window_days - hosp_info["total_imv_days"]
    )
    
    # Round to 1 decimal
    hosp_info["ventilator_free_days"] = hosp_info["ventilator_free_days"].round(1)
    hosp_info["total_imv_days"] = hosp_info["total_imv_days"].round(1)
    
    return hosp_info[["hospitalization_id", "ventilator_free_days", "total_imv_days", "died"]]


import pandas as pd
import numpy as np
from typing import Tuple, Optional


def stitch_encounters(
    hospitalization: pd.DataFrame, 
    adt: pd.DataFrame, 
    time_interval: int = 6
) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """
    Stitches together related hospital encounters that occur within a specified time interval.
    
    This function identifies and groups hospitalizations that occur within a specified time window
    of each other (default 6 hours), treating them as a single continuous encounter. This is useful
    for handling cases where patients are discharged and readmitted quickly (e.g., ED to inpatient
    transfers).
    
    Parameters
    ----------
    hospitalization : pd.DataFrame
        Hospitalization table with required columns:
        - patient_id
        - hospitalization_id
        - admission_dttm
        - discharge_dttm
        - age_at_admission
        - admission_type_category
        - discharge_category
        
    adt : pd.DataFrame
        ADT (Admission/Discharge/Transfer) table with required columns:
        - hospitalization_id
        - in_dttm
        - out_dttm
        - location_category
        - hospital_id
        
    time_interval : int, default=6
        Number of hours between discharge and next admission to consider encounters linked.
        If a patient is readmitted within this window, the encounters are stitched together.
        
    Returns
    -------
    Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]
        hospitalization_stitched : pd.DataFrame
            Enhanced hospitalization data with encounter_block column
        adt_stitched : pd.DataFrame
            Enhanced ADT data with encounter_block column
        encounter_mapping : pd.DataFrame
            Mapping of hospitalization_id to encounter_block
            
    Raises
    ------
    ValueError
        If required columns are missing from input DataFrames
    
    Examples
    --------
    >>> hosp_stitched, adt_stitched, mapping = stitch_encounters(
    ...     hospitalization_df, 
    ...     adt_df, 
    ...     time_interval=12  # 12-hour window
    ... )
    """
    # Validate input DataFrames
    hosp_required_cols = [
        "patient_id", "hospitalization_id", "admission_dttm", 
        "discharge_dttm", "age_at_admission", "admission_type_category", 
        "discharge_category"
    ]
    adt_required_cols = [
        "hospitalization_id", "in_dttm", "out_dttm", 
        "location_category", "hospital_id"
    ]
    
    missing_hosp_cols = [col for col in hosp_required_cols if col not in hospitalization.columns]
    if missing_hosp_cols:
        raise ValueError(f"Missing required columns in hospitalization DataFrame: {missing_hosp_cols}")
    
    missing_adt_cols = [col for col in adt_required_cols if col not in adt.columns]
    if missing_adt_cols:
        raise ValueError(f"Missing required columns in ADT DataFrame: {missing_adt_cols}")
    hospitalization_filtered = hospitalization[["patient_id","hospitalization_id","admission_dttm",
                                                "discharge_dttm","age_at_admission", "admission_type_category", "discharge_category"]].copy()
    hospitalization_filtered['admission_dttm'] = pd.to_datetime(hospitalization_filtered['admission_dttm'])
    hospitalization_filtered['discharge_dttm'] = pd.to_datetime(hospitalization_filtered['discharge_dttm'])

    hosp_adt_join = pd.merge(hospitalization_filtered[["patient_id","hospitalization_id","age_at_admission","admission_type_category",
                                                       "admission_dttm","discharge_dttm",
                                                        "discharge_category"]], 
                      adt[["hospitalization_id","in_dttm","out_dttm","location_category","hospital_id"]],
                 on="hospitalization_id",how="left")

    hospital_cat = hosp_adt_join[["hospitalization_id","in_dttm","out_dttm","hospital_id"]]

    # Step 1: Sort by patient_id and admission_dttm
    hospital_block = hosp_adt_join[["patient_id","hospitalization_id","admission_dttm","discharge_dttm", "age_at_admission",  "discharge_category", "admission_type_category"]]
    hospital_block = hospital_block.drop_duplicates()
    hospital_block = hospital_block.sort_values(by=["patient_id", "admission_dttm"]).reset_index(drop=True)
    hospital_block = hospital_block[["patient_id","hospitalization_id","admission_dttm","discharge_dttm", "age_at_admission",  "discharge_category", "admission_type_category"]]

    # Step 2: Calculate time between discharge and next admission
    hospital_block["next_admission_dttm"] = hospital_block.groupby("patient_id")["admission_dttm"].shift(-1)
    hospital_block["discharge_to_next_admission_hrs"] = (
        (hospital_block["next_admission_dttm"] - hospital_block["discharge_dttm"]).dt.total_seconds() / 3600
    )

    # Step 3: Create linked column based on time_interval
    eps = 1e-6  # tiny tolerance for float rounding
    hospital_block["linked_hrs"] = (
        hospital_block["discharge_to_next_admission_hrs"].le(time_interval + eps).fillna(False)
    )

    # Sort values to ensure correct order
    hospital_block = hospital_block.sort_values(by=["patient_id", "admission_dttm"]).reset_index(drop=True)

    # Initialize encounter_block with row indices + 1
    hospital_block['encounter_block'] = hospital_block.index + 1

    # Iteratively propagate the encounter_block values
    while True:
      shifted = hospital_block['encounter_block'].shift(-1)
      mask = hospital_block['linked_hrs'] & (hospital_block['patient_id'] == hospital_block['patient_id'].shift(-1))
      old_values = hospital_block['encounter_block'].copy()
      hospital_block.loc[mask, 'encounter_block'] = shifted[mask]
      if hospital_block['encounter_block'].equals(old_values):
          break

    hospital_block['encounter_block'] = hospital_block['encounter_block'].bfill().astype('int32')
    hospital_block = pd.merge(hospital_block,hospital_cat,how="left",on="hospitalization_id")
    hospital_block = hospital_block.sort_values(by=["patient_id", "admission_dttm","in_dttm","out_dttm"]).reset_index(drop=True)
    hospital_block = hospital_block.drop_duplicates()

    hospital_block2 = hospital_block.groupby(['patient_id','encounter_block']).agg(
        admission_dttm=pd.NamedAgg(column='admission_dttm', aggfunc='min'),
        discharge_dttm=pd.NamedAgg(column='discharge_dttm', aggfunc='max'),
        admission_type_category=pd.NamedAgg(column='admission_type_category', aggfunc='first'),
        discharge_category=pd.NamedAgg(column='discharge_category', aggfunc='last'),
        hospital_id = pd.NamedAgg(column='hospital_id', aggfunc='last'),
        age_at_admission=pd.NamedAgg(column='age_at_admission', aggfunc='last'),
        list_hospitalization_id=pd.NamedAgg(column='hospitalization_id', aggfunc=lambda x: sorted(x.unique()))
    ).reset_index()

    df = pd.merge(hospital_block[["patient_id",
                                  "hospitalization_id",
                                  "encounter_block"]].drop_duplicates(),
             hosp_adt_join[["hospitalization_id","location_category","in_dttm","out_dttm"]], on="hospitalization_id",how="left")

    df = pd.merge(df,hospital_block2[["encounter_block",
                                      "admission_dttm",
                                      "discharge_dttm",
                                      "discharge_category",
                                      "admission_type_category",
                                      "age_at_admission",
                                      "hospital_id",
                                     "list_hospitalization_id"]],on="encounter_block",how="left")
    df = df.drop_duplicates(subset=["patient_id","encounter_block","in_dttm","out_dttm","location_category"])
    
    # Create the mapping DataFrame
    encounter_mapping = hospital_block[["hospitalization_id", "encounter_block"]].drop_duplicates()
    
    # Create hospitalization_stitched DataFrame
    hospitalization_stitched = hospitalization.merge(
        encounter_mapping, 
        on="hospitalization_id", 
        how="left"
    )
    
    # Create adt_stitched DataFrame  
    adt_stitched = adt.merge(
        encounter_mapping,
        on="hospitalization_id",
        how="left"
    )
    
    return hospitalization_stitched, adt_stitched, encounter_mapping
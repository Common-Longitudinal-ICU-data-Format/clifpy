from typing import Optional, Dict
import pandas as pd
import duckdb
from .base_table import BaseTable


class MedicationAdminContinuous(BaseTable):
    """
    Medication administration continuous table wrapper inheriting from BaseTable.
    
    This class handles medication administration continuous data and validations
    while leveraging the common functionality provided by BaseTable.
    """
    
    def __init__(
        self,
        data_directory: str = None,
        filetype: str = None,
        timezone: str = "UTC",
        output_directory: Optional[str] = None,
        data: Optional[pd.DataFrame] = None
    ):
        """
        Initialize the medication_admin_continuous table.
        
        Parameters:
            data_directory (str): Path to the directory containing data files
            filetype (str): Type of data file (csv, parquet, etc.)
            timezone (str): Timezone for datetime columns
            output_directory (str, optional): Directory for saving output files and logs
            data (pd.DataFrame, optional): Pre-loaded data to use instead of loading from file
        """
        # For backward compatibility, handle the old signature
        if data_directory is None and filetype is None and data is not None:
            # Old signature: medication_admin_continuous(data)
            # Use dummy values for required parameters
            data_directory = "."
            filetype = "parquet"
        
        # Load medication mappings
        self._med_category_to_group = None
        
        super().__init__(
            data_directory=data_directory,
            filetype=filetype,
            timezone=timezone,
            output_directory=output_directory,
            data=data
        )
        
        # Load medication-specific schema data
        self._load_medication_schema_data()

    def _load_medication_schema_data(self):
        """Load medication category to group mappings from the YAML schema."""
        if self.schema:
            self._med_category_to_group = self.schema.get('med_category_to_group_mapping', {})

    @property
    def med_category_to_group_mapping(self) -> Dict[str, str]:
        """Get the medication category to group mapping from the schema."""
        return self._med_category_to_group.copy() if self._med_category_to_group else {}

    # Additional medication-specific methods can be added here if needed
    # The base functionality (validate, isvalid, from_file) is inherited from BaseTable
    
    def standardize_dose_unit(self, vitals_df: pd.DataFrame, med_df: pd.DataFrame = None) -> pd.DataFrame:
        """
        Standardize everything to mcg/min.
        Assumes the presentation of the following columns:
        - med_dose_unit: the original unit of the dose
        - med_dose: the original dose
        - weight_kg: the most recent weight of the patient
        """
        if med_df is None:
            med_df = self.df
        if med_df is None:
            raise ValueError("No data provided")
        
        if 'weight_kg' not in med_df.columns:
            query = """
            SELECT m.*
                , v.vital_value as weight_kg
                , v.recorded_dttm as weight_recorded_dttm
                -- rn = 1 for the weight w/ the latest recorded_dttm (and thus most recent)
                , ROW_NUMBER() OVER (
                    PARTITION BY m.hospitalization_id, m.admin_dttm, m.med_category
                    ORDER BY v.recorded_dttm DESC
                    ) as rn
            FROM med_df m
            LEFT JOIN vitals_df v 
                ON m.hospitalization_id = v.hospitalization_id 
                AND v.vital_category = 'weight_kg' AND v.vital_value IS NOT NULL
                AND v.recorded_dttm <= m.admin_dttm  -- only past weights
            QUALIFY (rn = 1) -- OR (weight_kg IS NULL) -- include meds even if no weight found
            ORDER BY m.hospitalization_id, m.admin_dttm, m.med_category, rn
            """
            med_df = duckdb.sql(query).to_df()
        
        # check if the required columns are present
        required_columns = {'med_dose_unit', 'med_dose', 'weight_kg'}
        missing_columns = required_columns - set(med_df.columns)
        if missing_columns:
            raise ValueError(f"The following columns are required: {missing_columns}")
        
        query = f"""
        SELECT *
            , LOWER(med_dose_unit) AS med_dose_unit_lower
            , CASE WHEN regexp_matches(med_dose_unit_lower, '/h(r|our)?\\b') THEN 1/60.0
                WHEN regexp_matches(med_dose_unit_lower, '/m(in|inute)?\\b') THEN 1.0
                ELSE NULL END as time_multiplier
            , CASE WHEN contains(med_dose_unit_lower, '/kg/') THEN weight_kg
                ELSE 1 END AS pt_weight_adjustment
            , CASE WHEN contains(med_dose_unit_lower, 'mcg/') THEN 1.0
                WHEN contains(med_dose_unit_lower, 'mg/') THEN 1000.0
                WHEN contains(med_dose_unit_lower, 'ng/') THEN 0.001
                WHEN contains(med_dose_unit_lower, 'milli') THEN 0.001
                WHEN contains(med_dose_unit_lower, 'units/') THEN 1
                WHEN contains(med_dose_unit_lower, 'ml/') THEN 1.0
                WHEN contains(med_dose_unit_lower, 'l/') AND NOT contains(med_dose_unit_lower, 'ml/') THEN 1000.0
                ELSE NULL END as dose_multiplier
            , med_dose * time_multiplier * pt_weight_adjustment * dose_multiplier as med_dose_converted
            , CASE WHEN contains(med_dose_unit_lower, 'units/') THEN 'units/min'
                WHEN contains(med_dose_unit_lower, 'l/') THEN 'ml/min'
                ELSE 'mcg/min' END as med_dose_unit_converted
        FROM med_df
        """
        return duckdb.sql(query).to_df()
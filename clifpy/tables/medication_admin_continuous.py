from typing import Optional, Dict, Tuple, Union, Set
import pandas as pd
from pyarrow import BooleanArray
from .base_table import BaseTable
import duckdb

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
    
    # Medication-specific methods can be added here if needed
    # The base functionality (validate, isvalid, from_file) is inherited from BaseTable
    
    @property
    def _acceptable_dose_unit_patterns(self) -> Set[str]:
        """
        Returns a set of post-cleaning acceptable dose unit patterns (all in lowercase, no white space)
        """
        acceptable_amounts = {
            "ml", "l", 
            "milli-units", "units", 
            "mcg", "mg", "ng"
            }
        acceptable_weights = {'/kg', ''}
        acceptable_times = {'/h', '/hr', '/hour', '/m', '/min', '/minute'}
        # find the cartesian product of the three sets
        return {a + b + c for a in acceptable_amounts for b in acceptable_weights for c in acceptable_times}
    
    def _standardize_dose_unit_pattern(
        self, med_df: Optional[pd.DataFrame] = None
        ) -> Tuple[pd.DataFrame, Union[Dict, bool]]:
        """
        Standardize dose unit to a consistent, convertible pattern, e.g. 'mL/ hr' -> 'ml/hr'.
        
        - removes white space (including internal spaces)
        - converts to lowercase
        - uses self.df by default if no argument is provided
        
        Returns:
            A tuple where the first element is the input df with an appended column 'med_dose_unit_clean'; 
            the second element is either a dictionary of the unaccounted-for dose units and their counts 
            if any; or False if no unaccounted-for dose units
        """
        if med_df is None:
            med_df = self.df
        if med_df is None:
            raise ValueError("No data provided")
        
        # Make a copy to avoid SettingWithCopyWarning
        med_df = med_df.copy()
        
        # Remove ALL whitespace (including internal) and convert to lowercase
        med_df['med_dose_unit_clean'] = med_df['med_dose_unit'].str.replace(r'\s+', '', regex=True).str.lower()
        
        # find any rows with unseen, unaccounted-for dose units which we do not know how to convert
        mask = ~med_df['med_dose_unit_clean'].isin(self._acceptable_dose_unit_patterns)
        unaccounted: pd.DataFrame = med_df[mask]
        
        if not unaccounted.empty:
            unaccounted_unit_counts = unaccounted.value_counts(subset=['med_dose_unit_clean']).to_dict()
            self.logger.warning(f"The following dose units are not accounted by the converter: {unaccounted_unit_counts}")
            return med_df, unaccounted_unit_counts
        
        return med_df, False
        
    def convert_dose_to_same_units(self, vitals_df: pd.DataFrame, med_df: pd.DataFrame = None) -> pd.DataFrame:
        """
        Standardize everything to mcg/min; ml/min; or units/min.
        
        Requires the following columns in the input `med_df`:
        - med_dose_unit: the original unit of the dose (case-insensitive)
        - med_dose: the original dose
        - weight_kg: the most recent weight of the patient
        
        Returns:
            pd.DataFrame: The input `med_df` with appended columns 'med_dose_converted' and 'med_dose_unit_converted'
        """
        if med_df is None:
            med_df = self.df
        if med_df is None:
            raise ValueError("No data provided")
        
        if 'weight_kg' not in med_df.columns:
            self.logger.info("No weight_kg column found, adding the most recent from vitals")
            query = """
            SELECT m.*
                , v.vital_value as weight_kg
                , v.recorded_dttm as weight_recorded_dttm
                , ROW_NUMBER() OVER (
                    PARTITION BY m.hospitalization_id, m.admin_dttm, m.med_category
                    ORDER BY v.recorded_dttm DESC
                    ) as rn
            FROM med_df m
            LEFT JOIN vitals_df v 
                ON m.hospitalization_id = v.hospitalization_id 
                AND v.vital_category = 'weight_kg' AND v.vital_value IS NOT NULL
                AND v.recorded_dttm <= m.admin_dttm  -- only past weights
            -- rn = 1 for the weight w/ the latest recorded_dttm (and thus most recent)
            QUALIFY (rn = 1) 
            ORDER BY m.hospitalization_id, m.admin_dttm, m.med_category, rn
            """
            med_df = duckdb.sql(query).to_df()
        
        # check if the required columns are present
        required_columns = {'med_dose_unit', 'med_dose', 'weight_kg'}
        missing_columns = required_columns - set(med_df.columns)
        if missing_columns:
            raise ValueError(f"The following column(s) are required but not found: {missing_columns}")
        
        med_df, unaccounted = self._standardize_dose_unit_pattern(med_df)
        if not unaccounted:
            self.logger.info("No unaccounted-for dose units found, continuing with conversion")
        else:
            self.logger.warning(f"Unaccounted-for dose units found: {unaccounted}")
        
        query = f"""
        SELECT *
            , CASE WHEN regexp_matches(med_dose_unit_clean, '/h(r|our)?\\b') THEN 1/60.0
                WHEN regexp_matches(med_dose_unit_clean, '/m(in|inute)?\\b') THEN 1.0
                ELSE NULL END as time_multiplier
            , CASE WHEN contains(med_dose_unit_clean, '/kg/') THEN weight_kg
                ELSE 1 END AS pt_weight_multiplier
            , CASE WHEN contains(med_dose_unit_clean, 'mcg/') THEN 1.0
                WHEN contains(med_dose_unit_clean, 'mg/') THEN 1000.0
                WHEN contains(med_dose_unit_clean, 'ng/') THEN 0.001
                WHEN contains(med_dose_unit_clean, 'milli') THEN 0.001
                WHEN contains(med_dose_unit_clean, 'units/') THEN 1
                WHEN contains(med_dose_unit_clean, 'ml/') THEN 1.0
                WHEN contains(med_dose_unit_clean, 'l/') AND NOT contains(med_dose_unit_clean, 'ml/') THEN 1000.0
                ELSE NULL END as amount_multiplier
            , med_dose * time_multiplier * pt_weight_multiplier * amount_multiplier as med_dose_converted
            , CASE WHEN contains(med_dose_unit_clean, 'units/') THEN 'units/min'
                WHEN contains(med_dose_unit_clean, 'l/') THEN 'ml/min'
                ELSE 'mcg/min' END as med_dose_unit_converted
        FROM med_df
        """
        return duckdb.sql(query).to_df()
    
    
    
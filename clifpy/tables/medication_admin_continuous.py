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
        Initialize the MedicationAdminContinuous table.
        
        This class handles continuous medication administration data, including validation,
        dose unit standardization, and unit conversion capabilities.
        
        Parameters
        ----------
        data_directory : str, optional
            Path to the directory containing data files. If None and data is provided,
            defaults to current directory.
        filetype : str, optional
            Type of data file (csv, parquet, etc.). If None and data is provided,
            defaults to 'parquet'.
        timezone : str, default="UTC"
            Timezone for datetime columns. Used for proper timestamp handling.
        output_directory : str, optional
            Directory for saving output files and logs. If not specified, outputs
            are saved to the current working directory.
        data : pd.DataFrame, optional
            Pre-loaded DataFrame to use instead of loading from file. Supports
            backward compatibility with direct DataFrame initialization.
        
        Notes
        -----
        The class supports two initialization patterns:
        1. Loading from file: provide data_directory and filetype
        2. Direct DataFrame: provide data parameter (legacy support)
        
        Upon initialization, the class loads medication schema data including
        category-to-group mappings from the YAML schema.
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
        """
        Load medication-specific schema data from the YAML configuration.
        
        This method extracts medication category to group mappings from the loaded
        schema, which are used for medication classification and grouping operations.
        The mappings define relationships between medication categories (e.g., 'Antibiotics')
        and their broader therapeutic groups (e.g., 'Antimicrobials').
        
        The method is called automatically during initialization after the base
        schema is loaded.
        """
        if self.schema:
            self._med_category_to_group = self.schema.get('med_category_to_group_mapping', {})

    @property
    def med_category_to_group_mapping(self) -> Dict[str, str]:
        """
        Get the medication category to group mapping from the schema.
        
        Returns
        -------
        Dict[str, str]
            A dictionary mapping medication categories to their therapeutic groups.
            Returns a copy to prevent external modification of the internal mapping.
            Returns an empty dict if no mappings are loaded.
        
        Examples
        --------
        >>> mac = MedicationAdminContinuous(data)
        >>> mappings = mac.med_category_to_group_mapping
        >>> mappings['Antibiotics']
        'Antimicrobials'
        """
        return self._med_category_to_group.copy() if self._med_category_to_group else {}
    
    # Medication-specific methods can be added here if needed
    # The base functionality (validate, isvalid, from_file) is inherited from BaseTable
    
    @property
    def _acceptable_dose_unit_patterns(self) -> Set[str]:
        """
        Generate the set of acceptable dose unit patterns after standardization.
        
        This property creates all valid combinations of dose units that the converter
        can handle. All patterns are in lowercase with no whitespace, matching the
        format produced by _normalize_dose_unit_pattern().
        
        Returns
        -------
        Set[str]
            A set containing all acceptable dose unit patterns. Patterns are formed
            by combining:
            - Amount units: ml, l, milli-units, units, mcg, mg, ng
            - Weight qualifiers: /kg or none
            - Time units: /h, /hr, /hour, /m, /min, /minute
        
        Examples
        --------
        Valid patterns include: 'mcg/kg/hr', 'mg/min', 'units/hr', 'ml/hr'
        Invalid patterns include: 'mcg/lb/min', 'mg/sec', 'tablespoon/hr'
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
    
    def _normalize_dose_unit_pattern(
        self, med_df: Optional[pd.DataFrame] = None
        ) -> Tuple[pd.DataFrame, Union[Dict, bool]]:
        """
        Standardize medication dose units to a consistent, convertible pattern.
        
        This method normalizes dose unit strings by removing all whitespace (including
        internal spaces) and converting to lowercase. For example, 'mL/ hr' becomes 'ml/hr'.
        It also identifies any dose units that don't match acceptable patterns.
        
        Parameters
        ----------
        med_df : pd.DataFrame, optional
            DataFrame containing a 'med_dose_unit' column to standardize.
            If None, uses self.df. Must not be None if self.df is also None.
        
        Returns
        -------
        Tuple[pd.DataFrame, Union[Dict, bool]]
            A tuple containing:
            - DataFrame with added 'med_dose_unit_clean' column containing standardized units
            - Either:
                - False if all units are acceptable
                - Dict mapping unrecognized unit patterns to their occurrence counts
        
        Raises
        ------
        ValueError
            If both med_df parameter and self.df are None.
        
        Warnings
        --------
        Logs a warning if unrecognized dose units are found.
        
        Examples
        --------
        >>> df = pd.DataFrame({'med_dose_unit': ['ML/HR', 'mcg / kg/ min', 'invalid']})
        >>> result_df, unrecognized = mac._normalize_dose_unit_pattern(df)
        >>> result_df['med_dose_unit_clean'].tolist()
        ['ml/hr', 'mcg/kg/min', 'invalid']
        >>> unrecognized
        {'invalid': 1}
        """
        if med_df is None:
            med_df = self.df
        if med_df is None:
            raise ValueError("No data provided")
        
        # Make a copy to avoid SettingWithCopyWarning
        med_df = med_df.copy()
        
        # Remove ALL whitespace (including internal) and convert to lowercase
        med_df['med_dose_unit_clean'] = med_df['med_dose_unit'].str.replace(r'\s+', '', regex=True).str.lower()
        
        # find any rows with unseen, unrecognized dose units which we do not know how to convert
        mask = ~med_df['med_dose_unit_clean'].isin(self._acceptable_dose_unit_patterns)
        unrecognized: pd.DataFrame = med_df[mask]
        
        if not unrecognized.empty:
            unrecognized_unit_counts = unrecognized.value_counts('med_dose_unit_clean').to_dict()
            self.logger.warning(f"The following dose units are not recognized by the converter: {unrecognized_unit_counts}")
            return med_df, unrecognized_unit_counts
        
        return med_df, False
        
    def convert_dose_to_limited_units(self, vitals_df: pd.DataFrame, med_df: pd.DataFrame = None) -> pd.DataFrame:
        """
        Convert medication doses to standardized units per minute.
        
        This method converts all medication doses to one of three standard units:
        - mcg/min for mass-based medications
        - ml/min for volume-based medications  
        - units/min for unit-based medications
        
        The conversion handles different time scales (per hour vs per minute) and
        weight-based dosing (per kg) by incorporating patient weights from vitals.
        
        Parameters
        ----------
        vitals_df : pd.DataFrame
            DataFrame containing patient vital signs, must include:
            - hospitalization_id: Patient identifier
            - recorded_dttm: Timestamp of vital recording
            - vital_category: Type of vital (looks for 'weight_kg')
            - vital_value: Numeric value of the vital
        med_df : pd.DataFrame, optional
            DataFrame containing medication administration data. If None, uses self.df.
            Required columns:
            - hospitalization_id: Patient identifier
            - admin_dttm: Medication administration timestamp
            - med_dose_unit: Original dose unit (case-insensitive)
            - med_dose: Original dose value
            - med_category: Medication category (used for SQL query)
            Optional columns:
            - weight_kg: Patient weight; if absent, pulled from vitals_df
        
        Returns
        -------
        pd.DataFrame
            Original med_df with additional columns:
            - med_dose_unit_clean: Standardized unit pattern
            - weight_kg: Patient weight used for conversion (if applicable)
            - med_dose_converted: Dose value in standardized units
            - med_dose_unit_converted: Standardized unit ('mcg/min', 'ml/min', or 'units/min')
            - Additional calculation columns (time_multiplier, pt_weight_multiplier, amount_multiplier)
        
        Raises
        ------
        ValueError
            If med_df is None and self.df is also None, or if required columns are missing.
        
        Warnings
        --------
        Logs warnings for unrecognized dose units that cannot be converted.
        
        Notes
        -----
        - Weight-based dosing (/kg) uses the most recent weight prior to administration
        - Unrecognized dose units result in NULL converted values
        - The conversion preserves the original columns and adds new ones
        
        Examples
        --------
        >>> vitals = pd.DataFrame({
        ...     'hospitalization_id': ['H001'],
        ...     'recorded_dttm': pd.to_datetime(['2023-01-01']),
        ...     'vital_category': ['weight_kg'],
        ...     'vital_value': [70.0]
        ... })
        >>> meds = pd.DataFrame({
        ...     'hospitalization_id': ['H001'],
        ...     'admin_dttm': pd.to_datetime(['2023-01-02']),
        ...     'med_dose': [5.0],
        ...     'med_dose_unit': ['mcg/kg/hr'],
        ...     'med_category': ['Vasopressors']
        ... })
        >>> result = mac.convert_dose_to_limited_units(vitals, meds)
        >>> result['med_dose_converted'].iloc[0]
        5.833333...  # 5 * 70 / 60 (mcg/kg/hr to mcg/min with 70kg patient)
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
        
        med_df, unrecognized = self._normalize_dose_unit_pattern(med_df)
        if not unrecognized:
            self.logger.info("No unrecognized dose units found, continuing with conversion")
        else:
            self.logger.warning(f"Unrecognized dose units found: {unrecognized}")
        
        acceptable_unit_patterns_str = "','".join(self._acceptable_dose_unit_patterns)
        
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
            , CASE WHEN med_dose_unit_clean NOT IN ('{acceptable_unit_patterns_str}') THEN NULL
                WHEN contains(med_dose_unit_clean, 'units/') THEN 'units/min'
                WHEN contains(med_dose_unit_clean, 'l/') THEN 'ml/min'
                ELSE 'mcg/min' END as med_dose_unit_converted
        FROM med_df
        """
        return duckdb.sql(query).to_df()
    
    
    
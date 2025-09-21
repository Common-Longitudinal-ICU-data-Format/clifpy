"""
ClifOrchestrator class for managing multiple CLIF table objects.

This module provides a unified interface for loading and managing
all CLIF table objects with consistent configuration.
"""

import os
import logging
import pandas as pd
import psutil
from typing import Optional, List, Dict, Any, Tuple

from .tables.patient import Patient
from .tables.hospitalization import Hospitalization
from .tables.adt import Adt
from .tables.labs import Labs
from .tables.vitals import Vitals
from .tables.medication_admin_continuous import MedicationAdminContinuous
from .tables.medication_admin_intermittent import MedicationAdminIntermittent
from .tables.patient_assessments import PatientAssessments
from .tables.respiratory_support import RespiratorySupport
from .tables.position import Position
from .utils.config import get_config_or_params
from .utils.stitching_encounters import stitch_encounters


TABLE_CLASSES = {
    'patient': Patient,
    'hospitalization': Hospitalization,
    'adt': Adt,
    'labs': Labs,
    'vitals': Vitals,
    'medication_admin_continuous': MedicationAdminContinuous,
    'medication_admin_intermittent': MedicationAdminIntermittent,
    'patient_assessments': PatientAssessments,
    'respiratory_support': RespiratorySupport,
    'position': Position
}


class ClifOrchestrator:
    """
    Orchestrator class for managing multiple CLIF table objects.
    
    This class provides a centralized interface for loading, managing,
    and validating multiple CLIF tables with consistent configuration.
    
    Attributes:
        config_path (str, optional): Path to configuration JSON file
        data_directory (str): Path to the directory containing data files
        filetype (str): Type of data file (csv, parquet, etc.)
        timezone (str): Timezone for datetime columns
        output_directory (str): Directory for saving output files and logs
        stitch_encounter (bool): Whether to stitch encounters within time interval
        stitch_time_interval (int): Hours between discharge and next admission to consider encounters linked
        encounter_mapping (pd.DataFrame): Mapping of hospitalization_id to encounter_block (after stitching)
        patient (Patient): Patient table object
        hospitalization (Hospitalization): Hospitalization table object
        adt (Adt): ADT table object
        labs (Labs): Labs table object
        vitals (Vitals): Vitals table object
        medication_admin_continuous (MedicationAdminContinuous): Medication administration continuous table object
        medication_admin_intermittent (MedicationAdminIntermittent): Medication administration intermittent table object
        patient_assessments (PatientAssessments): Patient assessments table object
        respiratory_support (RespiratorySupport): Respiratory support table object
        position (Position): Position table object
    """
    
    def __init__(
        self,
        config_path: Optional[str] = None,
        data_directory: Optional[str] = None,
        filetype: Optional[str] = None,
        timezone: Optional[str] = None,
        output_directory: Optional[str] = None,
        stitch_encounter: bool = False,
        stitch_time_interval: int = 6
    ):
        """
        Initialize the ClifOrchestrator.
        
        Parameters:
            config_path (str, optional): Path to configuration JSON file
            data_directory (str, optional): Path to the directory containing data files
            filetype (str, optional): Type of data file (csv, parquet, etc.)
            timezone (str, optional): Timezone for datetime columns
            output_directory (str, optional): Directory for saving output files and logs.
                If not provided, creates an 'output' directory in the current working directory.
            stitch_encounter (bool, optional): Whether to stitch encounters within time interval. Default False.
            stitch_time_interval (int, optional): Hours between discharge and next admission to consider 
                encounters linked. Default 6 hours.
                
        Loading priority:
            1. If all required params provided → use them
            2. If config_path provided → load from that path, allow param overrides
            3. If no params and no config_path → auto-detect config.json
            4. Parameters override config file values when both are provided
        """
        # Get configuration from config file or parameters
        config = get_config_or_params(
            config_path=config_path,
            data_directory=data_directory,
            filetype=filetype,
            timezone=timezone,
            output_directory=output_directory
        )
        
        self.data_directory = config['data_directory']
        self.filetype = config['filetype']
        self.timezone = config['timezone']
        
        # Set output directory
        self.output_directory = config.get('output_directory')
        if self.output_directory is None:
            self.output_directory = os.path.join(os.getcwd(), 'output')
        os.makedirs(self.output_directory, exist_ok=True)

        # Initialize logger
        self.logger = logging.getLogger('pyclif.ClifOrchestrator')

        
        # Set stitching parameters
        self.stitch_encounter = stitch_encounter
        self.stitch_time_interval = stitch_time_interval
        self.encounter_mapping = None
        
        # Initialize all table attributes to None
        self.patient: Patient = None
        self.hospitalization: Hospitalization = None
        self.adt: Adt = None
        self.labs: Labs = None
        self.vitals: Vitals = None
        self.medication_admin_continuous: MedicationAdminContinuous = None
        self.medication_admin_intermittent: MedicationAdminIntermittent = None
        self.patient_assessments: PatientAssessments = None
        self.respiratory_support: RespiratorySupport = None
        self.position: Position = None
        
        print('ClifOrchestrator initialized.')
    
    @classmethod
    def from_config(cls, config_path: str = "./config.json") -> 'ClifOrchestrator':
        """
        Create a ClifOrchestrator instance from a configuration file.
        
        Parameters:
            config_path (str): Path to the configuration JSON file
            
        Returns:
            ClifOrchestrator: Configured instance
        """
        return cls(config_path=config_path)
    
    def load_table(
        self,
        table_name: str,
        sample_size: Optional[int] = None,
        columns: Optional[List[str]] = None,
        filters: Optional[Dict[str, Any]] = None
    ) -> Any:
        """
        Load table data and create table object.
        
        Parameters:
            table_name (str): Name of the table to load
            sample_size (int, optional): Number of rows to load
            columns (List[str], optional): Specific columns to load
            filters (Dict, optional): Filters to apply when loading
            
        Returns:
            The loaded table object
        """
        if table_name not in TABLE_CLASSES:
            raise ValueError(f"Unknown table: {table_name}. Available tables: {list(TABLE_CLASSES.keys())}")
        
        table_class = TABLE_CLASSES[table_name]
        table_object = table_class.from_file(
            data_directory=self.data_directory,
            filetype=self.filetype,
            timezone=self.timezone,
            output_directory=self.output_directory,
            sample_size=sample_size,
            columns=columns,
            filters=filters
        )
        setattr(self, table_name, table_object)
        return table_object
    
    def initialize(
        self,
        tables: Optional[List[str]] = None,
        sample_size: Optional[int] = None,
        columns: Optional[Dict[str, List[str]]] = None,
        filters: Optional[Dict[str, Dict[str, Any]]] = None
    ):
        """
        Initialize specified tables with optional filtering and column selection.
        
        Parameters:
            tables (List[str], optional): List of table names to load. Defaults to ['patient'].
            sample_size (int, optional): Number of rows to load for each table.
            columns (Dict[str, List[str]], optional): Dictionary mapping table names to lists of columns to load.
            filters (Dict[str, Dict], optional): Dictionary mapping table names to filter dictionaries.
        """
        if tables is None:
            tables = ['patient']
        
        for table in tables:
            # Get table-specific columns and filters if provided
            table_columns = columns.get(table) if columns else None
            table_filters = filters.get(table) if filters else None
            
            try:
                self.load_table(table, sample_size, table_columns, table_filters)
            except ValueError as e:
                print(f"Warning: {e}")
        
        # Perform encounter stitching if enabled
        if self.stitch_encounter:
            self.run_stitch_encounters()
    
    def run_stitch_encounters(self):
        if (self.hospitalization is None) or (self.adt is None):
            # automatically load hospitalization and adt
            self.load_table('hospitalization')
            self.load_table('adt')
        else:
            print(f"Performing encounter stitching with time interval of {self.stitch_time_interval} hours...")
            try:
                hospitalization_stitched, adt_stitched, encounter_mapping = stitch_encounters(
                    self.hospitalization.df,
                    self.adt.df,
                    time_interval=self.stitch_time_interval
                )
                
                # Update the dataframes in place
                self.hospitalization.df = hospitalization_stitched
                self.adt.df = adt_stitched
                self.encounter_mapping = encounter_mapping
                
                print("Encounter stitching completed successfully.")
            except Exception as e:
                print(f"Error during encounter stitching: {e}")
                self.encounter_mapping = None
        
    def get_loaded_tables(self) -> List[str]:
        """
        Return list of currently loaded table names.
        
        Returns:
            List[str]: List of loaded table names
        """
        loaded = []
        for table_name in ['patient', 'hospitalization', 'adt', 'labs', 'vitals',
                          'medication_admin_continuous', 'medication_admin_intermittent',
                          'patient_assessments', 'respiratory_support', 'position']:
            if getattr(self, table_name) is not None:
                loaded.append(table_name)
        return loaded
    
    def get_tables_obj_list(self) -> List:
        """
        Return list of loaded table objects.
        
        Returns:
            List: List of loaded table objects
        """
        table_objects = []
        for table_name in ['patient', 'hospitalization', 'adt', 'labs', 'vitals',
                          'medication_admin_continuous', 'medication_admin_intermittent',
                          'patient_assessments', 'respiratory_support', 'position']:
            table_obj = getattr(self, table_name)
            if table_obj is not None:
                table_objects.append(table_obj)
        return table_objects
    
    def get_encounter_mapping(self) -> Optional[pd.DataFrame]:
        """
        Return the encounter mapping DataFrame if encounter stitching was performed.
        
        Returns:
            pd.DataFrame: Mapping of hospitalization_id to encounter_block if stitching was performed.
            None: If stitching was not performed or failed.
        """
        if self.encounter_mapping is None:
            self.run_stitch_encounters()
        return self.encounter_mapping
    
    def validate_all(self):
        """
        Run validation on all loaded tables.
        
        This method runs the validate() method on each loaded table
        and reports the results.
        """
        loaded_tables = self.get_loaded_tables()
        
        if not loaded_tables:
            print("No tables loaded to validate.")
            return
        
        print(f"Validating {len(loaded_tables)} table(s)...")
        
        for table_name in loaded_tables:
            table_obj = getattr(self, table_name)
            print(f"\nValidating {table_name}...")
            table_obj.validate()
    
    def create_wide_dataset(
        self,
        tables_to_load: Optional[List[str]] = None,
        category_filters: Optional[Dict[str, List[str]]] = None,
        sample: bool = False,
        hospitalization_ids: Optional[List[str]] = None,
        cohort_df: Optional[pd.DataFrame] = None,
        output_format: str = 'dataframe',
        save_to_data_location: bool = False,
        output_filename: Optional[str] = None,
        return_dataframe: bool = True,
        batch_size: int = 1000,
        memory_limit: Optional[str] = None,
        threads: Optional[int] = None,
        show_progress: bool = True
    ) -> Optional[pd.DataFrame]:
        """
        Create wide time-series dataset using DuckDB for high performance.
        
        Parameters
        ----------
        tables_to_load : List[str], optional
            List of table names to include in the wide dataset (e.g., ['vitals', 'labs', 'respiratory_support']).
            If None, only base tables (patient, hospitalization, adt) are loaded.
        category_filters : Dict[str, List[str]], optional
            Dictionary mapping table names to lists of categories to pivot into columns.
            Example: {
                'vitals': ['heart_rate', 'sbp', 'spo2'],
                'labs': ['hemoglobin', 'sodium'],
                'respiratory_support': ['device_category']
            }
        sample : bool, default=False
            If True, randomly sample 20 hospitalizations for testing purposes.
        hospitalization_ids : List[str], optional
            List of specific hospitalization IDs to include. When provided, only data for these
            hospitalizations will be loaded, improving performance for large datasets.
        cohort_df : pd.DataFrame, optional
            DataFrame containing cohort definitions with columns:
            - 'patient_id': Patient identifier
            - 'start_time': Start of time window (datetime)
            - 'end_time': End of time window (datetime)
            Used to filter data to specific time windows per patient.
        output_format : str, default='dataframe'
            Format for output data. Options: 'dataframe', 'csv', 'parquet'.
        save_to_data_location : bool, default=False
            If True, save output file to the data directory specified in orchestrator config.
        output_filename : str, optional
            Custom filename for saved output. If None, auto-generates filename with timestamp.
        return_dataframe : bool, default=True
            If True, return DataFrame even when saving to file. If False and saving,
            returns None to save memory.
        batch_size : int, default=1000
            Number of hospitalizations to process per batch. Lower values use less memory.
        memory_limit : str, optional
            DuckDB memory limit (e.g., '8GB', '16GB'). If None, uses DuckDB default.
        threads : int, optional
            Number of threads for DuckDB to use. If None, uses all available cores.
        show_progress : bool, default=True
            If True, display progress bars during processing.
            
        Returns
        -------
        pd.DataFrame or None
            Wide dataset with time-series data pivoted by categories. Returns None if
            return_dataframe=False and saving to file.
            
        Notes
        -----
        - When hospitalization_ids is provided, the function efficiently loads only the
          specified hospitalizations from all tables, significantly reducing memory usage
          and processing time for targeted analyses.
        - The wide dataset will have one row per hospitalization per time point, with
          columns for each category value specified in category_filters.
        """
        # Import the utility function
        from clifpy.utils.wide_dataset import create_wide_dataset as _create_wide
        filters = None
        if hospitalization_ids:
            filters = {'hospitalization_id': hospitalization_ids}
        
        # Auto-load base tables if not loaded
        if self.patient is None:
            print("Loading patient table...")
            self.load_table('patient')  # Patient doesn't need filters
        if self.hospitalization is None:
            print("Loading hospitalization table...")
            self.load_table('hospitalization', filters=filters)
        if self.adt is None:
            print("Loading adt table...")
            self.load_table('adt', filters=filters)
        
        # Load optional tables only if not already loaded
        if tables_to_load:
            for table_name in tables_to_load:
                if getattr(self, table_name, None) is None:
                    print(f"Loading {table_name} table...")
                    try:
                        self.load_table(table_name)
                    except Exception as e:
                        print(f"Warning: Could not load {table_name}: {e}")
        
        # Call utility function with self as clif_instance
        return _create_wide(
            clif_instance=self,
            optional_tables=tables_to_load,
            category_filters=category_filters,
            sample=sample,
            hospitalization_ids=hospitalization_ids,
            cohort_df=cohort_df,
            output_format=output_format,
            save_to_data_location=save_to_data_location,
            output_filename=output_filename,
            return_dataframe=return_dataframe,
            batch_size=batch_size,
            memory_limit=memory_limit,
            threads=threads,
            show_progress=show_progress
        )
    
    def convert_wide_to_hourly(
        self,
        wide_df: pd.DataFrame,
        aggregation_config: Dict[str, List[str]],
        memory_limit: str = '4GB',
        temp_directory: Optional[str] = None,
        batch_size: Optional[int] = None
    ) -> pd.DataFrame:
        """
        Convert wide dataset to hourly aggregation using DuckDB.
        
        Parameters:
            wide_df: Wide dataset from create_wide_dataset()
            aggregation_config: Dict mapping aggregation methods to columns
                Example: {
                    'mean': ['heart_rate', 'sbp'],
                    'max': ['spo2'],
                    'min': ['map'],
                    'median': ['glucose'],
                    'first': ['gcs_total'],
                    'last': ['assessment_value'],
                    'boolean': ['norepinephrine'],
                    'one_hot_encode': ['device_category']
                }
            memory_limit: DuckDB memory limit (e.g., '4GB', '8GB')
            temp_directory: Directory for DuckDB temp files
            batch_size: Process in batches if specified
            
        Returns:
            Hourly aggregated DataFrame with nth_hour column
        """
        from clifpy.utils.wide_dataset import convert_wide_to_hourly
        
        return convert_wide_to_hourly(
            wide_df=wide_df,
            aggregation_config=aggregation_config,
            memory_limit=memory_limit,
            temp_directory=temp_directory,
            batch_size=batch_size
        )
    
    def get_sys_resource_info(self, print_summary: bool = True) -> Dict[str, Any]:
        """
        Get system resource information including CPU, memory, and practical thread limits.
        
        Parameters:
            print_summary (bool): Whether to print a formatted summary
            
        Returns:
            Dict containing system resource information:
            - cpu_count_physical: Number of physical CPU cores
            - cpu_count_logical: Number of logical CPU cores
            - cpu_usage_percent: Current CPU usage percentage
            - memory_total_gb: Total RAM in GB
            - memory_available_gb: Available RAM in GB
            - memory_used_gb: Used RAM in GB
            - memory_usage_percent: Memory usage percentage
            - process_threads: Number of threads used by current process
            - max_recommended_threads: Recommended max threads for optimal performance
        """
        # Get current process
        current_process = psutil.Process()
        
        # CPU information
        cpu_count_physical = psutil.cpu_count(logical=False)
        cpu_count_logical = psutil.cpu_count(logical=True)
        cpu_usage_percent = psutil.cpu_percent(interval=1)
        
        # Memory information
        memory = psutil.virtual_memory()
        memory_total_gb = memory.total / (1024**3)
        memory_available_gb = memory.available / (1024**3)
        memory_used_gb = memory.used / (1024**3)
        memory_usage_percent = memory.percent
        
        # Thread information
        process_threads = current_process.num_threads()
        max_recommended_threads = cpu_count_physical  # Conservative recommendation
        
        resource_info = {
            'cpu_count_physical': cpu_count_physical,
            'cpu_count_logical': cpu_count_logical,
            'cpu_usage_percent': cpu_usage_percent,
            'memory_total_gb': memory_total_gb,
            'memory_available_gb': memory_available_gb,
            'memory_used_gb': memory_used_gb,
            'memory_usage_percent': memory_usage_percent,
            'process_threads': process_threads,
            'max_recommended_threads': max_recommended_threads
        }
        
        if print_summary:
            print("=" * 50)
            print("SYSTEM RESOURCES")
            print("=" * 50)
            print(f"CPU Cores (Physical): {cpu_count_physical}")
            print(f"CPU Cores (Logical):  {cpu_count_logical}")
            print(f"CPU Usage:            {cpu_usage_percent:.1f}%")
            print("-" * 50)
            print(f"Total RAM:            {memory_total_gb:.1f} GB")
            print(f"Available RAM:        {memory_available_gb:.1f} GB")
            print(f"Used RAM:             {memory_used_gb:.1f} GB")
            print(f"Memory Usage:         {memory_usage_percent:.1f}%")
            print("-" * 50)
            print(f"Process Threads:      {process_threads}")
            print(f"Max Recommended:      {max_recommended_threads} threads")
            print("-" * 50)
            print(f"RECOMMENDATION: Use {max(1, cpu_count_physical-2)}-{cpu_count_physical} threads for optimal performance")
            print(f"(Based on {cpu_count_physical} physical CPU cores)")
            print("=" * 50)
        
        return resource_info

    def convert_dose_units_for_continuous_meds(
        self,
        preferred_units: Dict[str, str],
        vitals_df: pd.DataFrame = None,
        show_intermediate: bool = False,
        override: bool = False,
        save_to_table: bool = True
    ) -> Optional[Tuple[pd.DataFrame, pd.DataFrame]]:
        """
        Convert dose units for continuous medication data.

        Parameters:
            preferred_units: Dict of preferred units for each medication category
            vitals_df: Vitals DataFrame for extracting patient weights (optional)
            show_intermediate: If True, includes intermediate calculation columns in output
            override: If True, continues processing with warnings for unacceptable units
            save_to_table: If True, saves the converted DataFrame to the table's df_converted
                property and stores conversion_counts as a table property. If False,
                returns the converted data without updating the table.

        Returns:
            Tuple[pd.DataFrame, pd.DataFrame]: (converted_df, counts_df) when save_to_table=False
        """
        from .utils.unit_converter import convert_dose_units_by_med_category

        # Log function entry with parameters
        self.logger.info(f"Starting dose unit conversion for continuous medications with parameters: "
                        f"preferred_units={preferred_units}, show_intermediate={show_intermediate}, "
                        f"override={override}, overwrite_table_df={save_to_table}")

        # use the vitals df loaded to the table instance if no stand-alone vitals_df is provided
        if vitals_df is None:
            self.logger.debug("No vitals_df provided, checking existing vitals table")
            if (self.vitals is None) or (self.vitals.df is None):
                self.logger.info("Loading vitals table...")
                self.load_table('vitals')
            vitals_df = self.vitals.df
            self.logger.debug(f"Using vitals data with shape: {vitals_df.shape}")
        else:
            self.logger.debug(f"Using provided vitals_df with shape: {vitals_df.shape}")
        
        if self.medication_admin_continuous is None:
            self.logger.info("Loading medication_admin_continuous table...")
            self.load_table('medication_admin_continuous')
            self.logger.debug("medication_admin_continuous table loaded successfully")

        # Call the conversion function with all parameters
        self.logger.info("Starting dose unit conversion")
        self.logger.debug(f"Input DataFrame shape: {self.medication_admin_continuous.df.shape}")

        converted_df, counts_df = convert_dose_units_by_med_category(
            self.medication_admin_continuous.df,
            vitals_df=vitals_df,
            preferred_units=preferred_units,
            show_intermediate=show_intermediate,
            override=override
        )

        self.logger.info("Dose unit conversion completed")
        self.logger.debug(f"Output DataFrame shape: {converted_df.shape}")
        self.logger.debug(f"Conversion counts summary: {len(counts_df)} conversions tracked")

        # If overwrite_raw_df is True, update the table's df and store conversion_counts
        if save_to_table:
            self.logger.info("Updating medication_admin_continuous table with converted data")
            self.medication_admin_continuous.df_converted = converted_df
            self.medication_admin_continuous.conversion_counts = counts_df
            self.logger.debug("Conversion counts stored as table property")
        else:
            self.logger.info("Returning converted data without updating table")
            return converted_df, counts_df
        
    def convert_dose_units_for_intermittent_meds(
        self,
        preferred_units: Dict[str, str],
        vitals_df: pd.DataFrame = None,
        show_intermediate: bool = False,
        override: bool = False,
        save_to_table: bool = True
    ) -> Optional[Tuple[pd.DataFrame, pd.DataFrame]]:
        """
        Convert dose units for intermittent medication data.

        Parameters:
            preferred_units: Dict of preferred units for each medication category
            vitals_df: Vitals DataFrame for extracting patient weights (optional)
            show_intermediate: If True, includes intermediate calculation columns in output
            override: If True, continues processing with warnings for unacceptable units
            save_to_table: If True, saves the converted DataFrame to the table's df_converted
                property and stores conversion_counts as a table property. If False,
                returns the converted data without updating the table.

        Returns:
            Tuple[pd.DataFrame, pd.DataFrame]: (converted_df, counts_df) when save_to_table=False
        """
        from .utils.unit_converter import convert_dose_units_by_med_category

        # Log function entry with parameters
        self.logger.info(f"Starting dose unit conversion for intermittent medications with parameters: "
                        f"preferred_units={preferred_units}, show_intermediate={show_intermediate}, "
                        f"override={override}, save_to_table={save_to_table}")

        # use the vitals df loaded to the table instance if no stand-alone vitals_df is provided
        if vitals_df is None:
            self.logger.debug("No vitals_df provided, checking existing vitals table")
            if (self.vitals is None) or (self.vitals.df is None):
                self.logger.info("Loading vitals table...")
                self.load_table('vitals')
            vitals_df = self.vitals.df
            self.logger.debug(f"Using vitals data with shape: {vitals_df.shape}")
        else:
            self.logger.debug(f"Using provided vitals_df with shape: {vitals_df.shape}")

        if self.medication_admin_intermittent is None:
            self.logger.info("Loading medication_admin_intermittent table...")
            self.load_table('medication_admin_intermittent')
            self.logger.debug("medication_admin_intermittent table loaded successfully")

        # Call the conversion function with all parameters
        self.logger.info("Starting dose unit conversion")
        self.logger.debug(f"Input DataFrame shape: {self.medication_admin_intermittent.df.shape}")

        converted_df, counts_df = convert_dose_units_by_med_category(
            self.medication_admin_intermittent.df,
            vitals_df=vitals_df,
            preferred_units=preferred_units,
            show_intermediate=show_intermediate,
            override=override
        )

        self.logger.info("Dose unit conversion completed")
        self.logger.debug(f"Output DataFrame shape: {converted_df.shape}")
        self.logger.debug(f"Conversion counts summary: {len(counts_df)} conversions tracked")

        # If save_to_table is True, update the table's df_converted and store conversion_counts
        if save_to_table:
            self.logger.info("Updating medication_admin_intermittent table with converted data")
            self.medication_admin_intermittent.df_converted = converted_df
            self.medication_admin_intermittent.conversion_counts = counts_df
            self.logger.debug("Conversion counts stored as table property")
        else:
            self.logger.info("Returning converted data without updating table")
            return converted_df, counts_df

    def compute_sofa_scores(
        self,
        wide_df: Optional[pd.DataFrame] = None,
        cohort_df: Optional[pd.DataFrame] = None,
        extremal_type: str = 'worst',
        id_name: str = 'encounter_block',
        fill_na_scores_with_zero: bool = True
    ) -> pd.DataFrame:
        """
        Compute SOFA (Sequential Organ Failure Assessment) scores.

        Parameters:
            wide_df: Optional wide dataset. If not provided, uses self.wide_df or creates one
            cohort_df: Optional DataFrame with columns [id_name, 'start_time', 'end_time']
                      to further filter observations by time windows
            extremal_type: 'worst' or 'latest' (currently only 'worst' is implemented)
            id_name: Column name for grouping (e.g., 'hospitalization_id', 'patient_id', 'encounter_block')

        Returns:
            DataFrame with SOFA component scores and total score for each ID
        """
        from .utils.sofa import compute_sofa, REQUIRED_SOFA_CATEGORIES_BY_TABLE

        self.logger.info(f"Computing SOFA scores with extremal_type='{extremal_type}', id_name='{id_name}'")
        
        if (cohort_df is not None) and (id_name not in cohort_df.columns):
            raise ValueError(f"id_name '{id_name}' not found in cohort_df columns")
        
        # Determine which wide_df to use
        if wide_df is not None:
            self.logger.debug("Using provided wide_df")
            df = wide_df
        elif hasattr(self, 'wide_df') and self.wide_df is not None:
            self.logger.debug("Using existing self.wide_df")
            df = self.wide_df
        else:
            self.logger.info("No wide dataset available, creating one...")
            # Create wide dataset with required categories for SOFA

            df = self.create_wide_dataset(
                tables_to_load=list(REQUIRED_SOFA_CATEGORIES_BY_TABLE.keys()),
                category_filters=REQUIRED_SOFA_CATEGORIES_BY_TABLE,
                cohort_df=cohort_df
            )
            # Store the created wide dataset
            self.wide_df = df
            self.logger.debug(f"Created wide dataset with shape: {df.shape}")

        if id_name not in df.columns:
            if self.encounter_mapping is None:
                try:
                    self.run_stitch_encounters()
                except Exception as e:
                    self.logger.error(f"Error during encounter stitching: {e}")
                    raise ValueError("Encounter stitching failed. Please run stitch_encounters() manually.")
            df = df.merge(self.encounter_mapping, on='hospitalization_id', how='left')   
            self.wide_df = df
            self.logger.debug(f"Mapped {id_name} to wide_df via encounter_mapping, with shape: {df.shape}")
    
        # Compute SOFA scores
        self.logger.debug("Calling compute_sofa function")
        sofa_scores = compute_sofa(
            wide_df=df,
            cohort_df=cohort_df,
            extremal_type=extremal_type,
            id_name=id_name,
            fill_na_scores_with_zero=fill_na_scores_with_zero
        )

        # Store results in orchestrator
        self.sofa_df = sofa_scores
        self.logger.info(f"SOFA computation completed. Results stored in self.sofa_df with shape: {sofa_scores.shape}")

        return sofa_scores
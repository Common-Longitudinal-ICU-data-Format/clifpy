"""
ClifOrchestrator class for managing multiple CLIF table objects.

This module provides a unified interface for loading and managing
all CLIF table objects with consistent configuration.
"""

import os
import pandas as pd
import psutil
from typing import Optional, List, Dict, Any

from .tables.patient import Patient
from .tables.hospitalization import Hospitalization
from .tables.adt import Adt
from .tables.labs import Labs
from .tables.vitals import Vitals
from .tables.medication_admin_continuous import MedicationAdminContinuous
from .tables.patient_assessments import PatientAssessments
from .tables.respiratory_support import RespiratorySupport
from .tables.position import Position


TABLE_CLASSES = {
    'patient': Patient,
    'hospitalization': Hospitalization,
    'adt': Adt,
    'labs': Labs,
    'vitals': Vitals,
    'medication_admin_continuous': MedicationAdminContinuous,
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
        data_directory (str): Path to the directory containing data files
        filetype (str): Type of data file (csv, parquet, etc.)
        timezone (str): Timezone for datetime columns
        output_directory (str): Directory for saving output files and logs
        patient (Patient): Patient table object
        hospitalization (Hospitalization): Hospitalization table object
        adt (Adt): ADT table object
        labs (Labs): Labs table object
        vitals (Vitals): Vitals table object
        medication_admin_continuous (MedicationAdminContinuous): Medication administration table object
        patient_assessments (PatientAssessments): Patient assessments table object
        respiratory_support (RespiratorySupport): Respiratory support table object
        position (Position): Position table object
    """
    
    def __init__(
        self,
        data_directory: str,
        filetype: str = 'csv',
        timezone: str = 'UTC',
        output_directory: Optional[str] = None
    ):
        """
        Initialize the ClifOrchestrator.
        
        Parameters:
            data_directory (str): Path to the directory containing data files
            filetype (str): Type of data file (csv, parquet, etc.)
            timezone (str): Timezone for datetime columns
            output_directory (str, optional): Directory for saving output files and logs.
                If not provided, creates an 'output' directory in the current working directory.
        """
        self.data_directory = data_directory
        self.filetype = filetype
        self.timezone = timezone
        
        # Set output directory (same logic as BaseTable)
        if output_directory is None:
            output_directory = os.path.join(os.getcwd(), 'output')
        self.output_directory = output_directory
        os.makedirs(self.output_directory, exist_ok=True)
        
        # Initialize all table attributes to None
        self.patient = None
        self.hospitalization = None
        self.adt = None
        self.labs = None
        self.vitals = None
        self.medication_admin_continuous = None
        self.patient_assessments = None
        self.respiratory_support = None
        self.position = None
        
        print('ClifOrchestrator initialized.')
    
    def load_table(
        self,
        table_name: str,
        sample_size: Optional[int] = None,
        columns: Optional[List[str]] = None,
        filters: Optional[Dict[str, Any]] = None
    ):
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
    
    def get_loaded_tables(self) -> List[str]:
        """
        Return list of currently loaded table names.
        
        Returns:
            List[str]: List of loaded table names
        """
        loaded = []
        for table_name in ['patient', 'hospitalization', 'adt', 'labs', 'vitals',
                          'medication_admin_continuous', 'patient_assessments',
                          'respiratory_support', 'position']:
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
                          'medication_admin_continuous', 'patient_assessments',
                          'respiratory_support', 'position']:
            table_obj = getattr(self, table_name)
            if table_obj is not None:
                table_objects.append(table_obj)
        return table_objects
    
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
        
        Parameters:
            tables_to_load: List of tables to include (e.g., ['vitals', 'labs'])
            category_filters: Dict of categories to pivot for each table
                Example: {
                    'vitals': ['heart_rate', 'sbp', 'spo2'],
                    'labs': ['hemoglobin', 'sodium'],
                    'respiratory_support': ['device_category']
                }
            sample: If True, use 20 random hospitalizations
            hospitalization_ids: Specific hospitalization IDs to include
            cohort_df: DataFrame with time windows for filtering
            output_format: 'dataframe', 'csv', or 'parquet'
            save_to_data_location: Save output to data directory
            output_filename: Custom filename for output
            return_dataframe: Return DataFrame even when saving
            batch_size: Number of hospitalizations per batch
            memory_limit: DuckDB memory limit (e.g., '8GB')
            threads: Number of threads for DuckDB
            show_progress: Show progress bars
            
        Returns:
            Wide dataset as DataFrame or None
        """
        # Import the utility function
        from clifpy.utils.wide_dataset import create_wide_dataset as _create_wide
        
        # Auto-load base tables if not loaded
        if self.patient is None:
            print("Loading patient table...")
            self.load_table('patient')
        if self.hospitalization is None:
            print("Loading hospitalization table...")
            self.load_table('hospitalization')
        if self.adt is None:
            print("Loading adt table...")
            self.load_table('adt')
        
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
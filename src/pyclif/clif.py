import os
import pandas as pd
import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import re

from .utils.io import load_data 
from .utils.wide_dataset import create_wide_dataset, convert_wide_to_hourly
from .tables.adt import adt
from .tables.hospitalization import Hospitalization
from .tables.labs import labs
from .tables.medication_admin_continuous import MedicationAdminContinuous
from .tables.patient import Patient
from .tables.patient_assessments import patient_assessments
from .tables.position import position
from .tables.respiratory_support import respiratory_support
from .tables.vitals import Vitals

class CLIF:
    def __init__(self, data_dir, filetype='csv', timezone ="UTC", output_dir=None):
        """
        Initialize the CLIF object.
        
        Parameters:
            data_dir (str): Directory containing data files
            filetype (str): Type of data file (csv, parquet, etc.)
            timezone (str): Timezone for datetime columns
            output_dir (str, optional): Directory for saving validation outputs
        """
        self.data_dir = data_dir
        self.filetype = filetype
        self.timezone = timezone
        self.output_dir = output_dir
        
        self.patient = None
        self.hospitalization = None
        self.lab = None
        self.adt = None
        self.respiratory_support = None
        self.vitals = None
        self.medication_admin_continuous = None
        self.patient_assessments = None
        self.position = None  # Added missing position table
        self.wide_df = None
        self.hourly_wide_df = None
        ## create a cohort object, check if cohort is not None, 
        # then only load those for each table
        print('CLIF Object Initialized.')
    
    def load_patient_data(self, sample_size=None, columns=None, filters=None):
        """
        Load patient data with optional filtering and column selection.
        
        Parameters:
            sample_size (int, optional): Number of rows to load.
            columns (list of str, optional): List of column names to load.
            filters (dict, optional): Dictionary of filters to apply.
            
        Returns:
            The initialized patient table object.
        """
        data = load_data('patient', self.data_dir, self.filetype, sample_size, columns, filters, site_tz=self.timezone)
        self.patient = Patient(
            data_directory=self.data_dir,
            filetype=self.filetype,
            timezone=self.timezone,
            output_directory=self.output_dir,
            data=data
        )
        return self.patient
    
    def load_hospitalization_data(self, sample_size=None, columns=None, filters=None):
        """
        Load hospitalization data with optional filtering and column selection.
        
        Parameters:
            sample_size (int, optional): Number of rows to load.
            columns (list of str, optional): List of column names to load.
            filters (dict, optional): Dictionary of filters to apply.
            
        Returns:
            The initialized hospitalization table object.
        """
        data = load_data('hospitalization', self.data_dir, self.filetype, sample_size, columns, filters, site_tz=self.timezone)
        self.hospitalization = Hospitalization(
            data_directory=self.data_dir,
            filetype=self.filetype,
            timezone=self.timezone,
            output_directory=self.output_dir,
            data=data
        )
        return self.hospitalization
    
    def load_lab_data(self, sample_size=None, columns=None, filters=None):
        """
        Load lab data with optional filtering and column selection.
        
        Parameters:
            sample_size (int, optional): Number of rows to load.
            columns (list of str, optional): List of column names to load.
            filters (dict, optional): Dictionary of filters to apply.
            
        Returns:
            The initialized labs table object.
        """
        data = load_data('labs', self.data_dir, self.filetype, sample_size, columns, filters, site_tz=self.timezone)
        self.lab = labs(
            data_directory=self.data_dir,
            filetype=self.filetype,
            timezone=self.timezone,
            output_directory=self.output_dir,
            data=data
        )
        return self.lab
    
    def load_adt_data(self, sample_size=None, columns=None, filters=None):
        """
        Load ADT data with optional filtering and column selection.
        
        Parameters:
            sample_size (int, optional): Number of rows to load.
            columns (list of str, optional): List of column names to load.
            filters (dict, optional): Dictionary of filters to apply.
            
        Returns:
            The initialized adt table object.
        """
        data = load_data('adt', self.data_dir, self.filetype, sample_size, columns, filters, site_tz=self.timezone)
        self.adt = adt(
            data_directory=self.data_dir,
            filetype=self.filetype,
            timezone=self.timezone,
            output_directory=self.output_dir,
            data=data
        )
        return self.adt
    
    def load_respiratory_support_data(self, sample_size=None, columns=None, filters=None):
        """
        Load respiratory support data with optional filtering and column selection.
        
        Parameters:
            sample_size (int, optional): Number of rows to load.
            columns (list of str, optional): List of column names to load.
            filters (dict, optional): Dictionary of filters to apply.
            
        Returns:
            The initialized respiratory_support table object.
        """
        data = load_data('respiratory_support', self.data_dir, self.filetype, sample_size, columns, filters, site_tz=self.timezone)
        self.respiratory_support = respiratory_support(
            data_directory=self.data_dir,
            filetype=self.filetype,
            timezone=self.timezone,
            output_directory=self.output_dir,
            data=data
        )
        return self.respiratory_support
    
    def load_vitals_data(self, sample_size=None, columns=None, filters=None):
        """
        Load vitals data with optional filtering and column selection.
        
        Parameters:
            sample_size (int, optional): Number of rows to load.
            columns (list of str, optional): List of column names to load.
            filters (dict, optional): Dictionary of filters to apply.
            
        Returns:
            The initialized vitals table object.
        """
        data = load_data('vitals', self.data_dir, self.filetype, sample_size, columns, filters, site_tz=self.timezone)
        self.vitals = Vitals(
            data_directory=self.data_dir,
            filetype=self.filetype,
            timezone=self.timezone,
            output_directory=self.output_dir,
            data=data
        )
        return self.vitals
    
    def load_medication_admin_continuous_data(self, sample_size=None, columns=None, filters=None):
        """
        Load medication administration continuous data with optional filtering and column selection.
        
        Parameters:
            sample_size (int, optional): Number of rows to load.
            columns (list of str, optional): List of column names to load.
            filters (dict, optional): Dictionary of filters to apply.
            
        Returns:
            The initialized medication_admin_continuous table object.
        """
        data = load_data('medication_admin_continuous', self.data_dir, self.filetype, sample_size, columns, filters, site_tz=self.timezone)
        self.medication_admin_continuous = MedicationAdminContinuous(
            data_directory=self.data_dir,
            filetype=self.filetype,
            timezone=self.timezone,
            output_directory=self.output_dir,
            data=data
        )
        return self.medication_admin_continuous
    
    def load_patient_assessments_data(self, sample_size=None, columns=None, filters=None):
        """
        Load patient assessments data with optional filtering and column selection.
        
        Parameters:
            sample_size (int, optional): Number of rows to load.
            columns (list of str, optional): List of column names to load.
            filters (dict, optional): Dictionary of filters to apply.
            
        Returns:
            The initialized patient_assessments table object.
        """
        data = load_data('patient_assessments', self.data_dir, self.filetype, sample_size, columns, filters, site_tz=self.timezone)
        self.patient_assessments = patient_assessments(
            data_directory=self.data_dir,
            filetype=self.filetype,
            timezone=self.timezone,
            output_directory=self.output_dir,
            data=data
        )
        return self.patient_assessments
    
    def load_position_data(self, sample_size=None, columns=None, filters=None):
        """
        Load position data with optional filtering and column selection.
        
        Parameters:
            sample_size (int, optional): Number of rows to load.
            columns (list of str, optional): List of column names to load.
            filters (dict, optional): Dictionary of filters to apply.
            
        Returns:
            The initialized position table object.
        """
        data = load_data('position', self.data_dir, self.filetype, sample_size, columns, filters, site_tz=self.timezone)
        self.position = position(
            data_directory=self.data_dir,
            filetype=self.filetype,
            timezone=self.timezone,
            output_directory=self.output_dir,
            data=data
        )
        return self.position
    
    def validate_all(self):
        """
        Run validation on all loaded tables and return a summary.
        
        Returns:
            dict: Summary of validation results for each table
        """
        validation_summary = {}
        
        # List of all table attributes and their names
        tables = [
            ('patient', self.patient),
            ('hospitalization', self.hospitalization),
            ('adt', self.adt),
            ('vitals', self.vitals),
            ('labs', self.lab),
            ('medication_admin_continuous', self.medication_admin_continuous),
            ('patient_assessments', self.patient_assessments),
            ('position', self.position),
            ('respiratory_support', self.respiratory_support)
        ]
        
        for table_name, table_obj in tables:
            if table_obj is not None:
                print(f"Validating {table_name}...")
                table_obj.validate()
                validation_summary[table_name] = {
                    'is_valid': table_obj.isvalid(),
                    'error_count': len(table_obj.errors),
                    'errors': table_obj.errors if not table_obj.isvalid() else []
                }
            else:
                validation_summary[table_name] = {
                    'is_valid': None,
                    'error_count': 0,
                    'errors': [],
                    'status': 'not_loaded'
                }
        
        # Print summary
        print("\n" + "="*50)
        print("VALIDATION SUMMARY")
        print("="*50)
        for table_name, result in validation_summary.items():
            if result.get('status') == 'not_loaded':
                print(f"{table_name:30} : Not loaded")
            elif result['is_valid']:
                print(f"{table_name:30} : ✓ Valid")
            else:
                print(f"{table_name:30} : ✗ {result['error_count']} errors")
        print("="*50 + "\n")
        
        return validation_summary

    def initialize(self, tables=None, sample_size=None, columns=None, filters=None):
        """
        Initialize the CLIF object by loading the specified tables with optional filtering.
        
        Parameters:
            tables (list, optional): List of table names to load. Defaults to ['patient'].
            sample_size (int, optional): Number of rows to load for each table.
            columns (dict, optional): Dictionary mapping table names to lists of columns to load.
            filters (dict, optional): Dictionary mapping table names to filter dictionaries.
        """
        if tables is None:
            tables = ['patient']
            
        for table in tables:
            # Get table-specific columns and filters if provided
            table_columns = columns.get(table) if columns else None
            table_filters = filters.get(table) if filters else None
            
            if table == 'patient':
                self.load_patient_data(sample_size, table_columns, table_filters)
            elif table == 'hospitalization':
                self.load_hospitalization_data(sample_size, table_columns, table_filters)
            elif table == 'lab':
                self.load_lab_data(sample_size, table_columns, table_filters)
            elif table == 'adt':
                self.load_adt_data(sample_size, table_columns, table_filters)
            elif table == 'respiratory_support':
                self.load_respiratory_support_data(sample_size, table_columns, table_filters)
            elif table == 'vitals':
                self.load_vitals_data(sample_size, table_columns, table_filters)
            elif table == 'medication_admin_continuous':
                self.load_medication_admin_continuous_data(sample_size, table_columns, table_filters)
            elif table == 'patient_assessments':
                self.load_patient_assessments_data(sample_size, table_columns, table_filters)
            elif table == 'position':
                self.load_position_data(sample_size, table_columns, table_filters)

    def create_wide_dataset(self, 
                          optional_tables=None, 
                          category_filters=None, 
                          sample=False, 
                          hospitalization_ids=None, 
                          cohort_df=None,
                          output_format='dataframe', 
                          save_to_data_location=False, 
                          output_filename=None, 
                          auto_load=True,
                          return_dataframe=True,
                          base_table_columns=None,
                          batch_size=1000,
                          memory_limit=None,
                          threads=None,
                          show_progress=True):
        """
        Create a wide dataset by joining multiple CLIF tables with pivoting support.
        
        Parameters:
            optional_tables: DEPRECATED - List of optional tables to include. Use category_filters instead.
            category_filters: Dict specifying which categories to include for each table.
                             Keys are table names ['vitals', 'labs', 'medication_admin_continuous', 'patient_assessments', 'respiratory_support']
                             Values are lists of categories/columns to filter. Tables will be auto-loaded if auto_load=True.
            sample: Boolean - if True, randomly select 20 hospitalizations
            hospitalization_ids: List of specific hospitalization IDs to filter
            cohort_df: Optional DataFrame with columns ['hospitalization_id', 'start_time', 'end_time']
                       If provided, data will be filtered to only include events within the specified
                       time windows for each hospitalization
            output_format: 'dataframe', 'csv', or 'parquet'
            save_to_data_location: Boolean - save output to data directory
            output_filename: Custom filename (default: 'wide_dataset_YYYYMMDD_HHMMSS')
            auto_load: Boolean - automatically load missing tables from category_filters or optional_tables (default=True)
            return_dataframe: Boolean - return DataFrame even when saving to file (default=True)
            base_table_columns: Dict specifying which columns to select from base tables {'patient': ['col1'], 'hospitalization': ['col1'], 'adt': ['col1']}
            batch_size: Number of hospitalizations to process in each batch (default=1000)
            memory_limit: DuckDB memory limit (e.g., '8GB')
            threads: Number of threads for DuckDB to use
            show_progress: Show progress bars for long operations (default=True)
        
        Returns:
            pd.DataFrame or None (if return_dataframe=False)
        """
        
        if auto_load:
            # Check and load base tables
            required_base = ['patient', 'hospitalization', 'adt']
            for table in required_base:
                table_attr = table if table != 'hospitalization' else 'hospitalization'
                if getattr(self, table_attr) is None:
                    print(f"Auto-loading required base table: {table}")
                    if table == 'patient':
                        self.load_patient_data()
                    elif table == 'hospitalization':
                        self.load_hospitalization_data()
                    elif table == 'adt':
                        self.load_adt_data()
            
            # Check and load optional tables
            tables_to_load = set()
            
            # Add tables from optional_tables parameter (backward compatibility)
            if optional_tables:
                tables_to_load.update(optional_tables)
            
            # Add tables from category_filters parameter (new approach)
            if category_filters:
                tables_to_load.update(category_filters.keys())
            
            # Load the required tables
            for table in tables_to_load:
                table_attr = table if table != 'labs' else 'lab'
                if getattr(self, table_attr) is None:
                    print(f"Auto-loading table: {table}")
                    if table == 'vitals':
                        self.load_vitals_data()
                    elif table == 'labs':
                        self.load_lab_data()
                    elif table == 'medication_admin_continuous':
                        self.load_medication_admin_continuous_data()
                    elif table == 'patient_assessments':
                        self.load_patient_assessments_data()
                    elif table == 'respiratory_support':
                        self.load_respiratory_support_data()
                    else:
                        print(f"Warning: Unknown table '{table}' in configuration, skipping auto-load")
        
        # Call utility function
        wide_df = create_wide_dataset(
            self, 
            optional_tables=optional_tables,
            category_filters=category_filters,
            sample=sample,
            hospitalization_ids=hospitalization_ids,
            cohort_df=cohort_df,
            output_format=output_format,
            save_to_data_location=save_to_data_location,
            output_filename=output_filename,
            return_dataframe=return_dataframe,
            base_table_columns=base_table_columns,
            batch_size=batch_size,
            memory_limit=memory_limit,
            threads=threads,
            show_progress=show_progress
        )
        
        # Store the wide dataset
        if wide_df is not None:
            self.wide_df = wide_df
            
        return wide_df
    
    def create_hourly_wide_dataset(self, aggregation_config):
        """
        Convert the wide dataset to hourly aggregation with user-defined aggregation methods.
        
        Parameters:
            aggregation_config: Dict mapping aggregation methods to list of columns
                Example: {
                    'max': ['map', 'temp_c', 'sbp'],
                    'mean': ['heart_rate', 'respiratory_rate'],
                    'min': ['spo2'],
                    'median': ['glucose'],
                    'first': ['gcs_total', 'rass'],
                    'last': ['assessment_value'],
                    'boolean': ['norepinephrine', 'propofol'],
                    'one_hot_encode': ['medication_name', 'assessment_category']
                }
        
        Returns:
            pd.DataFrame: Hourly aggregated wide dataset with nth_hour column
        """
        
        if self.wide_df is None:
            raise ValueError("No wide dataset available. Please run create_wide_dataset() first.")
        
        # Convert to hourly
        hourly_df = convert_wide_to_hourly(self.wide_df, aggregation_config)
        
        # Store the hourly dataset
        self.hourly_wide_df = hourly_df
        
        return hourly_df

    # def stitch - input is adt, hospitalization
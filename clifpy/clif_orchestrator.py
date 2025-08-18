"""
ClifOrchestrator class for managing multiple CLIF table objects.

This module provides a unified interface for loading and managing
all CLIF table objects with consistent configuration.
"""

import os
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
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
    
    def load_patient_data(
        self,
        sample_size: Optional[int] = None,
        columns: Optional[List[str]] = None,
        filters: Optional[Dict[str, Any]] = None
    ):
        """
        Load patient data and create Patient table object.
        
        Parameters:
            sample_size (int, optional): Number of rows to load
            columns (List[str], optional): Specific columns to load
            filters (Dict, optional): Filters to apply when loading
            
        Returns:
            Patient: The loaded Patient table object
        """
        self.patient = Patient.from_file(
            data_directory=self.data_directory,
            filetype=self.filetype,
            timezone=self.timezone,
            output_directory=self.output_directory,
            sample_size=sample_size,
            columns=columns,
            filters=filters
        )
        return self.patient
    
    def load_hospitalization_data(
        self,
        sample_size: Optional[int] = None,
        columns: Optional[List[str]] = None,
        filters: Optional[Dict[str, Any]] = None
    ):
        """
        Load hospitalization data and create Hospitalization table object.
        
        Parameters:
            sample_size (int, optional): Number of rows to load
            columns (List[str], optional): Specific columns to load
            filters (Dict, optional): Filters to apply when loading
            
        Returns:
            Hospitalization: The loaded Hospitalization table object
        """
        self.hospitalization = Hospitalization.from_file(
            data_directory=self.data_directory,
            filetype=self.filetype,
            timezone=self.timezone,
            output_directory=self.output_directory,
            sample_size=sample_size,
            columns=columns,
            filters=filters
        )
        return self.hospitalization
    
    def load_adt_data(
        self,
        sample_size: Optional[int] = None,
        columns: Optional[List[str]] = None,
        filters: Optional[Dict[str, Any]] = None
    ):
        """
        Load ADT data and create Adt table object.
        
        Parameters:
            sample_size (int, optional): Number of rows to load
            columns (List[str], optional): Specific columns to load
            filters (Dict, optional): Filters to apply when loading
            
        Returns:
            Adt: The loaded Adt table object
        """
        self.adt = Adt.from_file(
            data_directory=self.data_directory,
            filetype=self.filetype,
            timezone=self.timezone,
            output_directory=self.output_directory,
            sample_size=sample_size,
            columns=columns,
            filters=filters
        )
        return self.adt
    
    def load_labs_data(
        self,
        sample_size: Optional[int] = None,
        columns: Optional[List[str]] = None,
        filters: Optional[Dict[str, Any]] = None
    ):
        """
        Load labs data and create Labs table object.
        
        Parameters:
            sample_size (int, optional): Number of rows to load
            columns (List[str], optional): Specific columns to load
            filters (Dict, optional): Filters to apply when loading
            
        Returns:
            Labs: The loaded Labs table object
        """
        self.labs = Labs.from_file(
            data_directory=self.data_directory,
            filetype=self.filetype,
            timezone=self.timezone,
            output_directory=self.output_directory,
            sample_size=sample_size,
            columns=columns,
            filters=filters
        )
        return self.labs
    
    def load_vitals_data(
        self,
        sample_size: Optional[int] = None,
        columns: Optional[List[str]] = None,
        filters: Optional[Dict[str, Any]] = None
    ):
        """
        Load vitals data and create Vitals table object.
        
        Parameters:
            sample_size (int, optional): Number of rows to load
            columns (List[str], optional): Specific columns to load
            filters (Dict, optional): Filters to apply when loading
            
        Returns:
            Vitals: The loaded Vitals table object
        """
        self.vitals = Vitals.from_file(
            data_directory=self.data_directory,
            filetype=self.filetype,
            timezone=self.timezone,
            output_directory=self.output_directory,
            sample_size=sample_size,
            columns=columns,
            filters=filters
        )
        return self.vitals
    
    def load_medication_admin_continuous_data(
        self,
        sample_size: Optional[int] = None,
        columns: Optional[List[str]] = None,
        filters: Optional[Dict[str, Any]] = None
    ):
        """
        Load medication administration continuous data and create MedicationAdminContinuous table object.
        
        Parameters:
            sample_size (int, optional): Number of rows to load
            columns (List[str], optional): Specific columns to load
            filters (Dict, optional): Filters to apply when loading
            
        Returns:
            MedicationAdminContinuous: The loaded MedicationAdminContinuous table object
        """
        self.medication_admin_continuous = MedicationAdminContinuous.from_file(
            data_directory=self.data_directory,
            filetype=self.filetype,
            timezone=self.timezone,
            output_directory=self.output_directory,
            sample_size=sample_size,
            columns=columns,
            filters=filters
        )
        return self.medication_admin_continuous
    
    def load_patient_assessments_data(
        self,
        sample_size: Optional[int] = None,
        columns: Optional[List[str]] = None,
        filters: Optional[Dict[str, Any]] = None
    ):
        """
        Load patient assessments data and create PatientAssessments table object.
        
        Parameters:
            sample_size (int, optional): Number of rows to load
            columns (List[str], optional): Specific columns to load
            filters (Dict, optional): Filters to apply when loading
            
        Returns:
            PatientAssessments: The loaded PatientAssessments table object
        """
        self.patient_assessments = PatientAssessments.from_file(
            data_directory=self.data_directory,
            filetype=self.filetype,
            timezone=self.timezone,
            output_directory=self.output_directory,
            sample_size=sample_size,
            columns=columns,
            filters=filters
        )
        return self.patient_assessments
    
    def load_respiratory_support_data(
        self,
        sample_size: Optional[int] = None,
        columns: Optional[List[str]] = None,
        filters: Optional[Dict[str, Any]] = None
    ):
        """
        Load respiratory support data and create RespiratorySupport table object.
        
        Parameters:
            sample_size (int, optional): Number of rows to load
            columns (List[str], optional): Specific columns to load
            filters (Dict, optional): Filters to apply when loading
            
        Returns:
            RespiratorySupport: The loaded RespiratorySupport table object
        """
        self.respiratory_support = RespiratorySupport.from_file(
            data_directory=self.data_directory,
            filetype=self.filetype,
            timezone=self.timezone,
            output_directory=self.output_directory,
            sample_size=sample_size,
            columns=columns,
            filters=filters
        )
        return self.respiratory_support
    
    def load_position_data(
        self,
        sample_size: Optional[int] = None,
        columns: Optional[List[str]] = None,
        filters: Optional[Dict[str, Any]] = None
    ):
        """
        Load position data and create Position table object.
        
        Parameters:
            sample_size (int, optional): Number of rows to load
            columns (List[str], optional): Specific columns to load
            filters (Dict, optional): Filters to apply when loading
            
        Returns:
            Position: The loaded Position table object
        """
        self.position = Position.from_file(
            data_directory=self.data_directory,
            filetype=self.filetype,
            timezone=self.timezone,
            output_directory=self.output_directory,
            sample_size=sample_size,
            columns=columns,
            filters=filters
        )
        return self.position
    
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
            
            if table == 'patient':
                self.load_patient_data(sample_size, table_columns, table_filters)
            elif table == 'hospitalization':
                self.load_hospitalization_data(sample_size, table_columns, table_filters)
            elif table == 'adt':
                self.load_adt_data(sample_size, table_columns, table_filters)
            elif table == 'labs':
                self.load_labs_data(sample_size, table_columns, table_filters)
            elif table == 'vitals':
                self.load_vitals_data(sample_size, table_columns, table_filters)
            elif table == 'medication_admin_continuous':
                self.load_medication_admin_continuous_data(sample_size, table_columns, table_filters)
            elif table == 'patient_assessments':
                self.load_patient_assessments_data(sample_size, table_columns, table_filters)
            elif table == 'respiratory_support':
                self.load_respiratory_support_data(sample_size, table_columns, table_filters)
            elif table == 'position':
                self.load_position_data(sample_size, table_columns, table_filters)
            else:
                print(f"Warning: Unknown table '{table}', skipping.")
    
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
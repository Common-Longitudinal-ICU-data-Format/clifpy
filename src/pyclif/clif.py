import os
import pandas as pd
import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import re

from .utils.io import load_data 
from .tables.adt import adt
from .tables.hospitalization import hospitalization
from .tables.labs import labs
from .tables.medication_admin_continuous import medication_admin_continuous
from .tables.patient import patient
from .tables.patient_assessments import patient_assessments
from .tables.position import position
from .tables.respiratory_support import respiratory_support
from .tables.vitals import vitals

class CLIF:
    def __init__(self, data_dir, filetype='csv', timezone ="UTC"):
        self.data_dir = data_dir
        self.filetype = filetype
        self.timezone = timezone
        
        self.patient = None
        self.hospitalization = None
        self.lab = None
        self.adt = None
        self.respiratory_support = None
        self.vitals = None
        self.medication_admin_continuous = None
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
        data = load_data('patient', self.data_dir, self.filetype, sample_size, columns, filters)
        self.patient = patient(data)
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
        data = load_data('hospitalization', self.data_dir, self.filetype, sample_size, columns, filters)
        self.hospitalization = hospitalization(data)
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
        data = load_data('labs', self.data_dir, self.filetype, sample_size, columns, filters)
        self.lab = labs(data)
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
        data = load_data('adt', self.data_dir, self.filetype, sample_size, columns, filters)
        self.adt = adt(data)
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
        data = load_data('respiratory_support', self.data_dir, self.filetype, sample_size, columns, filters)
        self.respiratory_support = respiratory_support(data)
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
        data = load_data('vitals', self.data_dir, self.filetype, sample_size, columns, filters)
        self.vitals = vitals(data)
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
        data = load_data('medication_admin_continuous', self.data_dir, self.filetype, sample_size, columns, filters)
        self.medication_admin_continuous = medication_admin_continuous(data)
        return self.medication_admin_continuous

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

   

    # def stitch - input is adt, hospitalization
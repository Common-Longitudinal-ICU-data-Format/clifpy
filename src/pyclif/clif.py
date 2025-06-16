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
        ## create a cohort object, check if cohort is not None, 
        # then only load those for each table
        print('CLIF Object Initialized.')

    def initialize(self, tables=None):
        if tables is None:
            tables = ['patient']
        for table in tables:
            # need to add support for filters mcide and column required 
            data = load_data(table, self.data_dir,self.filetype)
            if table == 'patient':
                self.patient = patient(data)
            elif table == 'hospitalization':
                self.hospitalization = hospitalization(data)
            elif table == 'lab':
                self.lab = labs(data)
            elif table == 'adt':
                self.adt = adt(data)
            elif table == 'respiratory_support':
                self.respiratory_support = respiratory_support(data)
            elif table == 'vitals':
                self.vitals = vitals(data)
            elif table == 'medication_admin_continuous':
                self.vitals = medication_admin_continuous(data)

   

    # def stitch - input is adt, hospitalization
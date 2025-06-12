import os
import pandas as pd
import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import re

from .utils.io import load_data 
from .tables.patient import patient
from .tables.hospitalization import hospitalization
from .tables.lab import LabTable
from .tables.adt import AdtTable
from .tables.respiratory_support import RespiratorySupportTable
from .tables.vitals import VitalsTable

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
                self.lab = LabTable(data)
            elif table == 'adt':
                self.adt = AdtTable(data)
            elif table == 'respiratory_support':
                self.respiratory_support = RespiratorySupportTable(data)
            elif table == 'vitals':
                self.vitals = VitalsTable(data)


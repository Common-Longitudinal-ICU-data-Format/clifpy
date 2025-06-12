import os
import pandas as pd
import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import re

from .utils.io import load_data 
from .models.patient import patient
from .models.hospitalization import hospitalization
from .models.lab import LabTable
from .models.adt import AdtTable
from .models.respiratory_support import RespiratorySupportTable
from .models.vitals import VitalsTable

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


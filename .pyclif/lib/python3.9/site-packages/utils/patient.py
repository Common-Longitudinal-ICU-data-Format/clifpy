# src/pyclif/tables/patient.py
import pandas as pd
import duckdb
import json
import os
import re
from datetime import datetime
from ..utils.validators import Validator

class Patient:
    def __init__(self, data_dir=None, df: pd.DataFrame = None, schema=None):
        self.table_name = 'patient'
        self.data_dir = data_dir
        self.df = df
        self.schema = schema
        self.duck = duckdb

        self.get_duckdb_register()

    def get_duckdb_register(self):
        try:
            self.duck.unregister(self.table_name)
        except:
            pass
        finally:
            self.duck.register(self.table_name, self.df)

    def validate(self):
        # Perform validations using the schema info
        table_schema = self.schema.tables.get(self.table_name, {})
        validator = Validator(self.df, table_schema)
        validator.check_missing_columns()
        validator.check_data_types()
        validator.check_duplicates(id_cols=['patient_id'])
        validator.check_datetime_format()

    def add_clif_category(self, mappings_path=None, export=True):
        # Example function to map categories if needed
        # Implement logic similar to previous code if desired
        pass
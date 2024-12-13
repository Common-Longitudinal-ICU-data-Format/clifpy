# src/pyclif/utils/validators.py
import pandas as pd
import numpy as np
import re

class Validator:
    def __init__(self, df, table_schema):
        self.df = df
        self.table_schema = table_schema
        # table_schema expected to have {col: dtype}, etc.

    def check_missing_columns(self):
        required_cols = list(self.table_schema.keys())
        missing = [c for c in required_cols if c not in self.df.columns]
        if missing:
            print("❌ Missing required columns:", missing)
        else:
            print("✅ No missing required columns.")

    def check_data_types(self):
        # Check if columns match expected dtypes (roughly)
        for col, dtype in self.table_schema.items():
            if col in self.df.columns:
                # Simple heuristic: if dtype = VARCHAR -> string, DATETIME -> datetime64, INTEGER -> int, DOUBLE -> float
                if dtype == "VARCHAR":
                    # Just a check if it's object/string:
                    if not pd.api.types.is_string_dtype(self.df[col]):
                        print(f"❌ Column {col} should be string but is not.")
                elif dtype == "DATETIME":
                    if not pd.api.types.is_datetime64_any_dtype(self.df[col]):
                        # Try to convert:
                        self.df[col] = pd.to_datetime(self.df[col], errors='coerce', infer_datetime_format=True)
                        if not pd.api.types.is_datetime64_any_dtype(self.df[col]):
                            print(f"❌ Column {col} should be datetime but could not convert.")
                elif dtype == "INTEGER":
                    if not pd.api.types.is_integer_dtype(self.df[col]):
                        # Try converting
                        self.df[col] = pd.to_numeric(self.df[col], errors='coerce', downcast='integer')
                        if not pd.api.types.is_integer_dtype(self.df[col]):
                            print(f"❌ Column {col} should be integer but could not convert.")
                elif dtype == "DOUBLE":
                    if not pd.api.types.is_float_dtype(self.df[col]):
                        self.df[col] = pd.to_numeric(self.df[col], errors='coerce', downcast='float')
                        if not pd.api.types.is_float_dtype(self.df[col]):
                            print(f"❌ Column {col} should be float/double but could not convert.")
        print("✅ Data type checks completed.")

    def check_duplicates(self, id_cols):
        if all(c in self.df.columns for c in id_cols):
            dupes = self.df[self.df.duplicated(subset=id_cols, keep=False)]
            if not dupes.empty:
                print(f"❌ Found duplicates based on {id_cols}.")
            else:
                print("✅ No duplicates found.")
        else:
            print(f"❗ ID columns {id_cols} not present for duplicate check.")

    def check_datetime_format(self):
        # For datetime columns, just ensure they're properly parsed:
        # Already handled in check_data_types - here we can ensure formatting.
        # If you want strict checks (YYYY-MM-DD):
        date_pattern = re.compile(r'^\d{4}-\d{2}-\d{2}')
        for col, dtype in self.table_schema.items():
            if dtype == "DATETIME" and col in self.df.columns:
                # Check format
                non_na_values = self.df[col].dropna()
                # Convert again just to be sure
                self.df[col] = pd.to_datetime(self.df[col], errors='coerce', infer_datetime_format=True)
                # Check the string format
                formatted = self.df[col].dt.strftime('%Y-%m-%d')
                if not all(formatted.str.match(date_pattern)):
                    print(f"❌ Some values in {col} do not match the YYYY-MM-DD format.")
        print("✅ Datetime format checks completed.")

# src/pyclif/utils/schema.py
import json
import os

class Schema:
    def __init__(self, schema_path):
        if not os.path.exists(schema_path):
            raise FileNotFoundError(f"Schema file {schema_path} not found.")
        with open(schema_path, 'r') as f:
            self.tables = json.load(f)

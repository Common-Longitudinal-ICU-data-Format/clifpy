# src/pyclif/clif.py
import os
import json
from .utils.io import load_table
from .utils.patient import Patient
from .utils.hospitalization import Hospitalization
from .utils.respiratory_support import RespiratorySupport
    
class CLIF:
    def __init__(self, data_dir, filetype='csv', config_path=None):
        """
        Initialize the CLIF object.

        Args:
            data_dir (str): Directory where data files are stored.
            filetype (str): Type of files to load ('csv' or 'parquet').
            config_path (str): Path to configuration JSON. If None, uses default.
        """
        self.data_dir = data_dir
        self.filetype = filetype
        # self.config = Config(config_path=config_path)
        # self.schema = Schema(os.path.join(os.path.dirname(__file__), '..', '..', 'data', 'all_tables.json'))
        schema_path = os.path.join(os.path.dirname(__file__), '..', '..', 'data', 'all_tables.json')
        with open(schema_path, 'r') as f:
            self.schema = json.load(f)
        self.loaded_tables = {}
        print('CLIF Object Initialized.')

    def load(self, table_list=None, columns=None, filters=None, sample_size=None):
        """
        Load specified tables into the CLIF object as pandas DataFrames wrapped by table classes.

        Args:
            table_list (list[str]): Names of tables to load. If None, load all.
            columns (dict): Optional dict of {table_name: [col1, col2...]} to select columns.
            filters (dict): Optional dict of {table_name: {col_name: value_or_list}} for filtering.
            sample_size (int): Optional limit on the number of rows per table.
        """
        if table_list is None:
            table_list = list(self.schema.tables.keys())

        for table in table_list:
            table_path = os.path.join(self.data_dir, f"{table}.{self.filetype}")
            print("print table path", table_path)
            if not os.path.exists(table_path):
                print(f"Warning: {table_path} does not exist. Skipping.")
                continue
            c = columns[table] if (columns and table in columns) else None
            f = filters[table] if (filters and table in filters) else None
            df = load_table(table_path, filetype=self.filetype, columns=c, filters=f, sample_size=sample_size)

            # Instantiate table-specific class
            if table == 'clif_patient':
                self.patient = Patient(data_dir=self.data_dir, df=df, schema=self.schema)
                self.loaded_tables['clif_patient'] = True
            elif table == 'clif_hospitalization':
                self.hospitalization = Hospitalization(data_dir=self.data_dir, df=df, schema=self.schema)
                self.loaded_tables['clif_hospitalization'] = True
            elif table == 'clif_respiratory_support':
                self.respiratory_support = RespiratorySupport(data_dir=self.data_dir, df=df, schema=self.schema)
                self.loaded_tables['clif_respiratory_support'] = True
            else:
                # For other tables (if implemented)
                self.loaded_tables[table] = df

        print("Loaded tables:", list(self.loaded_tables.keys()))

    def get_loaded_tables(self):
        return list(self.loaded_tables.keys())

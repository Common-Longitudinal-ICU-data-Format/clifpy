from datetime import datetime
from enum import Enum
from typing import List, Optional
from tqdm import tqdm
import pandas as pd
from ..utils.io import load_data
from ..utils.validator import validate_table


class patient:
    """Patient table wrapper using JSON-spec validation."""

    def __init__(self, data: Optional[pd.DataFrame] = None):
        self.df: Optional[pd.DataFrame] = data
        self.errors: List[dict] = []

        if self.df is not None:
            self.validate()

    # ------------------------------------------------------------------
    # Constructors
    # ------------------------------------------------------------------
    @classmethod
    def from_file(cls, table_path: str, table_format_type: str = "parquet", timezone: str = "UTC"):
        """Load the patient table from *table_path* and build a :class:`patient`."""
        data = load_data("patient", table_path, table_format_type, site_tz=timezone)
        return cls(data)

    # ------------------------------------------------------------------
    # Public helpers
    # ------------------------------------------------------------------
    def isvalid(self) -> bool:
        """Return ``True`` if the last :pyfunc:`validate` finished without errors."""
        return not self.errors

    def validate(self):
        """Validate ``self.df`` against the mCIDE *PatientModel.json* spec."""
        if self.df is None:
            print("No dataframe to validate.")
            return

        # Run shared validation utility
        self.errors = validate_table(self.df, "patient")

        # User-friendly status output
        if not self.errors:
            print("Validation completed successfully.")
        else:
            print(f"Validation completed with {len(self.errors)} error(s). See `errors` attribute.")

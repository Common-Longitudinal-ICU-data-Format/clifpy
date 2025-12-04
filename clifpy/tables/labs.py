from typing import Optional, Dict, List, Union
import os
import pandas as pd
from .base_table import BaseTable


class Labs(BaseTable):
    """
    Labs table wrapper inheriting from BaseTable.
    
    This class handles laboratory data and validations including
    reference unit validation while leveraging the common functionality
    provided by BaseTable.
    """
    
    def __init__(
        self,
        data_directory: str = None,
        filetype: str = None,
        timezone: str = "UTC",
        output_directory: Optional[str] = None,
        data: Optional[pd.DataFrame] = None
    ):
        """
        Initialize the labs table.
        
        Parameters
        ----------
        data_directory : str
            Path to the directory containing data files
        filetype : str
            Type of data file (csv, parquet, etc.)
        timezone : str
            Timezone for datetime columns
        output_directory : str, optional
            Directory for saving output files and logs
        data : pd.DataFrame, optional
            Pre-loaded data to use instead of loading from file
        """
        # For backward compatibility, handle the old signature
        if data_directory is None and filetype is None and data is not None:
            # Old signature: labs(data)
            # Use dummy values for required parameters
            data_directory = "."
            filetype = "parquet"
        
        # Initialize lab reference units
        self._lab_reference_units = None
        
        super().__init__(
            data_directory=data_directory,
            filetype=filetype,
            timezone=timezone,
            output_directory=output_directory,
            data=data
        )
        
        # Load lab-specific schema data
        self._load_labs_schema_data()

    def _load_labs_schema_data(self):
        """Load lab reference units from the YAML schema."""
        if self.schema:
            self._lab_reference_units = self.schema.get('lab_reference_units', {})

    def get_lab_reference_units(
        self,
        as_dataframe: bool = True,
        save: bool = False
    ) -> Union[pd.DataFrame, Dict[str, Dict[str, int]]]:
        """
        Get all unique reference units observed in the data,
        grouped by lab_category (if present) along with their counts.

        Parameters
        ----------
        as_dataframe : bool, default True
            If True, return a DataFrame with columns ['lab_category', 'reference_unit', 'count'].
            If False, return a nested dict.
        save : bool, default False
            If True, save the results to the output directory as a CSV file.

        Returns
        -------
        Dict[str, Dict[str, int]] or pd.DataFrame
            If as_dataframe=False:
                Example:
                {
                    "lab_test1": {
                        "mg/dL": 100,
                        "mmol/L": 1000
                    },
                    ...
                }
                If 'lab_category' not in columns, returns a single key 'all'.
            If as_dataframe=True:
                pd.DataFrame with columns: ['lab_category', 'reference_unit', 'count']
        """
        if self.df is None or 'reference_unit' not in self.df.columns:
            if as_dataframe:
                return pd.DataFrame(columns=['lab_category', 'reference_unit', 'count'])
            return {}

        if 'lab_category' in self.df.columns:
            value_counts = (
                self.df.groupby(['lab_category', 'reference_unit'])
                .size()
                .reset_index(name='count')
                .sort_values(['lab_category', 'reference_unit'])
            )
            result_df = value_counts.reset_index(drop=True)
        else:
            vc = self.df['reference_unit'].value_counts()
            result_df = vc.reset_index()
            result_df.columns = ['reference_unit', 'count']
            result_df.insert(0, 'lab_category', 'all')
            result_df = result_df[['lab_category', 'reference_unit', 'count']]

        if save:
            csv_path = os.path.join(self.output_directory, 'lab_reference_units.csv')
            result_df.to_csv(csv_path, index=False)
            self.logger.info(f"Saved lab reference units to {csv_path}")

        if as_dataframe:
            return result_df
        else:
            if 'lab_category' in self.df.columns:
                result = {}
                for _, row in result_df.iterrows():
                    lab = row['lab_category']
                    unit = row['reference_unit']
                    count = int(row['count'])
                    result.setdefault(lab, {})[unit] = count
                return result
            return {'all': self.df['reference_unit'].value_counts().to_dict()}


    # def fix_lab_reference_units(self):
    #     """
    #     Standardize reference unit string formats for mild variations (e.g., mm[hg] -> mmHg, 10*3/ul -> 10^3/ul, second(s) -> sec).
    #     Does NOT perform conversions for different metrics (e.g., mg/dL to mmol/L). Warns user for such needed conversions.
    #     Modifies self.df in place.
    #     """
    #     import warnings

    #     if self.df is None or self._lab_reference_units is None:
    #         return

    #     if 'lab_category' not in self.df.columns or 'reference_unit' not in self.df.columns:
    #         return

    #     # Map of common unit string fixes (case insensitive matching)
    #     unit_string_fixes = {
    #         r"mm\[?hg\]?": "mmHg",
    #         r"\b10\s*\*\s*3\s*/\s*ul\b": "10^3/ul",
    #         r"\biu/l\b": "U/L",
    #         r"\biu\/l\b": "U/L",
    #         r"\bsecond\(s\)": "sec",
    #         r"\bseconds\b": "sec",
    #         r"\bsecond\b": "sec"
    #         # add more as needed
    #     }

    #     import re

    #     def clean_unit_string(from_unit):
    #         u = from_unit.strip() if isinstance(from_unit, str) else from_unit
    #         if not isinstance(u, str):
    #             return u
    #         for pattern, repl in unit_string_fixes.items():
    #             u_new = re.sub(pattern, repl, u, flags=re.IGNORECASE)
    #             if u_new != u:
    #                 return u_new
    #         return u

    #     # Inference for which units are "string-fixable" versus conversion-needed
    #     conversion_warnings = set()

    #     def convert_row(row):
    #         lab = row['lab_category']
    #         from_unit = row['reference_unit']
    #         value = row['lab_value_numeric']
    #         lab_unit_map = self._lab_reference_units.get(lab, {})

    #         if from_unit in lab_unit_map:
    #             unit_entry = lab_unit_map[from_unit]
    #             if (
    #                 isinstance(unit_entry, dict)
    #                 and "target_unit" in unit_entry
    #                 and "factor" in unit_entry
    #             ):
    #                 target_unit = unit_entry["target_unit"]
    #                 conversion_factor = unit_entry["factor"]
    #                 if (
    #                     from_unit.strip().lower() != target_unit.strip().lower()
    #                 ):
    #                     # Fix if this is a simple string formatting change (case, punct, super/subscripts, etc)
    #                     cleaned_from_unit = clean_unit_string(from_unit)
    #                     cleaned_target_unit = clean_unit_string(target_unit)
    #                     # Only "convert" if string-clean can match the target after cleaning
    #                     if cleaned_from_unit.lower() == cleaned_target_unit.lower():
    #                         return pd.Series([value, cleaned_target_unit])
    #                     else:
    #                         # Only string standardization is supported; warn user if more complex
    #                         conversion_warnings.add(
    #                             f"Lab '{lab}': Unit '{from_unit}' differs from target '{target_unit}'. "
    #                             f"No automatic conversion performed. Please review!"
    #                         )
    #                         return pd.Series([value, from_unit])
    #                 else:
    #                     # Already matches (possible only case difference)
    #                     return pd.Series([value, target_unit])
    #         # If there is no mapping or nonstandard entry, fallback to string cleaning for typical cases
    #         cleaned = clean_unit_string(from_unit)
    #         return pd.Series([value, cleaned])

    #     self.df[['lab_value_numeric', 'reference_unit']] = self.df.apply(convert_row, axis=1)

    #     # Show warnings for users if conversion-warranting units have been found
    #     for msg in conversion_warnings:
    #         warnings.warn(msg)

    # ------------------------------------------------------------------
    # Labs Specific Methods
    # ------------------------------------------------------------------
    def get_lab_category_stats(self) -> pd.DataFrame:
        """Return summary statistics for each lab category, including missingness and unique hospitalization_id counts."""
        if (
            self.df is None
            or 'lab_value_numeric' not in self.df.columns
            or 'hospitalization_id' not in self.df.columns        # remove this line if hosp-id is optional
        ):
            return {"status": "Missing columns"}
        
        stats = (
            self.df
            .groupby('lab_category')
            .agg(
                count=('lab_value_numeric', 'count'),
                unique=('hospitalization_id', 'nunique'),
                missing_pct=('lab_value_numeric', lambda x: 100 * x.isna().mean()),
                mean=('lab_value_numeric', 'mean'),
                std=('lab_value_numeric', 'std'),
                min=('lab_value_numeric', 'min'),
                q1=('lab_value_numeric', lambda x: x.quantile(0.25)),
                median=('lab_value_numeric', 'median'),
                q3=('lab_value_numeric', lambda x: x.quantile(0.75)),
                max=('lab_value_numeric', 'max'),
            )
            .round(2)
        )

        return stats
    
    def get_lab_specimen_stats(self) -> pd.DataFrame:
        """Return summary statistics for each lab category, including missingness and unique hospitalization_id counts."""
        if (
            self.df is None
            or 'lab_value_numeric' not in self.df.columns
            or 'hospitalization_id' not in self.df.columns 
            or 'lab_speciment_category' not in self.df.columns       # remove this line if hosp-id is optional
        ):
            return {"status": "Missing columns"}
        
        stats = (
            self.df
            .groupby('lab_specimen_category')
            .agg(
                count=('lab_value_numeric', 'count'),
                unique=('hospitalization_id', 'nunique'),
                missing_pct=('lab_value_numeric', lambda x: 100 * x.isna().mean()),
                mean=('lab_value_numeric', 'mean'),
                std=('lab_value_numeric', 'std'),
                min=('lab_value_numeric', 'min'),
                q1=('lab_value_numeric', lambda x: x.quantile(0.25)),
                median=('lab_value_numeric', 'median'),
                q3=('lab_value_numeric', lambda x: x.quantile(0.75)),
                max=('lab_value_numeric', 'max'),
            )
            .round(2)
        )

        return stats
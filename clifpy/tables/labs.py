from typing import Optional, Dict, List, Union
import os
import re
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


    def _normalize_unit(self, unit: str) -> str:
        """
        Normalize a unit string for comparison by removing special characters,
        standardizing common variations, and lowercasing.
        """
        if not isinstance(unit, str):
            return ""

        normalized = unit.lower().strip()

        # Remove brackets, parentheses, and trailing annotations like "(calc)"
        normalized = re.sub(r'[\[\]\(\)]', '', normalized)
        normalized = re.sub(r'\s*calc\s*$', '', normalized)

        # Standardize common variations
        replacements = [
            (r'\s+', ''),           # Remove whitespace
            (r'μ|µ', 'u'),          # Greek mu (U+03BC) and micro sign (U+00B5) to u
            (r'\^', ''),            # Remove caret
            (r'\*', ''),            # Remove asterisk
            (r'hours?', 'hr'),      # hour/hours -> hr
            (r'seconds?', 'sec'),   # second/seconds -> sec
            (r'^s$', 'sec'),        # lone 's' -> sec
            (r'minutes?', 'min'),   # minute/minutes -> min
            (r'iu', 'u'),           # iU -> U
            (r'\bgm\b', 'g'),       # gm -> g (gram)
            (r'k/ul', '103/ul'),    # k/uL -> 10^3/uL
            (r'10e3', '103'),       # 10e3 -> 10^3
            (r'x10e3', '103'),      # x10E3 -> 10^3
            (r'x103', '103'),       # x10^3 -> 10^3
            (r'\bpg\b', 'ng'),      # pg -> ng (picogram to nanogram context)
            (r',', ''),             # Remove commas
        ]

        for pattern, repl in replacements:
            normalized = re.sub(pattern, repl, normalized)

        return normalized

    def _find_matching_target_unit(
        self,
        source_unit: str,
        target_units: List[str]
    ) -> Optional[str]:
        """
        Find the best matching target unit for a source unit using normalized comparison.

        Parameters
        ----------
        source_unit : str
            The unit string from the data
        target_units : List[str]
            List of acceptable target units from schema (first is preferred)

        Returns
        -------
        Optional[str]
            The matching target unit, or None if no match found
        """
        if not source_unit or not target_units:
            return None

        normalized_source = self._normalize_unit(source_unit)

        # Check for exact match first
        if source_unit in target_units:
            return source_unit

        # Check normalized matches against all target units
        for target in target_units:
            if self._normalize_unit(target) == normalized_source:
                # Return the preferred (first) target unit
                return target_units[0]

        return None

    def standardize_reference_units(
        self,
        inplace: bool = True,
        save_mapping: bool = False,
        lowercase: bool = False
    ) -> Optional[pd.DataFrame]:
        """
        Standardize reference unit strings to match the schema's target units.

        Uses fuzzy matching to detect similar unit strings (e.g., 'mmhg' -> 'mmHg',
        '10*3/ul' -> '10^3/μL', 'hr' -> 'hour') and converts them to the preferred
        target unit defined in the schema.

        This does NOT perform value conversions between different unit types
        (e.g., mg/dL to mmol/L). Units that don't match any target will be logged
        as warnings.

        Parameters
        ----------
        inplace : bool, default True
            If True, modify self.df in place. If False, return a copy.
        save_mapping : bool, default False
            If True, save a CSV of the unit mappings applied to the output directory.
        lowercase : bool, default False
            If True, convert all reference units to lowercase instead of using
            the schema's original casing (e.g., 'mg/dl' instead of 'mg/dL').

        Returns
        -------
        Optional[pd.DataFrame]
            If inplace=False, returns the modified DataFrame. Otherwise None.
        """
        if self.df is None:
            self.logger.warning("No data loaded")
            return None

        if 'lab_category' not in self.df.columns or 'reference_unit' not in self.df.columns:
            self.logger.warning("Required columns 'lab_category' and/or 'reference_unit' not found")
            return None

        if not self._lab_reference_units:
            self.logger.warning("No lab reference units defined in schema")
            return None

        df = self.df if inplace else self.df.copy()

        # Track mappings for logging/saving
        mappings_applied = []
        unmatched_units = []

        # Get unique lab_category + reference_unit combinations
        unique_combos = df[['lab_category', 'reference_unit']].drop_duplicates()

        # Build mapping dictionary
        unit_mapping = {}

        for _, row in unique_combos.iterrows():
            lab_cat = row['lab_category']
            source_unit = row['reference_unit']

            if pd.isna(source_unit):
                continue

            target_units = self._lab_reference_units.get(lab_cat, [])

            if not target_units:
                continue

            matched_target = self._find_matching_target_unit(source_unit, target_units)

            if matched_target:
                # Apply lowercase if requested
                final_target = matched_target.lower() if lowercase else matched_target

                if final_target != source_unit:
                    unit_mapping[(lab_cat, source_unit)] = final_target

                    # Check if the only difference is mu character (µ vs μ) or case
                    source_normalized_mu = source_unit.replace('µ', 'μ')
                    is_mu_only_diff = source_normalized_mu == matched_target
                    is_case_only_diff = source_unit.lower() == matched_target.lower()

                    mappings_applied.append({
                        'lab_category': lab_cat,
                        'source_unit': source_unit,
                        'target_unit': final_target,
                        'silent': is_mu_only_diff or (lowercase and is_case_only_diff)
                    })

                    if not (is_mu_only_diff or (lowercase and is_case_only_diff)):
                        self.logger.info(
                            f"Mapping '{source_unit}' -> '{final_target}' for {lab_cat}"
                        )
            elif matched_target is None and source_unit not in target_units:
                unmatched_units.append({
                    'lab_category': lab_cat,
                    'source_unit': source_unit,
                    'expected_units': target_units
                })

        # Apply mappings
        if unit_mapping:
            def apply_mapping(row):
                key = (row['lab_category'], row['reference_unit'])
                return unit_mapping.get(key, row['reference_unit'])

            df['reference_unit'] = df.apply(apply_mapping, axis=1)

            # Count actual vs silent (mu-only) mappings
            actual_mappings = [m for m in mappings_applied if not m.get('silent')]
            if actual_mappings:
                self.logger.info(f"Applied {len(actual_mappings)} unit standardizations")

        # Apply lowercase to all reference units if requested (including unmatched ones)
        if lowercase:
            df['reference_unit'] = df['reference_unit'].str.lower()

        if not unit_mapping and not lowercase:
            self.logger.info("No unit standardizations needed")

        # Warn about unmatched units
        for item in unmatched_units:
            self.logger.warning(
                f"Unmatched unit '{item['source_unit']}' for {item['lab_category']}. "
                f"Expected one of: {item['expected_units']}"
            )

        # Save mapping if requested
        if save_mapping and mappings_applied:
            mapping_df = pd.DataFrame(mappings_applied)
            csv_path = os.path.join(self.output_directory, 'lab_unit_mappings.csv')
            mapping_df.to_csv(csv_path, index=False)
            self.logger.info(f"Saved unit mappings to {csv_path}")

        if not inplace:
            return df

        return None

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
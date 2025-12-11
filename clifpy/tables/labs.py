from typing import Optional, Dict, List, Union
import os
import re
import pandas as pd
from .base_table import BaseTable

# Pre-compiled regex patterns for unit normalization (compiled once at import)
_BRACKET_RE = re.compile(r'[\[\]\(\)]')
_CALC_RE = re.compile(r'\s*calc\s*$')
_UNIT_REPLACEMENTS = [
    (re.compile(r'\s+'), ''),           # Remove whitespace
    (re.compile(r'μ|µ'), 'u'),          # Greek mu to u
    (re.compile(r'\^'), ''),            # Remove caret
    (re.compile(r'\*'), ''),            # Remove asterisk
    (re.compile(r'hours?'), 'hr'),      # hour/hours -> hr
    (re.compile(r'seconds?'), 'sec'),   # second/seconds -> sec
    (re.compile(r'^s$'), 'sec'),        # lone 's' -> sec
    (re.compile(r'minutes?'), 'min'),   # minute/minutes -> min
    (re.compile(r'iu'), 'u'),           # iU -> U
    (re.compile(r'\bgrams?\b'), 'g'),   # gram/grams -> g
    (re.compile(r'\bgm\b'), 'g'),       # gm -> g
    (re.compile(r'k/ul'), '103/ul'),    # k/uL -> 10^3/uL
    (re.compile(r'10e3'), '103'),       # 10e3 -> 10^3
    (re.compile(r'x10e3'), '103'),      # x10E3 -> 10^3
    (re.compile(r'x103'), '103'),       # x10^3 -> 10^3
    (re.compile(r'\bpg/ml\b'), 'ng/l'),  # if it's only 'pg/ml', then it becomes 'ng/l'
    (re.compile(r','), ''),             # Remove commas
]


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

    def _validate_required_columns(self, required: set) -> set:
        """Check for required columns, return set of missing columns."""
        try:
            import polars as pl
            if isinstance(self.df, pl.LazyFrame):
                columns = set(self.df.collect_schema().names())
            else:
                columns = set(self.df.columns)
        except ImportError:
            columns = set(self.df.columns)
        return required - columns

    def _to_lazy_frame(self):
        """Convert self.df to a Polars LazyFrame."""
        import polars as pl
        if isinstance(self.df, pd.DataFrame):
            return pl.from_pandas(self.df).lazy()
        elif isinstance(self.df, pl.LazyFrame):
            return self.df
        elif isinstance(self.df, pl.DataFrame):
            return self.df.lazy()
        raise TypeError(f"Unsupported dataframe type: {type(self.df)}")

    def _get_lab_reference_units_polars(self) -> 'pl.DataFrame':
        """
        Polars-optimized implementation for get_lab_reference_units.

        Uses lazy evaluation and streaming to efficiently process large datasets
        without loading everything into memory at once.

        Returns
        -------
        pl.DataFrame
            Aggregated counts by (lab_category, reference_unit).
        """
        import polars as pl

        required = {'lab_category', 'reference_unit'}
        missing = self._validate_required_columns(required)
        if missing:
            self.logger.warning(f"Missing columns: {missing} - cannot compute reference units")
            return pl.DataFrame(schema={'lab_category': pl.Utf8, 'reference_unit': pl.Utf8, 'count': pl.UInt32})

        cols = ['lab_category', 'reference_unit']
        lf = self._to_lazy_frame().select(cols)

        # Build and execute query with streaming
        return (
            lf
            .group_by(['lab_category', 'reference_unit'])
            .agg(pl.len().alias('count'))
            .sort(['lab_category', 'reference_unit'])
            .collect(streaming=True)
        )

    def _get_lab_reference_units_pandas(self) -> 'pd.DataFrame':
        """
        Pandas implementation for get_lab_reference_units.

        Fallback for systems where Polars is not available.

        Returns
        -------
        pd.DataFrame
            Aggregated counts by (lab_category, reference_unit).
        """
        required = {'lab_category', 'reference_unit'}
        missing = self._validate_required_columns(required)
        if missing:
            self.logger.warning(f"Missing columns: {missing} - cannot compute reference units")
            return pd.DataFrame(columns=['lab_category', 'reference_unit', 'count'])

        return (
            self.df
            .groupby(['lab_category', 'reference_unit'], sort=False)
            .size()
            .reset_index(name='count')
            .sort_values(['lab_category', 'reference_unit'])
        )

    def get_lab_reference_units(
        self,
        save: bool = False,
        output_directory: Optional[str] = None
    ) -> pd.DataFrame:
        """
        Get all unique reference units observed in the data,
        grouped by lab_category along with their counts.

        Uses Polars for efficient processing of large datasets, with automatic
        fallback to pandas if Polars is unavailable or fails.

        Parameters
        ----------
        save : bool, default False
            If True, save the results to the output directory as a CSV file.
        output_directory : str, optional
            Directory to save results. If None, uses self.output_directory.

        Returns
        -------
        pd.DataFrame
            DataFrame with columns: ['lab_category', 'reference_unit', 'count']
        """
        if self.df is None:
            raise ValueError("No data")

        # Try Polars first (more efficient for large data), fall back to pandas
        try:
            result_pl = self._get_lab_reference_units_polars()
            result_df = result_pl.to_pandas()
            self.logger.debug("Used Polars for get_lab_reference_units")
        except Exception as e:
            self.logger.debug(f"Polars failed ({e}), falling back to pandas")
            # Ensure we have a pandas DataFrame for the fallback
            if not isinstance(self.df, pd.DataFrame):
                try:
                    self.df = self.df.to_pandas() if hasattr(self.df, 'to_pandas') else pd.DataFrame(self.df)
                except Exception:
                    return pd.DataFrame(columns=['lab_category', 'reference_unit', 'count'])
            result_df = self._get_lab_reference_units_pandas()

        if save:
            save_dir = output_directory if output_directory is not None else self.output_directory
            os.makedirs(save_dir, exist_ok=True)
            csv_path = os.path.join(save_dir, 'lab_reference_units.csv')
            result_df.to_csv(csv_path, index=False)
            self.logger.info(f"Saved lab reference units to {csv_path}")

        return result_df


    def _normalize_unit(self, unit: str) -> str:
        """
        Normalize a unit string for comparison by removing special characters,
        standardizing common variations, and lowercasing.
        """
        if not isinstance(unit, str):
            return ""

        normalized = unit.lower().strip()
        normalized = _BRACKET_RE.sub('', normalized)
        normalized = _CALC_RE.sub('', normalized)

        for pattern, repl in _UNIT_REPLACEMENTS:
            normalized = pattern.sub(repl, normalized)

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

    def _build_unit_mapping(
        self,
        unique_combos_df: pd.DataFrame,
        lowercase: bool
    ) -> tuple:
        """
        Build unit mapping dictionary from unique lab_category + reference_unit combinations.

        Returns tuple of (unit_mapping dict, mappings_applied list, unmatched_units list)
        """
        unit_mapping = {}
        mappings_applied = []
        unmatched_units = []
        loggable_mappings = []  # Batch logging at the end

        for lab_cat, source_unit in unique_combos_df.itertuples(index=False):
            if pd.isna(source_unit):
                continue

            target_units = self._lab_reference_units.get(lab_cat, [])
            if not target_units:
                continue

            matched_target = self._find_matching_target_unit(source_unit, target_units)

            if matched_target:
                final_target = matched_target.lower() if lowercase else matched_target

                if final_target != source_unit:
                    unit_mapping[(lab_cat, source_unit)] = final_target

                    # Check if change is cosmetic (mu char or case only)
                    is_mu_only_diff = source_unit.replace('µ', 'μ') == matched_target
                    is_case_only_diff = source_unit.lower() == matched_target.lower()
                    is_silent = is_mu_only_diff or (lowercase and is_case_only_diff)

                    mappings_applied.append({
                        'lab_category': lab_cat,
                        'source_unit': source_unit,
                        'target_unit': final_target,
                        'silent': is_silent
                    })

                    if not is_silent:
                        loggable_mappings.append((source_unit, final_target, lab_cat))

            elif source_unit not in target_units:
                unmatched_units.append({
                    'lab_category': lab_cat,
                    'source_unit': source_unit,
                    'expected_units': target_units
                })

        # Batch log all mappings at once
        for source, target, lab in loggable_mappings:
            self.logger.info(f"Mapping '{source}' -> '{target}' for {lab}")

        return unit_mapping, mappings_applied, unmatched_units

    def _standardize_reference_units_polars(
        self,
        unit_mapping: Dict,
        lowercase: bool
    ) -> 'pl.DataFrame':
        """
        Polars-optimized implementation for standardize_reference_units.

        Uses join-based mapping for O(n) performance instead of O(n*k) chained conditions.
        Always returns a new DataFrame; caller handles inplace assignment.
        """
        import polars as pl

        lf = self._to_lazy_frame()

        # Apply mappings using join (O(n) hash join vs O(n*k) chained conditions)
        if unit_mapping:
            mapping_df = pl.DataFrame({
                'lab_category': [k[0] for k in unit_mapping.keys()],
                '_source_unit': [k[1] for k in unit_mapping.keys()],
                '_target_unit': list(unit_mapping.values())
            }).lazy()

            lf = (
                lf
                .join(
                    mapping_df,
                    left_on=['lab_category', 'reference_unit'],
                    right_on=['lab_category', '_source_unit'],
                    how='left'
                )
                .with_columns(
                    pl.coalesce('_target_unit', 'reference_unit').alias('reference_unit')
                )
                .drop('_target_unit')
            )

        if lowercase:
            lf = lf.with_columns(
                pl.col('reference_unit').str.to_lowercase()
            )

        return lf.collect(streaming=True)

    def _standardize_reference_units_pandas(
        self,
        unit_mapping: Dict,
        lowercase: bool
    ) -> pd.DataFrame:
        """
        Pandas implementation for standardize_reference_units.

        Uses merge-based mapping for O(n) performance instead of O(n*k) row-wise apply.
        Always returns a new DataFrame; caller handles inplace assignment.
        """
        df = self.df.copy()

        # Apply mappings using merge (O(n) vs O(n*k) for apply)
        if unit_mapping:
            mapping_df = pd.DataFrame([
                {'lab_category': k[0], '_source_unit': k[1], '_target_unit': v}
                for k, v in unit_mapping.items()
            ])

            df = df.merge(
                mapping_df,
                left_on=['lab_category', 'reference_unit'],
                right_on=['lab_category', '_source_unit'],
                how='left'
            )
            df['reference_unit'] = df['_target_unit'].fillna(df['reference_unit'])
            df = df.drop(columns=['_source_unit', '_target_unit'])

        if lowercase:
            df['reference_unit'] = df['reference_unit'].str.lower()

        return df

    def standardize_reference_units(
        self,
        inplace: bool = True,
        save: bool = False,
        lowercase: bool = False,
        output_directory: Optional[str] = None
    ) -> Optional[pd.DataFrame]:
        """
        Standardize reference unit strings to match the schema's target units.

        Uses Polars for efficient processing of large datasets, with automatic
        fallback to pandas if Polars is unavailable or fails.

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
        save : bool, default False
            If True, save a CSV of the unit mappings applied to the output directory.
        lowercase : bool, default False
            If True, convert all reference units to lowercase instead of using
            the schema's original casing (e.g., 'mg/dl' instead of 'mg/dL').
        output_directory : str, optional
            Directory to save results. If None, uses self.output_directory.

        Returns
        -------
        Optional[pd.DataFrame]
            If inplace=False, returns the modified DataFrame. Otherwise None.
        """
        if self.df is None:
            raise ValueError(
                "No data loaded. Please provide data using one of these methods:\n"
                "  1. Labs.from_file(data_directory=..., filetype=..., timezone=...)\n"
                "  2. Labs(data=your_dataframe)"
            )

        # Check for required columns
        missing = self._validate_required_columns({'lab_category', 'reference_unit'})
        if missing:
            raise ValueError(f"Required columns not found: {missing}")

        if not self._lab_reference_units:
            self.logger.warning("No lab reference units defined in schema")
            return None

        # Get unique combinations (works for both pandas and polars)
        try:
            import polars as pl
            if isinstance(self.df, (pl.DataFrame, pl.LazyFrame)):
                if isinstance(self.df, pl.LazyFrame):
                    unique_combos_df = (
                        self.df
                        .select(['lab_category', 'reference_unit'])
                        .unique()
                        .collect()
                        .to_pandas()
                    )
                else:
                    unique_combos_df = (
                        self.df
                        .select(['lab_category', 'reference_unit'])
                        .unique()
                        .to_pandas()
                    )
            else:
                unique_combos_df = self.df[['lab_category', 'reference_unit']].drop_duplicates()
        except ImportError:
            unique_combos_df = self.df[['lab_category', 'reference_unit']].drop_duplicates()

        # Build mapping dictionary (shared logic)
        unit_mapping, mappings_applied, unmatched_units = self._build_unit_mapping(
            unique_combos_df, lowercase
        )

        # Try Polars first, fall back to pandas
        try:
            result_df = self._standardize_reference_units_polars(unit_mapping, lowercase)
            self.logger.debug("Used Polars for standardize_reference_units")
        except Exception as e:
            self.logger.debug(f"Polars failed ({e}), falling back to pandas")
            # Ensure we have a pandas DataFrame for the fallback
            if not isinstance(self.df, pd.DataFrame):
                try:
                    import polars as pl
                    if isinstance(self.df, (pl.DataFrame, pl.LazyFrame)):
                        if isinstance(self.df, pl.LazyFrame):
                            self.df = self.df.collect().to_pandas()
                        else:
                            self.df = self.df.to_pandas()
                    else:
                        self.df = pd.DataFrame(self.df)
                except Exception:
                    raise ValueError("Could not convert data to pandas DataFrame")
            result_df = self._standardize_reference_units_pandas(unit_mapping, lowercase)

        # Handle inplace at API level
        if inplace:
            self.df = result_df

        # Log results
        if unit_mapping:
            actual_mappings = [m for m in mappings_applied if not m.get('silent')]
            if actual_mappings:
                self.logger.info(f"Applied {len(actual_mappings)} unit standardizations")
        elif not lowercase:
            self.logger.info("No unit standardizations needed")

        # Warn about unmatched units
        for item in unmatched_units:
            self.logger.warning(
                f"Unmatched unit '{item['source_unit']}' for {item['lab_category']}. "
                f"Expected one of: {item['expected_units']}"
            )

        # Save mapping if requested
        if save and mappings_applied:
            save_dir = output_directory if output_directory is not None else self.output_directory
            os.makedirs(save_dir, exist_ok=True)
            mapping_df = pd.DataFrame(mappings_applied)
            csv_path = os.path.join(save_dir, 'lab_reference_unit_standardized.csv')
            mapping_df.to_csv(csv_path, index=False)
            self.logger.info(f"Saved unit mappings to {csv_path}")

        if not inplace:
            # Convert to pandas if returning
            try:
                import polars as pl
                if isinstance(result_df, pl.DataFrame):
                    return result_df.to_pandas()
            except ImportError:
                pass
            return result_df

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
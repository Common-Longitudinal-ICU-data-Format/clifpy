from typing import Optional, Dict
import os
import pandas as pd
import polars as pl
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
        """Load lab reference units from the YAML schema and unit variants."""
        if self.schema:
            self._lab_reference_units = self.schema.get('lab_reference_units', {})

        # Load unit variants and conversions from labs_standardization.yaml
        self._unit_variant_lookup = {}
        self._unit_conversions = {}
        self._lab_molecular_weights = {}
        try:
            import yaml
            schema_dir = os.path.join(
                os.path.dirname(os.path.dirname(__file__)),
                'schemas'
            )
            standardization_file = os.path.join(schema_dir, 'labs_standardization.yaml')

            if os.path.exists(standardization_file):
                with open(standardization_file, 'r') as f:
                    standardization_data = yaml.safe_load(f)

                # Build reverse lookup: normalized variant -> canonical unit (normalized)
                unit_variants = standardization_data.get('unit_variants', {})
                for canonical_unit, variants in unit_variants.items():
                    # Remove whitespace for normalization
                    canonical_normalized = canonical_unit.lower().replace(' ', '')
                    for variant in variants:
                        variant_normalized = variant.lower().replace(' ', '')
                        self._unit_variant_lookup[variant_normalized] = canonical_normalized

                self.logger.debug(f"Loaded {len(self._unit_variant_lookup)} unit variants")

                # Load conversion factors: target_unit -> {source_unit: factor}
                conversions = standardization_data.get('conversion', {})
                for target_unit, source_factors in conversions.items():
                    target_normalized = target_unit.lower().replace(' ', '')
                    self._unit_conversions[target_normalized] = {}
                    for source_unit, factor in source_factors.items():
                        source_normalized = source_unit.lower().replace(' ', '')
                        self._unit_conversions[target_normalized][source_normalized] = factor

                self.logger.debug(f"Loaded conversions for {len(self._unit_conversions)} target units")

                # Load molecular weights for MW-dependent conversions
                self._lab_molecular_weights = standardization_data.get('lab_molecular_weights', {})
                self.logger.debug(f"Loaded molecular weights for {len(self._lab_molecular_weights)} lab categories")

        except Exception as e:
            self.logger.warning(f"Could not load unit variants/conversions: {e}")

    def _is_mw_dependent_conversion(self, source_normalized: str, target_normalized: str) -> bool:
        """Check if conversion between these units requires molecular weight."""
        mmol_units = {'mmol/l', 'meq/l'}
        mg_units = {'mg/dl', 'mg/l'}
        return (source_normalized in mmol_units and target_normalized in mg_units) or \
               (source_normalized in mg_units and target_normalized in mmol_units)

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

    def _build_unit_mapping(
        self,
        unique_combos_df: pd.DataFrame,
        lowercase: bool
    ) -> tuple:
        """
        Build unit mapping dictionary from unique lab_category + reference_unit combinations.

        Uses the unit variants lookup from labs_standardization.yaml to match source units
        to their canonical forms, then maps to the target unit from labs_schema.yaml.

        Returns tuple of (unit_mapping dict, mappings_applied list, unmatched_units list)
        """
        unit_mapping = {}
        mappings_applied = []
        unmatched_units = []
        loggable_mappings = []  # Batch logging at the end

        # Acceptable values for unitless labs
        unitless_acceptable = {'', '(no units)', 'no units', 'none', 'unitless'}

        for lab_cat, source_unit in unique_combos_df.itertuples(index=False):
            if pd.isna(source_unit):
                continue

            # Strip whitespace from source unit
            source_unit_stripped = source_unit.strip() if source_unit else ''

            # Get target unit from schema (now a single string, not a list)
            target_unit = self._lab_reference_units.get(lab_cat)
            if not target_unit:
                # Unitless lab - warn if source has an unexpected unit
                source_lower = source_unit_stripped.lower()
                if source_lower and source_lower not in unitless_acceptable:
                    unmatched_units.append({
                        'lab_category': lab_cat,
                        'source_unit': source_unit,
                        'expected_unit': '(no units)'
                    })
                continue

            # Normalize source for lookup (lowercase, no whitespace)
            source_normalized = source_unit_stripped.lower().replace(' ', '')

            # Look up source unit's canonical form
            source_canonical = self._unit_variant_lookup.get(source_normalized)

            # Get target unit's canonical form (lowercase, no whitespace)
            target_normalized = target_unit.lower().replace(' ', '')
            target_canonical = self._unit_variant_lookup.get(target_normalized, target_normalized)

            # Check if source matches target's canonical form
            is_match = (source_canonical == target_canonical) if source_canonical else (source_normalized == target_normalized)

            if is_match:
                final_target = target_unit.lower() if lowercase else target_unit

                if final_target != source_unit:
                    unit_mapping[(lab_cat, source_unit)] = final_target

                    # Check if change is cosmetic (case/whitespace only)
                    is_case_only_diff = source_normalized == target_normalized
                    is_silent = lowercase and is_case_only_diff

                    mappings_applied.append({
                        'lab_category': lab_cat,
                        'source_unit': source_unit,
                        'target_unit': final_target,
                        'silent': is_silent
                    })

                    if not is_silent:
                        loggable_mappings.append((source_unit, final_target, lab_cat))

            else:
                unmatched_units.append({
                    'lab_category': lab_cat,
                    'source_unit': source_unit,
                    'expected_unit': target_unit
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
                f"Expected: {item['expected_unit']}"
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

    def convert_reference_units(
        self,
        inplace: bool = True,
        save: bool = False,
        output_directory: Optional[str] = None
    ) -> Optional[pd.DataFrame]:
        """
        Convert lab values from source units to target units using conversion factors.

        This method performs actual value conversions (e.g., g/dL to mg/dL) by applying
        conversion factors from labs_standardization.yaml. It updates both the numeric
        values and the reference_unit column.

        Conversion formula: target_value = source_value ÷ factor
        Where factor represents "how much of source unit equals 1 target unit"

        Parameters
        ----------
        inplace : bool, default True
            If True, modify self.df in place. If False, return a copy.
        save : bool, default False
            If True, save a CSV of the conversions applied to the output directory.
        output_directory : str, optional
            Directory to save results. If None, uses self.output_directory.

        Returns
        -------
        Optional[pd.DataFrame]
            If inplace=False, returns the modified DataFrame. Otherwise None.
        """
        if self.df is None:
            raise ValueError("No data loaded.")

        # Check for required columns
        missing = self._validate_required_columns({'lab_category', 'reference_unit', 'lab_value_numeric'})
        if missing:
            raise ValueError(f"Required columns not found: {missing}")

        if not self._unit_conversions:
            self.logger.warning("No unit conversions defined in schema")
            return None

        # Get unique combinations
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

        # Build conversion mapping
        conversions_to_apply = []
        for lab_cat, source_unit in unique_combos_df.itertuples(index=False):
            if pd.isna(source_unit):
                continue

            # Get target unit for this lab category
            target_unit = self._lab_reference_units.get(lab_cat)
            if not target_unit:
                continue

            # Normalize units for lookup
            source_stripped = source_unit.strip() if source_unit else ''
            source_normalized = source_stripped.lower().replace(' ', '')
            target_normalized = target_unit.lower().replace(' ', '')

            # Skip if already in target unit
            if source_normalized == target_normalized:
                continue

            # Check if this is a molecular weight-dependent conversion
            if self._is_mw_dependent_conversion(source_normalized, target_normalized):
                mw = self._lab_molecular_weights.get(lab_cat)
                if mw:
                    # Calculate factor based on MW and conversion direction
                    # Formula: mg/dL = mmol/L × mw / 10, so mmol/L = mg/dL × 10 / mw
                    # We use value / factor, so:
                    #   mmol/L → mg/dL: factor = 10 / mw (value / factor = value * mw / 10)
                    #   mg/dL → mmol/L: factor = mw / 10 (value / factor = value * 10 / mw)
                    mmol_units = {'mmol/l', 'meq/l'}
                    if source_normalized in mmol_units:
                        # mmol/L → mg/dL
                        factor = 10 / mw
                    else:
                        # mg/dL → mmol/L
                        factor = mw / 10

                    conversions_to_apply.append({
                        'lab_category': lab_cat,
                        'source_unit': source_unit,
                        'target_unit': target_unit,
                        'factor': factor
                    })
                    self.logger.info(
                        f"Converting '{source_unit}' -> '{target_unit}' for {lab_cat} "
                        f"(MW={mw}, ÷ {factor:.6f})"
                    )
                else:
                    self.logger.warning(
                        f"No molecular weight defined for {lab_cat}, cannot convert "
                        f"'{source_unit}' -> '{target_unit}'"
                    )
                continue

            # Look up conversion factor from YAML
            target_conversions = self._unit_conversions.get(target_normalized, {})

            # Try direct lookup first
            factor = target_conversions.get(source_normalized)

            # If not found, try looking up via canonical form
            if factor is None:
                source_canonical = self._unit_variant_lookup.get(source_normalized)
                if source_canonical:
                    factor = target_conversions.get(source_canonical)

            if factor is not None and factor != 1:
                conversions_to_apply.append({
                    'lab_category': lab_cat,
                    'source_unit': source_unit,
                    'target_unit': target_unit,
                    'factor': factor
                })
                self.logger.info(
                    f"Converting '{source_unit}' -> '{target_unit}' for {lab_cat} (÷ {factor})"
                )
            elif factor is None and source_normalized != target_normalized:
                self.logger.warning(
                    f"No conversion factor found for '{source_unit}' -> '{target_unit}' ({lab_cat})"
                )

        if not conversions_to_apply:
            self.logger.info("No unit conversions needed")
            return None

        # Apply conversions
        try:
            import polars as pl
            result_df = self._convert_reference_units_polars(conversions_to_apply)
            self.logger.debug("Used Polars for convert_reference_units")
        except Exception as e:
            self.logger.debug(f"Polars failed ({e}), falling back to pandas")
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
            result_df = self._convert_reference_units_pandas(conversions_to_apply)

        if inplace:
            self.df = result_df

        self.logger.info(f"Applied {len(conversions_to_apply)} unit conversions")

        # Save conversions if requested
        if save and conversions_to_apply:
            save_dir = output_directory if output_directory is not None else self.output_directory
            os.makedirs(save_dir, exist_ok=True)
            conversion_df = pd.DataFrame(conversions_to_apply)
            csv_path = os.path.join(save_dir, 'lab_unit_conversions.csv')
            conversion_df.to_csv(csv_path, index=False)
            self.logger.info(f"Saved unit conversions to {csv_path}")

        if not inplace:
            try:
                import polars as pl
                if isinstance(result_df, pl.DataFrame):
                    return result_df.to_pandas()
            except ImportError:
                pass
            return result_df

        return None

    def _convert_reference_units_polars(self, conversions_to_apply: list) -> 'pl.DataFrame':
        """Polars implementation for convert_reference_units."""
        import polars as pl

        lf = self._to_lazy_frame()

        for conv in conversions_to_apply:
            lab_cat = conv['lab_category']
            source_unit = conv['source_unit']
            target_unit = conv['target_unit']
            factor = conv['factor']

            # Apply conversion: divide value by factor, update unit
            lf = lf.with_columns([
                pl.when(
                    (pl.col('lab_category') == lab_cat) &
                    (pl.col('reference_unit') == source_unit)
                )
                .then(pl.col('lab_value_numeric') / factor)
                .otherwise(pl.col('lab_value_numeric'))
                .alias('lab_value_numeric'),

                pl.when(
                    (pl.col('lab_category') == lab_cat) &
                    (pl.col('reference_unit') == source_unit)
                )
                .then(pl.lit(target_unit))
                .otherwise(pl.col('reference_unit'))
                .alias('reference_unit')
            ])

        return lf.collect(streaming=True)

    def _convert_reference_units_pandas(self, conversions_to_apply: list) -> pd.DataFrame:
        """Pandas implementation for convert_reference_units."""
        df = self.df.copy()

        for conv in conversions_to_apply:
            lab_cat = conv['lab_category']
            source_unit = conv['source_unit']
            target_unit = conv['target_unit']
            factor = conv['factor']

            # Create mask for rows to convert
            mask = (df['lab_category'] == lab_cat) & (df['reference_unit'] == source_unit)

            # Apply conversion: divide value by factor
            df.loc[mask, 'lab_value_numeric'] = df.loc[mask, 'lab_value_numeric'] / factor
            df.loc[mask, 'reference_unit'] = target_unit

        return df

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
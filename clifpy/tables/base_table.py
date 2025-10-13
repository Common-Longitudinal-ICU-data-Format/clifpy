"""
BaseTable class for pyCLIF tables.

This module provides the base class that all pyCLIF table classes inherit from.
It handles common functionality including data loading, validation, and reporting.
"""

import os
import logging
import pandas as pd
import polars as pl
import yaml
import numpy as np
from typing import Optional, List, Dict, Any, Tuple
from pathlib import Path
from datetime import datetime

from ..utils.io import load_data
from ..utils import validator
from ..utils.config import get_config_or_params
from ..utils.logging_config import setup_logging


class BaseTable:
    """
    Base class for all pyCLIF table classes.
    
    Provides common functionality for loading data, running validations,
    and generating reports. All table-specific classes should inherit from this.
    
    Attributes
    ----------
    data_directory : str
        Path to the directory containing data files
    filetype : str
        Type of data file (csv, parquet, etc.)
    timezone : str
        Timezone for datetime columns
    output_directory : str
        Directory for saving output files and logs
    table_name : str
        Name of the table (from class name)
    df : pd.DataFrame
        The loaded data
    schema : dict
        The YAML schema for this table
    errors : List[dict]
        Validation errors from last validation run
    logger : logging.Logger
        Logger for this table
    """
    
    def __init__(
        self, 
        data_directory: str,
        filetype: str,
        timezone: str,
        output_directory: Optional[str] = None,
        data: Optional[pd.DataFrame] = None
    ):
        """
        Initialize the BaseTable.
        
        Parameters
        ----------
        data_directory : str
            Path to the directory containing data files
        filetype : str
            Type of data file (csv, parquet, etc.)
        timezone : str
            Timezone for datetime columns
        output_directory : str, optional
            Directory for saving output files and logs.
            If not provided, creates an 'output' directory in the current working directory.
        data : pd.DataFrame, optional
            Pre-loaded data to use instead of loading from file
        """
        # Store configuration
        self.data_directory = data_directory
        self.filetype = filetype
        self.timezone = timezone
        
        # Set output directory
        if output_directory is None:
            output_directory = os.path.join(os.getcwd(), 'output')
        self.output_directory = output_directory
        os.makedirs(self.output_directory, exist_ok=True)

        # Initialize centralized logging
        setup_logging(output_directory=self.output_directory)

        # Derive snake_case table name from PascalCase class name
        # Example: Adt -> adt, RespiratorySupport -> respiratory_support
        self.table_name = ''.join(['_' + c.lower() if c.isupper() else c for c in self.__class__.__name__]).lstrip('_')

        # Initialize data and validation state
        self.df: Optional[pd.DataFrame] = data
        self.errors: List[Dict[str, Any]] = []
        self.schema: Optional[Dict[str, Any]] = None
        self.outlier_config: Optional[Dict[str, Any]] = None
        self._validated: bool = False

        # Setup table-specific logging
        self._setup_logging()

        # Load schema
        self._load_schema()

        # Load outlier config
        self._load_outlier_config()
        

    def _setup_logging(self):
        """Set up table-specific logging (supplementary to centralized logs)."""
        # Get logger from centralized system
        self.logger = logging.getLogger(f'clifpy.tables.{self.table_name}')

        # Add supplementary file handler for table-specific validation logs
        # These go to output/logs/validation_log_{table}.log in addition to main logs
        log_dir = os.path.join(self.output_directory, 'logs')
        os.makedirs(log_dir, exist_ok=True)

        log_file = os.path.join(log_dir, f'validation_log_{self.table_name}.log')

        # Check if this handler already exists (avoid duplicates)
        existing_handlers = [h for h in self.logger.handlers
                           if isinstance(h, logging.FileHandler) and h.baseFilename == log_file]

        if not existing_handlers:
            file_handler = logging.FileHandler(log_file, mode='w')
            file_handler.setLevel(logging.INFO)
            formatter = logging.Formatter(
                '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
            )
            file_handler.setFormatter(formatter)
            self.logger.addHandler(file_handler)

        # Log initialization
        self.logger.info(f"Initialized {self.table_name} table")
        self.logger.info(f"Data directory: {self.data_directory}")
        self.logger.info(f"File type: {self.filetype}")
        self.logger.info(f"Timezone: {self.timezone}")
        self.logger.info(f"Output directory: {self.output_directory}")
    
    def _load_schema(self):
        """Load the YAML schema for this table."""
        try:
            # Construct schema file path
            schema_dir = os.path.join(
                os.path.dirname(os.path.dirname(__file__)),
                'schemas'
            )
            schema_file = os.path.join(
                schema_dir,
                f'{self.table_name}_schema.yaml'
            )

            # Check if schema file exists
            if not os.path.exists(schema_file):
                self.logger.warning(f"Schema file not found: {schema_file}")
                return

            # Load YAML schema
            with open(schema_file, 'r') as f:
                self.schema = yaml.safe_load(f)

            self.logger.info(f"Loaded schema from {schema_file}")

        except Exception as e:
            self.logger.error(f"Error loading schema: {str(e)}")
            self.schema = None

    def _load_outlier_config(self):
        """Load the outlier configuration for validation."""
        try:
            self.outlier_config = validator.load_outlier_config()
            if self.outlier_config:
                self.logger.info("Loaded outlier configuration")
            else:
                self.logger.warning("Could not load outlier configuration")
        except Exception as e:
            self.logger.error(f"Error loading outlier config: {str(e)}")
            self.outlier_config = None
    
    @classmethod
    def from_file(
        cls,
        data_directory: Optional[str] = None,
        filetype: Optional[str] = None,
        timezone: Optional[str] = None,
        config_path: Optional[str] = None,
        output_directory: Optional[str] = None,
        sample_size: Optional[int] = None,
        columns: Optional[List[str]] = None,
        filters: Optional[Dict[str, Any]] = None,
        verbose: bool = False
    ) -> 'BaseTable':
        """
        Load data from file and create a table instance.
        
        Parameters
        ----------
        data_directory : str, optional
            Path to the directory containing data files
        filetype : str, optional
            Type of data file (csv, parquet, etc.)
        timezone : str, optional
            Timezone for datetime columns
        config_path : str, optional
            Path to configuration JSON file
        output_directory : str, optional
            Directory for saving output files and logs
        sample_size : int, optional
            Number of rows to load
        columns : List[str], optional
            Specific columns to load
        filters : Dict, optional
            Filters to apply when loading
        verbose : bool, optional
            If True, show detailed loading messages. Default is False
            
        Notes
        -----
        Loading priority:
            1. If all required params provided → use them
            2. If config_path provided → load from that path, allow param overrides
            3. If no params and no config_path → auto-detect config.json
            4. Parameters override config file values when both are provided
            
        Returns
        -------
        BaseTable
            Instance of the table class with loaded data
        """
        # Get configuration from config file or parameters
        config = get_config_or_params(
            config_path=config_path,
            data_directory=data_directory,
            filetype=filetype,
            timezone=timezone,
            output_directory=output_directory
        )
        
        # Derive snake_case table name from PascalCase class name
        table_name = ''.join(['_' + c.lower() if c.isupper() else c for c in cls.__name__]).lstrip('_')
        
        # Load data using existing io utility
        data = load_data(
            table_name,
            config['data_directory'],
            config['filetype'],
            sample_size=sample_size,
            columns=columns,
            filters=filters,
            site_tz=config['timezone'],
            verbose=verbose
        )
        
        # Create instance with loaded data
        return cls(
            data_directory=config['data_directory'],
            filetype=config['filetype'],
            timezone=config['timezone'],
            output_directory=config.get('output_directory', output_directory),
            data=data
        )
    
    def validate(self):
        """
        Run comprehensive validation on the data.

        This method runs all validation checks including:

        - Schema validation (required columns, data types, categories)
        - Missing data analysis
        - Duplicate checking
        - Statistical analysis
        - Table-specific validations (if overridden in child class)
        """
        if self.df is None:
            self.logger.warning("No dataframe to validate")
            return

        self.logger.info("Starting validation")
        self.errors = []
        self._validated = True

        try:
            # Run basic schema validation
            if self.schema:
                self.logger.info("Running schema validation")
                schema_errors = validator.validate_dataframe(self.df, self.schema)
                self.errors.extend(schema_errors)

                if schema_errors:
                    self.logger.warning(f"Schema validation found {len(schema_errors)} errors")
                else:
                    self.logger.info("Schema validation passed")

            # Run enhanced validations (these will be implemented in Phase 3)
            self._run_enhanced_validations()

            # Run table-specific validations (can be overridden in child classes)
            self._run_table_specific_validations()

            # Log validation results
            if not self.errors:
                self.logger.info("Validation completed successfully")
            else:
                self.logger.warning(f"Validation completed with {len(self.errors)} error(s). See `errors` attribute.")

                # Save errors to CSV
                self._save_validation_errors()

        except Exception as e:
            self.logger.error(f"Error during validation: {str(e)}")
            self.errors.append({
                "type": "validation_error",
                "message": str(e)
            })
    
    def _run_tz_validation(self):

        datetime_columns = [
            col['name'] for col in self.schema.get('columns', [])
            if col.get('data_type') == 'DATETIME' and col['name'] in self.df.columns and col['name'] != 'birth_date'
        ]
        if datetime_columns:
            self.logger.info(f"Validating timezone for datetime columns: {datetime_columns}")
            tz_results = validator.validate_datetime_timezone(self.df, datetime_columns)
            for result in tz_results:
                if result.get('status') in ['warning', 'error']:
                    self.errors.append(result)

    def _run_enhanced_validations(self):
        """
        Run enhanced validation checks.
        
        This method integrates with the enhanced validator functions
        to provide comprehensive data quality checks.
        """
        if not self.schema:
            return
        
        try:
            checks_to_run = ['_run_duplicate_check', '_run_*', ]
            

            # 1. Check for duplicates on composite keys
            if 'composite_keys' in self.schema:
                self.logger.info("Checking for duplicates on composite keys")
                duplicate_result = validator.check_for_duplicates(
                    self.df, 
                    self.schema['composite_keys']
                )
                if duplicate_result.get('status') == 'warning':
                    self.errors.append(duplicate_result)
                    self.logger.warning(f"Found {duplicate_result['duplicate_rows']} duplicate rows")
            
            # 2. Validate datetime timezone
            self._run_tz_validation()
            
            # 3. Calculate and save missing data statistics
            self.logger.info("Calculating missing data statistics")
            missing_stats = validator.calculate_missing_stats(self.df, format='long')
            if not missing_stats.empty:
                missing_file = os.path.join(
                    self.output_directory,
                    f'missing_data_stats_{self.table_name}.csv'
                )
                missing_stats.to_csv(missing_file, index=False)
                self.logger.info(f"Saved missing data statistics to {missing_file}")
            
            # 4. Generate missing data summary
            missing_summary = validator.report_missing_data_summary(self.df)
            if missing_summary.get('total_missing_cells', 0) > 0:
                self.logger.info(
                    f"Missing data: {missing_summary['overall_missing_percent']:.2f}% "
                    f"({missing_summary['total_missing_cells']} cells)"
                )
            
            # 5. Validate categorical values
            cat_errors = validator.validate_categorical_values(self.df, self.schema)
            if cat_errors:
                self.errors.extend(cat_errors)
                self.logger.warning(f"Found {len(cat_errors)} categorical validation errors")
            
            # 6. Generate summary statistics for numeric columns
            numeric_columns = [
                col['name'] for col in self.schema.get('columns', [])
                if col.get('data_type') in ['DOUBLE', 'FLOAT', 'INT', 'INTEGER'] 
                and col['name'] in self.df.columns
            ]
            if numeric_columns:
                self.logger.info(f"Generating summary statistics for numeric columns")
                summary_stats = validator.generate_summary_statistics(
                    self.df, 
                    numeric_columns,
                    self.output_directory,
                    self.table_name
                )
                if not summary_stats.empty:
                    self.logger.info("Generated summary statistics")
            
            # 7. Analyze skewed distributions
            skew_analysis = validator.analyze_skewed_distributions(
                self.df,
                self.output_directory,
                self.table_name
            )
            if not skew_analysis.empty:
                self.logger.info("Analyzed skewed distributions")
            
            # 8. Validate units (for vitals and labs tables)
            if self.table_name in ['vitals', 'labs']:
                unit_mappings = self.schema.get(f'{self.table_name[:-1]}_units') or \
                               self.schema.get('lab_reference_units', {})
                if unit_mappings:
                    self.logger.info("Validating units")
                    unit_results = validator.validate_units(
                        self.df, 
                        unit_mappings, 
                        self.table_name
                    )
                    for result in unit_results:
                        if result.get('status') == 'warning':
                            self.errors.append(result)
            
            # 9. Calculate cohort sizes
            id_columns = ['patient_id', 'hospitalization_id']
            existing_id_cols = [col for col in id_columns if col in self.df.columns]
            if existing_id_cols:
                cohort_sizes = validator.calculate_cohort_sizes(self.df, existing_id_cols)
                self.logger.info(f"Cohort sizes: {cohort_sizes}")

            # 10. Validate numeric ranges for outliers
            if self.outlier_config:
                self.logger.info("Validating numeric ranges for outliers")
                outlier_results = validator.validate_numeric_ranges_from_config(
                    self.df,
                    self.table_name,
                    self.schema,
                    self.outlier_config
                )
                if outlier_results:
                    self.errors.extend(outlier_results)
                    self.logger.warning(f"Found {len(outlier_results)} outlier summaries")
                else:
                    self.logger.info("No outliers detected")

        except Exception as e:
            self.logger.error(f"Error in enhanced validations: {str(e)}")
            self.errors.append({
                "type": "enhanced_validation_error",
                "message": str(e)
            })
    
    def _run_table_specific_validations(self):
        """
        Run table-specific validations.
        
        This method should be overridden in child classes to implement
        table-specific validation logic (e.g., range validation for vitals).
        """
        pass
    
    def _save_validation_errors(self):
        """Save validation errors to a CSV file."""
        if not self.errors:
            return
        
        try:
            # Convert errors to DataFrame
            errors_df = pd.DataFrame(self.errors)
            
            # Save to CSV
            error_file = os.path.join(
                self.output_directory,
                f'validation_errors_{self.table_name}.csv'
            )
            errors_df.to_csv(error_file, index=False)
            
            self.logger.info(f"Saved validation errors to {error_file}")
            
        except Exception as e:
            self.logger.error(f"Error saving validation errors: {str(e)}")
    
    def isvalid(self) -> bool:
        """
        Check if the data is valid based on the last validation run.

        Returns:
            bool: True if validation has been run and no errors were found,
                  False if validation found errors or hasn't been run yet
        """
        if not self._validated:
            self.logger.warning("Validation has not been run yet. Please call validate() first.")
            return False
        return not self.errors
    
    def get_summary(self) -> Dict[str, Any]:
        """
        Get a summary of the table data.
        
        Returns:
            dict: Summary statistics and information about the table
        """
        if self.df is None:
            return {"status": "No data loaded"}
        
        summary = {
            "table_name": self.table_name,
            "num_rows": len(self.df),
            "num_columns": len(self.df.columns),
            "columns": list(self.df.columns),
            "memory_usage_mb": self.df.memory_usage(deep=True).sum() / 1024 / 1024,
            "validation_run": self._validated,
            "validation_errors": len(self.errors) if self._validated else None,
            "is_valid": self.isvalid()
        }
        
        # Add basic statistics for numeric columns
        numeric_cols = self.df.select_dtypes(include=['number']).columns
        if len(numeric_cols) > 0:
            summary["numeric_columns"] = list(numeric_cols)
            summary["numeric_stats"] = self.df[numeric_cols].describe().to_dict()
        
        # Add missing data summary
        missing_counts = self.df.isnull().sum()
        if missing_counts.any():
            summary["missing_data"] = missing_counts[missing_counts > 0].to_dict()
        
        return summary
    
    def save_summary(self):
        """Save table summary to a JSON file."""
        try:
            import json
            
            summary = self.get_summary()
            
            # Save to JSON
            summary_file = os.path.join(
                self.output_directory,
                f'summary_{self.table_name}.json'
            )
            
            with open(summary_file, 'w') as f:
                json.dump(summary, f, indent=2, default=str)
            
            self.logger.info(f"Saved summary to {summary_file}")
            
        except Exception as e:
            self.logger.error(f"Error saving summary: {str(e)}")

    def analyze_categorical_distributions(self, save: bool = True) -> Dict[str, pd.DataFrame]:
        """
        Analyze distributions of categorical variables.

        For each categorical variable, returns the distribution of categories
        based on unique hospitalization_id (or patient_id if hospitalization_id is not present).

        Parameters
        ----------
        save : bool, default=True
            If True, saves distribution data to CSV files in the output directory.

        Returns
        -------
        Dict[str, pd.DataFrame]
            Dictionary where keys are categorical column names and values are
            DataFrames with category distributions (unique ID counts and %).
        """
        if self.df is None:
            self.logger.warning("No dataframe to analyze")
            return {}

        if not self.schema:
            self.logger.warning("No schema available for categorical analysis")
            return {}

        # Determine ID column to use (prefer hospitalization_id)
        if 'hospitalization_id' in self.df.columns:
            id_col = 'hospitalization_id'
        elif 'patient_id' in self.df.columns:
            id_col = 'patient_id'
        else:
            self.logger.warning("No hospitalization_id or patient_id column found")
            return {}

        # Get categorical columns from schema
        categorical_columns = [
            col['name'] for col in self.schema.get('columns', [])
            if col.get('is_category_column', False) and col['name'] in self.df.columns
        ]

        if not categorical_columns:
            self.logger.info("No categorical columns found in schema")
            return {}

        results = {}

        for col in categorical_columns:
            try:
                # Count unique IDs per category
                id_counts = self.df.groupby(col, dropna=False)[id_col].nunique().sort_values(ascending=False)
                # Calculate % as (unique IDs in category) / (total unique IDs in entire table)
                total_unique_ids = self.df[id_col].nunique()
                percent = (id_counts / total_unique_ids * 100).round(2)

                distribution_df = pd.DataFrame({
                    'category': id_counts.index,
                    'count': id_counts.values,
                    '%': percent.values
                })

                results[col] = distribution_df

                # Save to CSV if requested
                if save:
                    csv_filename = f'categorical_dist_{self.table_name}_{col}.csv'
                    csv_path = os.path.join(self.output_directory, csv_filename)
                    distribution_df.to_csv(csv_path, index=False)
                    self.logger.info(f"Saved distribution data to {csv_path}")

                self.logger.info(f"Analyzed categorical distribution for {col}")

            except Exception as e:
                self.logger.error(f"Error analyzing categorical distribution for {col}: {str(e)}")
                continue

        return results

    def plot_categorical_distributions(self, columns: Optional[List[str]] = None, figsize: Tuple[int, int] = (10, 6), save: bool = True, dpi: int = 300):
        """
        Create bar plots for categorical variable distributions.

        Counts unique hospitalization_id (or patient_id if hospitalization_id is not present)
        for each category.

        Parameters
        ----------
        columns : List[str], optional
            Specific categorical columns to plot. If None, plots all categorical columns.
        figsize : Tuple[int, int], default=(10, 6)
            Figure size for each plot (width, height).
        save : bool, default=True
            If True, saves plots to output directory as PNG files.
        dpi : int, default=300
            Resolution for saved plots (dots per inch).

        Returns
        -------
        Dict[str, Figure]
            Dictionary where keys are categorical column names and values are
            matplotlib Figure objects.
        """
        import matplotlib.pyplot as plt

        if self.df is None:
            self.logger.warning("No dataframe to plot")
            return {}

        if not self.schema:
            self.logger.warning("No schema available for categorical plotting")
            return {}

        # Determine ID column to use (prefer hospitalization_id)
        if 'hospitalization_id' in self.df.columns:
            id_col = 'hospitalization_id'
        elif 'patient_id' in self.df.columns:
            id_col = 'patient_id'
        else:
            self.logger.warning("No hospitalization_id or patient_id column found")
            return {}

        # Get categorical columns from schema
        categorical_columns = [
            col['name'] for col in self.schema.get('columns', [])
            if col.get('is_category_column', False) and col['name'] in self.df.columns
        ]

        if not categorical_columns:
            self.logger.info("No categorical columns found in schema")
            return {}

        # Filter to requested columns if specified
        if columns is not None:
            categorical_columns = [col for col in categorical_columns if col in columns]

        if not categorical_columns:
            self.logger.warning("No matching categorical columns found")
            return {}

        plots = {}

        for col in categorical_columns:
            try:
                # Count unique IDs per category
                id_counts = self.df.groupby(col, dropna=False)[id_col].nunique().sort_values(ascending=False)

                # Create modern bar plot
                fig, ax = plt.subplots(figsize=figsize, facecolor='white')

                # Use colorblind-friendly color palette (cividis)
                colors = plt.cm.cividis(np.linspace(0.3, 0.9, len(id_counts)))
                bars = ax.bar(range(len(id_counts)), id_counts.values, color=colors, edgecolor='white', linewidth=1.5)

                # Styling
                ax.set_xlabel('Category', fontsize=12, fontweight='bold', color='#333333')
                ax.set_ylabel(f'Unique {id_col} counts', fontsize=12, fontweight='bold', color='#333333')
                ax.set_title(f'Distribution of {col}', fontsize=14, fontweight='bold', pad=20, color='#1a1a1a')
                ax.set_xticks(range(len(id_counts)))
                ax.set_xticklabels([str(x) for x in id_counts.index], rotation=45, ha='right', fontsize=10)

                # Remove top and right spines
                ax.spines['top'].set_visible(False)
                ax.spines['right'].set_visible(False)
                ax.spines['left'].set_color('#cccccc')
                ax.spines['bottom'].set_color('#cccccc')

                # Add grid for readability
                ax.yaxis.grid(True, linestyle='--', alpha=0.3, color='#cccccc')
                ax.set_axisbelow(True)

                # Add value labels on top of bars (adjust font size and rotation based on number of categories)
                num_categories = len(id_counts)
                if num_categories <= 10:
                    label_fontsize = 9
                    label_rotation = 0
                elif num_categories <= 20:
                    label_fontsize = 7
                    label_rotation = 45
                else:
                    label_fontsize = 6
                    label_rotation = 90

                for i, (bar, value) in enumerate(zip(bars, id_counts.values)):
                    height = bar.get_height()
                    ax.text(bar.get_x() + bar.get_width()/2., height,
                           f'{int(value)}',
                           ha='center', va='bottom', fontsize=label_fontsize,
                           color='#333333', rotation=label_rotation)

                plt.tight_layout()

                # Save plot if requested
                if save:
                    plot_filename = f'categorical_dist_{self.table_name}_{col}.png'
                    plot_path = os.path.join(self.output_directory, plot_filename)
                    fig.savefig(plot_path, dpi=dpi, bbox_inches='tight')
                    self.logger.info(f"Saved plot to {plot_path}")

                plots[col] = fig

                self.logger.info(f"Created plot for {col}")

            except Exception as e:
                self.logger.error(f"Error creating plot for {col}: {str(e)}")
                continue

        return plots

    def calculate_stratified_ecdf(
        self,
        value_column: str,
        category_column: str,
        category_values: Optional[List[str]] = None,
        save: bool = True
    ) -> Optional[List['pl.DataFrame']]:
        """
        Calculate ECDF for a continuous variable stratified by categories using loaded DataFrame (self.df).
    
        Parameters
        ----------
        value_column : str
            Name of the continuous/numeric column to calculate ECDF for.
        category_column : str
            Name of the categorical column to stratify by.
        category_values : List[str], optional
            Specific category values to include. If None, uses permissible values from schema,
            or all unique values in the data if schema doesn't specify permissible values.
        save : bool, default=True
            If True, saves stratified ECDF data to CSV files (one per category).
    
        Returns
        -------
        List[pl.DataFrame] or None
            List of DataFrames (one per category), each with x-values and their corresponding cumulative probabilities.
            If save=True, saves the resulting DataFrame to CSV.
        """
        import polars as pl
    
        # Check if self.df is loaded
        if self.df is None:
            self.logger.error("Loaded dataframe (self.df) is not available.")
            return None
    
        # Convert to Polars DataFrame if it's not already
        if not isinstance(self.df, pl.DataFrame):
            try:
                df_pl = pl.from_pandas(self.df)
            except Exception as e:
                self.logger.error(f"Could not convert self.df to Polars DataFrame: {str(e)}")
                return None
        else:
            df_pl = self.df
    
        # Check if columns exist
        columns = df_pl.columns
        if value_column not in columns:
            self.logger.error(f"Value column '{value_column}' not found in dataframe")
            return None
        if category_column not in columns:
            self.logger.error(f"Category column '{category_column}' not found in dataframe")
            return None
    
        # Determine which category values to use
        if category_values is None:
            # Try permissible values from schema
            category_values = None
            if self.schema:
                for col_def in self.schema.get('columns', []):
                    if col_def.get('name') == category_column:
                        category_values = col_def.get('permissible_values')
                        if category_values:
                            self.logger.info(f"Using permissible values from schema for {category_column}")
                        break
            # Otherwise use all unique values from data
            if not category_values:
                category_values = (
                    df_pl
                    .select(pl.col(category_column).drop_nulls().unique())
                    .to_series()
                    .to_list()
                )
                self.logger.info(f"Using all unique values from data for {category_column}")
    
        all_ecdf_rows = []
    
        for category in category_values:
            try:
                # Filter data for this category
                cat_df = (
                    df_pl
                    .filter(pl.col(category_column) == category)
                    .select([pl.col(value_column)])
                    .drop_nulls()
                    .sort(value_column)
                )
    
                n = cat_df.shape[0]
                if n == 0:
                    self.logger.warning(f"No valid data for category '{category}'")
                    continue
    
                # Calculate ECDF: each value gets rank = position, cumulative_probability = rank/n
                ecdf_df = cat_df.with_columns([
                    (pl.arange(1, n + 1) / n).alias('cumulative_probability'),
                ])
                # Add category for later clarity
                ecdf_df = ecdf_df.with_columns([
                    pl.lit(category).alias(category_column)
                ])
    
                all_ecdf_rows.append(ecdf_df)
    
                self.logger.info(f"Calculated ECDF for {category_column}={category} with {n} measurements")
    
            except Exception as e:
                self.logger.error(f"Error calculating ECDF for category '{category}': {str(e)}")
                continue
    
        if not all_ecdf_rows:
            self.logger.warning("No valid ECDF data for any category.")
            return None
    
        # Concatenate all
        all_ecdf_pl = pl.concat(all_ecdf_rows)
    
        if save:
            csv_filename = f'ecdf_{self.table_name}_{value_column}_by_{category_column}.csv'
            csv_path = os.path.join(self.output_directory, csv_filename)
            try:
                all_ecdf_pl.write_csv(csv_path)
                self.logger.info(f"Saved ECDF data for all categories to {csv_path}")
            except Exception as e:
                self.logger.error(f"Failed to save ECDF CSV: {str(e)}")
    
        return all_ecdf_rows

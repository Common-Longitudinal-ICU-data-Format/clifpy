"""
BaseTable class for pyCLIF tables.

This module provides the base class that all pyCLIF table classes inherit from.
It handles common functionality including data loading, validation, and reporting.
"""

import os
import logging
import pandas as pd
import yaml
from typing import Optional, List, Dict, Any
from pathlib import Path
from datetime import datetime

from ..utils.io import load_data
from ..utils import validator
from ..utils.config import get_config_or_params


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
        
        # Derive snake_case table name from PascalCase class name
        # Example: Adt -> adt, RespiratorySupport -> respiratory_support
        self.table_name = ''.join(['_' + c.lower() if c.isupper() else c for c in self.__class__.__name__]).lstrip('_')
        
        # Initialize data and validation state
        self.df: Optional[pd.DataFrame] = data
        self.errors: List[Dict[str, Any]] = []
        self.schema: Optional[Dict[str, Any]] = None
        self._validated: bool = False
        
        # Setup logging
        self._setup_logging()
        
        # Load schema
        self._load_schema()
        
    
    def _setup_logging(self):
        """Set up logging for this table."""
        # Create logger
        self.logger = logging.getLogger(f'pyclif.{self.table_name}')
        self.logger.setLevel(logging.INFO)
        
        # Remove existing handlers to avoid duplicates
        self.logger.handlers = []
        
        # Create file handler
        log_file = os.path.join(
            self.output_directory, 
            f'validation_log_{self.table_name}.log'
        )
        file_handler = logging.FileHandler(log_file, mode='w')
        file_handler.setLevel(logging.INFO)
        
        # Create formatter
        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        file_handler.setFormatter(formatter)
        
        # Add handler to logger
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
        filters: Optional[Dict[str, Any]] = None
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
            site_tz=config['timezone']
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
            print("No dataframe to validate.")
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
                print("Validation completed successfully.")
            else:
                self.logger.warning(f"Validation completed with {len(self.errors)} error(s)")
                print(f"Validation completed with {len(self.errors)} error(s). See `errors` attribute.")
                
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
            print("Validation has not been run yet. Please call validate() first.")
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
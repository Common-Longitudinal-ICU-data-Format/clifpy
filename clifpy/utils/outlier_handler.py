"""
Outlier handling utilities for pyCLIF tables.

This module provides functions to detect and handle outliers in clinical data
based on configurable range specifications. Values outside the specified ranges
are converted to NaN.
"""

import os
import yaml
import pandas as pd
import polars as pl
from typing import Optional, Dict, Any, Tuple
from pathlib import Path


def apply_outlier_handling(table_obj, outlier_config_path: Optional[str] = None) -> None:
    """
    Apply outlier handling to a table object's dataframe.
    
    This function identifies numeric values that fall outside acceptable ranges
    and converts them to NaN. For category-dependent columns (vitals, labs, 
    medications, assessments), ranges are applied based on the category value.
    
    Parameters:
        table_obj: A pyCLIF table object with .df (DataFrame) and .table_name attributes
        outlier_config_path (str, optional): Path to custom outlier configuration YAML.
                                           If None, uses internal CLIF standard config.
    
    Returns:
        None (modifies table_obj.df in-place)
    """
    if table_obj.df is None or table_obj.df.empty:
        print("No data to process for outlier handling.")
        return
    
    # Load outlier configuration
    config = _load_outlier_config(outlier_config_path)
    if not config:
        print("Failed to load outlier configuration.")
        return
    
    # Print which configuration is being used
    if outlier_config_path is None:
        print("Using CLIF standard outlier ranges\n")
    else:
        print(f"Using custom outlier ranges from: {outlier_config_path}\n")
    
    # Get table-specific configuration
    table_config = config.get('tables', {}).get(table_obj.table_name, {})
    if not table_config:
        print(f"No outlier configuration found for table: {table_obj.table_name}")
        return
    
    # Process each numeric column
    for column_name, column_config in table_config.items():
        if column_name not in table_obj.df.columns:
            continue
            
        if table_obj.table_name in ['vitals', 'labs', 'patient_assessments'] and column_name in ['vital_value', 'lab_value_numeric', 'numerical_value']:
            # Category-dependent processing with detailed statistics
            _process_category_dependent_column_pandas(table_obj, column_name, column_config)
        elif table_obj.table_name == 'medication_admin_continuous' and column_name == 'med_dose':
            # Unit-dependent processing for medications with detailed statistics
            _process_medication_column_pandas(table_obj, column_config)
        else:
            # Simple range processing
            _process_simple_range_column_pandas(table_obj, column_name, column_config)


def _load_outlier_config(config_path: Optional[str] = None) -> Optional[Dict[str, Any]]:
    """Load outlier configuration from YAML file."""
    try:
        if config_path is None:
            # Use internal CLIF config
            config_path = os.path.join(
                os.path.dirname(os.path.dirname(__file__)),
                'schemas',
                'outlier_config.yaml'
            )
        
        if not os.path.exists(config_path):
            print(f"Outlier configuration file not found: {config_path}")
            return None
        
        with open(config_path, 'r') as f:
            return yaml.safe_load(f)
            
    except Exception as e:
        print(f"Error loading outlier configuration: {str(e)}")
        return None


def _get_category_statistics_pandas(df: pd.DataFrame, column_name: str, category_col: str) -> Dict[str, Dict[str, int]]:
    """Get per-category statistics for non-null values using polars for optimization."""
    try:
        # Convert to Polars for faster processing
        df_pl = pl.from_pandas(df)

        # Single group_by operation to get statistics for all categories
        stats_df = (
            df_pl
            .filter(pl.col(category_col).is_not_null())
            .group_by(category_col)
            .agg([
                pl.col(column_name).is_not_null().sum().alias('non_null_count'),
                pl.len().alias('total_count')
            ])
        )

        # Convert back to the expected dictionary format
        stats = {}
        for row in stats_df.to_dicts():
            category = row[category_col]
            stats[category] = {
                'non_null_count': row['non_null_count'],
                'total_count': row['total_count']
            }

        return stats
    except Exception as e:
        print(f"Warning: Could not get category statistics: {str(e)}")
        return {}


def _get_medication_statistics_pandas(df: pd.DataFrame) -> Dict[str, Dict[str, int]]:
    """Get per-medication-unit statistics for non-null values using polars for optimization."""
    try:
        # Convert to Polars for faster processing
        df_pl = pl.from_pandas(df)

        # Single group_by operation to get statistics for all medication/unit combinations
        stats_df = (
            df_pl
            .filter(
                pl.col('med_category').is_not_null() &
                pl.col('med_dose_unit').is_not_null()
            )
            .group_by(['med_category', 'med_dose_unit'])
            .agg([
                pl.col('med_dose').is_not_null().sum().alias('non_null_count'),
                pl.len().alias('total_count')
            ])
        )

        # Convert back to the expected dictionary format
        stats = {}
        for row in stats_df.to_dicts():
            med_category = row['med_category']
            unit = row['med_dose_unit']
            key = f"{med_category} ({unit})"
            stats[key] = {
                'non_null_count': row['non_null_count'],
                'total_count': row['total_count']
            }

        return stats
    except Exception as e:
        print(f"Warning: Could not get medication statistics: {str(e)}")
        return {}


def _process_category_dependent_column_pandas(table_obj, column_name: str, column_config: Dict[str, Any]) -> None:
    """Process columns where ranges depend on category values using polars for optimization."""
    # Determine the category column name and table display name
    if table_obj.table_name == 'vitals':
        category_col = 'vital_category'
        table_display_name = "Vitals"
    elif table_obj.table_name == 'labs':
        category_col = 'lab_category'
        table_display_name = "Labs"
    elif table_obj.table_name == 'patient_assessments':
        category_col = 'assessment_category'
        table_display_name = "Patient Assessments"
    else:
        return

    # Get before statistics
    before_stats = _get_category_statistics_pandas(table_obj.df, column_name, category_col)

    # Convert to Polars for faster processing
    df_pl = pl.from_pandas(table_obj.df)

    # Build single expression chain for all categories
    expr = pl.col(column_name)
    for category, range_config in column_config.items():
        if isinstance(range_config, dict) and 'min' in range_config and 'max' in range_config:
            min_val = range_config['min']
            max_val = range_config['max']

            # Create condition for this category and outlier values
            condition = (
                (pl.col(category_col).str.to_lowercase() == category.lower()) &
                ((pl.col(column_name) < min_val) | (pl.col(column_name) > max_val))
            )

            # Chain the when-then-otherwise for outlier nullification
            expr = pl.when(condition).then(None).otherwise(expr)

    # Apply all transformations in single operation
    df_pl = df_pl.with_columns(expr.alias(column_name))

    # Convert back to pandas and update the original dataframe
    table_obj.df = df_pl.to_pandas()

    # Get after statistics
    after_stats = _get_category_statistics_pandas(table_obj.df, column_name, category_col)

    # Print detailed category statistics
    print(f"\n{table_display_name} Table - Category Statistics:")
    for category in sorted(set(before_stats.keys()) | set(after_stats.keys())):
        before_count = before_stats.get(category, {}).get('non_null_count', 0)
        after_count = after_stats.get(category, {}).get('non_null_count', 0)
        nullified = before_count - after_count

        if before_count > 0:
            percentage = (nullified / before_count) * 100
            print(f"  {category:<20}: {before_count:>6} values → {nullified:>6} nullified ({percentage:>5.1f}%)")
        else:
            print(f"  {category:<20}: {before_count:>6} values → {nullified:>6} nullified (  0.0%)")


def _process_medication_column_pandas(table_obj, column_config: Dict[str, Any]) -> None:
    """Process medication dose column with unit-dependent ranges using polars for optimization."""

    # Get before statistics
    before_stats = _get_medication_statistics_pandas(table_obj.df)

    # Convert to Polars for faster processing
    df_pl = pl.from_pandas(table_obj.df)

    # Build single expression chain for all medication/unit combinations
    expr = pl.col('med_dose')
    for med_category, unit_configs in column_config.items():
        if isinstance(unit_configs, dict):
            for unit, range_config in unit_configs.items():
                if isinstance(range_config, dict) and 'min' in range_config and 'max' in range_config:
                    min_val = range_config['min']
                    max_val = range_config['max']

                    # Create condition for this medication category/unit and outlier values
                    condition = (
                        (pl.col('med_category').str.to_lowercase() == med_category.lower()) &
                        (pl.col('med_dose_unit').str.to_lowercase() == unit.lower()) &
                        ((pl.col('med_dose') < min_val) | (pl.col('med_dose') > max_val))
                    )

                    # Chain the when-then-otherwise for outlier nullification
                    expr = pl.when(condition).then(None).otherwise(expr)

    # Apply all transformations in single operation
    df_pl = df_pl.with_columns(expr.alias('med_dose'))

    # Convert back to pandas and update the original dataframe
    table_obj.df = df_pl.to_pandas()

    # Get after statistics
    after_stats = _get_medication_statistics_pandas(table_obj.df)

    # Print detailed medication statistics
    print(f"\nMedication Table - Category/Unit Statistics:")
    for med_unit in sorted(set(before_stats.keys()) | set(after_stats.keys())):
        before_count = before_stats.get(med_unit, {}).get('non_null_count', 0)
        after_count = after_stats.get(med_unit, {}).get('non_null_count', 0)
        nullified = before_count - after_count

        if before_count > 0:
            percentage = (nullified / before_count) * 100
            print(f"  {med_unit:<30}: {before_count:>6} values → {nullified:>6} nullified ({percentage:>5.1f}%)")
        else:
            print(f"  {med_unit:<30}: {before_count:>6} values → {nullified:>6} nullified (  0.0%)")


def _process_simple_range_column_pandas(table_obj, column_name: str, column_config: Dict[str, Any]) -> None:
    """Process columns with simple min/max ranges using polars for optimization."""
    if isinstance(column_config, dict) and 'min' in column_config and 'max' in column_config:
        min_val = column_config['min']
        max_val = column_config['max']

        # Get before count using pandas before conversion
        before_count = table_obj.df[column_name].notna().sum()

        # Convert to Polars for faster processing
        df_pl = pl.from_pandas(table_obj.df)

        # Apply outlier filtering in single vectorized operation
        expr = pl.when(
            (pl.col(column_name) < min_val) | (pl.col(column_name) > max_val)
        ).then(None).otherwise(pl.col(column_name))

        df_pl = df_pl.with_columns(expr.alias(column_name))

        # Convert back to pandas and update the original dataframe
        table_obj.df = df_pl.to_pandas()

        # Get after count and print statistics
        after_count = table_obj.df[column_name].notna().sum()
        nullified = before_count - after_count

        if before_count > 0:
            percentage = (nullified / before_count) * 100
            print(f"{column_name:<30}: {before_count:>6} values → {nullified:>6} nullified ({percentage:>5.1f}%)")
        else:
            print(f"{column_name:<30}: {before_count:>6} values → {nullified:>6} nullified (  0.0%)")


def get_outlier_summary(table_obj, outlier_config_path: Optional[str] = None) -> Dict[str, Any]:
    """
    Get a summary of potential outliers without modifying the data.
    
    Parameters:
        table_obj: A pyCLIF table object with .df and .table_name attributes
        outlier_config_path (str, optional): Path to custom outlier configuration
        
    Returns:
        dict: Summary of outliers by column and category
    """
    if table_obj.df is None or table_obj.df.empty:
        return {"status": "No data to analyze"}
    
    config = _load_outlier_config(outlier_config_path)
    if not config:
        return {"status": "Failed to load configuration"}
    
    table_config = config.get('tables', {}).get(table_obj.table_name, {})
    if not table_config:
        return {"status": f"No configuration for table: {table_obj.table_name}"}
    
    summary = {
        "table_name": table_obj.table_name,
        "total_rows": len(table_obj.df),
        "columns_analyzed": {},
        "config_source": "CLIF standard" if outlier_config_path is None else "Custom"
    }
    
    # Analyze each column without modifying data
    for column_name, column_config in table_config.items():
        if column_name not in table_obj.df.columns:
            continue
        
        column_summary = _analyze_column_outliers_pandas(table_obj, column_name, column_config)
        if column_summary:
            summary["columns_analyzed"][column_name] = column_summary
    
    return summary


def _analyze_column_outliers_pandas(table_obj, column_name: str, _column_config: Dict[str, Any]) -> Dict[str, Any]:
    """Analyze outliers in a column without modifying data using pandas."""
    # This is a simplified version - could be expanded to provide detailed outlier analysis
    total_non_null = table_obj.df[column_name].notna().sum()
    
    return {
        "total_non_null_values": total_non_null,
        "configuration_type": "category_dependent" if table_obj.table_name in ['vitals', 'labs', 'patient_assessments', 'medication_admin_continuous'] else "simple_range"
    }
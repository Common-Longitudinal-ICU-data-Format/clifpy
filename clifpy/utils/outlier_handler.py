"""
Outlier handling utilities for pyCLIF tables.

This module provides functions to detect and handle outliers in clinical data
based on configurable range specifications. Values outside the specified ranges
are converted to NaN.
"""

import os
import yaml
import pandas as pd
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
    """Get per-category statistics for non-null values using pandas."""
    try:
        # Filter out rows where category column is null
        valid_data = df[df[category_col].notna()]
        
        # Group by category and calculate statistics
        stats = {}
        for category in valid_data[category_col].unique():
            category_data = valid_data[valid_data[category_col] == category]
            non_null_count = category_data[column_name].notna().sum()
            total_count = len(category_data)
            
            stats[category] = {
                'non_null_count': non_null_count,
                'total_count': total_count
            }
        
        return stats
    except Exception as e:
        print(f"Warning: Could not get category statistics: {str(e)}")
        return {}


def _get_medication_statistics_pandas(df: pd.DataFrame) -> Dict[str, Dict[str, int]]:
    """Get per-medication-unit statistics for non-null values using pandas."""
    try:
        # Filter out rows where category or unit columns are null
        valid_data = df[(df['med_category'].notna()) & (df['med_dose_unit'].notna())]
        
        stats = {}
        for (med_category, unit) in valid_data[['med_category', 'med_dose_unit']].drop_duplicates().values:
            mask = (valid_data['med_category'] == med_category) & (valid_data['med_dose_unit'] == unit)
            category_data = valid_data[mask]
            non_null_count = category_data['med_dose'].notna().sum()
            total_count = len(category_data)
            
            key = f"{med_category} ({unit})"
            stats[key] = {
                'non_null_count': non_null_count,
                'total_count': total_count
            }
        
        return stats
    except Exception as e:
        print(f"Warning: Could not get medication statistics: {str(e)}")
        return {}


def _process_category_dependent_column_pandas(table_obj, column_name: str, column_config: Dict[str, Any]) -> None:
    """Process columns where ranges depend on category values using pandas."""
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
    
    # Apply outlier filtering for each category
    for category, range_config in column_config.items():
        if isinstance(range_config, dict) and 'min' in range_config and 'max' in range_config:
            min_val = range_config['min']
            max_val = range_config['max']
            
            # Create mask for this category (case-insensitive)
            category_mask = (table_obj.df[category_col].astype(str).str.lower() == category.lower())
            
            # Create mask for values outside range
            outlier_mask = (
                (table_obj.df[column_name] < min_val) | 
                (table_obj.df[column_name] > max_val)
            )
            
            # Combine masks and set outliers to NaN
            combined_mask = category_mask & outlier_mask
            table_obj.df.loc[combined_mask, column_name] = pd.NA
    
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
    """Process medication dose column with unit-dependent ranges using pandas."""
    
    # Get before statistics
    before_stats = _get_medication_statistics_pandas(table_obj.df)
    
    # Apply outlier filtering for each medication/unit combination
    for med_category, unit_configs in column_config.items():
        if isinstance(unit_configs, dict):
            for unit, range_config in unit_configs.items():
                if isinstance(range_config, dict) and 'min' in range_config and 'max' in range_config:
                    min_val = range_config['min']
                    max_val = range_config['max']
                    
                    # Create mask for this medication category and unit (case-insensitive)
                    med_mask = (table_obj.df['med_category'].astype(str).str.lower() == med_category.lower()) & (table_obj.df['med_dose_unit'].astype(str).str.lower() == unit.lower())
                    
                    # Create mask for values outside range
                    outlier_mask = (
                        (table_obj.df['med_dose'] < min_val) | 
                        (table_obj.df['med_dose'] > max_val)
                    )
                    
                    # Combine masks and set outliers to NaN
                    combined_mask = med_mask & outlier_mask
                    table_obj.df.loc[combined_mask, 'med_dose'] = pd.NA
    
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
    """Process columns with simple min/max ranges using pandas."""
    if isinstance(column_config, dict) and 'min' in column_config and 'max' in column_config:
        min_val = column_config['min']
        max_val = column_config['max']
        
        # Get before count
        before_count = table_obj.df[column_name].notna().sum()
        
        # Create mask for values outside range
        outlier_mask = (
            (table_obj.df[column_name] < min_val) | 
            (table_obj.df[column_name] > max_val)
        )
        
        # Set outliers to NaN
        table_obj.df.loc[outlier_mask, column_name] = pd.NA
        
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
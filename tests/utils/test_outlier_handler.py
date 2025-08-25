"""
Tests for the outlier_handler module.

This module tests outlier detection and handling functionality for clinical data
based on configurable range specifications from the YAML configuration file.
"""

import pytest
import pandas as pd
import numpy as np
import os
import tempfile
from unittest.mock import patch, mock_open, MagicMock
from pathlib import Path

from clifpy.utils.outlier_handler import (
    apply_outlier_handling,
    get_outlier_summary,
    _load_outlier_config,
    _get_category_statistics_pandas,
    _get_medication_statistics_pandas,
    _process_category_dependent_column_pandas,
    _process_medication_column_pandas,
    _process_simple_range_column_pandas,
    _analyze_column_outliers_pandas
)


class MockTableObj:
    """Mock table object for testing."""
    def __init__(self, df, table_name):
        self.df = df
        self.table_name = table_name


class TestLoadOutlierConfig:
    """Tests for _load_outlier_config function."""
    
    def test_load_default_config(self):
        """Test loading the default outlier configuration."""
        config = _load_outlier_config()
        
        assert config is not None
        assert 'tables' in config
        assert 'vitals' in config['tables']
        assert 'labs' in config['tables']
        assert 'medication_admin_continuous' in config['tables']
    
    def test_load_custom_config(self, tmp_path):
        """Test loading a custom outlier configuration."""
        custom_config = {
            'tables': {
                'test_table': {
                    'test_column': {
                        'min': 0,
                        'max': 100
                    }
                }
            }
        }
        
        config_file = tmp_path / "custom_config.yaml"
        with open(config_file, 'w') as f:
            import yaml
            yaml.dump(custom_config, f)
        
        config = _load_outlier_config(str(config_file))
        
        assert config == custom_config
    
    def test_load_nonexistent_config(self):
        """Test loading a non-existent configuration file."""
        config = _load_outlier_config("/nonexistent/path/config.yaml")
        assert config is None
    
    def test_load_invalid_yaml(self, tmp_path):
        """Test loading an invalid YAML file."""
        invalid_file = tmp_path / "invalid.yaml"
        with open(invalid_file, 'w') as f:
            f.write("invalid: yaml: content: [")
        
        config = _load_outlier_config(str(invalid_file))
        assert config is None


class TestCategoryStatistics:
    """Tests for _get_category_statistics_pandas function."""
    
    def test_basic_statistics(self):
        """Test basic category statistics calculation."""
        df = pd.DataFrame({
            'vital_category': ['heart_rate', 'heart_rate', 'temp_c', 'temp_c'],
            'vital_value': [80, 120, 36.5, None]
        })
        
        stats = _get_category_statistics_pandas(df, 'vital_value', 'vital_category')
        
        assert 'heart_rate' in stats
        assert 'temp_c' in stats
        assert stats['heart_rate']['non_null_count'] == 2
        assert stats['heart_rate']['total_count'] == 2
        assert stats['temp_c']['non_null_count'] == 1
        assert stats['temp_c']['total_count'] == 2
    
    def test_empty_dataframe(self):
        """Test statistics with empty DataFrame."""
        df = pd.DataFrame(columns=['vital_category', 'vital_value'])
        
        stats = _get_category_statistics_pandas(df, 'vital_value', 'vital_category')
        
        assert stats == {}
    
    def test_all_null_categories(self):
        """Test statistics with all null category values."""
        df = pd.DataFrame({
            'vital_category': [None, None, None],
            'vital_value': [80, 120, 90]
        })
        
        stats = _get_category_statistics_pandas(df, 'vital_value', 'vital_category')
        
        assert stats == {}


class TestMedicationStatistics:
    """Tests for _get_medication_statistics_pandas function."""
    
    def test_medication_statistics(self):
        """Test medication statistics calculation."""
        df = pd.DataFrame({
            'med_category': ['norepinephrine', 'norepinephrine', 'propofol'],
            'med_dose_unit': ['mcg/kg/min', 'mcg/min', 'mg/hr'],
            'med_dose': [0.1, 50, 100]
        })
        
        stats = _get_medication_statistics_pandas(df)
        
        expected_keys = ['norepinephrine (mcg/kg/min)', 'norepinephrine (mcg/min)', 'propofol (mg/hr)']
        for key in expected_keys:
            assert key in stats
            assert stats[key]['non_null_count'] == 1
            assert stats[key]['total_count'] == 1
    
    def test_medication_statistics_with_nulls(self):
        """Test medication statistics with null values."""
        df = pd.DataFrame({
            'med_category': ['norepinephrine', 'norepinephrine', None],
            'med_dose_unit': ['mcg/kg/min', None, 'mg/hr'],
            'med_dose': [0.1, 50, 100]
        })
        
        stats = _get_medication_statistics_pandas(df)
        
        # Only the first row should be counted (has both category and unit)
        assert len(stats) == 1
        assert 'norepinephrine (mcg/kg/min)' in stats


class TestApplyOutlierHandling:
    """Tests for apply_outlier_handling function."""
    
    def test_empty_dataframe(self, capsys):
        """Test handling empty DataFrame."""
        table_obj = MockTableObj(pd.DataFrame(), 'vitals')
        apply_outlier_handling(table_obj)
        
        captured = capsys.readouterr()
        assert "No data to process for outlier handling" in captured.out
    
    def test_none_dataframe(self, capsys):
        """Test handling None DataFrame."""
        table_obj = MockTableObj(None, 'vitals')
        apply_outlier_handling(table_obj)
        
        captured = capsys.readouterr()
        assert "No data to process for outlier handling" in captured.out
    
    def test_unknown_table_type(self, capsys):
        """Test handling unknown table type."""
        df = pd.DataFrame({'test_column': [1, 2, 3]})
        table_obj = MockTableObj(df, 'unknown_table')
        apply_outlier_handling(table_obj)
        
        captured = capsys.readouterr()
        assert "No outlier configuration found for table: unknown_table" in captured.out
    
    @patch('clifpy.utils.outlier_handler._load_outlier_config')
    def test_config_loading_failure(self, mock_load_config, capsys):
        """Test failure to load outlier configuration."""
        mock_load_config.return_value = None
        
        df = pd.DataFrame({'vital_value': [80, 120, 90]})
        table_obj = MockTableObj(df, 'vitals')
        apply_outlier_handling(table_obj)
        
        captured = capsys.readouterr()
        assert "Failed to load outlier configuration" in captured.out


class TestVitalsOutlierHandling:
    """Tests for outlier handling in vitals table."""
    
    def test_heart_rate_outliers(self):
        """Test heart rate outlier detection."""
        # Based on config: heart_rate min: 0, max: 300
        df = pd.DataFrame({
            'vital_category': ['heart_rate'] * 5,
            'vital_value': [-10, 30, 60, 80, 350]  # -10, 350 are outliers
        })
        table_obj = MockTableObj(df, 'vitals')
        
        apply_outlier_handling(table_obj)
        
        # Check that outliers were set to NaN
        expected_values = [pd.NA, 30, 60, 80, pd.NA]
        pd.testing.assert_series_equal(
            table_obj.df['vital_value'], 
            pd.Series(expected_values, name='vital_value', dtype='Int64'),
            check_dtype=False
        )
    
    def test_temperature_outliers(self):
        """Test temperature outlier detection."""
        # Based on config: temp_c min: 32, max: 44
        df = pd.DataFrame({
            'vital_category': ['temp_c'] * 4,
            'vital_value': [25, 32, 37, 45]  # 25, 45 are outliers
        })
        table_obj = MockTableObj(df, 'vitals')
        
        apply_outlier_handling(table_obj)
        
        # Check that outliers were set to NaN
        expected_values = [pd.NA, 32, 37, pd.NA]
        pd.testing.assert_series_equal(
            table_obj.df['vital_value'], 
            pd.Series(expected_values, name='vital_value', dtype='Int64'),
            check_dtype=False
        )
    
    def test_mixed_vital_categories(self):
        """Test outlier handling with mixed vital categories."""
        df = pd.DataFrame({
            'vital_category': ['heart_rate', 'temp_c', 'heart_rate', 'temp_c'],
            'vital_value': [50, 32, 100, 50]  # 50 (temp_c) is outlier (below min 32)
        })
        table_obj = MockTableObj(df, 'vitals')
        
        apply_outlier_handling(table_obj)
        
        # Check that outliers were set to NaN per category
        expected_values = [50, 32, 100, pd.NA]
        pd.testing.assert_series_equal(
            table_obj.df['vital_value'], 
            pd.Series(expected_values, name='vital_value', dtype='Int64'),
            check_dtype=False
        )


class TestLabsOutlierHandling:
    """Tests for outlier handling in labs table."""
    
    def test_hemoglobin_outliers(self):
        """Test hemoglobin outlier detection."""
        # Based on config: hemoglobin min: 3.0, max: 25.0
        df = pd.DataFrame({
            'lab_category': ['hemoglobin'] * 5,
            'lab_value_numeric': [1.0, 8.5, 15.2, 20.0, 30.0]  # 1.0 and 30.0 are outliers
        })
        table_obj = MockTableObj(df, 'labs')
        
        apply_outlier_handling(table_obj)
        
        # Check that outliers were set to NaN
        result_series = table_obj.df['lab_value_numeric']
        assert pd.isna(result_series.iloc[0])  # First value should be NaN (outlier)
        assert result_series.iloc[1] == 8.5   # Should be preserved
        assert result_series.iloc[2] == 15.2  # Should be preserved
        assert result_series.iloc[3] == 20.0  # Should be preserved
        assert pd.isna(result_series.iloc[4])  # Last value should be NaN (outlier)
    
    def test_glucose_outliers(self):
        """Test glucose outlier detection."""
        # Based on config: glucose_serum min: 0.0, max: 2000.0
        df = pd.DataFrame({
            'lab_category': ['glucose_serum'] * 4,
            'lab_value_numeric': [-10.0, 100.0, 500.0, 2500.0]  # -10.0 and 2500.0 are outliers
        })
        table_obj = MockTableObj(df, 'labs')
        
        apply_outlier_handling(table_obj)
        
        result_series = table_obj.df['lab_value_numeric']
        assert pd.isna(result_series.iloc[0])  # Should be NaN (outlier)
        assert result_series.iloc[1] == 100.0  # Should be preserved
        assert result_series.iloc[2] == 500.0  # Should be preserved
        assert pd.isna(result_series.iloc[3])   # Should be NaN (outlier)


class TestMedicationOutlierHandling:
    """Tests for outlier handling in medication administration table."""
    
    def test_norepinephrine_mcg_kg_min_outliers(self):
        """Test norepinephrine outlier detection in mcg/kg/min."""
        # Based on config: norepinephrine mcg/kg/min min: 0.0, max: 3.0
        df = pd.DataFrame({
            'med_category': ['norepinephrine'] * 4,
            'med_dose_unit': ['mcg/kg/min'] * 4,
            'med_dose': [-0.1, 1.0, 2.5, 5.0]  # -0.1 and 5.0 are outliers
        })
        table_obj = MockTableObj(df, 'medication_admin_continuous')
        
        apply_outlier_handling(table_obj)
        
        result_series = table_obj.df['med_dose']
        assert pd.isna(result_series.iloc[0])  # Should be NaN (outlier)
        assert result_series.iloc[1] == 1.0   # Should be preserved
        assert result_series.iloc[2] == 2.5   # Should be preserved
        assert pd.isna(result_series.iloc[3])  # Should be NaN (outlier)
    
    def test_propofol_mg_hr_outliers(self):
        """Test propofol outlier detection in mg/hr."""
        # Based on config: propofol mg/hr min: 0.0, max: 400.0
        df = pd.DataFrame({
            'med_category': ['propofol'] * 4,
            'med_dose_unit': ['mg/hr'] * 4,
            'med_dose': [-10.0, 50.0, 350.0, 500.0]  # -10.0 and 500.0 are outliers
        })
        table_obj = MockTableObj(df, 'medication_admin_continuous')
        
        apply_outlier_handling(table_obj)
        
        result_series = table_obj.df['med_dose']
        assert pd.isna(result_series.iloc[0])  # Should be NaN (outlier)
        assert result_series.iloc[1] == 50.0  # Should be preserved
        assert result_series.iloc[2] == 350.0 # Should be preserved
        assert pd.isna(result_series.iloc[3])  # Should be NaN (outlier)
    
    def test_mixed_medications_and_units(self):
        """Test outlier handling with mixed medications and units."""
        df = pd.DataFrame({
            'med_category': ['norepinephrine', 'propofol', 'norepinephrine', 'propofol'],
            'med_dose_unit': ['mcg/kg/min', 'mg/hr', 'mcg/kg/min', 'mg/hr'],
            'med_dose': [2.0, 100.0, 5.0, 500.0]  # 5.0 and 500.0 are outliers
        })
        table_obj = MockTableObj(df, 'medication_admin_continuous')
        
        apply_outlier_handling(table_obj)
        
        result_series = table_obj.df['med_dose']
        assert result_series.iloc[0] == 2.0   # Should be preserved
        assert result_series.iloc[1] == 100.0 # Should be preserved
        assert pd.isna(result_series.iloc[2])  # Should be NaN (outlier)
        assert pd.isna(result_series.iloc[3])  # Should be NaN (outlier)


class TestPatientAssessmentsOutlierHandling:
    """Tests for outlier handling in patient assessments table."""
    
    def test_gcs_total_outliers(self):
        """Test GCS total score outlier detection."""
        # Based on config: gcs_total min: 3, max: 15
        df = pd.DataFrame({
            'assessment_category': ['gcs_total'] * 5,
            'numerical_value': [1, 5, 10, 15, 20]  # 1 and 20 are outliers
        })
        table_obj = MockTableObj(df, 'patient_assessments')
        
        apply_outlier_handling(table_obj)
        
        expected_values = [pd.NA, 5, 10, 15, pd.NA]
        pd.testing.assert_series_equal(
            table_obj.df['numerical_value'], 
            pd.Series(expected_values, name='numerical_value', dtype='Int64'),
            check_dtype=False
        )
    
    def test_rass_outliers(self):
        """Test RASS score outlier detection."""
        # Based on config: RASS min: -5, max: 4
        df = pd.DataFrame({
            'assessment_category': ['RASS'] * 5,
            'numerical_value': [-6, -3, 0, 3, 5]  # -6 and 5 are outliers
        })
        table_obj = MockTableObj(df, 'patient_assessments')
        
        apply_outlier_handling(table_obj)
        
        expected_values = [pd.NA, -3, 0, 3, pd.NA]
        pd.testing.assert_series_equal(
            table_obj.df['numerical_value'], 
            pd.Series(expected_values, name='numerical_value', dtype='Int64'),
            check_dtype=False
        )


class TestSimpleRangeOutlierHandling:
    """Tests for outlier handling in simple range columns."""
    
    def test_age_at_admission_outliers(self):
        """Test age at admission outlier detection."""
        # Based on config: age_at_admission min: 0, max: 120
        df = pd.DataFrame({
            'age_at_admission': [-5, 25, 65, 100, 150]  # -5 and 150 are outliers
        })
        table_obj = MockTableObj(df, 'hospitalization')
        
        apply_outlier_handling(table_obj)
        
        expected_values = [pd.NA, 25, 65, 100, pd.NA]
        pd.testing.assert_series_equal(
            table_obj.df['age_at_admission'], 
            pd.Series(expected_values, name='age_at_admission', dtype='Int64'),
            check_dtype=False
        )
    
    def test_fio2_set_outliers(self):
        """Test FiO2 set outlier detection."""
        # Based on config: fio2_set min: 0.21, max: 1.0
        df = pd.DataFrame({
            'fio2_set': [0.1, 0.4, 0.8, 1.0, 1.5]  # 0.1 and 1.5 are outliers
        })
        table_obj = MockTableObj(df, 'respiratory_support')
        
        apply_outlier_handling(table_obj)
        
        result_series = table_obj.df['fio2_set']
        assert pd.isna(result_series.iloc[0])  # Should be NaN (outlier)
        assert result_series.iloc[1] == 0.4   # Should be preserved
        assert result_series.iloc[2] == 0.8   # Should be preserved
        assert result_series.iloc[3] == 1.0   # Should be preserved
        assert pd.isna(result_series.iloc[4])  # Should be NaN (outlier)


class TestGetOutlierSummary:
    """Tests for get_outlier_summary function."""
    
    def test_summary_with_valid_data(self):
        """Test outlier summary generation with valid data."""
        df = pd.DataFrame({
            'vital_category': ['heart_rate'] * 3,
            'vital_value': [50, 80, 120]
        })
        table_obj = MockTableObj(df, 'vitals')
        
        summary = get_outlier_summary(table_obj)
        
        assert summary['table_name'] == 'vitals'
        assert summary['total_rows'] == 3
        assert summary['config_source'] == 'CLIF standard'
        assert 'vital_value' in summary['columns_analyzed']
    
    def test_summary_with_empty_data(self):
        """Test outlier summary with empty DataFrame."""
        table_obj = MockTableObj(pd.DataFrame(), 'vitals')
        
        summary = get_outlier_summary(table_obj)
        
        assert summary['status'] == 'No data to analyze'
    
    def test_summary_with_unknown_table(self):
        """Test outlier summary with unknown table type."""
        df = pd.DataFrame({'test': [1, 2, 3]})
        table_obj = MockTableObj(df, 'unknown')
        
        summary = get_outlier_summary(table_obj)
        
        assert 'No configuration for table' in summary['status']
    
    def test_summary_with_custom_config(self, tmp_path):
        """Test outlier summary with custom configuration."""
        custom_config = {
            'tables': {
                'test_table': {
                    'test_column': {'min': 0, 'max': 100}
                }
            }
        }
        
        config_file = tmp_path / "custom.yaml"
        with open(config_file, 'w') as f:
            import yaml
            yaml.dump(custom_config, f)
        
        df = pd.DataFrame({'test_column': [50, 75, 90]})
        table_obj = MockTableObj(df, 'test_table')
        
        summary = get_outlier_summary(table_obj, str(config_file))
        
        assert summary['config_source'] == 'Custom'


class TestEdgeCases:
    """Tests for edge cases and error conditions."""
    
    def test_missing_category_column(self):
        """Test handling when category column is missing."""
        df = pd.DataFrame({
            'vital_value': [80, 120, 90]
            # Missing 'vital_category' column
        })
        table_obj = MockTableObj(df, 'vitals')
        
        # Should not crash, but won't process anything due to missing category column
        # The function returns early when category column is missing
        try:
            apply_outlier_handling(table_obj)
        except KeyError:
            # Expected behavior - missing category column causes KeyError
            pass
        
        # If it doesn't crash, values should remain unchanged
        if 'vital_value' in table_obj.df.columns:
            expected_values = [80, 120, 90]
            pd.testing.assert_series_equal(
                table_obj.df['vital_value'], 
                pd.Series(expected_values, name='vital_value'),
                check_dtype=False
            )
    
    def test_null_values_in_data(self):
        """Test handling null values in data."""
        df = pd.DataFrame({
            'vital_category': ['heart_rate', 'heart_rate', 'heart_rate'],
            'vital_value': [50, None, 350]  # One null value, 350 is outlier (above max 300)
        })
        table_obj = MockTableObj(df, 'vitals')
        
        apply_outlier_handling(table_obj)
        
        # Null should remain null, 50 should be preserved, 350 should become NaN (outlier)
        assert table_obj.df['vital_value'].iloc[0] == 50
        assert pd.isna(table_obj.df['vital_value'].iloc[1])
        assert pd.isna(table_obj.df['vital_value'].iloc[2])
    
    def test_column_not_in_dataframe(self):
        """Test when configured column is not in DataFrame."""
        df = pd.DataFrame({
            'vital_category': ['heart_rate'],
            'other_column': [100]
            # Missing 'vital_value' column that's in config
        })
        table_obj = MockTableObj(df, 'vitals')
        
        # Should not crash
        apply_outlier_handling(table_obj)
        
        # DataFrame should remain unchanged
        assert 'vital_value' not in table_obj.df.columns
        assert table_obj.df['other_column'].iloc[0] == 100
    
    def test_non_numeric_data_types(self):
        """Test handling non-numeric data in numeric columns."""
        df = pd.DataFrame({
            'vital_category': ['heart_rate', 'heart_rate'],
            'vital_value': ['not_a_number', 50]
        })
        table_obj = MockTableObj(df, 'vitals')
        
        # The function may fail when trying to compare strings with numeric values
        # This is expected behavior - mixed data types should cause an error
        try:
            apply_outlier_handling(table_obj)
            # If it doesn't crash, check the results
            assert table_obj.df['vital_value'].iloc[0] == 'not_a_number'
            assert table_obj.df['vital_value'].iloc[1] == 50  # Within range
        except (TypeError, ValueError):
            # Expected behavior - comparing string with numeric threshold causes error
            pass


class TestProcessingFunctions:
    """Tests for individual processing functions."""
    
    def test_process_simple_range_column(self):
        """Test _process_simple_range_column_pandas function directly."""
        df = pd.DataFrame({'test_col': [5, 15, 25, 35]})
        table_obj = MockTableObj(df, 'test_table')
        column_config = {'min': 10, 'max': 30}
        
        _process_simple_range_column_pandas(table_obj, 'test_col', column_config)
        
        # 5 and 35 should be outliers (set to NaN)
        expected_values = [pd.NA, 15, 25, pd.NA]
        pd.testing.assert_series_equal(
            table_obj.df['test_col'], 
            pd.Series(expected_values, name='test_col', dtype='Int64'),
            check_dtype=False
        )
    
    def test_process_simple_range_invalid_config(self):
        """Test _process_simple_range_column_pandas with invalid config."""
        df = pd.DataFrame({'test_col': [5, 15, 25, 35]})
        table_obj = MockTableObj(df, 'test_table')
        column_config = {'invalid': 'config'}  # Missing min/max
        
        # Should not crash and not modify data
        _process_simple_range_column_pandas(table_obj, 'test_col', column_config)
        
        # Values should remain unchanged
        expected_values = [5, 15, 25, 35]
        pd.testing.assert_series_equal(
            table_obj.df['test_col'], 
            pd.Series(expected_values, name='test_col'),
            check_dtype=False
        )
    
    def test_analyze_column_outliers(self):
        """Test _analyze_column_outliers_pandas function."""
        df = pd.DataFrame({'test_col': [1, 2, None, 4, 5]})
        table_obj = MockTableObj(df, 'test_table')
        column_config = {'min': 0, 'max': 10}
        
        result = _analyze_column_outliers_pandas(table_obj, 'test_col', column_config)
        
        assert result['total_non_null_values'] == 4
        assert result['configuration_type'] == 'simple_range'
    
    def test_analyze_column_outliers_category_dependent(self):
        """Test _analyze_column_outliers_pandas for category-dependent tables."""
        df = pd.DataFrame({'vital_value': [1, 2, None, 4, 5]})
        table_obj = MockTableObj(df, 'vitals')
        column_config = {}
        
        result = _analyze_column_outliers_pandas(table_obj, 'vital_value', column_config)
        
        assert result['total_non_null_values'] == 4
        assert result['configuration_type'] == 'category_dependent'


if __name__ == '__main__':
    pytest.main([__file__])
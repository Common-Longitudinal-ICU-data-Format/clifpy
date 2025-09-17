"""
Comprehensive tests for clifpy.utils.comorbidity module.

This module tests the calculate_cci function with comprehensive coverage including:
- All ICD codes from cci.yaml configuration
- Different input types and data formats
- Hierarchy logic (assign0) functionality
- Edge cases and error handling
- Data type consistency verification
"""

import pytest
import pandas as pd
import polars as pl
import numpy as np
from unittest.mock import Mock, patch, MagicMock
from pathlib import Path

from clifpy.utils.comorbidity import calculate_cci, _load_cci_config, _apply_hierarchy_logic, _calculate_cci_score


class TestCalculateCCI:
    """Main test class for calculate_cci function."""

    @pytest.fixture
    def sample_hospital_diagnosis_df(self):
        """Create a sample hospital diagnosis DataFrame for testing."""
        return pd.DataFrame({
            'hospitalization_id': ['HOSP_001', 'HOSP_001', 'HOSP_002', 'HOSP_003', 'HOSP_003'],
            'diagnosis_code': ['I21.45', 'E10.1', 'K25.5', 'I50.9', 'E11.2'],
            'diagnosis_code_format': ['ICD10CM', 'ICD10CM', 'ICD10CM', 'ICD10CM', 'ICD10CM']
        })

    @pytest.fixture
    def all_conditions_df(self):
        """
        Create a DataFrame with ICD codes representing all 17 CCI conditions.
        Based on the actual codes from cci.yaml.
        Uses separate hospitalizations for hierarchical conditions to avoid conflicts.
        """
        data = []

        # Non-hierarchical conditions - all in one hospitalization
        non_hierarchical = [
            ('I21', 'myocardial_infarction'),           # MI
            ('I50', 'congestive_heart_failure'),       # CHF
            ('I70', 'peripheral_vascular_disease'),    # PVD
            ('I63', 'cerebrovascular_disease'),        # Stroke
            ('F00', 'dementia'),                       # Dementia
            ('J44', 'chronic_pulmonary_disease'),      # COPD
            ('M05', 'connective_tissue_disease'),      # Rheumatoid arthritis
            ('K25', 'peptic_ulcer_disease'),           # Peptic ulcer
            ('G81', 'hemiplegia'),                     # Hemiplegia
            ('N18', 'renal_disease'),                  # Renal disease
            ('B20', 'aids')                            # AIDS
        ]

        for code, condition in non_hierarchical:
            data.append({
                'hospitalization_id': 'HOSP_NON_HIER',
                'diagnosis_code': code,
                'diagnosis_code_format': 'ICD10CM'
            })

        # Hierarchical conditions - separate hospitalizations to test each independently
        hierarchical = [
            ('B18', 'mild_liver_disease', 'HOSP_LIVER_MILD'),
            ('E100', 'diabetes_uncomplicated', 'HOSP_DIABETES_MILD'),
            ('C50', 'cancer', 'HOSP_CANCER'),
            ('I850', 'moderate_severe_liver_disease', 'HOSP_LIVER_SEVERE'),
            ('E102', 'diabetes_with_complications', 'HOSP_DIABETES_SEVERE'),
            ('C78', 'metastatic_solid_tumor', 'HOSP_METASTATIC')
        ]

        for code, condition, hosp_id in hierarchical:
            data.append({
                'hospitalization_id': hosp_id,
                'diagnosis_code': code,
                'diagnosis_code_format': 'ICD10CM'
            })

        return pd.DataFrame(data)

    @pytest.fixture
    def hierarchy_test_df(self):
        """Create DataFrame to test hierarchy logic (assign0)."""
        return pd.DataFrame({
            'hospitalization_id': ['HOSP_H1', 'HOSP_H1', 'HOSP_H2', 'HOSP_H2', 'HOSP_H3', 'HOSP_H3'],
            'diagnosis_code': [
                'E102',   # diabetes_with_complications
                'E100',   # diabetes_uncomplicated (should be 0 due to hierarchy)
                'I850',   # moderate_severe_liver_disease
                'K700',   # mild_liver_disease (should be 0 due to hierarchy)
                'C78',    # metastatic_solid_tumor
                'C20'     # cancer (should be 0 due to hierarchy)
            ],
            'diagnosis_code_format': ['ICD10CM'] * 6
        })

    @pytest.fixture
    def mock_hospital_diagnosis_object(self, sample_hospital_diagnosis_df):
        """Create a mock HospitalDiagnosis object for testing."""
        mock_obj = Mock()
        mock_obj.df = sample_hospital_diagnosis_df
        return mock_obj

    def test_calculate_cci_with_pandas_dataframe(self, sample_hospital_diagnosis_df):
        """Test calculate_cci with pandas DataFrame input."""
        result = calculate_cci(sample_hospital_diagnosis_df)

        # Verify result structure
        assert isinstance(result, pd.DataFrame)
        assert 'hospitalization_id' in result.columns
        assert 'cci_score' in result.columns

        # Verify data types - all should be integers
        for col in result.columns:
            if col != 'hospitalization_id':
                assert result[col].dtype in [np.int32, np.int64], f"Column {col} should be integer, got {result[col].dtype}"

    def test_calculate_cci_with_polars_dataframe(self, sample_hospital_diagnosis_df):
        """Test calculate_cci with polars DataFrame input."""
        pl_df = pl.from_pandas(sample_hospital_diagnosis_df)
        result = calculate_cci(pl_df)

        # Verify result structure
        assert isinstance(result, pd.DataFrame)
        assert 'hospitalization_id' in result.columns
        assert 'cci_score' in result.columns

    def test_calculate_cci_with_hospital_diagnosis_object(self, mock_hospital_diagnosis_object):
        """Test calculate_cci with HospitalDiagnosis object input."""
        result = calculate_cci(mock_hospital_diagnosis_object)

        # Verify result structure
        assert isinstance(result, pd.DataFrame)
        assert 'hospitalization_id' in result.columns
        assert 'cci_score' in result.columns

    def test_calculate_cci_invalid_input_type(self):
        """Test calculate_cci with invalid input type raises ValueError."""
        with pytest.raises(ValueError, match="hospital_diagnosis must be"):
            calculate_cci("invalid_input")

    def test_calculate_cci_all_conditions_present(self, all_conditions_df):
        """Test calculate_cci with all 17 CCI conditions present."""
        result = calculate_cci(all_conditions_df, hierarchy=False)

        # Should have all 17 condition columns plus hospitalization_id and cci_score
        expected_columns = {
            'hospitalization_id', 'cci_score',
            'myocardial_infarction', 'congestive_heart_failure', 'peripheral_vascular_disease',
            'cerebrovascular_disease', 'dementia', 'chronic_pulmonary_disease',
            'connective_tissue_disease', 'peptic_ulcer_disease', 'mild_liver_disease',
            'diabetes_uncomplicated', 'diabetes_with_complications', 'hemiplegia',
            'renal_disease', 'cancer', 'moderate_severe_liver_disease',
            'metastatic_solid_tumor', 'aids'
        }

        assert set(result.columns) == expected_columns

        # Verify all conditions are detected across all hospitalizations
        condition_checks = {
            'HOSP_NON_HIER': ['myocardial_infarction', 'congestive_heart_failure', 'peripheral_vascular_disease',
                             'cerebrovascular_disease', 'dementia', 'chronic_pulmonary_disease',
                             'connective_tissue_disease', 'peptic_ulcer_disease', 'hemiplegia',
                             'renal_disease', 'aids'],
            'HOSP_LIVER_MILD': ['mild_liver_disease'],
            'HOSP_DIABETES_MILD': ['diabetes_uncomplicated'],
            'HOSP_CANCER': ['cancer'],
            'HOSP_LIVER_SEVERE': ['moderate_severe_liver_disease'],
            'HOSP_DIABETES_SEVERE': ['diabetes_with_complications'],
            'HOSP_METASTATIC': ['metastatic_solid_tumor']
        }

        for hosp_id, conditions in condition_checks.items():
            hosp_row = result[result['hospitalization_id'] == hosp_id].iloc[0]
            for condition in conditions:
                assert hosp_row[condition] == 1, f"Condition {condition} should be 1 in {hosp_id}, got {hosp_row[condition]}"

    def test_calculate_cci_score_calculation(self):
        """Test CCI score calculation with known conditions and weights."""
        # Create test data with specific conditions and known weights
        test_df = pd.DataFrame({
            'hospitalization_id': ['HOSP_TEST'],
            'diagnosis_code': ['I50'],  # CHF (weight=2)
            'diagnosis_code_format': ['ICD10CM']
        })

        result = calculate_cci(test_df)

        # CHF has weight 2, so CCI score should be 2
        assert result.iloc[0]['cci_score'] == 2
        assert result.iloc[0]['congestive_heart_failure'] == 1

    def test_hierarchy_logic_diabetes(self, hierarchy_test_df):
        """Test hierarchy logic for diabetes conditions."""
        result = calculate_cci(hierarchy_test_df, hierarchy=True)

        # HOSP_H1 has both diabetes conditions - uncomplicated should be 0
        h1_row = result[result['hospitalization_id'] == 'HOSP_H1'].iloc[0]
        assert h1_row['diabetes_with_complications'] == 1
        assert h1_row['diabetes_uncomplicated'] == 0  # Should be 0 due to hierarchy

    def test_hierarchy_logic_liver_disease(self, hierarchy_test_df):
        """Test hierarchy logic for liver disease conditions."""
        result = calculate_cci(hierarchy_test_df, hierarchy=True)

        # HOSP_H2 has both liver conditions - mild should be 0
        h2_row = result[result['hospitalization_id'] == 'HOSP_H2'].iloc[0]
        assert h2_row['moderate_severe_liver_disease'] == 1
        assert h2_row['mild_liver_disease'] == 0  # Should be 0 due to hierarchy

    def test_hierarchy_logic_cancer(self, hierarchy_test_df):
        """Test hierarchy logic for cancer conditions."""
        result = calculate_cci(hierarchy_test_df, hierarchy=True)

        # HOSP_H3 has both cancer conditions - localized should be 0
        h3_row = result[result['hospitalization_id'] == 'HOSP_H3'].iloc[0]
        assert h3_row['metastatic_solid_tumor'] == 1
        assert h3_row['cancer'] == 0  # Should be 0 due to hierarchy

    def test_hierarchy_disabled(self, hierarchy_test_df):
        """Test that hierarchy logic can be disabled."""
        result = calculate_cci(hierarchy_test_df, hierarchy=False)

        # With hierarchy=False, both conditions in each pair should be 1
        h1_row = result[result['hospitalization_id'] == 'HOSP_H1'].iloc[0]
        assert h1_row['diabetes_with_complications'] == 1
        assert h1_row['diabetes_uncomplicated'] == 1  # Should be 1 when hierarchy disabled

    def test_icd_code_preprocessing(self):
        """Test that ICD codes are properly preprocessed (decimal removal)."""
        test_df = pd.DataFrame({
            'hospitalization_id': ['HOSP_TEST'],
            'diagnosis_code': ['I21.45'],  # Should match I21 prefix
            'diagnosis_code_format': ['ICD10CM']
        })

        result = calculate_cci(test_df)

        # I21.45 should be processed to I21 and match myocardial_infarction
        assert result.iloc[0]['myocardial_infarction'] == 1

    def test_non_icd10cm_codes_filtered(self):
        """Test that non-ICD10CM codes are filtered out."""
        test_df = pd.DataFrame({
            'hospitalization_id': ['HOSP_TEST', 'HOSP_TEST'],
            'diagnosis_code': ['I21', '410.1'],  # I21 is ICD10CM, 410.1 is ICD9
            'diagnosis_code_format': ['ICD10CM', 'ICD9CM']
        })

        result = calculate_cci(test_df)

        # Only ICD10CM code should be processed
        assert result.iloc[0]['myocardial_infarction'] == 1

    def test_empty_dataframe(self):
        """Test calculate_cci with empty DataFrame."""
        empty_df = pd.DataFrame(columns=['hospitalization_id', 'diagnosis_code', 'diagnosis_code_format'])

        result = calculate_cci(empty_df)

        # Should return empty DataFrame with correct columns
        assert len(result) == 0
        assert 'hospitalization_id' in result.columns
        assert 'cci_score' in result.columns

    def test_no_matching_codes(self):
        """Test with valid DataFrame but no matching ICD codes."""
        test_df = pd.DataFrame({
            'hospitalization_id': ['HOSP_TEST'],
            'diagnosis_code': ['Z99.9'],  # Not a CCI condition
            'diagnosis_code_format': ['ICD10CM']
        })

        result = calculate_cci(test_df)

        # Should have row with all conditions = 0 and cci_score = 0
        assert len(result) == 1
        assert result.iloc[0]['cci_score'] == 0

    def test_multiple_hospitalizations(self):
        """Test calculate_cci with multiple hospitalizations."""
        test_df = pd.DataFrame({
            'hospitalization_id': ['HOSP_001', 'HOSP_001', 'HOSP_002'],
            'diagnosis_code': ['I21', 'I50', 'E100'],
            'diagnosis_code_format': ['ICD10CM', 'ICD10CM', 'ICD10CM']
        })

        result = calculate_cci(test_df)

        # Should have 2 rows (one per hospitalization)
        assert len(result) == 2

        # HOSP_001 should have MI + CHF
        hosp1 = result[result['hospitalization_id'] == 'HOSP_001'].iloc[0]
        assert hosp1['myocardial_infarction'] == 1
        assert hosp1['congestive_heart_failure'] == 1

        # HOSP_002 should have diabetes uncomplicated
        hosp2 = result[result['hospitalization_id'] == 'HOSP_002'].iloc[0]
        assert hosp2['diabetes_uncomplicated'] == 1

    def test_data_types_consistency(self, all_conditions_df):
        """Test that all columns (except hospitalization_id) are integers."""
        result = calculate_cci(all_conditions_df)

        for col in result.columns:
            if col == 'hospitalization_id':
                continue  # Skip ID column

            # All condition and score columns should be integer types
            assert result[col].dtype in [np.int32, np.int64], \
                f"Column {col} should be integer type, got {result[col].dtype}"

            # Values should be 0 or 1 for conditions, or non-negative integer for cci_score
            if col == 'cci_score':
                assert (result[col] >= 0).all(), f"CCI score should be non-negative"
            else:
                assert result[col].isin([0, 1]).all(), f"Condition {col} should be 0 or 1"

    def test_specific_icd_codes_from_yaml(self):
        """Test specific ICD codes from each condition category in cci.yaml."""
        test_cases = [
            # (ICD_code, expected_condition, description)
            ('I22', 'myocardial_infarction', 'Subsequent STEMI'),
            ('I099', 'congestive_heart_failure', 'Rheumatic heart failure'),
            ('I731', 'peripheral_vascular_disease', 'Thromboangiitis obliterans'),
            ('G45', 'cerebrovascular_disease', 'TIA'),
            ('F01', 'dementia', 'Vascular dementia'),
            ('J60', 'chronic_pulmonary_disease', 'Pneumoconiosis'),
            ('M32', 'connective_tissue_disease', 'Systemic lupus'),
            ('K26', 'peptic_ulcer_disease', 'Duodenal ulcer'),
            ('B18', 'mild_liver_disease', 'Chronic viral hepatitis'),
            ('E149', 'diabetes_uncomplicated', 'Unspecified diabetes'),
            ('E103', 'diabetes_with_complications', 'Type 1 with ophthalmic complications'),
            ('G041', 'hemiplegia', 'Acute flaccid myelitis'),
            ('I120', 'renal_disease', 'Hypertensive CKD'),
            ('C25', 'cancer', 'Pancreatic cancer'),
            ('I850', 'moderate_severe_liver_disease', 'Esophageal varices'),
            ('C79', 'metastatic_solid_tumor', 'Secondary neoplasm'),
            ('B21', 'aids', 'HIV with associated malignancy')
        ]

        for icd_code, expected_condition, description in test_cases:
            test_df = pd.DataFrame({
                'hospitalization_id': ['TEST_HOSP'],
                'diagnosis_code': [icd_code],
                'diagnosis_code_format': ['ICD10CM']
            })

            result = calculate_cci(test_df)

            assert result.iloc[0][expected_condition] == 1, \
                f"ICD code {icd_code} ({description}) should map to {expected_condition}"

    @patch('clifpy.utils.comorbidity._load_cci_config')
    def test_config_loading_error(self, mock_load_config):
        """Test behavior when CCI config cannot be loaded."""
        mock_load_config.side_effect = FileNotFoundError("Config file not found")

        test_df = pd.DataFrame({
            'hospitalization_id': ['HOSP_TEST'],
            'diagnosis_code': ['I21'],
            'diagnosis_code_format': ['ICD10CM']
        })

        with pytest.raises(FileNotFoundError):
            calculate_cci(test_df)

    def test_missing_required_columns(self):
        """Test behavior with missing required columns."""
        # Missing diagnosis_code_format column
        test_df = pd.DataFrame({
            'hospitalization_id': ['HOSP_TEST'],
            'diagnosis_code': ['I21']
        })

        with pytest.raises(Exception):  # Should raise some exception for missing column
            calculate_cci(test_df)


class TestHelperFunctions:
    """Test helper functions used by calculate_cci."""

    @pytest.fixture
    def sample_config(self):
        """Sample CCI configuration for testing."""
        return {
            'weights': {
                'condition_a': 1,
                'condition_b': 2,
                'condition_c': 0
            },
            'hierarchies': {
                'test_hierarchy': ['condition_severe', 'condition_mild']
            }
        }

    def test_apply_hierarchy_logic(self, sample_config):
        """Test _apply_hierarchy_logic function."""
        # Create test DataFrame with hierarchical conditions
        test_df = pl.DataFrame({
            'hospitalization_id': ['HOSP_1'],
            'condition_severe': [1],
            'condition_mild': [1]
        })

        result = _apply_hierarchy_logic(test_df, sample_config['hierarchies'])

        # Mild condition should be set to 0 when severe is present
        assert result['condition_severe'][0] == 1
        assert result['condition_mild'][0] == 0

    def test_calculate_cci_score(self, sample_config):
        """Test _calculate_cci_score function."""
        test_df = pl.DataFrame({
            'hospitalization_id': ['HOSP_1'],
            'condition_a': [1],
            'condition_b': [1],
            'condition_c': [1]
        })

        result = _calculate_cci_score(test_df, sample_config['weights'])

        # Score should be 1*1 + 1*2 + 1*0 = 3
        assert result['cci_score'][0] == 3

    def test_load_cci_config(self):
        """Test _load_cci_config function loads real config."""
        config = _load_cci_config()

        # Verify config structure
        assert 'name' in config
        assert 'weights' in config
        assert 'diagnosis_code_mappings' in config
        assert 'hierarchies' in config
        assert 'ICD10CM' in config['diagnosis_code_mappings']

        # Verify all 17 conditions are present
        conditions = list(config['diagnosis_code_mappings']['ICD10CM'].keys())
        assert len(conditions) == 17

        # Verify weights for all conditions
        assert len(config['weights']) == 17

    def test_comprehensive_score_calculation(self):
        """Test comprehensive CCI score calculation with multiple conditions."""
        # Create test with conditions that have known weights
        test_df = pd.DataFrame({
            'hospitalization_id': ['HOSP_SCORE_TEST', 'HOSP_SCORE_TEST', 'HOSP_SCORE_TEST'],
            'diagnosis_code': ['I50', 'N18', 'E102'],  # CHF(2) + Renal(1) + DiabComp(1) = 4
            'diagnosis_code_format': ['ICD10CM', 'ICD10CM', 'ICD10CM']
        })

        result = calculate_cci(test_df)

        # Expected score: CHF(2) + Renal(1) + DiabWithComp(1) = 4
        assert result.iloc[0]['cci_score'] == 4
        assert result.iloc[0]['congestive_heart_failure'] == 1
        assert result.iloc[0]['renal_disease'] == 1
        assert result.iloc[0]['diabetes_with_complications'] == 1

    def test_hierarchy_complex_scenario(self):
        """Test complex hierarchy scenario with multiple overlapping conditions."""
        test_df = pd.DataFrame({
            'hospitalization_id': ['COMPLEX_HIER'] * 6,
            'diagnosis_code': ['E100', 'E102', 'B18', 'I850', 'C50', 'C78'],
            'diagnosis_code_format': ['ICD10CM'] * 6
        })

        result = calculate_cci(test_df, hierarchy=True)

        hosp_row = result.iloc[0]

        # Diabetes: only complications should be counted
        assert hosp_row['diabetes_with_complications'] == 1
        assert hosp_row['diabetes_uncomplicated'] == 0

        # Liver: only severe should be counted
        assert hosp_row['moderate_severe_liver_disease'] == 1
        assert hosp_row['mild_liver_disease'] == 0

        # Cancer: only metastatic should be counted
        assert hosp_row['metastatic_solid_tumor'] == 1
        assert hosp_row['cancer'] == 0

        # Expected score: diabetes_comp(1) + severe_liver(4) + metastatic(6) = 11
        assert hosp_row['cci_score'] == 11

    def test_rare_icd_codes(self):
        """Test with rare but valid ICD codes from the configuration."""
        rare_codes = [
            ('P290', 'congestive_heart_failure'),      # Congenital heart failure
            ('Z944', 'mild_liver_disease'),            # Liver transplant status
            ('I982', 'moderate_severe_liver_disease'), # Esophageal varices w/o bleeding
            ('G041', 'hemiplegia'),                    # Acute flaccid myelitis
            ('Z958', 'peripheral_vascular_disease')    # Peripheral vascular prosthesis
        ]

        for code, expected_condition in rare_codes:
            test_df = pd.DataFrame({
                'hospitalization_id': ['RARE_CODE_TEST'],
                'diagnosis_code': [code],
                'diagnosis_code_format': ['ICD10CM']
            })

            result = calculate_cci(test_df)
            assert result.iloc[0][expected_condition] == 1, \
                f"Rare code {code} should map to {expected_condition}"

    def test_maximum_possible_score(self):
        """Test maximum possible CCI score with all highest-weight conditions."""
        # Conditions with highest weights (avoiding hierarchy conflicts)
        max_weight_df = pd.DataFrame({
            'hospitalization_id': ['MAX_SCORE'] * 4,
            'diagnosis_code': ['C78', 'I850', 'I50', 'F00'],  # Metastatic(6) + SevereLiver(4) + CHF(2) + Dementia(2) = 14
            'diagnosis_code_format': ['ICD10CM'] * 4
        })

        result = calculate_cci(max_weight_df)

        # Expected maximum realistic score
        assert result.iloc[0]['cci_score'] == 14
        assert result.iloc[0]['metastatic_solid_tumor'] == 1
        assert result.iloc[0]['moderate_severe_liver_disease'] == 1
        assert result.iloc[0]['congestive_heart_failure'] == 1
        assert result.iloc[0]['dementia'] == 1
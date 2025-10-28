"""
Tests for SAT and SBT flag identification functions.

This module tests the identify_sat_events and identify_sbt_events functions
with comprehensive coverage including:
- Basic functionality with minimal data
- Edge cases and error handling
- Different stability modes for SBT
- Respiratory and hemodynamic stability calculations
"""

import pytest
import pandas as pd
import numpy as np
from datetime import datetime, timedelta

from clifpy.utils.sat_sbt_flags import (
    identify_sat_events,
    identify_sbt_events,
    calculate_respiratory_stability,
    calculate_hemodynamic_stability
)


class TestIdentifySATEvents:
    """Test class for identify_sat_events function."""
    
    @pytest.fixture
    def basic_sat_data(self):
        """Create basic data for SAT testing."""
        # Create a scenario where patient meets SAT criteria for 4+ hours
        base_time = pd.Timestamp('2024-01-01 22:00:00')  # 10 PM
        
        data = []
        # Generate hourly records for 6 hours (10 PM to 4 AM next day)
        for i in range(7):
            data.append({
                'hospitalization_id': 'H001',
                'event_time': base_time + pd.Timedelta(hours=i),
                'device_category': 'imv',
                'location_category': 'icu',
                'min_sedation_dose_2': 5.0,  # Has sedation
                'max_paralytics': 0.0  # No paralytics
            })
        
        return pd.DataFrame(data)
    
    @pytest.fixture
    def no_sat_data(self):
        """Create data that should NOT trigger SAT (insufficient duration)."""
        base_time = pd.Timestamp('2024-01-01 22:00:00')
        
        data = []
        # Only 3 hours of meeting criteria (below 4-hour threshold)
        for i in range(4):
            data.append({
                'hospitalization_id': 'H001',
                'event_time': base_time + pd.Timedelta(hours=i),
                'device_category': 'imv',
                'location_category': 'icu',
                'min_sedation_dose_2': 5.0,
                'max_paralytics': 0.0
            })
        
        return pd.DataFrame(data)
    
    def test_sat_basic_identification(self, basic_sat_data):
        """Test basic SAT event identification."""
        result = identify_sat_events(basic_sat_data, show_progress=False)
        
        assert len(result) == 1
        assert result['hospitalization_id'].iloc[0] == 'H001'
        assert pd.notna(result['event_time_at_threshold'].iloc[0])
    
    def test_sat_no_event_insufficient_duration(self, no_sat_data):
        """Test that SAT is not identified when duration is insufficient."""
        result = identify_sat_events(no_sat_data, show_progress=False)
        
        assert len(result) == 0
    
    def test_sat_missing_columns(self):
        """Test that missing required columns raises error."""
        df = pd.DataFrame({
            'hospitalization_id': ['H001'],
            'event_time': [pd.Timestamp('2024-01-01')],
            # Missing required columns
        })
        
        with pytest.raises(ValueError, match="Missing required columns"):
            identify_sat_events(df, show_progress=False)
    
    def test_sat_with_paralytics(self, basic_sat_data):
        """Test that SAT is not identified when paralytics are present."""
        data = basic_sat_data.copy()
        data['max_paralytics'] = 1.0  # Add paralytics
        
        result = identify_sat_events(data, show_progress=False)
        
        assert len(result) == 0
    
    def test_sat_no_sedation(self, basic_sat_data):
        """Test that SAT is not identified when no sedation is present."""
        data = basic_sat_data.copy()
        data['min_sedation_dose_2'] = 0.0  # No sedation
        
        result = identify_sat_events(data, show_progress=False)
        
        assert len(result) == 0
    
    def test_sat_not_icu(self, basic_sat_data):
        """Test that SAT is not identified outside ICU."""
        data = basic_sat_data.copy()
        data['location_category'] = 'ward'
        
        result = identify_sat_events(data, show_progress=False)
        
        assert len(result) == 0
    
    def test_sat_not_imv(self, basic_sat_data):
        """Test that SAT is not identified without IMV."""
        data = basic_sat_data.copy()
        data['device_category'] = 'nippv'
        
        result = identify_sat_events(data, show_progress=False)
        
        assert len(result) == 0
    
    def test_sat_custom_threshold(self, basic_sat_data):
        """Test SAT with custom threshold."""
        # With 7 timestamps (6 hours duration), threshold of 5 hours should trigger
        result = identify_sat_events(basic_sat_data, threshold_hours=5.0, show_progress=False)
        
        assert len(result) == 1
        
        # With threshold of 7 hours, should not trigger (only 6 hours duration)
        result = identify_sat_events(basic_sat_data, threshold_hours=7.0, show_progress=False)
        
        assert len(result) == 0
    
    def test_sat_multiple_hospitalizations(self):
        """Test SAT identification across multiple hospitalizations."""
        base_time = pd.Timestamp('2024-01-01 22:00:00')
        
        data = []
        # H001 meets criteria - 6 timestamps = 5 hours duration, meets 4-hour threshold
        for i in range(6):
            data.append({
                'hospitalization_id': 'H001',
                'event_time': base_time + pd.Timedelta(hours=i),
                'device_category': 'imv',
                'location_category': 'icu',
                'min_sedation_dose_2': 5.0,
                'max_paralytics': 0.0
            })
        
        # H002 does not meet criteria (no sedation)
        for i in range(6):
            data.append({
                'hospitalization_id': 'H002',
                'event_time': base_time + pd.Timedelta(hours=i),
                'device_category': 'imv',
                'location_category': 'icu',
                'min_sedation_dose_2': 0.0,  # No sedation
                'max_paralytics': 0.0
            })
        
        df = pd.DataFrame(data)
        result = identify_sat_events(df, show_progress=False)
        
        assert len(result) == 1
        assert result['hospitalization_id'].iloc[0] == 'H001'
    
    def test_sat_custom_column_names(self, basic_sat_data):
        """Test SAT with custom column names."""
        data = basic_sat_data.copy()
        data = data.rename(columns={
            'event_time': 'timestamp',
            'hospitalization_id': 'encounter_id',
            'device_category': 'device',
            'location_category': 'location',
            'min_sedation_dose_2': 'sedation',
            'max_paralytics': 'paralytics'
        })
        
        result = identify_sat_events(
            data,
            time_column='timestamp',
            hospitalization_id_column='encounter_id',
            device_category_column='device',
            location_category_column='location',
            sedation_column='sedation',
            paralytics_column='paralytics',
            show_progress=False
        )
        
        assert len(result) == 1
        assert 'encounter_id' in result.columns


class TestIdentifySBTEvents:
    """Test class for identify_sbt_events function."""
    
    @pytest.fixture
    def basic_sbt_data(self):
        """Create basic data for SBT testing."""
        base_time = pd.Timestamp('2024-01-01 22:00:00')  # 10 PM
        
        data = []
        # Generate hourly records for 8 hours (10 PM to 6 AM next day)
        for i in range(9):
            data.append({
                'hospitalization_id': 'H001',
                'event_time': base_time + pd.Timedelta(hours=i),
                'device_category': 'imv',
                'location_category': 'icu',
                'max_paralytics': 0.0
            })
        
        return pd.DataFrame(data)
    
    def test_sbt_standard_mode(self, basic_sbt_data):
        """Test basic SBT identification in standard mode."""
        result = identify_sbt_events(
            basic_sbt_data,
            stability_mode='Standard',
            show_progress=False
        )
        
        assert 'eligible_day' in result.columns
        assert result['eligible_day'].sum() > 0
        assert result['vent_day'].sum() > 0
        assert result['vent_day_without_paralytics'].sum() > 0
    
    def test_sbt_with_paralytics(self, basic_sbt_data):
        """Test that SBT is not eligible when paralytics are present."""
        data = basic_sbt_data.copy()
        data['max_paralytics'] = 1.0
        
        result = identify_sbt_events(
            data,
            stability_mode='Standard',
            show_progress=False
        )
        
        # Should have vent_day marked, but eligible_day should be 0
        # Note: vent_day_without_paralytics may have some values due to day grouping,
        # but the key is that eligible_day should be 0
        assert result['vent_day'].sum() > 0
        assert result['eligible_day'].sum() == 0
    
    def test_sbt_respiratory_stability_mode(self, basic_sbt_data):
        """Test SBT with respiratory stability requirement."""
        data = basic_sbt_data.copy()
        data['Respiratory_Stability'] = 1
        
        result = identify_sbt_events(
            data,
            stability_mode='Respiratory_Stability',
            respiratory_stability_column='Respiratory_Stability',
            show_progress=False
        )
        
        assert result['eligible_day'].sum() > 0
    
    def test_sbt_respiratory_stability_not_met(self, basic_sbt_data):
        """Test SBT when respiratory stability is not met."""
        data = basic_sbt_data.copy()
        data['Respiratory_Stability'] = 0  # Not stable
        
        result = identify_sbt_events(
            data,
            stability_mode='Respiratory_Stability',
            respiratory_stability_column='Respiratory_Stability',
            show_progress=False
        )
        
        assert result['eligible_day'].sum() == 0
    
    def test_sbt_hemodynamic_stability_mode(self, basic_sbt_data):
        """Test SBT with hemodynamic stability requirement."""
        data = basic_sbt_data.copy()
        data['Hemodynamic_Stability_by_NEE'] = 1
        
        result = identify_sbt_events(
            data,
            stability_mode='Hemodynamic_Stability',
            hemodynamic_stability_column='Hemodynamic_Stability_by_NEE',
            show_progress=False
        )
        
        assert result['eligible_day'].sum() > 0
    
    def test_sbt_both_stabilities_mode(self, basic_sbt_data):
        """Test SBT with both stability requirements."""
        data = basic_sbt_data.copy()
        data['Respiratory_Stability'] = 1
        data['Hemodynamic_Stability_by_NEE'] = 1
        
        result = identify_sbt_events(
            data,
            stability_mode='Both_stabilities',
            respiratory_stability_column='Respiratory_Stability',
            hemodynamic_stability_column='Hemodynamic_Stability_by_NEE',
            show_progress=False
        )
        
        assert result['eligible_day'].sum() > 0
    
    def test_sbt_both_stabilities_partial(self, basic_sbt_data):
        """Test SBT when only one stability is met."""
        data = basic_sbt_data.copy()
        data['Respiratory_Stability'] = 1  # Met
        data['Hemodynamic_Stability_by_NEE'] = 0  # Not met
        
        result = identify_sbt_events(
            data,
            stability_mode='Both_stabilities',
            respiratory_stability_column='Respiratory_Stability',
            hemodynamic_stability_column='Hemodynamic_Stability_by_NEE',
            show_progress=False
        )
        
        assert result['eligible_day'].sum() == 0
    
    def test_sbt_invalid_stability_mode(self, basic_sbt_data):
        """Test that invalid stability mode raises error."""
        with pytest.raises(ValueError, match="Invalid stability_mode"):
            identify_sbt_events(
                basic_sbt_data,
                stability_mode='InvalidMode',
                show_progress=False
            )
    
    def test_sbt_missing_stability_column(self, basic_sbt_data):
        """Test that missing stability column raises error."""
        with pytest.raises(ValueError, match="required for stability_mode"):
            identify_sbt_events(
                basic_sbt_data,
                stability_mode='Respiratory_Stability',
                show_progress=False
            )
    
    def test_sbt_insufficient_duration(self):
        """Test SBT with insufficient duration."""
        base_time = pd.Timestamp('2024-01-01 22:00:00')
        
        data = []
        # Only 5 hours (below 6-hour threshold)
        for i in range(6):
            data.append({
                'hospitalization_id': 'H001',
                'event_time': base_time + pd.Timedelta(hours=i),
                'device_category': 'imv',
                'location_category': 'icu',
                'max_paralytics': 0.0
            })
        
        df = pd.DataFrame(data)
        result = identify_sbt_events(df, show_progress=False)
        
        # Should have vent days but not eligible
        assert result['vent_day'].sum() > 0
        assert result['eligible_day'].sum() == 0
    
    def test_sbt_custom_threshold(self, basic_sbt_data):
        """Test SBT with custom threshold."""
        # With 9 hours of data, threshold of 8 hours should trigger
        result = identify_sbt_events(
            basic_sbt_data,
            threshold_hours=8.0,
            show_progress=False
        )
        
        assert result['eligible_day'].sum() > 0
        
        # With threshold of 10 hours, should not trigger
        result = identify_sbt_events(
            basic_sbt_data,
            threshold_hours=10.0,
            show_progress=False
        )
        
        assert result['eligible_day'].sum() == 0
    
    def test_sbt_not_icu(self, basic_sbt_data):
        """Test SBT outside ICU."""
        data = basic_sbt_data.copy()
        data['location_category'] = 'ward'
        
        result = identify_sbt_events(data, show_progress=False)
        
        assert result['eligible_day'].sum() == 0


class TestCalculateRespiratoryStability:
    """Test class for calculate_respiratory_stability function."""
    
    @pytest.fixture
    def respiratory_data(self):
        """Create respiratory support data for testing."""
        return pd.DataFrame({
            'mode_category': ['Pressure Support', 'CPAP', 'Volume Control', 'Pressure Support'],
            'pressure_support_set': [5.0, 0.0, 15.0, 10.0],
            'peep_set': [5.0, 5.0, 10.0, 5.0],
            'fio2_set': [0.4, 0.3, 0.6, 0.45]
        })
    
    def test_respiratory_stability_calculation(self, respiratory_data):
        """Test basic respiratory stability calculation."""
        result = calculate_respiratory_stability(respiratory_data)
        
        assert 'Respiratory_Stability' in result.columns
        # First two should be stable, last two unstable
        assert result['Respiratory_Stability'].iloc[0] == 1
        assert result['Respiratory_Stability'].iloc[1] == 1
        assert result['Respiratory_Stability'].iloc[2] == 0  # Volume Control not PS/CPAP
        assert result['Respiratory_Stability'].iloc[3] == 0  # PS too high (>8)
    
    def test_respiratory_stability_custom_thresholds(self, respiratory_data):
        """Test respiratory stability with custom thresholds."""
        result = calculate_respiratory_stability(
            respiratory_data,
            ps_threshold=12.0,  # More lenient
            fio2_threshold=0.6
        )
        
        # More rows should be stable with higher thresholds
        assert result['Respiratory_Stability'].iloc[3] == 1  # PS 10 now stable
    
    def test_respiratory_stability_missing_values(self):
        """Test respiratory stability with missing values."""
        data = pd.DataFrame({
            'mode_category': ['Pressure Support', 'CPAP'],
            'pressure_support_set': [5.0, np.nan],
            'peep_set': [5.0, 5.0],
            'fio2_set': [0.4, 0.3]
        })
        
        result = calculate_respiratory_stability(data)
        
        # First should be stable
        assert result['Respiratory_Stability'].iloc[0] == 1
        # Second should be unstable due to missing PS
        assert result['Respiratory_Stability'].iloc[1] == 0


class TestCalculateHemodynamicStability:
    """Test class for calculate_hemodynamic_stability function."""
    
    @pytest.fixture
    def hemodynamic_data(self):
        """Create hemodynamic data for testing."""
        return pd.DataFrame({
            'norepinephrine': [0.05, 0.0, 0.15, 0.0],
            'epinephrine': [0.0, 0.05, 0.0, 0.0],
            'phenylephrine': [0.0, 0.0, 0.0, 100.0],
            'vasopressin': [0.0, 0.0, 0.04, 0.0],
            'dopamine': [0.0, 0.0, 0.0, 5.0]
        })
    
    def test_hemodynamic_stability_calculation(self, hemodynamic_data):
        """Test basic hemodynamic stability calculation."""
        result = calculate_hemodynamic_stability(hemodynamic_data)
        
        assert 'NEE' in result.columns
        assert 'Hemodynamic_Stability_by_NEE' in result.columns
        
        # First two should be stable (NEE <= 0.1)
        assert result['Hemodynamic_Stability_by_NEE'].iloc[0] == 1  # NEE = 0.05
        assert result['Hemodynamic_Stability_by_NEE'].iloc[1] == 1  # NEE = 0.05
        # Third should be unstable (NEE > 0.1)
        assert result['Hemodynamic_Stability_by_NEE'].iloc[2] == 0  # NEE = 0.25 (0.15 + 0.04*2.5)
    
    def test_hemodynamic_stability_nee_calculation(self, hemodynamic_data):
        """Test NEE calculation formula."""
        result = calculate_hemodynamic_stability(hemodynamic_data)
        
        # Row 0: 0.05 norepinephrine
        assert abs(result['NEE'].iloc[0] - 0.05) < 0.001
        
        # Row 1: 0.05 epinephrine
        assert abs(result['NEE'].iloc[1] - 0.05) < 0.001
        
        # Row 2: 0.15 norepi + 0.04*2.5 vaso = 0.25
        assert abs(result['NEE'].iloc[2] - 0.25) < 0.001
        
        # Row 3: 100/10 phenylephrine + 5/100 dopamine = 10.05
        assert abs(result['NEE'].iloc[3] - 10.05) < 0.001
    
    def test_hemodynamic_stability_custom_threshold(self, hemodynamic_data):
        """Test hemodynamic stability with custom threshold."""
        result = calculate_hemodynamic_stability(
            hemodynamic_data,
            nee_threshold=0.3  # More lenient
        )
        
        # More rows should be stable with higher threshold
        assert result['Hemodynamic_Stability_by_NEE'].iloc[2] == 1  # NEE 0.25 now stable
    
    def test_hemodynamic_stability_missing_values(self):
        """Test hemodynamic stability with missing values."""
        data = pd.DataFrame({
            'norepinephrine': [0.05, np.nan],
            'epinephrine': [np.nan, 0.05],
            'phenylephrine': [np.nan, np.nan],
            'vasopressin': [np.nan, np.nan],
            'dopamine': [np.nan, np.nan]
        })
        
        result = calculate_hemodynamic_stability(data)
        
        # Missing values should be treated as 0
        assert abs(result['NEE'].iloc[0] - 0.05) < 0.001
        assert abs(result['NEE'].iloc[1] - 0.05) < 0.001
        assert result['Hemodynamic_Stability_by_NEE'].iloc[0] == 1
        assert result['Hemodynamic_Stability_by_NEE'].iloc[1] == 1
    
    def test_hemodynamic_stability_all_zeros(self):
        """Test hemodynamic stability with no vasopressors."""
        data = pd.DataFrame({
            'norepinephrine': [0.0],
            'epinephrine': [0.0],
            'phenylephrine': [0.0],
            'vasopressin': [0.0],
            'dopamine': [0.0]
        })
        
        result = calculate_hemodynamic_stability(data)
        
        assert result['NEE'].iloc[0] == 0.0
        assert result['Hemodynamic_Stability_by_NEE'].iloc[0] == 1


class TestIntegration:
    """Integration tests combining multiple functions."""
    
    def test_full_sbt_pipeline(self):
        """Test full SBT pipeline with stability calculations."""
        base_time = pd.Timestamp('2024-01-01 22:00:00')
        
        # Create comprehensive data
        data = []
        for i in range(9):
            data.append({
                'hospitalization_id': 'H001',
                'event_time': base_time + pd.Timedelta(hours=i),
                'device_category': 'imv',
                'location_category': 'icu',
                'max_paralytics': 0.0,
                'mode_category': 'Pressure Support',
                'pressure_support_set': 5.0,
                'peep_set': 5.0,
                'fio2_set': 0.4,
                'norepinephrine': 0.05,
                'epinephrine': 0.0,
                'phenylephrine': 0.0,
                'vasopressin': 0.0,
                'dopamine': 0.0
            })
        
        df = pd.DataFrame(data)
        
        # Calculate stability flags
        df = calculate_respiratory_stability(df)
        df = calculate_hemodynamic_stability(df)
        
        # Identify SBT events with both stabilities
        result = identify_sbt_events(
            df,
            stability_mode='Both_stabilities',
            show_progress=False
        )
        
        assert result['eligible_day'].sum() > 0
        assert result['vent_day'].sum() > 0
        assert 'IMV_Controlled_met_time' in result.columns

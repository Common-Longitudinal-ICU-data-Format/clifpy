"""
Tests for IMV episode detection functionality.
"""

import pytest
import pandas as pd
import numpy as np
from datetime import datetime, timedelta

from clifpy.utils.imv_episodes import (
    detect_imv_episodes,
    calculate_ventilator_free_days,
    IMV_OBS_COLUMNS,
    NON_IMV_DEVICE_CATEGORIES,
)


@pytest.fixture
def sample_respiratory_data():
    """Create sample respiratory support data with known IMV episodes."""
    base_time = datetime(2024, 1, 1, 8, 0, 0)
    
    data = []
    
    # Episode 1: IMV from hour 0-6
    for hour in range(7):
        data.append({
            "hospitalization_id": "H001",
            "recorded_dttm": base_time + timedelta(hours=hour),
            "device_category": "IMV",
            "mode_category": "Assist Control-Volume Control",
            "lpm_set": None,
            "tidal_volume_obs": 450 + np.random.randint(-20, 20),
            "peep_obs": 5.0,
            "resp_rate_obs": 14,
            "tracheostomy": 0,
        })
    
    # Extubation: Non-IMV documentation at hour 7
    data.append({
        "hospitalization_id": "H001",
        "recorded_dttm": base_time + timedelta(hours=7),
        "device_category": "High Flow NC",
        "mode_category": "Other",
        "lpm_set": 40.0,
        "tidal_volume_obs": None,
        "peep_obs": None,
        "resp_rate_obs": 18,
        "tracheostomy": 0,
    })
    
    # Continue on high flow for a few hours
    for hour in range(8, 12):
        data.append({
            "hospitalization_id": "H001",
            "recorded_dttm": base_time + timedelta(hours=hour),
            "device_category": "High Flow NC",
            "mode_category": "Other",
            "lpm_set": 40.0,
            "tidal_volume_obs": None,
            "peep_obs": None,
            "resp_rate_obs": 16 + np.random.randint(-2, 2),
            "tracheostomy": 0,
        })
    
    # Episode 2: Re-intubation at hour 12
    for hour in range(12, 20):
        data.append({
            "hospitalization_id": "H001",
            "recorded_dttm": base_time + timedelta(hours=hour),
            "device_category": "IMV",
            "mode_category": "Pressure Control",
            "lpm_set": None,
            "tidal_volume_obs": 420 + np.random.randint(-20, 20),
            "peep_obs": 8.0,
            "resp_rate_obs": 16,
            "tracheostomy": 1 if hour >= 18 else 0,  # Trach placed at hour 18
        })
    
    return pd.DataFrame(data)


@pytest.fixture
def sample_hospitalization_data():
    """Create sample hospitalization data."""
    return pd.DataFrame([
        {
            "hospitalization_id": "H001",
            "patient_id": "P001",
            "admission_dttm": datetime(2024, 1, 1, 6, 0, 0),
            "discharge_dttm": datetime(2024, 1, 5, 12, 0, 0),
            "discharge_category": "Home",
        },
        {
            "hospitalization_id": "H002",
            "patient_id": "P002",
            "admission_dttm": datetime(2024, 1, 2, 10, 0, 0),
            "discharge_dttm": datetime(2024, 1, 4, 8, 0, 0),
            "discharge_category": "Expired",
        },
    ])


class TestDetectIMVEpisodes:
    """Tests for detect_imv_episodes function."""
    
    def test_basic_episode_detection(self, sample_respiratory_data):
        """Test that basic IMV episodes are detected correctly."""
        episodes = detect_imv_episodes(sample_respiratory_data)
        
        assert len(episodes) == 2, "Should detect 2 IMV episodes"
        assert episodes.iloc[0]["imv_episode_id"] == 1
        assert episodes.iloc[1]["imv_episode_id"] == 2
    
    def test_episode_start_times(self, sample_respiratory_data):
        """Test that episode start times are correct."""
        episodes = detect_imv_episodes(sample_respiratory_data)
        
        base_time = datetime(2024, 1, 1, 8, 0, 0)
        
        # Episode 1 starts at hour 0
        assert episodes.iloc[0]["imv_start_dttm"] == base_time
        
        # Episode 2 starts at hour 12
        assert episodes.iloc[1]["imv_start_dttm"] == base_time + timedelta(hours=12)
    
    def test_episode_end_times(self, sample_respiratory_data):
        """Test that episode end times are correct."""
        episodes = detect_imv_episodes(sample_respiratory_data)
        
        base_time = datetime(2024, 1, 1, 8, 0, 0)
        
        # Episode 1 ends at hour 6 (last IMV obs before HFNC at hour 7)
        assert episodes.iloc[0]["imv_end_dttm"] == base_time + timedelta(hours=6)
        
        # Episode 2 ends at hour 19 (last observation)
        assert episodes.iloc[1]["imv_end_dttm"] == base_time + timedelta(hours=19)
    
    def test_duration_calculation(self, sample_respiratory_data):
        """Test that episode durations are calculated correctly."""
        episodes = detect_imv_episodes(sample_respiratory_data)
        
        # Episode 1: 6 hours (hour 0 to hour 6)
        assert episodes.iloc[0]["duration_hours"] == 6.0
        
        # Episode 2: 7 hours (hour 12 to hour 19)
        assert episodes.iloc[1]["duration_hours"] == 7.0
    
    def test_tracheostomy_tracking(self, sample_respiratory_data):
        """Test that tracheostomy is tracked during episodes."""
        episodes = detect_imv_episodes(sample_respiratory_data, include_tracheostomy=True)
        
        # Episode 1: No trach
        assert episodes.iloc[0]["has_tracheostomy"] == 0
        
        # Episode 2: Trach placed during episode
        assert episodes.iloc[1]["has_tracheostomy"] == 1
    
    def test_missing_columns_raises_error(self):
        """Test that missing required columns raises ValueError."""
        df = pd.DataFrame({"hospitalization_id": ["H001"], "recorded_dttm": [datetime.now()]})
        
        with pytest.raises(ValueError, match="Missing required columns"):
            detect_imv_episodes(df)
    
    def test_no_obs_columns_raises_error(self):
        """Test that missing all *_obs columns raises ValueError."""
        df = pd.DataFrame({
            "hospitalization_id": ["H001"],
            "recorded_dttm": [datetime.now()],
            "device_category": ["IMV"],
            "lpm_set": [None],
        })
        
        with pytest.raises(ValueError, match="observation column required"):
            detect_imv_episodes(df)
    
    def test_empty_result_for_no_imv(self):
        """Test that empty DataFrame is returned when no IMV data exists."""
        df = pd.DataFrame({
            "hospitalization_id": ["H001", "H001"],
            "recorded_dttm": [datetime.now(), datetime.now() + timedelta(hours=1)],
            "device_category": ["High Flow NC", "Nasal Cannula"],
            "lpm_set": [40.0, 4.0],
            "tidal_volume_obs": [None, None],
        })
        
        episodes = detect_imv_episodes(df)
        assert len(episodes) == 0
        assert "hospitalization_id" in episodes.columns
    
    def test_min_gap_hours_parameter(self):
        """Test that min_gap_hours correctly splits episodes."""
        base_time = datetime(2024, 1, 1, 8, 0, 0)
        
        # Create data with 3-hour gap between IMV observations
        df = pd.DataFrame({
            "hospitalization_id": ["H001", "H001"],
            "recorded_dttm": [base_time, base_time + timedelta(hours=3)],
            "device_category": ["IMV", "IMV"],
            "lpm_set": [None, None],
            "tidal_volume_obs": [450, 460],
        })
        
        # With 2-hour min gap, should be 2 episodes
        episodes_2hr = detect_imv_episodes(df, min_gap_hours=2.0)
        assert len(episodes_2hr) == 2
        
        # With 4-hour min gap, should be 1 episode
        episodes_4hr = detect_imv_episodes(df, min_gap_hours=4.0)
        assert len(episodes_4hr) == 1


class TestCalculateVentilatorFreeDays:
    """Tests for calculate_ventilator_free_days function."""
    
    def test_basic_vfd_calculation(self, sample_respiratory_data, sample_hospitalization_data):
        """Test basic VFD calculation."""
        episodes = detect_imv_episodes(sample_respiratory_data)
        vfds = calculate_ventilator_free_days(episodes, sample_hospitalization_data)
        
        # H001 should have VFDs (not dead)
        h001_vfd = vfds[vfds["hospitalization_id"] == "H001"].iloc[0]
        assert h001_vfd["ventilator_free_days"] > 0
        assert h001_vfd["died"] == 0
    
    def test_vfd_zero_for_death(self, sample_hospitalization_data):
        """Test that VFD is 0 for patients who died."""
        # Empty episodes
        episodes = pd.DataFrame(columns=["hospitalization_id", "imv_episode_id", 
                                          "imv_start_dttm", "imv_end_dttm", "duration_hours"])
        
        vfds = calculate_ventilator_free_days(episodes, sample_hospitalization_data)
        
        # H002 died, so VFD should be 0
        h002_vfd = vfds[vfds["hospitalization_id"] == "H002"].iloc[0]
        assert h002_vfd["ventilator_free_days"] == 0
        assert h002_vfd["died"] == 1
    
    def test_vfd_capped_at_observation_window(self, sample_hospitalization_data):
        """Test that IMV days are capped at observation window."""
        # Create episode with 30 days of IMV
        episodes = pd.DataFrame({
            "hospitalization_id": ["H001"],
            "imv_episode_id": [1],
            "imv_start_dttm": [datetime(2024, 1, 1)],
            "imv_end_dttm": [datetime(2024, 1, 31)],
            "duration_hours": [30 * 24],  # 30 days
        })
        
        vfds = calculate_ventilator_free_days(
            episodes, 
            sample_hospitalization_data,
            observation_window_days=28
        )
        
        h001_vfd = vfds[vfds["hospitalization_id"] == "H001"].iloc[0]
        assert h001_vfd["total_imv_days"] == 28.0  # Capped at 28
        assert h001_vfd["ventilator_free_days"] == 0.0


class TestConstants:
    """Tests for module constants."""
    
    def test_imv_obs_columns_excludes_resp_rate(self):
        """Verify resp_rate_obs is not in IMV_OBS_COLUMNS."""
        assert "resp_rate_obs" not in IMV_OBS_COLUMNS
    
    def test_non_imv_device_categories(self):
        """Verify non-IMV categories are complete."""
        expected = {"NIPPV", "CPAP", "High Flow NC", "Face Mask", 
                    "Trach Collar", "Nasal Cannula", "Room Air", "Other"}
        assert set(NON_IMV_DEVICE_CATEGORIES) == expected

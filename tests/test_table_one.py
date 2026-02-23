"""Tests for Table One generator with privacy protection."""

import pytest
import pandas as pd
import numpy as np
import json
import tempfile
from pathlib import Path

from clifpy.utils.table_one import (
    TableOneGenerator,
    PrivacyConfig,
    VariableConfig,
    generate_table_one,
    DEFAULT_CLIF_VARIABLES,
)


@pytest.fixture
def sample_cohort():
    """Create a sample cohort DataFrame for testing."""
    np.random.seed(42)
    n = 100
    return pd.DataFrame({
        "hospitalization_id": range(n),
        "age_at_admission": np.random.normal(65, 15, n),
        "sex_category": np.random.choice(["Male", "Female"], n),
        "race_category": np.random.choice(
            ["White", "Black", "Asian", "Other", None], n, p=[0.6, 0.2, 0.1, 0.05, 0.05]
        ),
        "ethnicity_category": np.random.choice(
            ["Non-Hispanic", "Hispanic", None], n, p=[0.8, 0.15, 0.05]
        ),
        "icu_los_hours": np.random.exponential(48, n),
        "hospital_los_days": np.random.exponential(7, n),
        "hospital_mortality": np.random.choice([0, 1], n, p=[0.85, 0.15]),
        "received_imv": np.random.choice([0, 1], n, p=[0.7, 0.3]),
        "received_vasopressors": np.random.choice([0, 1], n, p=[0.6, 0.4]),
        "received_crrt": np.random.choice([0, 1], n, p=[0.95, 0.05]),
    })


@pytest.fixture
def small_cohort():
    """Create a small cohort to test privacy suppression."""
    return pd.DataFrame({
        "hospitalization_id": range(15),
        "age_at_admission": [60, 65, 70, 55, 80, 45, 72, 68, 63, 58, 75, 82, 67, 71, 59],
        "sex_category": ["Male"] * 12 + ["Female"] * 3,  # Female count < 10
        "rare_category": ["Common"] * 14 + ["Rare"],  # Rare count = 1
        "hospital_mortality": [0] * 14 + [1],  # Mortality = 1
    })


class TestPrivacyConfig:
    """Tests for PrivacyConfig."""

    def test_default_config(self):
        config = PrivacyConfig()
        assert config.min_cell_size == 10
        assert config.round_counts_to is None
        assert config.suppress_percentages is True
        assert config.suppression_label == "<10"

    def test_custom_config(self):
        config = PrivacyConfig(min_cell_size=5, round_counts_to=5, suppression_label="<5")
        assert config.min_cell_size == 5
        assert config.round_counts_to == 5
        assert config.suppression_label == "<5"

    def test_invalid_min_cell_size(self):
        with pytest.raises(ValueError, match="min_cell_size must be at least 1"):
            PrivacyConfig(min_cell_size=0)

    def test_invalid_round_counts_to(self):
        with pytest.raises(ValueError, match="round_counts_to must be at least 1"):
            PrivacyConfig(round_counts_to=0)


class TestTableOneGenerator:
    """Tests for TableOneGenerator."""

    def test_basic_generation(self, sample_cohort):
        generator = TableOneGenerator(sample_cohort, site_name="TEST")
        result = generator.generate()

        assert isinstance(result, pd.DataFrame)
        assert len(result) > 0
        assert "variable" in result.columns
        assert "n" in result.columns

    def test_total_n_included(self, sample_cohort):
        generator = TableOneGenerator(sample_cohort)
        result = generator.generate()

        total_row = result[result["variable"] == "Total N"]
        assert len(total_row) == 1
        assert total_row.iloc[0]["n"] == 100

    def test_privacy_suppression(self, small_cohort):
        """Test that small counts are suppressed."""
        privacy = PrivacyConfig(min_cell_size=10)
        variables = [
            VariableConfig("sex_category", "Sex", "categorical"),
        ]
        generator = TableOneGenerator(
            small_cohort, variables=variables, privacy_config=privacy
        )
        result = generator.generate()

        # Female count (3) should be suppressed
        female_row = result[
            (result["variable"] == "Sex") & (result["category"] == "Female")
        ]
        assert female_row.iloc[0]["n"] == "<10"

        # Male count (12) should not be suppressed
        male_row = result[
            (result["variable"] == "Sex") & (result["category"] == "Male")
        ]
        assert male_row.iloc[0]["n"] == 12

    def test_count_rounding(self, sample_cohort):
        """Test count rounding for privacy."""
        privacy = PrivacyConfig(round_counts_to=5)
        generator = TableOneGenerator(
            sample_cohort, privacy_config=privacy, site_name="TEST"
        )
        result = generator.generate()

        # All counts should be divisible by 5
        for _, row in result.iterrows():
            n = row.get("n")
            if isinstance(n, (int, float)) and not isinstance(n, str):
                assert n % 5 == 0, f"Count {n} not rounded to nearest 5"

    def test_continuous_variable_summary(self, sample_cohort):
        """Test continuous variable summarization."""
        variables = [VariableConfig("age_at_admission", "Age", "continuous")]
        generator = TableOneGenerator(sample_cohort, variables=variables)
        result = generator.generate()

        age_row = result[result["variable"] == "Age"].iloc[0]
        assert "median" in age_row
        assert "q1" in age_row
        assert "q3" in age_row
        assert isinstance(age_row["median"], (int, float))

    def test_binary_variable_summary(self, sample_cohort):
        """Test binary variable summarization."""
        variables = [VariableConfig("hospital_mortality", "Mortality", "binary")]
        generator = TableOneGenerator(sample_cohort, variables=variables)
        result = generator.generate()

        yes_row = result[
            (result["variable"] == "Mortality") & (result["category"] == "Yes")
        ]
        no_row = result[
            (result["variable"] == "Mortality") & (result["category"] == "No")
        ]
        assert len(yes_row) == 1
        assert len(no_row) == 1

    def test_missing_variable_handled(self, sample_cohort):
        """Test that missing variables in data are skipped gracefully."""
        variables = [
            VariableConfig("age_at_admission", "Age", "continuous"),
            VariableConfig("nonexistent_column", "Missing", "continuous"),
        ]
        generator = TableOneGenerator(sample_cohort, variables=variables)
        result = generator.generate()

        # Should only have Total N and Age rows
        variables_in_result = result["variable"].unique()
        assert "Missing" not in variables_in_result
        assert "Age" in variables_in_result

    def test_metadata_generation(self, sample_cohort):
        """Test metadata is properly generated."""
        generator = TableOneGenerator(
            sample_cohort,
            site_name="UCMC",
            cohort_description="Test cohort",
        )
        generator.generate()
        metadata = generator.get_metadata()

        assert metadata["site_name"] == "UCMC"
        assert metadata["cohort_description"] == "Test cohort"
        assert "generated_at" in metadata
        assert "privacy_config" in metadata

    def test_csv_export(self, sample_cohort, tmp_path):
        """Test CSV export functionality."""
        generator = TableOneGenerator(sample_cohort, site_name="TEST")
        generator.generate()

        output_path = tmp_path / "table_one.csv"
        generator.to_csv(str(output_path))

        assert output_path.exists()
        # Read back and verify
        df = pd.read_csv(output_path, comment="#")
        assert len(df) > 0

    def test_json_export(self, sample_cohort, tmp_path):
        """Test JSON export functionality."""
        generator = TableOneGenerator(sample_cohort, site_name="TEST")
        generator.generate()

        output_path = tmp_path / "table_one.json"
        generator.to_json(str(output_path))

        assert output_path.exists()
        with open(output_path) as f:
            data = json.load(f)
        assert "metadata" in data
        assert "table_one" in data

    def test_to_dict(self, sample_cohort):
        """Test dictionary export."""
        generator = TableOneGenerator(sample_cohort)
        generator.generate()
        result = generator.to_dict()

        assert "metadata" in result
        assert "table_one" in result
        assert isinstance(result["table_one"], list)


class TestConvenienceFunction:
    """Tests for generate_table_one convenience function."""

    def test_basic_usage(self, sample_cohort):
        result = generate_table_one(sample_cohort, site_name="TEST")
        assert isinstance(result, pd.DataFrame)
        assert len(result) > 0

    def test_with_privacy_params(self, sample_cohort):
        result = generate_table_one(
            sample_cohort,
            min_cell_size=5,
            round_counts_to=10,
            site_name="TEST",
        )
        assert isinstance(result, pd.DataFrame)


class TestDefaultVariables:
    """Tests for default CLIF variables."""

    def test_default_variables_defined(self):
        assert len(DEFAULT_CLIF_VARIABLES) > 0
        assert all(isinstance(v, VariableConfig) for v in DEFAULT_CLIF_VARIABLES)

    def test_default_variables_have_required_fields(self):
        for var in DEFAULT_CLIF_VARIABLES:
            assert var.name is not None
            assert var.label is not None
            assert var.var_type in ["continuous", "categorical", "binary"]

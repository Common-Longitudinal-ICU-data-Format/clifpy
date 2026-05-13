"""
Tests for clifpy.utils.table_one – generate_table_one function.
"""
import pytest
import pandas as pd
import numpy as np

from clifpy.utils.table_one import generate_table_one


# ---------------------------------------------------------------------------
# Shared fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def sample_patient_df():
    return pd.DataFrame({
        "patient_id": ["P001", "P002", "P003", "P004"],
        "sex_category": ["Male", "Female", "Male", "Female"],
        "race_category": ["White", "Black or African American", "White", "Other"],
        "ethnicity_category": ["Non-Hispanic", "Non-Hispanic", "Hispanic", "Non-Hispanic"],
        "language_category": ["English", "Spanish", "English", "English"],
    })


@pytest.fixture
def sample_hosp_df():
    return pd.DataFrame({
        "patient_id": ["P001", "P001", "P002", "P003", "P004"],
        "hospitalization_id": ["H001", "H002", "H003", "H004", "H005"],
        "admission_dttm": pd.to_datetime([
            "2023-01-01 10:00:00",
            "2023-06-01 08:00:00",
            "2023-02-01 14:30:00",
            "2023-03-01 08:15:00",
            "2023-04-01 12:00:00",
        ]).tz_localize("UTC"),
        "discharge_dttm": pd.to_datetime([
            "2023-01-05 16:00:00",
            "2023-06-04 10:00:00",
            "2023-02-08 12:00:00",
            "2023-03-10 09:30:00",
            "2023-04-03 08:00:00",
        ]).tz_localize("UTC"),
        "age_at_admission": [65, 67, 45, 72, 55],
        "admission_type_category": ["Emergency", "Emergency", "Elective", "Urgent", "Emergency"],
        "discharge_category": ["Home", "Expired", "Home", "Skilled Nursing Facility (SNF)", "Home"],
    })


# ---------------------------------------------------------------------------
# Basic structure tests
# ---------------------------------------------------------------------------

class TestGenerateTableOneBasic:

    def test_returns_dataframe(self, sample_patient_df, sample_hosp_df):
        result = generate_table_one(sample_patient_df, sample_hosp_df)
        assert isinstance(result, pd.DataFrame)

    def test_has_expected_columns(self, sample_patient_df, sample_hosp_df):
        result = generate_table_one(sample_patient_df, sample_hosp_df)
        assert list(result.columns) == ["Variable", "Value"]

    def test_non_empty(self, sample_patient_df, sample_hosp_df):
        result = generate_table_one(sample_patient_df, sample_hosp_df)
        assert len(result) > 0

    def test_n_patients_row(self, sample_patient_df, sample_hosp_df):
        result = generate_table_one(sample_patient_df, sample_hosp_df)
        row = result[result["Variable"] == "N (patients)"]
        assert len(row) == 1
        assert row.iloc[0]["Value"] == "4"

    def test_n_hospitalizations_row(self, sample_patient_df, sample_hosp_df):
        result = generate_table_one(sample_patient_df, sample_hosp_df)
        row = result[result["Variable"] == "N (hospitalizations)"]
        assert len(row) == 1
        assert row.iloc[0]["Value"] == "5"


# ---------------------------------------------------------------------------
# Continuous variable formatting
# ---------------------------------------------------------------------------

class TestContinuousFormat:

    def test_default_format_is_median_iqr(self, sample_patient_df, sample_hosp_df):
        result = generate_table_one(sample_patient_df, sample_hosp_df)
        age_row = result[result["Variable"].str.startswith("Age at admission")]
        assert len(age_row) == 1
        assert "(median [IQR])" in age_row.iloc[0]["Variable"]

    def test_mean_sd_format(self, sample_patient_df, sample_hosp_df):
        result = generate_table_one(
            sample_patient_df, sample_hosp_df, continuous_format="mean_sd"
        )
        age_row = result[result["Variable"].str.startswith("Age at admission")]
        assert len(age_row) == 1
        assert "(mean ± SD)" in age_row.iloc[0]["Variable"]
        assert "±" in age_row.iloc[0]["Value"]

    def test_invalid_continuous_format_raises(self, sample_patient_df, sample_hosp_df):
        with pytest.raises(ValueError, match="continuous_format"):
            generate_table_one(
                sample_patient_df, sample_hosp_df, continuous_format="bad_format"
            )

    def test_los_computed_from_datetimes(self, sample_patient_df, sample_hosp_df):
        result = generate_table_one(sample_patient_df, sample_hosp_df)
        los_row = result[result["Variable"].str.startswith("Length of stay")]
        assert len(los_row) == 1
        # Value should contain a numeric value
        assert any(c.isdigit() for c in los_row.iloc[0]["Value"])


# ---------------------------------------------------------------------------
# Categorical variable tests
# ---------------------------------------------------------------------------

class TestCategoricalVariables:

    def test_sex_categories_present(self, sample_patient_df, sample_hosp_df):
        result = generate_table_one(sample_patient_df, sample_hosp_df)
        assert any(result["Variable"] == "Sex")
        assert any(result["Variable"] == "  Male")
        assert any(result["Variable"] == "  Female")

    def test_sex_counts_correct(self, sample_patient_df, sample_hosp_df):
        result = generate_table_one(sample_patient_df, sample_hosp_df)
        male_row = result[result["Variable"] == "  Male"]
        assert len(male_row) == 1
        # 2 males out of 4 patients → 50.0 %
        assert "2" in male_row.iloc[0]["Value"]
        assert "50.0%" in male_row.iloc[0]["Value"]

    def test_mortality_row(self, sample_patient_df, sample_hosp_df):
        result = generate_table_one(sample_patient_df, sample_hosp_df)
        mort_row = result[result["Variable"] == "In-hospital mortality"]
        assert len(mort_row) == 1
        # 1 expired out of 5 hospitalizations → 20.0 %
        assert "1" in mort_row.iloc[0]["Value"]
        assert "20.0%" in mort_row.iloc[0]["Value"]

    def test_discharge_category_header_row(self, sample_patient_df, sample_hosp_df):
        result = generate_table_one(sample_patient_df, sample_hosp_df)
        assert any(result["Variable"] == "Discharge disposition")

    def test_admission_type_header_row(self, sample_patient_df, sample_hosp_df):
        result = generate_table_one(sample_patient_df, sample_hosp_df)
        assert any(result["Variable"] == "Admission type")


# ---------------------------------------------------------------------------
# include_vars filtering
# ---------------------------------------------------------------------------

class TestIncludeVars:

    def test_include_vars_restricts_output(self, sample_patient_df, sample_hosp_df):
        result = generate_table_one(
            sample_patient_df, sample_hosp_df,
            include_vars=["n_patients", "sex"]
        )
        # Should have 'N (patients)' + header + 2 sex rows
        assert any(result["Variable"] == "N (patients)")
        assert any(result["Variable"] == "Sex")
        # Should NOT have hospitalisations count
        assert not any(result["Variable"] == "N (hospitalizations)")

    def test_include_vars_invalid_raises(self, sample_patient_df, sample_hosp_df):
        with pytest.raises(ValueError, match="Unknown variable"):
            generate_table_one(
                sample_patient_df, sample_hosp_df,
                include_vars=["n_patients", "nonexistent_var"]
            )

    def test_include_vars_case_insensitive(self, sample_patient_df, sample_hosp_df):
        # Should not raise; comparison is normalized to lower-case
        result = generate_table_one(
            sample_patient_df, sample_hosp_df,
            include_vars=["N_PATIENTS", "SEX"]
        )
        assert any(result["Variable"] == "N (patients)")


# ---------------------------------------------------------------------------
# Cohort filtering
# ---------------------------------------------------------------------------

class TestCohortFilter:

    def test_cohort_by_hospitalization_id(self, sample_patient_df, sample_hosp_df):
        cohort = pd.DataFrame({"hospitalization_id": ["H001", "H002"]})
        result = generate_table_one(
            sample_patient_df, sample_hosp_df,
            cohort=cohort,
            include_vars=["n_hospitalizations", "n_patients"]
        )
        n_hosp_row = result[result["Variable"] == "N (hospitalizations)"]
        assert n_hosp_row.iloc[0]["Value"] == "2"
        # Both H001 and H002 belong to P001 so n_patients should be 1

        n_pts_row = result[result["Variable"] == "N (patients)"]
        assert n_pts_row.iloc[0]["Value"] == "1"

    def test_cohort_by_patient_id(self, sample_patient_df, sample_hosp_df):
        cohort = pd.DataFrame({"patient_id": ["P002"]})
        result = generate_table_one(
            sample_patient_df, sample_hosp_df,
            cohort=cohort,
            include_vars=["n_hospitalizations", "n_patients"]
        )
        # P002 has 1 hospitalisation
        n_hosp_row = result[result["Variable"] == "N (hospitalizations)"]
        assert n_hosp_row.iloc[0]["Value"] == "1"

    def test_cohort_non_dataframe_raises(self, sample_patient_df, sample_hosp_df):
        with pytest.raises(TypeError, match="cohort must be a pandas DataFrame"):
            generate_table_one(sample_patient_df, sample_hosp_df, cohort=["H001"])


# ---------------------------------------------------------------------------
# Table object inputs
# ---------------------------------------------------------------------------

class TestTableObjectInputs:
    """Verify the function works when CLIF table objects (with .df attribute) are passed."""

    def _make_mock(self, df):
        class MockTable:
            pass
        obj = MockTable()
        obj.df = df
        return obj

    def test_patient_object_accepted(self, sample_patient_df, sample_hosp_df):
        patient_obj = self._make_mock(sample_patient_df)
        result = generate_table_one(patient_obj, sample_hosp_df)
        assert isinstance(result, pd.DataFrame)

    def test_hosp_object_accepted(self, sample_patient_df, sample_hosp_df):
        hosp_obj = self._make_mock(sample_hosp_df)
        result = generate_table_one(sample_patient_df, hosp_obj)
        assert isinstance(result, pd.DataFrame)

    def test_invalid_type_raises(self, sample_hosp_df):
        with pytest.raises(TypeError, match="Expected a pandas DataFrame"):
            generate_table_one("not_a_df", sample_hosp_df)


# ---------------------------------------------------------------------------
# Edge cases
# ---------------------------------------------------------------------------

class TestEdgeCases:

    def test_empty_patient_df(self, sample_hosp_df):
        empty_patient = pd.DataFrame(columns=["patient_id", "sex_category", "race_category",
                                               "ethnicity_category"])
        result = generate_table_one(empty_patient, sample_hosp_df,
                                    include_vars=["n_patients", "sex"])
        n_row = result[result["Variable"] == "N (patients)"]
        assert n_row.iloc[0]["Value"] == "0"

    def test_missing_optional_columns_skipped(self, sample_patient_df, sample_hosp_df):
        # Drop 'language_category' – 'language' var should simply be absent
        patient_no_lang = sample_patient_df.drop(columns=["language_category"])
        result = generate_table_one(
            patient_no_lang, sample_hosp_df,
            include_vars=["n_patients", "language"]
        )
        assert not any(result["Variable"] == "Language")

    def test_demo_data_integration(self):
        """Smoke test: run against actual demo data."""
        from clifpy.data import load_demo_patient, load_demo_hospitalization
        patient = load_demo_patient()
        hosp = load_demo_hospitalization()
        result = generate_table_one(patient, hosp)
        assert isinstance(result, pd.DataFrame)
        assert len(result) > 5
        assert list(result.columns) == ["Variable", "Value"]

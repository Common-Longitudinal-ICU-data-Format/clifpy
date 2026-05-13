"""
Table One generation utility for CLIF data.

This module provides the ``generate_table_one`` function, which produces a
standard "Table 1" summary DataFrame for a patient cohort—typically used in
clinical manuscripts.  The table combines demographics from the *patient* table
with clinical characteristics from the *hospitalization* table.
"""

from __future__ import annotations

from typing import List, Optional, Union

import numpy as np
import pandas as pd


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def generate_table_one(
    patient: Union["Patient", pd.DataFrame],  # noqa: F821
    hospitalization: Union["Hospitalization", pd.DataFrame],  # noqa: F821
    cohort: Optional[pd.DataFrame] = None,
    continuous_format: str = "median_iqr",
    include_vars: Optional[List[str]] = None,
) -> pd.DataFrame:
    """Generate a standard "Table 1" summary for a patient cohort.

    The function is designed to be run *after* cohort identification.  It
    combines data from the CLIF *patient* and *hospitalization* tables and
    returns a tidy ``pandas.DataFrame`` that is ready to be exported (e.g. to
    CSV or a Word table for a manuscript).

    Parameters
    ----------
    patient:
        A :class:`~clifpy.tables.patient.Patient` object **or** a plain
        ``pandas.DataFrame`` containing patient-level data.  Expected columns:
        ``patient_id``, ``sex_category``, ``race_category``,
        ``ethnicity_category``.  Additional columns (``language_category``) are
        included when present.
    hospitalization:
        A :class:`~clifpy.tables.hospitalization.Hospitalization` object **or**
        a plain ``pandas.DataFrame`` containing encounter-level data.  Expected
        columns: ``patient_id``, ``hospitalization_id``, ``admission_dttm``,
        ``discharge_dttm``, ``age_at_admission``, ``discharge_category``,
        ``admission_type_category``.
    cohort:
        Optional ``pandas.DataFrame`` used to restrict the analysis to a
        specific cohort.  Must contain at least one of ``hospitalization_id``
        or ``patient_id``.  When provided, only encounters (or patients) whose
        identifiers appear in ``cohort`` are included.
    continuous_format:
        How to summarise continuous variables.  One of:

        * ``"median_iqr"`` *(default)* – "median [Q1–Q3]"
        * ``"mean_sd"`` – "mean ± SD"
    include_vars:
        Optional list of variable names to include.  When ``None`` all
        available variables are included.  Valid names (case-insensitive):

        ``"n_patients"``, ``"n_hospitalizations"``, ``"age_at_admission"``,
        ``"sex"``, ``"race"``, ``"ethnicity"``, ``"language"``,
        ``"length_of_stay_days"``, ``"admission_type"``,
        ``"discharge_category"``, ``"in_hospital_mortality"``.

    Returns
    -------
    pandas.DataFrame
        A two-column DataFrame with columns ``"Variable"`` and ``"Value"``.
        Categorical variables are expanded so that each category appears on
        its own row formatted as ``"n (pct%)"``; continuous variables appear
        on a single row.

    Examples
    --------
    >>> from clifpy.data import load_demo_patient, load_demo_hospitalization
    >>> from clifpy.utils.table_one import generate_table_one
    >>> patient = load_demo_patient()
    >>> hosp = load_demo_hospitalization()
    >>> t1 = generate_table_one(patient, hosp)
    >>> t1.head(10)
    """
    if continuous_format not in ("median_iqr", "mean_sd"):
        raise ValueError(
            f"continuous_format must be 'median_iqr' or 'mean_sd', got {continuous_format!r}"
        )

    # ------------------------------------------------------------------
    # Extract raw DataFrames from table objects
    # ------------------------------------------------------------------
    patient_df = _extract_df(patient)
    hosp_df = _extract_df(hospitalization)

    # ------------------------------------------------------------------
    # Apply cohort filter
    # ------------------------------------------------------------------
    patient_df, hosp_df = _apply_cohort_filter(patient_df, hosp_df, cohort)

    # ------------------------------------------------------------------
    # Pre-compute derived columns
    # ------------------------------------------------------------------
    hosp_df = _add_los(hosp_df)

    # ------------------------------------------------------------------
    # Determine which variables to include
    # ------------------------------------------------------------------
    all_vars = [
        "n_patients",
        "n_hospitalizations",
        "age_at_admission",
        "sex",
        "race",
        "ethnicity",
        "language",
        "length_of_stay_days",
        "admission_type",
        "discharge_category",
        "in_hospital_mortality",
    ]

    if include_vars is not None:
        # Normalise to lower-case for comparison
        requested = [v.lower() for v in include_vars]
        invalid = set(requested) - {v.lower() for v in all_vars}
        if invalid:
            raise ValueError(
                f"Unknown variable(s) in include_vars: {sorted(invalid)}. "
                f"Valid options: {all_vars}"
            )
        vars_to_include = [v for v in all_vars if v.lower() in requested]
    else:
        vars_to_include = list(all_vars)

    # ------------------------------------------------------------------
    # Build the rows of the table
    # ------------------------------------------------------------------
    rows: List[dict] = []

    n_pts = patient_df["patient_id"].nunique() if "patient_id" in patient_df.columns else len(patient_df)
    n_hosp = hosp_df["hospitalization_id"].nunique() if "hospitalization_id" in hosp_df.columns else len(hosp_df)

    for var in vars_to_include:
        if var == "n_patients":
            rows.append({"Variable": "N (patients)", "Value": str(n_pts)})

        elif var == "n_hospitalizations":
            rows.append({"Variable": "N (hospitalizations)", "Value": str(n_hosp)})

        elif var == "age_at_admission":
            if "age_at_admission" in hosp_df.columns:
                rows += _continuous_rows(
                    hosp_df["age_at_admission"],
                    label="Age at admission (years)",
                    fmt=continuous_format,
                )

        elif var == "sex":
            if "sex_category" in patient_df.columns:
                rows += _categorical_rows(
                    patient_df["sex_category"],
                    label="Sex",
                    total=n_pts,
                )

        elif var == "race":
            if "race_category" in patient_df.columns:
                rows += _categorical_rows(
                    patient_df["race_category"],
                    label="Race",
                    total=n_pts,
                )

        elif var == "ethnicity":
            if "ethnicity_category" in patient_df.columns:
                rows += _categorical_rows(
                    patient_df["ethnicity_category"],
                    label="Ethnicity",
                    total=n_pts,
                )

        elif var == "language":
            if "language_category" in patient_df.columns:
                rows += _categorical_rows(
                    patient_df["language_category"],
                    label="Language",
                    total=n_pts,
                )

        elif var == "length_of_stay_days":
            if "length_of_stay_days" in hosp_df.columns:
                rows += _continuous_rows(
                    hosp_df["length_of_stay_days"],
                    label="Length of stay (days)",
                    fmt=continuous_format,
                )

        elif var == "admission_type":
            if "admission_type_category" in hosp_df.columns:
                rows += _categorical_rows(
                    hosp_df["admission_type_category"],
                    label="Admission type",
                    total=n_hosp,
                )

        elif var == "discharge_category":
            if "discharge_category" in hosp_df.columns:
                rows += _categorical_rows(
                    hosp_df["discharge_category"],
                    label="Discharge disposition",
                    total=n_hosp,
                )

        elif var == "in_hospital_mortality":
            if "discharge_category" in hosp_df.columns:
                expired = (hosp_df["discharge_category"] == "Expired").sum()
                pct = expired / n_hosp * 100 if n_hosp > 0 else 0.0
                rows.append({
                    "Variable": "In-hospital mortality",
                    "Value": f"{int(expired)} ({pct:.1f}%)",
                })

    return pd.DataFrame(rows, columns=["Variable", "Value"])


# ---------------------------------------------------------------------------
# Private helpers
# ---------------------------------------------------------------------------

def _extract_df(obj: Union[pd.DataFrame, object]) -> pd.DataFrame:
    """Return a pandas DataFrame regardless of whether a table object was given."""
    if isinstance(obj, pd.DataFrame):
        return obj.copy()
    if hasattr(obj, "df") and isinstance(obj.df, pd.DataFrame):
        return obj.df.copy()
    raise TypeError(
        f"Expected a pandas DataFrame or a CLIF table object, got {type(obj).__name__!r}"
    )


def _apply_cohort_filter(
    patient_df: pd.DataFrame,
    hosp_df: pd.DataFrame,
    cohort: Optional[pd.DataFrame],
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Filter patient and hospitalization DataFrames to the specified cohort."""
    if cohort is None:
        return patient_df, hosp_df

    if not isinstance(cohort, pd.DataFrame):
        raise TypeError("cohort must be a pandas DataFrame")

    # Filter hospitalizations first, then derive patient list
    if "hospitalization_id" in cohort.columns and "hospitalization_id" in hosp_df.columns:
        cohort_hosp_ids = cohort["hospitalization_id"].dropna().unique()
        hosp_df = hosp_df[hosp_df["hospitalization_id"].isin(cohort_hosp_ids)].copy()
    elif "patient_id" in cohort.columns and "patient_id" in hosp_df.columns:
        cohort_patient_ids = cohort["patient_id"].dropna().unique()
        hosp_df = hosp_df[hosp_df["patient_id"].isin(cohort_patient_ids)].copy()

    # Restrict patients to those appearing in the filtered hospitalizations
    if "patient_id" in hosp_df.columns and "patient_id" in patient_df.columns:
        remaining_patient_ids = hosp_df["patient_id"].dropna().unique()
        patient_df = patient_df[patient_df["patient_id"].isin(remaining_patient_ids)].copy()

    return patient_df, hosp_df


def _add_los(hosp_df: pd.DataFrame) -> pd.DataFrame:
    """Add a ``length_of_stay_days`` column if admission/discharge datetimes are present."""
    if "length_of_stay_days" in hosp_df.columns:
        return hosp_df

    if "admission_dttm" in hosp_df.columns and "discharge_dttm" in hosp_df.columns:
        adm = pd.to_datetime(hosp_df["admission_dttm"])
        dis = pd.to_datetime(hosp_df["discharge_dttm"])
        hosp_df = hosp_df.copy()
        hosp_df["length_of_stay_days"] = (dis - adm).dt.total_seconds() / 86_400.0

    return hosp_df


def _continuous_rows(
    series: pd.Series,
    label: str,
    fmt: str,
) -> List[dict]:
    """Return one-element list with a formatted continuous variable row."""
    data = series.dropna()
    if data.empty:
        return [{"Variable": label, "Value": "N/A"}]

    if fmt == "mean_sd":
        value = f"{data.mean():.1f} \u00b1 {data.std():.1f}"
        suffix = "(mean \u00b1 SD)"
    else:  # median_iqr
        q1 = data.quantile(0.25)
        q3 = data.quantile(0.75)
        value = f"{data.median():.1f} [{q1:.1f}\u2013{q3:.1f}]"
        suffix = "(median [IQR])"

    return [{"Variable": f"{label} {suffix}", "Value": value}]


def _categorical_rows(
    series: pd.Series,
    label: str,
    total: int,
) -> List[dict]:
    """Return a header row followed by one row per category value."""
    counts = series.value_counts(dropna=False)

    rows: List[dict] = [{"Variable": label, "Value": ""}]
    for cat, n in counts.items():
        cat_label = "Missing" if pd.isna(cat) else str(cat)
        pct = n / total * 100 if total > 0 else 0.0
        rows.append({"Variable": f"  {cat_label}", "Value": f"{int(n)} ({pct:.1f}%)"})

    return rows

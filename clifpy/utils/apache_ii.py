"""APACHE II score implementation for the first 24 hours of ICU care.

This module calculates the APACHE II severity score from a row-per-hospitalization
summary table using the standard public scoring weights published for the first
24 hours of ICU admission.
"""

from __future__ import annotations

from typing import Any, Iterable, Optional, Sequence, Union

import pandas as pd
import polars as pl


# Clinical APACHE II weights for the first 24 hours.
# Values are drawn from the standard APACHE II physiology tables used in ICU
# severity scoring, as published in public calculators and original literature.
APACHE_II_COLUMN_ALIASES = {
    "temperature": ("temperature_c", "temp_c", "temperature", "temp"),
    "map": ("map_mm_hg", "mean_arterial_pressure_mm_hg", "map", "mean_arterial_pressure"),
    "heart_rate": ("heart_rate", "hr", "pulse"),
    "respiratory_rate": ("respiratory_rate", "rr", "resp_rate"),
    "pao2": ("pao2_mm_hg", "pao2", "po2_arterial", "arterial_po2_mm_hg"),
    "fio2": ("fio2", "fi_o2", "fio2_percent", "fraction_inspired_oxygen"),
    "a_a_gradient": ("a_a_gradient_mm_hg", "a_a_gradient", "alveolar_arterial_gradient"),
    "ph": ("ph", "pH", "arterial_ph"),
    "sodium": ("sodium_meq_l", "sodium", "na_meq_l"),
    "potassium": ("potassium_meq_l", "potassium", "k_meq_l"),
    "creatinine": ("creatinine_mg_dl", "creatinine", "serum_creatinine"),
    "hematocrit": ("hematocrit_pct", "hematocrit", "hct_pct"),
    "wbc": ("wbc_x10_9_l", "wbc", "white_blood_cell_count"),
    "gcs": ("gcs_total", "gcs", "glasgow_coma_scale_total"),
    "age": ("age_years", "age", "age_yrs"),
    "chronic_health": ("chronic_health", "chronic_health_points", "chronic_disease"),
}


def _first_present_value(row: pd.Series, aliases: Sequence[str]) -> Any:
    for alias in aliases:
        if alias in row.index and not pd.isna(row[alias]):
            return row[alias]
    return pd.NA


def _score_temperature(value: Any) -> int:
    if pd.isna(value):
        return 0
    value = float(value)
    if value >= 41.0:
        return 4
    if value >= 39.0:
        return 2
    if value >= 38.5:
        return 1
    if value >= 36.0:
        return 0
    if value >= 34.0:
        return 1
    if value >= 32.0:
        return 2
    if value >= 30.0:
        return 3
    return 4


def _score_map(value: Any) -> int:
    if pd.isna(value):
        return 0
    value = float(value)
    if value >= 160:
        return 4
    if value >= 130:
        return 3
    if value >= 110:
        return 2
    if value >= 70:
        return 0
    if value >= 50:
        return 2
    if value >= 35:
        return 3
    return 4


def _score_heart_rate(value: Any) -> int:
    if pd.isna(value):
        return 0
    value = float(value)
    if value >= 180:
        return 4
    if value >= 140:
        return 3
    if value >= 110:
        return 2
    if value >= 70:
        return 0
    if value >= 55:
        return 2
    if value >= 40:
        return 3
    return 4


def _score_respiratory_rate(value: Any) -> int:
    if pd.isna(value):
        return 0
    value = float(value)
    if value >= 50:
        return 4
    if value >= 35:
        return 2
    if value >= 25:
        return 1
    if value >= 12:
        return 0
    if value >= 10:
        return 1
    if value >= 6:
        return 2
    return 4


def _score_oxygenation(pao2: Any, fio2: Any, aa_gradient: Any) -> int:
    if not pd.isna(aa_gradient):
        aa = float(aa_gradient)
        if aa < 200:
            return 0
        if aa < 350:
            return 2
        if aa < 500:
            return 3
        return 4

    if pd.isna(pao2):
        return 0
    pao2_value = float(pao2)
    fio2_value = float(fio2) if not pd.isna(fio2) else 0.21
    if fio2_value > 1:
        fio2_value = fio2_value / 100.0

    if fio2_value >= 0.5:
        if pao2_value > 70:
            return 0
        if pao2_value >= 61:
            return 1
        if pao2_value >= 55:
            return 3
        return 4

    if pao2_value > 70:
        return 0
    if pao2_value >= 61:
        return 1
    if pao2_value >= 55:
        return 3
    return 4


def _score_ph(value: Any) -> int:
    if pd.isna(value):
        return 0
    value = float(value)
    if value >= 7.70:
        return 4
    if value >= 7.60:
        return 3
    if value >= 7.50:
        return 1
    if value >= 7.33:
        return 0
    if value >= 7.25:
        return 1
    if value >= 7.15:
        return 2
    return 4


def _score_sodium(value: Any) -> int:
    if pd.isna(value):
        return 0
    value = float(value)
    if value >= 160:
        return 4
    if value >= 155:
        return 3
    if value >= 150:
        return 2
    if value >= 130:
        return 0
    if value >= 120:
        return 2
    if value >= 111:
        return 3
    return 4


def _score_potassium(value: Any) -> int:
    if pd.isna(value):
        return 0
    value = float(value)
    if value >= 7.0:
        return 4
    if value >= 6.0:
        return 2
    if value >= 5.5:
        return 1
    if value >= 3.5:
        return 0
    if value >= 3.0:
        return 1
    if value >= 2.5:
        return 2
    if value >= 2.0:
        return 3
    return 4


def _score_creatinine(value: Any) -> int:
    if pd.isna(value):
        return 0
    value = float(value)
    if value >= 3.5:
        return 4
    if value >= 2.0:
        return 2
    if value >= 1.5:
        return 1
    if value >= 0.6:
        return 0
    return 1


def _score_hematocrit(value: Any) -> int:
    if pd.isna(value):
        return 0
    value = float(value)
    if value >= 60:
        return 4
    if value >= 50:
        return 2
    if value >= 46:
        return 1
    if value >= 30:
        return 0
    if value >= 20:
        return 2
    return 4


def _score_wbc(value: Any) -> int:
    if pd.isna(value):
        return 0
    value = float(value)
    if value >= 40:
        return 4
    if value >= 20:
        return 2
    if value >= 15:
        return 1
    if value >= 3:
        return 0
    if value >= 1:
        return 2
    return 4


def _score_gcs(value: Any) -> int:
    if pd.isna(value):
        return 0
    value = float(value)
    if value >= 15:
        return 0
    if value >= 13:
        return 1
    if value >= 10:
        return 2
    if value >= 7:
        return 3
    if value >= 4:
        return 4
    return 4


def _score_age(value: Any) -> int:
    if pd.isna(value):
        return 0
    value = float(value)
    if value >= 75:
        return 6
    if value >= 65:
        return 5
    if value >= 55:
        return 3
    if value >= 45:
        return 2
    return 0


def _score_chronic_health(value: Any) -> int:
    if pd.isna(value):
        return 0
    if isinstance(value, str):
        normalized = value.strip().lower()
        mapping = {
            "none": 0,
            "no chronic health": 0,
            "elective surgery": 2,
            "elective_surgery": 2,
            "postoperative": 2,
            "emergency surgery": 5,
            "emergency_surgery": 5,
            "nonoperative": 5,
            "non-operative": 5,
        }
        return mapping.get(normalized, 0)
    try:
        numeric = float(value)
    except (TypeError, ValueError):
        return 0
    if numeric >= 5:
        return 5
    if numeric >= 2:
        return 2
    return 0


def calculate_apache_ii(
    df: Union[pd.DataFrame, pl.DataFrame],
    id_col: str = "hospitalization_id",
    age_col: Optional[str] = None,
    chronic_health_col: Optional[str] = None,
) -> pd.DataFrame:
    """Calculate APACHE II scores for each hospitalization from first-24h data.

    Parameters
    ----------
    df : pandas.DataFrame or polars.DataFrame
        Input data with one row per hospitalization. Values should represent the
        first 24 hours of ICU admission.
    id_col : str, default="hospitalization_id"
        Column used as the patient/stay identifier.
    age_col : str, optional
        Explicit age column override.
    chronic_health_col : str, optional
        Explicit chronic-health column override.

    Returns
    -------
    pandas.DataFrame
        A copy of the input data with APACHE II points and total score columns.
    """
    if isinstance(df, pl.DataFrame):
        df = df.to_pandas()
    if not isinstance(df, pd.DataFrame):
        raise ValueError("df must be a pandas.DataFrame or polars.DataFrame")
    if id_col not in df.columns:
        raise ValueError(f"Missing required id column: '{id_col}'")

    result = df.copy()
    aliases = dict(APACHE_II_COLUMN_ALIASES)
    if age_col is not None:
        aliases["age"] = (age_col,)
    if chronic_health_col is not None:
        aliases["chronic_health"] = (chronic_health_col,)

    score_columns = {}
    for key, column_aliases in aliases.items():
        score_columns[key] = _first_present_value(result.iloc[0], column_aliases) if len(result) == 0 else None

    def _compute_row(row: pd.Series) -> pd.Series:
        values = {
            "apache_ii_temp_points": _score_temperature(_first_present_value(row, aliases["temperature"])),
            "apache_ii_map_points": _score_map(_first_present_value(row, aliases["map"])),
            "apache_ii_heart_rate_points": _score_heart_rate(_first_present_value(row, aliases["heart_rate"])),
            "apache_ii_respiratory_rate_points": _score_respiratory_rate(_first_present_value(row, aliases["respiratory_rate"])),
            "apache_ii_oxygenation_points": _score_oxygenation(
                _first_present_value(row, aliases["pao2"]),
                _first_present_value(row, aliases["fio2"]),
                _first_present_value(row, aliases["a_a_gradient"]),
            ),
            "apache_ii_ph_points": _score_ph(_first_present_value(row, aliases["ph"])),
            "apache_ii_sodium_points": _score_sodium(_first_present_value(row, aliases["sodium"])),
            "apache_ii_potassium_points": _score_potassium(_first_present_value(row, aliases["potassium"])),
            "apache_ii_creatinine_points": _score_creatinine(_first_present_value(row, aliases["creatinine"])),
            "apache_ii_hematocrit_points": _score_hematocrit(_first_present_value(row, aliases["hematocrit"])),
            "apache_ii_wbc_points": _score_wbc(_first_present_value(row, aliases["wbc"])),
            "apache_ii_gcs_points": _score_gcs(_first_present_value(row, aliases["gcs"])),
            "apache_ii_age_points": _score_age(_first_present_value(row, aliases["age"])),
            "apache_ii_chronic_health_points": _score_chronic_health(_first_present_value(row, aliases["chronic_health"])),
        }
        values["apache_ii_score"] = sum(values.values())
        return pd.Series(values)

    component_scores = result.apply(_compute_row, axis=1)
    result = pd.concat([result, component_scores], axis=1)
    return result


__all__ = [
    "APACHE_II_COLUMN_ALIASES",
    "calculate_apache_ii",
    "_score_temperature",
    "_score_map",
    "_score_heart_rate",
    "_score_respiratory_rate",
    "_score_oxygenation",
    "_score_ph",
    "_score_sodium",
    "_score_potassium",
    "_score_creatinine",
    "_score_hematocrit",
    "_score_wbc",
    "_score_gcs",
    "_score_age",
    "_score_chronic_health",
]

"""Crosswalk a site's standardized CLIF columns from one schema version to another.

The dominant change from CLIF 2.1 to 3.0 is that the standardized
``*_category`` / ``*_group`` / ``*_type`` column **values** were lowercased and
snake_cased, with a minority of non-derivable renames (abbreviations and
semantic changes, e.g. ``High Flow NC`` -> ``hfnc``, ``DNR/DNI`` -> ``dnr_or_dni``,
``Psychiatric Hospital`` -> ``mental_health_hosp``).

This module provides a standalone converter that maps those values to their 3.0
form. Most values are handled by :func:`normalize_category_value` (a
deterministic lowercase + snake_case transform); the non-derivable ones come
from a curated resource (``clifpy/schemas/crosswalks/clif_2.1_to_3.0.yaml``).

The converter is non-mutating: it returns a converted *copy* of the input
DataFrame plus a structured report. Values it cannot confidently map (1->many
splits like ``albumin`` -> ``albumin_5``/``albumin_25``, or anything that does
not resolve to a valid 3.0 value) are left unchanged and surfaced in the report.

This is intentionally separate from ``validator._normalize_columns_pandas``,
which only case-normalizes *all* string columns for validation; the rules here
(``&`` -> ``and``, ``/`` -> ``or``) are value-token specific.
"""

import os
import re
from typing import Any, Dict, Optional, Tuple

import pandas as pd
import yaml

from ..schemas import load_schema

# The 16 CLIF "beta" tables — the default scope for 2.1 -> 3.0 migration.
BETA_TABLES = (
    "patient",
    "hospitalization",
    "adt",
    "vitals",
    "labs",
    "patient_assessments",
    "medication_admin_continuous",
    "medication_admin_intermittent",
    "respiratory_support",
    "position",
    "patient_procedures",
    "code_status",
    "crrt_therapy",
    "hospital_diagnosis",
    "microbiology_culture",
    "microbiology_susceptibility",
)

# Suffixes that mark a "standardized" column whose values are crosswalked.
_STANDARDIZED_SUFFIXES = ("_category", "_group", "_type")

_CROSSWALK_FILENAME = "clif_2.1_to_3.0.yaml"
_MULTI_UNDERSCORE = re.compile(r"_+")

# Cache the parsed crosswalk resource so repeated calls don't re-read the file.
_CROSSWALK_CACHE: Optional[Dict[str, Any]] = None


def normalize_category_value(value: Any) -> Any:
    """Deterministically lowercase + snake_case a single category value.

    Reproduces the CLIF 3.0 token convention for values that changed by case /
    punctuation only. Non-string input (``None``, ``NaN``, numbers, bools) is
    returned unchanged so numeric flag columns (e.g. ``tracheostomy`` 0/1) pass
    through untouched.

    Rules, applied in order:

    1. Non-string / null -> returned unchanged.
    2. ``str``, strip surrounding whitespace, lowercase.
    3. ``&`` -> ``_and_``, ``/`` -> ``_or_`` (so ``l&d`` -> ``l_and_d`` and
       ``DNR/DNI`` -> ``dnr_or_dni`` after collapsing underscores).
    4. Replace space, ``-``, ``,``, ``(``, ``)`` with ``_``.
    5. Collapse runs of ``_`` to a single ``_``; strip leading/trailing ``_``.

    Examples
    --------
    >>> normalize_category_value("Non-Hispanic")
    'non_hispanic'
    >>> normalize_category_value("l&d")
    'l_and_d'
    >>> normalize_category_value("pulmonary vasodilators (IV)")
    'pulmonary_vasodilators_iv'
    """
    if value is None or not isinstance(value, str):
        # Covers None, NaN (float), ints, bools — leave as-is.
        return value

    s = value.strip().lower()
    s = s.replace("&", "_and_").replace("/", "_or_")
    for ch in (" ", "-", ",", "(", ")"):
        s = s.replace(ch, "_")
    s = _MULTI_UNDERSCORE.sub("_", s).strip("_")
    return s


def _crosswalk_path() -> str:
    """Absolute path to the bundled 2.1 -> 3.0 crosswalk resource."""
    return os.path.join(
        os.path.dirname(os.path.dirname(__file__)),
        "schemas",
        "crosswalks",
        _CROSSWALK_FILENAME,
    )


def load_crosswalk(crosswalk_path: Optional[str] = None) -> Dict[str, Any]:
    """Load (and cache) the curated 2.1 -> 3.0 crosswalk resource.

    Returns a dict with ``from_version``, ``to_version``, ``renames`` and
    ``unresolved`` keys. Missing ``renames``/``unresolved`` default to ``{}``.
    """
    global _CROSSWALK_CACHE
    if crosswalk_path is None:
        if _CROSSWALK_CACHE is not None:
            return _CROSSWALK_CACHE
        path = _crosswalk_path()
    else:
        path = crosswalk_path

    with open(path, "r") as f:
        data = yaml.safe_load(f) or {}
    data.setdefault("renames", {})
    data.setdefault("unresolved", {})

    if crosswalk_path is None:
        _CROSSWALK_CACHE = data
    return data


def _permissible_map(schema: Optional[Dict[str, Any]]) -> Dict[str, list]:
    """Map column name -> permissible_values list for columns that define one."""
    if not schema:
        return {}
    out = {}
    for col in schema.get("columns", []) or []:
        if col.get("permissible_values"):
            out[col["name"]] = col["permissible_values"]
    return out


def _standardized_columns(table_name: str) -> set:
    """Union of ``*_category``/``*_group``/``*_type`` columns named in the 2.1
    or 3.0 schema for ``table_name``.

    Taking the union makes column discovery robust to the 2.1->3.0 flag flip
    (e.g. ``assessment_group``/``med_group`` move from category to group), since
    the column *name* is unchanged across versions.
    """
    cols = set()
    for version in ("2.1", "3.0"):
        schema = load_schema(table_name, version)
        if not schema:
            continue
        for col in schema.get("columns", []) or []:
            name = col.get("name", "")
            if name.endswith(_STANDARDIZED_SUFFIXES):
                cols.add(name)
    return cols


def crosswalk_table_2_1_to_3_0(
    df: pd.DataFrame,
    table_name: str,
    crosswalk_path: Optional[str] = None,
) -> Tuple[pd.DataFrame, Dict[str, Any]]:
    """Convert a table's standardized column values from CLIF 2.1 to 3.0.

    Transforms values in ``*_category``/``*_group``/``*_type`` columns to their
    CLIF 3.0 form. Each value is mapped via (1) the curated rename map, else
    (2) :func:`normalize_category_value`. Values flagged as 1->many
    ``unresolved`` (e.g. ``albumin``) are left unchanged, as are values that do
    not resolve to a valid 3.0 permissible value — all such cases are reported.

    Column header names are NOT changed, and the input ``df`` is never mutated.

    Parameters
    ----------
    df : pd.DataFrame
        A site's table data in CLIF 2.1 format.
    table_name : str
        CLIF table name (e.g. ``"respiratory_support"``).
    crosswalk_path : str, optional
        Override path to the crosswalk resource (defaults to the bundled file).

    Returns
    -------
    (converted_df, report) : Tuple[pd.DataFrame, dict]
        ``converted_df`` is a copy of ``df`` with values converted.
        ``report`` has keys: ``table``, ``from_version``, ``to_version``,
        ``columns`` (per-column ``n_values_converted`` / ``ambiguous`` /
        ``unresolved``), and ``is_complete`` (True iff nothing was flagged).
    """
    crosswalk = load_crosswalk(crosswalk_path)
    renames_tbl = crosswalk.get("renames", {}).get(table_name, {})
    unresolved_tbl = crosswalk.get("unresolved", {}).get(table_name, {})

    permissible_30 = _permissible_map(load_schema(table_name, "3.0"))
    std_cols = _standardized_columns(table_name)
    target_cols = [c for c in df.columns if c in std_cols]

    out = df.copy()
    report: Dict[str, Any] = {
        "table": table_name,
        "from_version": "2.1",
        "to_version": "3.0",
        "columns": {},
        "is_complete": True,
    }

    for col in target_cols:
        col_renames = renames_tbl.get(col, {})
        col_unresolved = unresolved_tbl.get(col, {})
        allowed_30 = set(permissible_30.get(col, []))

        # Count occurrences of each unique non-null value once.
        counts = out[col].value_counts(dropna=True)
        value_map: Dict[Any, Any] = {}
        ambiguous = []
        unresolved = []
        n_converted = 0

        for raw_value, count in counts.items():
            if raw_value in col_unresolved:
                # 1->many: leave unchanged, report candidates/reason.
                entry = col_unresolved[raw_value] or {}
                value_map[raw_value] = raw_value
                ambiguous.append({
                    "original": raw_value,
                    "candidates": entry.get("candidates", []),
                    "reason": entry.get("reason", ""),
                    "count": int(count),
                })
                continue

            if raw_value in col_renames:
                produced = col_renames[raw_value]
            else:
                produced = normalize_category_value(raw_value)

            value_map[raw_value] = produced
            if produced != raw_value:
                n_converted += int(count)

            # Validate against 3.0 permissible set (only when one is defined).
            if allowed_30 and produced not in allowed_30:
                unresolved.append({
                    "original": raw_value,
                    "produced": produced,
                    "count": int(count),
                })

        # Apply the unique-value map (NaN/None untouched: not in value_map).
        out[col] = out[col].map(lambda v: value_map.get(v, v))

        report["columns"][col] = {
            "n_values_converted": n_converted,
            "ambiguous": ambiguous,
            "unresolved": unresolved,
        }
        if ambiguous or unresolved:
            report["is_complete"] = False

    return out, report

"""Tests for the CLIF 2.1 -> 3.0 standardized-column crosswalk.

The completeness test is the correctness gate: it proves that every CLIF 2.1
permissible value for a beta-table standardized column resolves (via the
normalizer or the curated rename map) to a valid CLIF 3.0 permissible value,
except values explicitly documented as `unresolved`.

Run in isolation: `uv run pytest tests/test_crosswalk.py`
"""

import numpy as np
import pandas as pd
import pytest

from clifpy.schemas import load_schema
from clifpy.utils.crosswalk import (
    BETA_TABLES,
    crosswalk_table_2_1_to_3_0,
    load_crosswalk,
    normalize_category_value,
    _standardized_columns,
)

CROSSWALK = load_crosswalk()
RENAMES = CROSSWALK["renames"]
UNRESOLVED = CROSSWALK["unresolved"]


def _perm(schema, col):
    if not schema:
        return None
    for c in schema.get("columns", []) or []:
        if c["name"] == col:
            return c.get("permissible_values")
    return None


def _covered_columns():
    """(table, col, p21, p30) for beta standardized cols with permissible in both versions."""
    out = []
    for table in BETA_TABLES:
        s21, s30 = load_schema(table, "2.1"), load_schema(table, "3.0")
        for col in sorted(_standardized_columns(table)):
            p21, p30 = _perm(s21, col), _perm(s30, col)
            if p21 and p30:
                out.append((table, col, p21, p30))
    return out


COVERED = _covered_columns()


# --------------------------------------------------------------------------- #
# Normalizer unit tests
# --------------------------------------------------------------------------- #

@pytest.mark.parametrize("raw,expected", [
    ("IMV", "imv"),
    ("Non-Hispanic", "non_hispanic"),
    ("l&d", "l_and_d"),
    ("DNR/DNI", "dnr_or_dni"),
    ("Mobility/Activity", "mobility_or_activity"),
    ("SAT Screen Pass/Fail", "sat_screen_pass_or_fail"),
    ("pulmonary vasodilators (IV)", "pulmonary_vasodilators_iv"),
    ("Pressure Control", "pressure_control"),
    ("AM-PAC", "am_pac"),
    ("  Trailing  ", "trailing"),
])
def test_normalizer_examples(raw, expected):
    assert normalize_category_value(raw) == expected


def test_normalizer_idempotent_on_30_tokens():
    for tok in ("imv", "dnr_or_dni", "mobility_or_activity", "l_and_d", "snf"):
        assert normalize_category_value(tok) == tok


def test_normalizer_passes_through_non_strings():
    assert normalize_category_value(None) is None
    assert normalize_category_value(1) == 1
    assert np.isnan(normalize_category_value(np.nan))


# --------------------------------------------------------------------------- #
# Completeness gate (the proof)
# --------------------------------------------------------------------------- #

def test_every_2_1_value_resolves_to_valid_3_0():
    failures = []
    for table, col, p21, p30 in COVERED:
        set30 = set(p30)
        col_renames = RENAMES.get(table, {}).get(col, {})
        col_unresolved = UNRESOLVED.get(table, {}).get(col, {})
        for v in p21:
            if v in col_unresolved:
                continue  # documented as not auto-resolvable
            produced = col_renames.get(v, normalize_category_value(v))
            if produced not in set30:
                failures.append(f"{table}.{col}: {v!r} -> {produced!r} not in 3.0")
    assert not failures, "Unresolved 2.1 values:\n" + "\n".join(failures)


def test_rename_targets_are_valid_3_0_values():
    failures = []
    for table, cols in RENAMES.items():
        s30 = load_schema(table, "3.0")
        for col, mapping in cols.items():
            allowed = set(_perm(s30, col) or [])
            for old, new in mapping.items():
                if allowed and new not in allowed:
                    failures.append(f"{table}.{col}: target {new!r} (from {old!r}) not a 3.0 value")
    assert not failures, "\n".join(failures)


def test_unresolved_candidates_are_valid_3_0_values():
    failures = []
    for table, cols in UNRESOLVED.items():
        s30 = load_schema(table, "3.0")
        for col, entries in cols.items():
            allowed = set(_perm(s30, col) or [])
            for old, meta in entries.items():
                for cand in (meta or {}).get("candidates", []) or []:
                    if allowed and cand not in allowed:
                        failures.append(f"{table}.{col}: candidate {cand!r} (for {old!r}) not a 3.0 value")
    assert not failures, "\n".join(failures)


def test_no_dead_or_redundant_rename_entries():
    """Every rename key must be a real 2.1 value, and genuinely non-derivable
    (else it belongs to the normalizer, not the curated file)."""
    dead, redundant = [], []
    for table, cols in RENAMES.items():
        s21 = load_schema(table, "2.1")
        for col, mapping in cols.items():
            p21 = set(_perm(s21, col) or [])
            for old, new in mapping.items():
                if old not in p21:
                    dead.append(f"{table}.{col}: {old!r} is not a 2.1 permissible value")
                if normalize_category_value(old) == new:
                    redundant.append(f"{table}.{col}: {old!r}->{new!r} is normalizer-derivable; remove it")
    assert not dead, "Dead entries:\n" + "\n".join(dead)
    assert not redundant, "Redundant entries:\n" + "\n".join(redundant)


# --------------------------------------------------------------------------- #
# Apply behavior
# --------------------------------------------------------------------------- #

def test_apply_respiratory_support():
    df = pd.DataFrame({
        "hospitalization_id": ["a", "b", "c"],
        "device_category": ["IMV", "High Flow NC", "Nasal Cannula"],
        "mode_category": ["Assist Control-Volume Control", "Pressure Support/CPAP", None],
    })
    out, report = crosswalk_table_2_1_to_3_0(df, "respiratory_support")
    assert list(out["device_category"]) == ["imv", "hfnc", "nasal_cannula"]
    assert out["mode_category"].tolist()[:2] == ["acvc", "ps_or_cpap"]
    assert report["is_complete"] is True


def test_apply_code_status_pure_normalizer():
    df = pd.DataFrame({"code_status_category": ["DNR", "DNR/DNI", "Full", "Presume Full"]})
    out, report = crosswalk_table_2_1_to_3_0(df, "code_status")
    assert list(out["code_status_category"]) == ["dnr", "dnr_or_dni", "full", "presume_full"]
    assert report["is_complete"] is True


def test_albumin_is_ambiguous_and_left_unchanged():
    df = pd.DataFrame({"med_category": ["albumin", "norepinephrine"]})
    out, report = crosswalk_table_2_1_to_3_0(df, "medication_admin_continuous")
    assert out["med_category"].iloc[0] == "albumin"  # unchanged
    amb = report["columns"]["med_category"]["ambiguous"]
    assert any(a["original"] == "albumin" and set(a["candidates"]) == {"albumin_5", "albumin_25"} for a in amb)
    assert report["is_complete"] is False


def test_does_not_mutate_input():
    df = pd.DataFrame({"device_category": ["IMV", "CPAP"]})
    snapshot = df.copy()
    crosswalk_table_2_1_to_3_0(df, "respiratory_support")
    pd.testing.assert_frame_equal(df, snapshot)


def test_only_standardized_columns_touched():
    df = pd.DataFrame({
        "patient_id": ["X1"],
        "race_name": ["White"],           # *_name: not standardized -> untouched
        "race_category": ["White"],       # *_category: converted
    })
    out, _ = crosswalk_table_2_1_to_3_0(df, "patient")
    assert out["race_name"].iloc[0] == "White"
    assert out["race_category"].iloc[0] == "white"

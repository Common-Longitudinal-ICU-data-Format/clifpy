"""Tests for the CLIF 2.1 -> 3.0 standardized-column crosswalk.

The completeness test is the correctness gate: it proves that every CLIF 2.1
permissible value for a beta-table standardized column resolves (via the
normalizer or the curated rename map) to a valid CLIF 3.0 permissible value,
except values explicitly documented as `unresolved`.

Run in isolation: `uv run pytest tests/test_crosswalk.py`
"""

import textwrap

import numpy as np
import pandas as pd
import pytest

from clifpy.schemas import load_schema
from clifpy.utils.crosswalk import (
    BETA_TABLES,
    crosswalk_table_2_1_to_3_0,
    crosswalk_file_2_1_to_3_0,
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


# --------------------------------------------------------------------------- #
# Real curated renames across other beta tables
# --------------------------------------------------------------------------- #

def test_apply_hospitalization_discharge_renames():
    df = pd.DataFrame({"discharge_category": [
        "Skilled Nursing Facility (SNF)",   # curated abbreviation rename
        "Psychiatric Hospital",             # curated semantic rename
        "Home",                             # pure normalizer
    ]})
    out, report = crosswalk_table_2_1_to_3_0(df, "hospitalization")
    assert list(out["discharge_category"]) == ["snf", "mental_health_hosp", "home"]
    assert report["is_complete"] is True


def test_apply_labs_glucose_fingerstick_folds_to_glucose():
    df = pd.DataFrame({"lab_category": ["glucose_fingerstick", "glucose_serum"]})
    out, report = crosswalk_table_2_1_to_3_0(df, "labs")
    # 3.0 folds the fingerstick/POC variant into plain `glucose`.
    assert out["lab_category"].iloc[0] == "glucose"
    assert report["is_complete"] is True


def test_apply_microbiology_organism_renames():
    df = pd.DataFrame({"organism_category": [
        "clostridium_difficile",     # genus reclassification
        "staphyloccocus_coagneg",    # 2.1 spelling typo corrected
    ]})
    out, _ = crosswalk_table_2_1_to_3_0(df, "microbiology_culture")
    assert list(out["organism_category"]) == [
        "clostridioides_difficile",
        "staphylococcus_coagneg",
    ]


# --------------------------------------------------------------------------- #
# Unresolved (removed-in-3.0) vs ambiguous (1->many) reporting
# --------------------------------------------------------------------------- #

def test_removed_values_are_ambiguous_with_empty_candidates():
    """`csf`/`other` were dropped from lab_specimen_category in 3.0: surfaced as
    ambiguous with no candidates, left unchanged, and block completeness."""
    df = pd.DataFrame({"lab_specimen_category": ["csf", "other", "blood/plasma/serum"]})
    out, report = crosswalk_table_2_1_to_3_0(df, "labs")
    assert list(out["lab_specimen_category"]) == ["csf", "other", "plasma_blood"]
    amb = report["columns"]["lab_specimen_category"]["ambiguous"]
    originals = {a["original"]: a["candidates"] for a in amb}
    assert originals == {"csf": [], "other": []}
    assert report["is_complete"] is False


def test_unknown_value_is_reported_unresolved_not_ambiguous():
    """A value that is neither a curated rename nor a valid 3.0 token after
    normalizing lands in `unresolved` (distinct from the curated `ambiguous`
    1->many splits) and is left unchanged."""
    df = pd.DataFrame({"device_category": ["IMV", "TotallyMadeUp"]})
    out, report = crosswalk_table_2_1_to_3_0(df, "respiratory_support")
    col = report["columns"]["device_category"]
    assert out["device_category"].iloc[1] == "totallymadeup"  # normalized but invalid
    assert col["ambiguous"] == []
    assert [u["original"] for u in col["unresolved"]] == ["TotallyMadeUp"]
    assert col["unresolved"][0]["produced"] == "totallymadeup"
    assert report["is_complete"] is False


# --------------------------------------------------------------------------- #
# Report shape and counting
# --------------------------------------------------------------------------- #

def test_report_metadata_keys():
    df = pd.DataFrame({"device_category": ["IMV"]})
    _, report = crosswalk_table_2_1_to_3_0(df, "respiratory_support")
    assert report["table"] == "respiratory_support"
    assert report["from_version"] == "2.1"
    assert report["to_version"] == "3.0"


def test_n_values_converted_counts_rows_not_unique_values():
    """The count reflects every row whose value changed, not unique values."""
    df = pd.DataFrame({"device_category": ["IMV", "IMV", "IMV", "Nasal Cannula"]})
    _, report = crosswalk_table_2_1_to_3_0(df, "respiratory_support")
    # 3 IMV -> imv plus 1 Nasal Cannula -> nasal_cannula = 4 changed rows.
    assert report["columns"]["device_category"]["n_values_converted"] == 4


def test_ambiguous_count_reflects_row_occurrences():
    df = pd.DataFrame({"med_category": ["albumin", "albumin", "norepinephrine"]})
    _, report = crosswalk_table_2_1_to_3_0(df, "medication_admin_continuous")
    amb = report["columns"]["med_category"]["ambiguous"]
    assert amb[0]["original"] == "albumin"
    assert amb[0]["count"] == 2


# --------------------------------------------------------------------------- #
# NaN / empty / no-op edge cases
# --------------------------------------------------------------------------- #

def test_nan_and_none_pass_through_unchanged():
    df = pd.DataFrame({"device_category": ["IMV", None, np.nan]})
    out, report = crosswalk_table_2_1_to_3_0(df, "respiratory_support")
    assert out["device_category"].iloc[0] == "imv"
    assert out["device_category"].iloc[1] is None
    assert pd.isna(out["device_category"].iloc[2])
    # Nulls are not "converted".
    assert report["columns"]["device_category"]["n_values_converted"] == 1


def test_empty_dataframe_with_standardized_column():
    df = pd.DataFrame({"device_category": pd.Series([], dtype="object")})
    out, report = crosswalk_table_2_1_to_3_0(df, "respiratory_support")
    assert out.empty
    assert report["columns"]["device_category"]["n_values_converted"] == 0
    assert report["is_complete"] is True


def test_no_standardized_columns_present_is_a_noop():
    df = pd.DataFrame({"patient_id": ["X1"], "race_name": ["White"]})
    out, report = crosswalk_table_2_1_to_3_0(df, "patient")
    pd.testing.assert_frame_equal(out, df)
    assert report["columns"] == {}
    assert report["is_complete"] is True


# --------------------------------------------------------------------------- #
# load_crosswalk loader behavior
# --------------------------------------------------------------------------- #

def test_load_crosswalk_default_is_cached_singleton():
    assert load_crosswalk() is load_crosswalk()


def test_load_crosswalk_defaults_missing_sections_to_empty_dicts(tmp_path):
    p = tmp_path / "minimal.yaml"
    p.write_text('from_version: "2.1"\nto_version: "3.0"\n')
    data = load_crosswalk(str(p))
    assert data["renames"] == {}
    assert data["unresolved"] == {}


def test_load_crosswalk_custom_path_bypasses_cache(tmp_path):
    p = tmp_path / "custom.yaml"
    p.write_text('from_version: "2.1"\nto_version: "3.0"\nrenames: {}\n')
    # An explicit path must never return the cached default singleton.
    assert load_crosswalk(str(p)) is not load_crosswalk()


def test_crosswalk_path_override_drives_renames(tmp_path):
    """A custom crosswalk file passed through `crosswalk_path` takes effect:
    here it remaps a value the default file leaves to the normalizer."""
    custom = tmp_path / "cw.yaml"
    custom.write_text(textwrap.dedent("""\
        from_version: "2.1"
        to_version: "3.0"
        renames:
          respiratory_support:
            device_category:
              "Nasal Cannula": custom_token
        unresolved: {}
    """))
    df = pd.DataFrame({"device_category": ["Nasal Cannula"]})
    out, _ = crosswalk_table_2_1_to_3_0(df, "respiratory_support", crosswalk_path=str(custom))
    assert out["device_category"].iloc[0] == "custom_token"


# --------------------------------------------------------------------------- #
# File-to-file converter (out-of-core backends)
# --------------------------------------------------------------------------- #

@pytest.fixture
def rs_2_1_frame():
    return pd.DataFrame({
        "hospitalization_id": ["a", "b", "c", "d"],
        "device_category": ["IMV", "High Flow NC", "Nasal Cannula", "CPAP"],
        "mode_category": ["Assist Control-Volume Control", "Pressure Support/CPAP", "SIMV", None],
        "fio2_set": [0.4, 0.6, 0.21, 0.5],
    })


@pytest.mark.parametrize("backend", ["duckdb", "pandas"])
def test_file_backend_matches_in_memory(tmp_path, rs_2_1_frame, backend):
    ref, ref_report = crosswalk_table_2_1_to_3_0(rs_2_1_frame, "respiratory_support")

    src = tmp_path / "rs.parquet"
    rs_2_1_frame.to_parquet(src, index=False)
    out = tmp_path / f"rs_{backend}.parquet"
    report = crosswalk_file_2_1_to_3_0(str(src), str(out), "respiratory_support",
                                       backend=backend, chunk_size=2)
    got = pd.read_parquet(out)

    for col in ["device_category", "mode_category"]:
        assert got[col].equals(ref[col]), f"{backend} {col} mismatch"
        assert report["columns"][col]["n_values_converted"] == ref_report["columns"][col]["n_values_converted"]
    assert report["is_complete"] == ref_report["is_complete"]


def test_file_backend_csv_roundtrip(tmp_path, rs_2_1_frame):
    src = tmp_path / "rs.csv"
    rs_2_1_frame.to_csv(src, index=False)
    out = tmp_path / "rs_out.csv"
    crosswalk_file_2_1_to_3_0(str(src), str(out), "respiratory_support", backend="duckdb")
    got = pd.read_csv(out)
    assert list(got["device_category"]) == ["imv", "hfnc", "nasal_cannula", "cpap"]


def test_file_backend_unknown_raises(tmp_path, rs_2_1_frame):
    src = tmp_path / "rs.parquet"
    rs_2_1_frame.to_parquet(src, index=False)
    with pytest.raises(ValueError, match="Unknown backend"):
        crosswalk_file_2_1_to_3_0(str(src), str(tmp_path / "o.parquet"),
                                  "respiratory_support", backend="polars")

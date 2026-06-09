"""Tests for CLIF schema versioning (2.1 default + 3.0 support).

Covers the schema registry, version threading through BaseTable and
ClifOrchestrator, the ecmo_mcs -> mcs rename, and integrity of the 3.0
schema set. These tests are self-contained (no data files needed) and
avoid the legacy validator-spec fixtures.
"""

import glob
import os

import pandas as pd
import pytest
import yaml

from clifpy.schemas import (
    DEFAULT_CLIF_VERSION,
    SUPPORTED_CLIF_VERSIONS,
    load_schema,
    resolve_schema_filename,
    schema_dir,
)
from clifpy import EcmoMcs, Mcs, Patient, Vitals, ClifOrchestrator
from clifpy.clif_orchestrator import TABLE_CLASSES

SCHEMAS_ROOT = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'clifpy', 'schemas')


# --------------------------------------------------------------------------- #
# Registry
# --------------------------------------------------------------------------- #

def test_default_and_supported_versions():
    assert DEFAULT_CLIF_VERSION == "2.1"
    assert set(SUPPORTED_CLIF_VERSIONS) == {"2.1", "3.0"}


def test_load_schema_defaults_to_21():
    s = load_schema("patient")
    assert s is not None
    assert s["version"] == "2.1"
    assert s["table_name"] == "patient"


@pytest.mark.parametrize("version", ["2.1", "3.0"])
def test_load_schema_returns_requested_version(version):
    s = load_schema("vitals", version)
    assert s is not None
    assert s["version"] == version


def test_unsupported_version_raises():
    with pytest.raises(ValueError, match="Unsupported CLIF version"):
        load_schema("patient", "9.9")
    with pytest.raises(ValueError):
        schema_dir("nope")


def test_missing_schema_returns_none():
    # 'input' is a 3.0-only table; it does not exist in 2.1
    assert load_schema("input", "2.1") is None
    assert load_schema("input", "3.0") is not None


# --------------------------------------------------------------------------- #
# ecmo_mcs -> mcs rename
# --------------------------------------------------------------------------- #

def test_ecmo_mcs_rename_override():
    # The 2.1 table 'ecmo_mcs' was renamed to 'mcs' in 3.0.
    assert resolve_schema_filename("ecmo_mcs", "2.1") == "ecmo_mcs_schema.yaml"
    assert resolve_schema_filename("ecmo_mcs", "3.0") == "mcs_schema.yaml"
    # Loading the 2.1 table name under 3.0 yields the redesigned mcs schema
    s = load_schema("ecmo_mcs", "3.0")
    assert s is not None and s["table_name"] == "mcs"


def test_ecmo_mcs_class_versions():
    em_21 = EcmoMcs(data=pd.DataFrame(), clif_version="2.1")
    assert em_21.schema["table_name"] == "ecmo_mcs"
    assert em_21.schema["version"] == "2.1"

    em_30 = EcmoMcs(data=pd.DataFrame(), clif_version="3.0")
    assert em_30.schema["table_name"] == "mcs"  # override

    mcs = Mcs(data=pd.DataFrame(), clif_version="3.0")
    assert mcs.schema["table_name"] == "mcs"


# --------------------------------------------------------------------------- #
# BaseTable version threading
# --------------------------------------------------------------------------- #

def test_basetable_defaults_to_21():
    p = Patient(data=pd.DataFrame())
    assert p.clif_version == "2.1"
    assert p.schema["version"] == "2.1"


def test_basetable_honors_version():
    v = Vitals(data=pd.DataFrame(), clif_version="3.0")
    assert v.clif_version == "3.0"
    assert v.schema["version"] == "3.0"
    # 3.0 adds intracranial_pressure + pulse_pressure_variation
    cats = next(c for c in v.schema["columns"] if c["name"] == "vital_category")["permissible_values"]
    assert "intracranial_pressure" in cats
    assert "pulse_pressure_variation" in cats


def test_none_version_coerced_to_default():
    p = Patient(data=pd.DataFrame(), clif_version=None)
    assert p.clif_version == "2.1"


# --------------------------------------------------------------------------- #
# Orchestrator version threading
# --------------------------------------------------------------------------- #

def test_orchestrator_default_version(tmp_path):
    co = ClifOrchestrator(data_directory=str(tmp_path), filetype="parquet", timezone="UTC")
    assert co.clif_version == "2.1"


def test_orchestrator_explicit_version(tmp_path):
    co = ClifOrchestrator(data_directory=str(tmp_path), filetype="parquet",
                          timezone="UTC", clif_version="3.0")
    assert co.clif_version == "3.0"


def test_orchestrator_version_from_config(tmp_path):
    cfg = tmp_path / "config.yaml"
    cfg.write_text(
        f"data_directory: {tmp_path}\nfiletype: parquet\ntimezone: UTC\nclif_version: '3.0'\n"
    )
    co = ClifOrchestrator(config_path=str(cfg))
    assert co.clif_version == "3.0"


# --------------------------------------------------------------------------- #
# Integrity of the full 3.0 schema set
# --------------------------------------------------------------------------- #

def test_30_schema_set_parses_and_is_consistent():
    files = sorted(glob.glob(os.path.join(SCHEMAS_ROOT, "3.0", "*_schema.yaml")))
    assert len(files) == 41
    for fp in files:
        table = os.path.basename(fp).replace("_schema.yaml", "")
        s = yaml.safe_load(open(fp))
        assert s["version"] == "3.0", table
        assert s["table_name"] == table, table
        cols = s.get("columns") or []
        # per-column flags must agree with the summary lists
        req_flags = {c["name"] for c in cols if c.get("required")}
        cat_flags = {c["name"] for c in cols if c.get("is_category_column")}
        grp_flags = {c["name"] for c in cols if c.get("is_group_column")}
        assert req_flags == set(s.get("required_columns") or []), table
        assert cat_flags == set(s.get("category_columns") or []), table
        assert grp_flags == set(s.get("group_columns") or []), table


def test_all_30_tables_registered():
    files = {os.path.basename(f).replace("_schema.yaml", "")
             for f in glob.glob(os.path.join(SCHEMAS_ROOT, "3.0", "*_schema.yaml"))}
    missing = files - set(TABLE_CLASSES)
    assert not missing, f"3.0 tables missing from TABLE_CLASSES: {missing}"


def test_21_schemas_relocated():
    # The 18 original schemas now live under schemas/2.1/, not at the root.
    root_schemas = glob.glob(os.path.join(SCHEMAS_ROOT, "*_schema.yaml"))
    assert root_schemas == []
    v21 = glob.glob(os.path.join(SCHEMAS_ROOT, "2.1", "*_schema.yaml"))
    assert len(v21) == 18
    # shared configs remain at the root
    for shared in ("validation_rules.yaml", "outlier_config.yaml", "wide_tables_config.yaml"):
        assert os.path.exists(os.path.join(SCHEMAS_ROOT, shared))

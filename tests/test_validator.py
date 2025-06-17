import pytest
import pandas as pd
from pyclif.utils.validator import validate_dataframe, validate_table, _load_spec

# Test cases for validate_dataframe

def test_validate_dataframe_success():
    spec = {
        "required_columns": ["id", "name"],
        "columns": [
            {"name": "id", "required": True, "data_type": "INTEGER"},
            {"name": "name", "required": True, "data_type": "VARCHAR"},
            {"name": "age", "data_type": "INTEGER"},
            {"name": "status", "is_category_column": True, "permissible_values": ["active", "inactive"]}
        ]
    }
    df = pd.DataFrame({
        "id": [1, 2, 3],
        "name": ["Alice", "Bob", "Charlie"],
        "age": [25, 30, 35],
        "status": ["active", "inactive", "active"]
    })
    errors = validate_dataframe(df, spec)
    assert not errors

def test_validate_dataframe_missing_columns():
    spec = {"required_columns": ["id", "name"]}
    df = pd.DataFrame({"id": [1, 2]})
    errors = validate_dataframe(df, spec)
    assert len(errors) == 1
    assert errors[0]["type"] == "missing_columns"
    assert errors[0]["columns"] == ["name"]

def test_validate_dataframe_null_values():
    spec = {"columns": [{"name": "id", "required": True}]}
    df = pd.DataFrame({"id": [1, None, 3]})
    errors = validate_dataframe(df, spec)
    assert len(errors) == 1
    assert errors[0]["type"] == "null_values"
    assert errors[0]["column"] == "id"
    assert errors[0]["count"] == 1

def test_validate_dataframe_datatype_mismatch():
    spec = {"columns": [{"name": "id", "data_type": "INTEGER"}]}
    df = pd.DataFrame({"id": [1, "two", 3]})
    errors = validate_dataframe(df, spec)
    assert len(errors) == 1
    assert errors[0]["type"] == "datatype_mismatch"
    assert errors[0]["column"] == "id"
    assert errors[0]["expected"] == "INTEGER"

def test_validate_dataframe_invalid_category():
    spec = {
        "columns": [
            {
                "name": "status",
                "is_category_column": True,
                "permissible_values": ["active", "inactive"]
            }
        ]
    }
    df = pd.DataFrame({"status": ["active", "pending", "inactive", "pending"]})
    errors = validate_dataframe(df, spec)
    assert len(errors) == 1
    assert errors[0]["type"] == "invalid_category"
    assert errors[0]["column"] == "status"
    assert sorted(errors[0]["values"]) == ["pending"]

# Test cases for validate_table and _load_spec

def test_validate_table(tmp_path):
    spec_dir = tmp_path / "specs"
    spec_dir.mkdir()
    spec_file = spec_dir / "TestModel.json"
    spec_file.write_text('{"columns": [{"name": "value", "data_type": "INTEGER"}]}')

    df_good = pd.DataFrame({"value": [1, 2]})
    errors_good = validate_table(df_good, "test", str(spec_dir))
    assert not errors_good

    df_bad = pd.DataFrame({"value": [1.1, 2.2]})
    errors_bad = validate_table(df_bad, "test", str(spec_dir))
    assert len(errors_bad) == 1
    assert errors_bad[0]["type"] == "datatype_mismatch"

def test_load_spec_not_found(tmp_path):
    with pytest.raises(FileNotFoundError):
        _load_spec("nonexistent", str(tmp_path))

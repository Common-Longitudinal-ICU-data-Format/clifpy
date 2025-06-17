import pytest
import pandas as pd
import os
import json
import tempfile
from unittest.mock import patch, mock_open
from pyclif.utils.validator import (
    _is_varchar_dtype,
    _is_integer_dtype,
    _is_float_dtype,
    validate_dataframe,
    validate_table,
    _load_spec,
    ValidationError
)


class TestDtypeCheckers:
    """Tests for the internal datatype checker functions."""
    
    def test_is_varchar_dtype_with_string_dtype(self):
        """Test _is_varchar_dtype with pandas string dtype."""
        series = pd.Series(['a', 'b', 'c'], dtype='string')
        assert _is_varchar_dtype(series) is True
    
    def test_is_varchar_dtype_with_object_strings(self):
        """Test _is_varchar_dtype with object dtype containing strings."""
        series = pd.Series(['a', 'b', 'c'])
        assert _is_varchar_dtype(series) is True
    
    def test_is_varchar_dtype_with_mixed_objects(self):
        """Test _is_varchar_dtype with mixed object types."""
        series = pd.Series(['a', 1, 'c'])
        assert _is_varchar_dtype(series) is False
    
    def test_is_varchar_dtype_with_empty_series(self):
        """Test _is_varchar_dtype with empty series."""
        series = pd.Series([], dtype='object')
        assert _is_varchar_dtype(series) is True
    
    def test_is_integer_dtype(self):
        """Test _is_integer_dtype with various series."""
        # Integer series should pass
        assert _is_integer_dtype(pd.Series([1, 2, 3])) is True
        assert _is_integer_dtype(pd.Series([1, 2, 3], dtype='Int64')) is True
        
        # Float series should fail
        assert _is_integer_dtype(pd.Series([1.0, 2.0, 3.0])) is False
        
        # String series should fail
        assert _is_integer_dtype(pd.Series(['1', '2', '3'])) is False
    
    def test_is_float_dtype(self):
        """Test _is_float_dtype with various series."""
        # Float series should pass
        assert _is_float_dtype(pd.Series([1.0, 2.0, 3.0])) is True
        
        # Integer series should also pass (integers are numeric)
        assert _is_float_dtype(pd.Series([1, 2, 3])) is True
        
        # String series should fail
        assert _is_float_dtype(pd.Series(['1.0', '2.0', '3.0'])) is False


class TestValidateDataframe:
    """Tests for the validate_dataframe function."""
    
    def test_validate_dataframe_success(self):
        """Test successful validation with no errors."""
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
    
    def test_validate_dataframe_missing_columns(self):
        """Test validation with missing required columns."""
        spec = {"required_columns": ["id", "name", "age"]}
        df = pd.DataFrame({"id": [1, 2], "name": ["Alice", "Bob"]})
        errors = validate_dataframe(df, spec)
        assert len(errors) == 1
        assert errors[0]["type"] == "missing_columns"
        assert errors[0]["columns"] == ["age"]
    
    def test_validate_dataframe_null_values(self):
        """Test validation with null values in required columns."""
        spec = {
            "columns": [
                {"name": "id", "required": True},
                {"name": "name", "required": True}
            ]
        }
        df = pd.DataFrame({
            "id": [1, None, 3],
            "name": ["Alice", "Bob", None]
        })
        errors = validate_dataframe(df, spec)
        assert len(errors) == 2
        assert any(e["type"] == "null_values" and e["column"] == "id" and e["count"] == 1 for e in errors)
        assert any(e["type"] == "null_values" and e["column"] == "name" and e["count"] == 1 for e in errors)
    
    def test_validate_dataframe_datatype_mismatch(self):
        """Test validation with datatype mismatches."""
        spec = {
            "columns": [
                {"name": "id", "data_type": "INTEGER"},
                {"name": "age", "data_type": "FLOAT"},
                {"name": "name", "data_type": "VARCHAR"}
            ]
        }
        df = pd.DataFrame({
            "id": [1, "two", 3],  # Mixed types
            "age": ["25", "30", "35"],  # Strings instead of floats
            "name": ["Alice", "Bob", "Charlie"]  # Correct type
        })
        errors = validate_dataframe(df, spec)
        assert len(errors) == 2
        assert any(e["type"] == "datatype_mismatch" and e["column"] == "id" for e in errors)
        assert any(e["type"] == "datatype_mismatch" and e["column"] == "age" for e in errors)
    
    def test_validate_dataframe_invalid_category(self):
        """Test validation with invalid category values."""
        spec = {
            "columns": [
                {
                    "name": "status",
                    "is_category_column": True,
                    "permissible_values": ["active", "inactive", "pending"]
                },
                {
                    "name": "priority",
                    "is_category_column": True,
                    "permissible_values": ["high", "medium", "low"]
                }
            ]
        }
        df = pd.DataFrame({
            "status": ["active", "unknown", "inactive", "expired"],
            "priority": ["high", "medium", "critical", "urgent"]
        })
        errors = validate_dataframe(df, spec)
        assert len(errors) == 2
        
        status_error = next(e for e in errors if e["column"] == "status")
        assert status_error["type"] == "invalid_category"
        assert sorted(status_error["values"]) == ["expired", "unknown"]
        
        priority_error = next(e for e in errors if e["column"] == "priority")
        assert priority_error["type"] == "invalid_category"
        assert sorted(priority_error["values"]) == ["critical", "urgent"]
    
    def test_validate_dataframe_multiple_errors(self):
        """Test validation with multiple types of errors."""
        spec = {
            "required_columns": ["id", "name", "age", "status"],
            "columns": [
                {"name": "id", "required": True, "data_type": "INTEGER"},
                {"name": "name", "required": True, "data_type": "VARCHAR"},
                {"name": "status", "is_category_column": True, "permissible_values": ["active", "inactive"]}
            ]
        }
        df = pd.DataFrame({
            "id": [1, None, "3"],
            "name": ["Alice", "Bob", None],
            "status": ["active", "pending", "inactive"]
        })
        errors = validate_dataframe(df, spec)
        
        # Should have 4 errors: missing column, null values in id, null values in name, invalid category in status
        assert len(errors) == 4
        
        error_types = [e["type"] for e in errors]
        assert "missing_columns" in error_types
        assert "null_values" in error_types
        assert "datatype_mismatch" in error_types
        assert "invalid_category" in error_types


class TestLoadSpec:
    """Tests for the _load_spec function."""
    
    def test_load_spec_success(self, tmp_path):
        """Test successful loading of a spec file."""
        spec_dir = tmp_path / "specs"
        spec_dir.mkdir()
        spec_file = spec_dir / "PatientModel.json"
        
        test_spec = {
            "required_columns": ["patient_id"],
            "columns": [{"name": "patient_id", "required": True}]
        }
        
        with open(spec_file, "w", encoding="utf-8") as f:
            json.dump(test_spec, f)
        
        loaded_spec = _load_spec("patient", str(spec_dir))
        assert loaded_spec == test_spec
    
    def test_load_spec_not_found(self, tmp_path):
        """Test loading a non-existent spec file."""
        with pytest.raises(FileNotFoundError):
            _load_spec("nonexistent", str(tmp_path))
    
    @patch("builtins.open", new_callable=mock_open, read_data='{"invalid": "json"')
    def test_load_spec_invalid_json(self, mock_file):
        """Test loading an invalid JSON spec file."""
        with pytest.raises(json.JSONDecodeError):
            _load_spec("invalid", "/fake/path")


class TestValidateTable:
    """Tests for the validate_table function."""
    
    @patch("pyclif.utils.validator._load_spec")
    @patch("pyclif.utils.validator.validate_dataframe")
    def test_validate_table_calls_correct_functions(self, mock_validate_df, mock_load_spec):
        """Test that validate_table calls _load_spec and validate_dataframe with correct args."""
        # Setup mocks
        mock_spec = {"test": "spec"}
        mock_load_spec.return_value = mock_spec
        mock_validate_df.return_value = []
        
        # Test data
        df = pd.DataFrame({"test": [1, 2, 3]})
        
        # Call function
        result = validate_table(df, "patient", "/path/to/specs")
        
        # Verify calls
        mock_load_spec.assert_called_once_with("patient", "/path/to/specs")
        mock_validate_df.assert_called_once_with(df, mock_spec)
        assert result == []
    
    def test_validate_table_integration(self, tmp_path):
        """Integration test for validate_table with a real spec file."""
        # Create a temporary spec file
        spec_dir = tmp_path / "specs"
        spec_dir.mkdir()
        spec_file = spec_dir / "TestModel.json"
        
        test_spec = {
            "required_columns": ["id", "name"],
            "columns": [
                {"name": "id", "required": True, "data_type": "INTEGER"},
                {"name": "name", "required": True, "data_type": "VARCHAR"}
            ]
        }
        
        with open(spec_file, "w", encoding="utf-8") as f:
            json.dump(test_spec, f)
        
        # Test with valid data
        df_valid = pd.DataFrame({
            "id": [1, 2, 3],
            "name": ["Alice", "Bob", "Charlie"]
        })
        
        errors_valid = validate_table(df_valid, "test", str(spec_dir))
        assert not errors_valid
        
        # Test with invalid data
        df_invalid = pd.DataFrame({
            "id": [1, 2, 3]
            # Missing 'name' column
        })
        
        errors_invalid = validate_table(df_invalid, "test", str(spec_dir))
        assert len(errors_invalid) == 1
        assert errors_invalid[0]["type"] == "missing_columns"


class TestValidationError:
    """Tests for the ValidationError exception class."""
    
    def test_validation_error_creation(self):
        """Test creating a ValidationError with error details."""
        errors = [
            {"type": "missing_columns", "columns": ["name"]},
            {"type": "null_values", "column": "id", "count": 1}
        ]
        
        error = ValidationError(errors)
        
        assert str(error) == "Validation failed"
        assert error.errors == errors

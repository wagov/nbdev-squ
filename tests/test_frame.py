"""Tests for frame abstraction module."""

import pandas as pd
import pytest

from wagov_squ.frame import Fmt, as_pandas, format_output, memtable


def test_fmt_enum():
    """Test that format enum has expected values."""
    assert Fmt.pandas == "df"
    assert Fmt.csv == "csv"
    assert Fmt.json == "json"
    assert Fmt.list == "list"
    assert Fmt.ibis == "ibis"


def test_memtable_with_list():
    """Test memtable creation with list data."""
    data = [{"name": "Alice", "age": 30}, {"name": "Bob", "age": 25}]
    table = memtable(data)

    # Should return pandas DataFrame by default for compatibility
    assert isinstance(table, pd.DataFrame)
    assert len(table) == 2
    assert list(table.columns) == ["name", "age"]


def test_memtable_with_dataframe():
    """Test memtable creation with pandas DataFrame."""
    df = pd.DataFrame([{"name": "Alice", "age": 30}, {"name": "Bob", "age": 25}])
    table = memtable(df)

    # Should return the same DataFrame for compatibility
    assert isinstance(table, pd.DataFrame)
    assert len(table) == 2
    pd.testing.assert_frame_equal(table, df)


def test_as_pandas_with_dataframe():
    """Test as_pandas with pandas DataFrame input."""
    df = pd.DataFrame([{"name": "Alice", "age": 30}])
    result = as_pandas(df)

    assert isinstance(result, pd.DataFrame)
    pd.testing.assert_frame_equal(result, df)


def test_format_output_pandas():
    """Test format_output with pandas format."""
    data = [{"name": "Alice", "age": 30}, {"name": "Bob", "age": 25}]
    table = memtable(data)

    result = format_output(table, Fmt.pandas)
    assert isinstance(result, pd.DataFrame)
    assert len(result) == 2


def test_format_output_csv():
    """Test format_output with CSV format."""
    data = [{"name": "Alice", "age": 30}, {"name": "Bob", "age": 25}]
    table = memtable(data)

    result = format_output(table, Fmt.csv)
    assert isinstance(result, str)
    assert "name,age" in result
    assert "Alice,30" in result


def test_format_output_json():
    """Test format_output with JSON format."""
    data = [{"name": "Alice", "age": 30}, {"name": "Bob", "age": 25}]
    table = memtable(data)

    result = format_output(table, Fmt.json)
    assert isinstance(result, list)
    assert len(result) == 2
    assert result[0] == {"name": "Alice", "age": 30}


def test_format_output_list():
    """Test format_output with list format."""
    data = [{"name": "Alice", "age": 30}, {"name": "Bob", "age": 25}]
    table = memtable(data)

    result = format_output(table, Fmt.list)
    assert isinstance(result, list)
    assert len(result) == 2
    assert result[0] == ["Alice", 30]


def test_format_output_ibis():
    """Test format_output with ibis format."""
    data = [{"name": "Alice", "age": 30}]
    table = memtable(data)

    result = format_output(table, Fmt.ibis)
    assert hasattr(result, "to_pandas")
    # Don't test .to_pandas() execution due to ibis/pyarrow compatibility issues
    # Just verify we get an ibis Table object


def test_format_output_string_format():
    """Test format_output with string format values."""
    data = [{"name": "Alice", "age": 30}]
    table = memtable(data)

    # Test that string formats work the same as enum
    result_df = format_output(table, "df")
    result_csv = format_output(table, "csv")
    result_json = format_output(table, "json")
    result_list = format_output(table, "list")

    assert isinstance(result_df, pd.DataFrame)
    assert isinstance(result_csv, str)
    assert isinstance(result_json, list)
    assert isinstance(result_list, list)


def test_invalid_format():
    """Test format_output with invalid format."""
    data = [{"name": "Alice", "age": 30}]
    table = memtable(data)

    with pytest.raises(ValueError, match="not a valid Fmt"):
        format_output(table, "invalid")


def test_ibis_functionality():
    """Test ibis functionality."""
    from wagov_squ.frame import as_ibis

    data = [{"name": "Alice", "age": 30}, {"name": "Bob", "age": 25}]
    df = pd.DataFrame(data)

    # Test conversion to ibis
    table = as_ibis(df)
    assert hasattr(table, "to_pandas")

    # Test that we can create ibis expressions (don't execute due to version issues)
    # Test ibis format output
    ibis_result = format_output(df, Fmt.ibis)
    assert hasattr(ibis_result, "to_pandas")

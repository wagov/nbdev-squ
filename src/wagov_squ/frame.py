"""Frame abstraction layer for pandas/ibis compatibility."""

from __future__ import annotations

from enum import Enum
from typing import Any

import ibis
import pandas as pd


class Fmt(str, Enum):
    """Output format options."""

    pandas = "df"  # legacy default
    csv = "csv"
    json = "json"
    list = "list"
    ibis = "ibis"


def as_ibis(obj) -> Any:
    """Convert pandas DataFrame to ibis expression."""
    if hasattr(obj, "to_pandas"):  # Already an ibis expression
        return obj
    if isinstance(obj, pd.DataFrame):
        return ibis.memtable(obj)
    if isinstance(obj, (list, dict)):
        df = pd.DataFrame(obj)
        return ibis.memtable(df)
    raise TypeError(f"Cannot convert {type(obj)} to ibis")


def as_pandas(expr) -> pd.DataFrame:
    """Convert ibis expression to pandas DataFrame."""
    if hasattr(expr, "to_pandas"):  # ibis expression
        return expr.to_pandas()
    if isinstance(expr, pd.DataFrame):
        return expr
    if isinstance(expr, (list, dict)):
        return pd.DataFrame(expr)
    raise TypeError(f"Cannot convert {type(expr)} to pandas")


def format_output(expr: Any, fmt: str | Fmt) -> Any:
    """Format expression to requested output format."""
    fmt = Fmt(fmt) if isinstance(fmt, str) else fmt

    if fmt == Fmt.ibis:
        return as_ibis(expr)

    # Convert to pandas for all other formats
    df = as_pandas(expr)

    if fmt == Fmt.pandas:
        return df
    elif fmt == Fmt.csv:
        return df.to_csv(index=False)
    elif fmt == Fmt.json:
        return df.to_dict("records")
    elif fmt == Fmt.list:
        return df.values.tolist()
    else:
        raise ValueError(f"Unknown format: {fmt}")


def read_csv(path, **kwargs) -> Any:
    """Read CSV file using pandas for compatibility."""
    return pd.read_csv(path, **kwargs)


def read_parquet(path, **kwargs) -> Any:
    """Read Parquet file using pandas for compatibility."""
    return pd.read_parquet(path, **kwargs)


def memtable(data) -> Any:
    """Create in-memory table - prefer pandas for compatibility."""
    if isinstance(data, (list, dict)):
        return pd.DataFrame(data)
    return data


def concat(tables: list) -> Any:
    """Concatenate tables using appropriate backend."""
    if all(hasattr(t, "to_pandas") for t in tables):
        # Use ibis union for ibis tables
        return ibis.union(*tables, distinct=False)
    else:
        # Convert all to pandas and concat
        dfs = [as_pandas(t) for t in tables]
        return pd.concat(dfs, ignore_index=True)

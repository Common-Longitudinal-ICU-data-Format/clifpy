"""Shared test helpers for SOFA-2 subscore tests."""

from pathlib import Path

import pandas as pd


def to_total_seconds(x):
    """Convert timedelta/interval to total seconds for comparison."""
    if pd.isna(x):
        return None
    if isinstance(x, pd.Timedelta):
        return x.total_seconds()
    if isinstance(x, str):
        negative = x.startswith('-')
        time_str = x.lstrip('-')
        parts = time_str.split(':')
        hours, mins, secs = int(parts[0]), int(parts[1]), float(parts[2])
        total = hours * 3600 + mins * 60 + secs
        return -total if negative else total
    return None


def load_expected(fixtures_dir: Path, filename: str, case: str) -> pd.DataFrame:
    """Load expected CSV filtered to a specific case."""
    sort_cols = ['hospitalization_id', 'start_dttm']
    df = pd.read_csv(
        str(fixtures_dir / filename),
        dtype={'hospitalization_id': str},
    )
    return df[df['case'] == case].sort_values(sort_cols).reset_index(drop=True)


def _compare_column(result_series, expected_series, comparison_type):
    """Compare two series using the specified comparison type.

    Returns a boolean mask where True = mismatch.
    """
    if comparison_type == 'offset':
        r = result_series.apply(to_total_seconds)
        e = expected_series.apply(to_total_seconds)
    elif comparison_type == 'str':
        r = result_series.fillna('')
        e = expected_series.fillna('')
    else:
        # Int64, Float64
        r = result_series.astype(comparison_type)
        e = expected_series.astype(comparison_type)

    # NaN-safe equality: both NaN = match, one NaN = mismatch
    both_na = r.isna() & e.isna()
    equal = r == e
    return ~(both_na | equal), r, e


def assert_columns_match(result_df, expected_df, column_specs):
    """Check multiple columns against expected, collecting all failures.

    Parameters
    ----------
    result_df : pd.DataFrame
        Actual output from the subscore function.
    expected_df : pd.DataFrame
        Expected output loaded from CSV (must have 'hospitalization_id',
        'start_dttm', 'notes' columns for error context).
    column_specs : list of (column_name, comparison_type)
        Each tuple is (col, type) where type is one of:
        'Int64', 'Float64', 'str', 'offset'.
    """
    # Row count check first
    assert len(result_df) == len(expected_df), (
        f"Row count mismatch: expected {len(expected_df)}, got {len(result_df)}"
    )

    failures = {}
    for col, ctype in column_specs:
        mismatch_mask, r_vals, e_vals = _compare_column(
            result_df[col], expected_df[col], ctype,
        )
        if mismatch_mask.any():
            ctx = expected_df[['hospitalization_id', 'start_dttm', 'notes']].copy()
            ctx['expected'] = expected_df[col]
            ctx['actual'] = result_df[col]
            failures[col] = ctx[mismatch_mask]

    if failures:
        parts = [f"{len(failures)} column(s) have mismatches:\n"]
        for col, mismatch_df in failures.items():
            n = len(mismatch_df)
            parts.append(f"  Column '{col}' — {n} mismatch(es):")
            parts.append(
                mismatch_df.to_string(index=False, max_colwidth=60)
            )
            parts.append('')
        assert False, '\n'.join(parts)

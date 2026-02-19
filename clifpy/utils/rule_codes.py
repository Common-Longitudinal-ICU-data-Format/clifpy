"""Standardized DQA rule code registry and issue enrichment helpers."""

import re
from typing import Any, Dict, Optional

# (category, check_type) -> (code, description)
RULE_CODES: Dict[tuple, tuple] = {
    ('conformance', 'table_presence'):              ('C.1', 'Table presence verification'),
    ('conformance', 'required_columns'):            ('C.2', 'Required columns presence check'),
    ('conformance', 'column_dtypes'):               ('C.3', 'Column data type validation'),
    ('conformance', 'datetime_format'):             ('C.4', 'Datetime format validation'),
    ('conformance', 'categorical_values'):          ('C.5', 'Categorical values conformance'),
    ('conformance', 'category_group_mapping'):      ('C.6', 'Category-to-group mapping validation'),
    ('conformance', 'lab_reference_units'):         ('C.7', 'Lab reference unit validation'),

    ('completeness', 'missingness'):                ('K.1', 'Required column missingness'),
    ('completeness', 'conditional_requirements'):   ('K.2', 'Conditional field requirements'),
    ('completeness', 'mcide_value_coverage'):       ('K.3', 'mCIDE value coverage'),
    ('completeness', 'relational_integrity'):       ('K.4', 'Foreign key referential coverage'),

    ('plausibility', 'temporal_ordering'):          ('P.1', 'Temporal ordering constraints'),
    ('plausibility', 'numeric_range_plausibility'): ('P.2', 'Numeric range plausibility'),
    ('plausibility', 'field_plausibility'):         ('P.3', 'Field-level plausibility rules'),
    ('plausibility', 'medication_dose_unit_consistency'): ('P.4', 'Medication dose-unit consistency'),
    ('plausibility', 'overlapping_periods'):        ('P.5', 'Overlapping time period detection'),
    ('plausibility', 'category_temporal_consistency'): ('P.6', 'Category temporal consistency'),
    ('plausibility', 'duplicate_composite_keys'):   ('P.7', 'Duplicate composite key detection'),
    ('plausibility', 'cross_table_temporal'):       ('P.8', 'Cross-table temporal plausibility'),
}

# INFO messages that indicate a check was not applicable (never actually ran).
# These are filtered out of issue tables to reduce noise.
_NOT_APPLICABLE_PREFIXES = (
    "No lab reference units defined in schema",
    "No category-to-group mappings defined in schema",
    "No conditional requirements defined for this table",
    "No temporal ordering rules defined for this table",
    "No field plausibility rules defined for this table",
    "No numeric range configuration for this table",
    "Medication dose unit check not applicable",
    "Missing hospitalization_id column; skipping",
    "No composite keys defined for this table",
    "No suitable datetime column found for temporal consistency check",
    "No category columns found for temporal consistency check",
    "No numeric columns with range configuration to check",
)


def extract_column_field(issue: Dict[str, Any]) -> str:
    """Extract the affected column name from an issue dict.

    Priority: details.column > details.extra_columns > parse from message > 'NA'.
    """
    details = issue.get('details', {})
    if not isinstance(details, dict):
        return 'NA'

    # Direct column field
    col = details.get('column')
    if col:
        return str(col)

    # extra_columns (list of column names)
    extra = details.get('extra_columns')
    if extra and isinstance(extra, list):
        return ', '.join(str(c) for c in extra[:3])

    # required_column (from conditional_requirements check)
    req_col = details.get('required_column')
    if req_col:
        return str(req_col)

    # columns_checked (from temporal consistency and similar checks)
    checked = details.get('columns_checked')
    if checked and isinstance(checked, list):
        return ', '.join(str(c) for c in checked[:3])

    # missing_columns (from required_columns check)
    missing = details.get('missing_columns')
    if missing and isinstance(missing, list):
        return ', '.join(str(c) for c in missing[:3])

    # Category-group mapping columns
    cat_col = details.get('category_column')
    grp_col = details.get('group_column')
    if cat_col and grp_col:
        return f"{cat_col}, {grp_col}"

    # invalid_values list with column names
    invalid = details.get('invalid_values')
    if invalid and isinstance(invalid, list) and isinstance(invalid[0], dict):
        col_name = invalid[0].get('column')
        if col_name:
            return str(col_name)

    # Try to parse column name from message: patterns like "Column 'foo'" or "'foo' column"
    msg = issue.get('message', '')
    m = re.search(r"[Cc]olumn\s+'([^']+)'", msg)
    if m:
        return m.group(1)
    m = re.search(r"'([^']+)'\s+column", msg)
    if m:
        return m.group(1)

    return 'NA'


def build_finding(message: str, details: Dict[str, Any]) -> str:
    """Build a rich finding string by appending key detail excerpts to the message.

    Inspects the details dict for known structures (top_invalid, missing_columns,
    orphan IDs, etc.) and appends a concise summary to the base message.
    """
    if not isinstance(details, dict) or not details:
        return message

    parts = [message]

    # Categorical: top invalid values with counts (replaces generic message)
    top_invalid = details.get('top_invalid')
    if top_invalid and isinstance(top_invalid, list):
        items = []
        for it in top_invalid[:5]:
            if isinstance(it, dict) and 'value' in it:
                count = it.get('count')
                if count is not None:
                    items.append(f"'{it['value']}' ({count:,} rows)")
                else:
                    items.append(f"'{it['value']}'")
            else:
                items.append(str(it))
        suffix = f" ... ({len(top_invalid)} total)" if len(top_invalid) > 5 else ""
        parts = [f"Invalid: {', '.join(items)}{suffix}"]

    # Missing columns — skip if the base message already lists them
    missing_cols = details.get('missing_columns')
    if missing_cols and isinstance(missing_cols, list) and 'required columns' not in message:
        cols = ', '.join(str(c) for c in missing_cols[:5])
        suffix = f" ... ({len(missing_cols)} total)" if len(missing_cols) > 5 else ""
        parts.append(f"Missing: {cols}{suffix}")

    # Lab reference: top invalid units
    top_units = details.get('top_invalid_units')
    if top_units and isinstance(top_units, list):
        items = []
        for it in top_units[:5]:
            if isinstance(it, dict):
                cat = it.get('lab_category', it.get('category', '?'))
                unit = it.get('unit', it.get('reference_unit', '?'))
                items.append(f"{cat}: '{unit}'")
            else:
                items.append(str(it))
        parts.append(f"Units: {', '.join(items)}")

    # Category-group mapping: mismatched pairs (replaces generic message)
    mismatched = details.get('mismatched_pairs')
    if mismatched and isinstance(mismatched, list):
        items = []
        for it in mismatched[:3]:
            if isinstance(it, dict):
                cat = it.get('category', '?')
                actual = it.get('actual_group', '?')
                expected = it.get('expected_group', '?')
                items.append(f"{cat}: found '{actual}', expected '{expected}'")
            else:
                items.append(str(it))
        suffix = f" ... ({len(mismatched)} total)" if len(mismatched) > 3 else ""
        parts = [f"Mismatched: {', '.join(items)}{suffix}"]

    # Conditional requirements: missing counts
    rows_missing = details.get('rows_with_missing')
    if rows_missing is not None:
        total = details.get('rows_meeting_condition', 0)
        pct = details.get('percent_missing', 0)
        req_col = details.get('required_column', '')
        parts.append(f"{req_col}: {rows_missing:,}/{total:,} rows missing ({pct}%)")

    # Return joined parts if we added or replaced content beyond the original message
    if parts == [message]:
        return message
    return ' | '.join(parts)


def truncate_comment(message: str, max_len: int = 400) -> str:
    """Truncate a message for PDF display, preserving full text in CSV."""
    if len(message) <= max_len:
        return message
    return message[:max_len - 3] + '...'


def enrich_issue(issue: Dict[str, Any], check_key: Optional[str] = None) -> Optional[Dict[str, Any]]:
    """Add rule_code, rule_description, column_field, and finding to an issue dict.

    Parameters
    ----------
    issue : dict
        Must have 'category' and 'check_type' keys.
    check_key : str, optional
        The dict key from the results (e.g. FK column name for relational checks).

    Returns
    -------
    dict or None
        The same dict, mutated with added fields.  Returns ``None`` for
        INFO-level messages that indicate a check was not applicable, so
        callers can skip them.
    """
    # Filter out "not applicable" INFO messages
    if issue.get('severity') == 'info':
        msg = issue.get('message', '')
        if any(msg.startswith(prefix) for prefix in _NOT_APPLICABLE_PREFIXES):
            return None

    category = issue.get('category', '')
    check_type = issue.get('check_type', '')

    # Look up rule code
    code, desc = RULE_CODES.get((category, check_type), ('', ''))

    issue['rule_code'] = code
    issue['rule_description'] = desc
    issue['column_field'] = extract_column_field(issue)
    issue['finding'] = build_finding(issue.get('message', ''), issue.get('details', {}))

    # For relational checks, the check_key IS the FK column
    if check_type == 'relational_integrity' and check_key and issue['column_field'] == 'NA':
        issue['column_field'] = check_key

    return issue

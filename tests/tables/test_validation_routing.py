"""validate() sorts each validator finding by the DQA group that raised it.

`BaseTable.isvalid()` documents that "Valid" means schema conformance, and
that completeness and plausibility findings live in `warnings` without
affecting the result. The validator stamps every finding with its
`check_group` ('conformance', 'completeness' or 'plausibility'); before, it
set none, so every finding defaulted to conformance and a one-row table that
could not cover every mCIDE value read as invalid.
"""
import logging
import tempfile

import pandas as pd
import pytest

from clifpy.tables.medication_admin_continuous import MedicationAdminContinuous


def _table(**overrides):
    row = {
        'hospitalization_id': 'H001', 'admin_dttm': pd.Timestamp('2023-01-01 10:00'),
        'med_category': 'propofol', 'med_group': 'sedation', 'med_dose': 10.0,
        'med_dose_unit': 'mcg/kg/min', 'med_route_category': 'iv',
        'mar_action_category': 'start', 'mar_action_group': 'administered',
    }
    row.update(overrides)
    table = MedicationAdminContinuous(
        data=pd.DataFrame([row]), output_directory=tempfile.mkdtemp())
    table.validate()
    return table


def test_data_quality_findings_are_warnings_not_errors():
    """One conforming row cannot cover every mCIDE value: the coverage gap is
    a data-quality finding, so the table is still valid."""
    table = _table()
    assert 'mCIDE Coverage Gap' in {f['type'] for f in table.warnings}
    assert {f['check_group'] for f in table.warnings} <= {'completeness', 'plausibility'}
    assert table.errors == []
    assert table.isvalid() is True


@pytest.mark.parametrize('overrides, expected', [
    ({'med_category': 'not_a_category'}, 'Invalid Categorical Values'),
    ({'med_dose': 'ten'}, 'Data Type Mismatch'),
])
def test_conformance_findings_stay_errors(overrides, expected):
    """A value outside the vocabulary or a wrong type is a conformance
    finding, so the table is invalid, while the coverage gap beside it is
    still only a warning."""
    table = _table(**overrides)
    assert expected in {f['type'] for f in table.errors}
    assert {f['check_group'] for f in table.errors} == {'conformance'}
    assert 'mCIDE Coverage Gap' not in {f['type'] for f in table.errors}
    assert table.isvalid() is False

"""Generate ``golden_legacy_parity.csv`` from the pre-refactor converter (run once).

The fixture freezes what the single-file converter at commit 5cd5fcd
(``clifpy/utils/unit_converter.py``, before the package split) returned for
every legacy unit pairing, so that ``test_golden_legacy_parity.py`` can prove
the package still returns the same dose, unit and status.

Grid (synthetic; no patient data):

- targets: the 72 legacy acceptable units (``ALL_ACCEPTABLE_UNITS`` at
  5cd5fcd), one category each (``c00`` .. ``c71``), plus one category with no
  preferred unit (``base_fallback``) that pins the base-unit fallback path;

- raw spellings: 86 slots, the same 72 units followed by the 14 in
  ``EXTRA_RAW_SPELLINGS`` (85 distinct strings: ``mcg/lb/hr`` is in both lists
  and so runs at two doses);

- weight: a pre-filled ``weight_kg`` of 70.0 or missing, so no vitals lookup runs.

73 x 86 x 2 = 12,556 rows. Columns: the inputs (``grid_row`` is the join key)
and the legacy outputs ``med_dose_converted``, ``med_dose_unit_converted`` and
``_convert_status``. Missing values are written as ``<NA>``, so the empty
spelling ``''`` survives the round trip.

Run once, from the repository root (Python >= 3.10, because the monolith
imports ``types.NoneType``)::

    uv run python tests/utils/med_unit_converter/_make_golden_legacy_parity.py

Rerunning it reproduces the CSV byte for byte. Never regenerate the fixture to
make a failing parity test pass: a failure means legacy behaviour changed.
"""
from __future__ import annotations

import importlib.util
import logging
import subprocess
from pathlib import Path
from types import ModuleType

import pandas as pd

LEGACY_REV = '5cd5fcd'
LEGACY_SOURCE = 'clifpy/utils/unit_converter.py'
REPO_ROOT = Path(__file__).resolve().parents[3]
FIXTURE = Path(__file__).with_name('golden_legacy_parity.csv')

# Raw spellings beyond the 72 canonical legacy units (the same 14 the audit's
# coverage lens used): case and space variants, long names, a per-dose unit and
# free text the monolith rejects, empty and missing units, and
# 'Nanogram/kg/min', which the monolith rejected and the CLIF 3.0 grammar
# converts (a declared change; see test_golden_legacy_parity.py).
EXTRA_RAW_SPELLINGS = (
    'MCG/KG/HR', 'mL / hr', 'units/hour', 'Units/Minute', 'milliliter/hour',
    'µg/kg/h', 'mg/kg/dose', 'asord', '', None, 'gram', 'Nanogram/kg/min',
    'L/min', 'mcg/lb/hr',
)
WEIGHTS_KG = (70.0, None)
FALLBACK_CATEGORY = 'base_fallback'
NA_TOKEN = '<NA>'
INPUT_COLUMNS = [
    'grid_row', 'med_category', 'preferred_unit', 'med_dose_unit', 'weight_kg', 'med_dose',
]
OUTPUT_COLUMNS = ['med_dose_converted', 'med_dose_unit_converted', '_convert_status']


def load_legacy_converter() -> ModuleType:
    """Import the 5cd5fcd monolith from git under a private module name."""
    source = subprocess.run(
        ['git', '-C', str(REPO_ROOT), 'show', f'{LEGACY_REV}:{LEGACY_SOURCE}'],
        capture_output=True, text=True, check=True,
    ).stdout
    name = f'_legacy_unit_converter_{LEGACY_REV}'
    module = importlib.util.module_from_spec(importlib.util.spec_from_loader(name, loader=None))
    exec(compile(source, f'{LEGACY_REV}:{LEGACY_SOURCE}', 'exec'), module.__dict__)
    return module


def build_grid(legacy_units: list[str]) -> pd.DataFrame:
    """One row per target x raw-spelling slot x weight, in a fixed order."""
    raw_spellings = list(legacy_units) + list(EXTRA_RAW_SPELLINGS)
    targets = [(f'c{i:02d}', unit) for i, unit in enumerate(legacy_units)]
    targets.append((FALLBACK_CATEGORY, None))
    rows = []
    for category, preferred in targets:
        for j, raw in enumerate(raw_spellings):
            for weight in WEIGHTS_KG:
                rows.append({
                    'grid_row': len(rows),
                    'med_category': category,
                    'preferred_unit': preferred,
                    'med_dose_unit': raw,
                    'weight_kg': weight,
                    'med_dose': (j + 1) * 1.5,
                })
    return pd.DataFrame(rows, columns=INPUT_COLUMNS)


def run_converter(convert, grid: pd.DataFrame) -> pd.DataFrame:
    """Convert ``grid`` with ``convert``; return the inputs plus the three outputs."""
    med = grid.drop(columns='preferred_unit').assign(
        hospitalization_id='H0', admin_dttm=pd.Timestamp('2024-01-01'),
    )
    targets = grid.dropna(subset=['preferred_unit']).drop_duplicates('med_category')
    out, _ = convert(
        med,
        preferred_units=dict(zip(targets['med_category'], targets['preferred_unit'])),
        override=True,
    )
    if len(out) != len(grid) or not out['grid_row'].is_unique:
        raise RuntimeError(f'converter returned {len(out)} rows for a {len(grid)}-row grid')
    return grid.join(out.set_index('grid_row')[OUTPUT_COLUMNS], on='grid_row')


def main() -> None:
    logging.disable(logging.WARNING)  # override=True logs one warning per failure class
    legacy = load_legacy_converter()
    grid = build_grid(sorted(legacy.ALL_ACCEPTABLE_UNITS))
    golden = run_converter(legacy.convert_dose_units_by_med_category, grid)
    golden.to_csv(FIXTURE, index=False, na_rep=NA_TOKEN, lineterminator='\n')
    print(f'{FIXTURE.name}: {len(golden):,} rows, {FIXTURE.stat().st_size:,} bytes')
    print(golden['_convert_status'].value_counts().to_string())


if __name__ == '__main__':
    main()

"""The med unit guide's weight figures stay true to the code.

The figures are drawn by `dev/docs_figures/med_unit_conversion.py`, which
computes every served weight with `attach_weight` itself. These tests pin
that table to the values the guide states, check that the script writes
both PNGs reproducibly, and that the guide still embeds them.
"""
import importlib.util
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[3]
SCRIPT = ROOT / 'dev' / 'docs_figures' / 'med_unit_conversion.py'
GUIDE = ROOT / 'docs' / 'user-guide' / 'med-unit-conversion.md'
FIGURES = ('weight-strategies.png', 'staleness-window.png')


def _figures():
    spec = importlib.util.spec_from_file_location('med_unit_figures', SCRIPT)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@pytest.mark.unit_conversion
def test_figure_one_serves_what_the_guide_states():
    served = _figures().served_weights()['strategies']
    assert served.rows() == [
        ('H_ASOF', 'asof', 72.0, 1.0),
        ('H_AFTER', 'same_hosp_earliest', 75.0, -1.0),
        ('H_PRIOR', 'prior_hosp_latest', 69.0, 1609.0),
        ('H_LATER', 'later_hosp_earliest', 90.0, -1271.0),
        ('H_NONE', None, None, None),
    ]


@pytest.mark.unit_conversion
def test_figure_two_serves_what_the_guide_states():
    served = _figures().served_weights()['staleness']
    assert {
        (r['scenario'], r['cap']): (r['_weight_source'], r['weight_kg'], r['_weight_age_hours'])
        for r in served.iter_rows(named=True)
    } == {
        ('on the boundary', None): ('asof', 62.0, 168.0),
        ('on the boundary', 7): ('asof', 62.0, 168.0),
        ('a day past it', None): ('asof', 62.0, 192.0),
        ('a day past it', 7): ('same_hosp_earliest', 64.0, -1.0),
    }


@pytest.mark.unit_conversion
def test_figure_script_writes_both_pngs_reproducibly(tmp_path):
    figures = _figures()
    first, second = tmp_path / 'a', tmp_path / 'b'
    figures.main(first)
    figures.main(second)
    for name in FIGURES:
        png = (first / name).read_bytes()
        assert png.startswith(b'\x89PNG'), name
        assert png == (second / name).read_bytes(), f'{name} differs between runs'


@pytest.mark.unit_conversion
def test_guide_embeds_both_figures():
    text = GUIDE.read_text(encoding='utf-8')
    for name in FIGURES:
        assert f'](../images/med-unit-conversion/{name})' in text, name
        assert (ROOT / 'docs' / 'images' / 'med-unit-conversion' / name).is_file(), name

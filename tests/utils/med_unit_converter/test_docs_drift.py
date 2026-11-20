"""Drift guards between the unit-converter docs and the code.

The user guide (docs/user-guide/med-unit-conversion.md) and the API page
(docs/api/utilities.md) restate facts the code defines: the accepted unit
vocabulary, the base unit of every reference-table spelling, the countable
token set, the status strings, the return formats and the public surface.
Each test re-derives one of those facts from the code and compares it with the
page or the docstring, so a change that forgets the docs fails here instead of
shipping a stale page.

Sybil (root conftest.py) executes the pages' python fences; these tests cover
the prose, tables and docstrings Sybil cannot execute.
"""

from __future__ import annotations

import importlib
import inspect
import re
from pathlib import Path

import polars as pl
import pytest

import clifpy.utils.unit_converter as uc

REPO_ROOT = Path(__file__).resolve().parents[3]
GUIDE = REPO_ROOT / 'docs' / 'user-guide' / 'med-unit-conversion.md'
API_PAGE = REPO_ROOT / 'docs' / 'api' / 'utilities.md'
PACKAGE_DIR = Path(uc.__file__).parent

# A numpydoc section header: a title line followed by a dashed underline.
_NEXT_SECTION = r'(?=^\w[\w ]*\n-+\n|\Z)'


def _numpy_section(doc: str, name: str) -> str:
    """Body of one numpydoc section (e.g. 'Parameters'), or '' if absent."""
    m = re.search(rf'^{name}\n-+\n(.*?){_NEXT_SECTION}', doc, flags=re.S | re.M)
    return m.group(1) if m else ''


# ===========================================
# Docstrings
# ===========================================

PUBLIC_FUNCTIONS = (
    uc.convert_dose_units_by_med_category,
    uc.standardize_med_dose_units,
    uc.standardize_dose_to_base_units,
    uc.attach_weight,
    uc.find_most_recent_weight,
    uc.load_dose_unit_targets,
    uc.register_target_reader,
)

# The entry points that take `return_format` (D-261022-return-format).
RETURN_FORMAT_FUNCTIONS = PUBLIC_FUNCTIONS[:4]

# Internal stages: a DuckDB relation in, a DuckDB relation out.
INTERNAL_RELATION_FUNCTIONS = (
    uc._convert_clean_units_to_base_units,
    uc._convert_base_units_to_preferred_units,
    uc._create_unit_conversion_counts_table,
)

# Every function whose docstring types a frame argument or result.
FRAME_FUNCTIONS = PUBLIC_FUNCTIONS[:5] + INTERNAL_RELATION_FUNCTIONS + (
    uc._clean_dose_unit_formats_duckdb,
    uc._clean_dose_unit_names_duckdb,
)


def _entry_names(header: str) -> list:
    """Parameter names on one numpydoc entry line ('a, b : str', '**kwargs')."""
    return [part.strip().lstrip('*') for part in header.split(' : ', 1)[0].split(',')]


def _documented_parameters(func) -> list:
    """Names in a numpydoc Parameters section, in order and with repeats."""
    names = []
    for line in _numpy_section(inspect.getdoc(func) or '', 'Parameters').splitlines():
        if line and not line[0].isspace():
            names.extend(_entry_names(line))
    return names


def _parameter_entry(func, name: str) -> str:
    """One Parameters entry of `func`'s docstring, header and body, or ''."""
    lines = _numpy_section(inspect.getdoc(func) or '', 'Parameters').splitlines()
    for i, line in enumerate(lines):
        if line and not line[0].isspace() and name in _entry_names(line):
            body = [line]
            for nxt in lines[i + 1:]:
                if nxt and not nxt[0].isspace():
                    break
                body.append(nxt)
            return '\n'.join(body)
    return ''


@pytest.mark.unit_conversion
@pytest.mark.parametrize('func', PUBLIC_FUNCTIONS, ids=lambda f: f.__name__)
def test_public_docstrings_document_every_parameter(func):
    documented = _documented_parameters(func)
    missing = sorted(set(inspect.signature(func).parameters) - set(documented))
    repeated = sorted({name for name in documented if documented.count(name) > 1})
    assert not missing and not repeated, (
        f'{func.__name__} docstring: undocumented {missing}, '
        f'documented more than once {repeated}'
    )


@pytest.mark.unit_conversion
@pytest.mark.parametrize('func', INTERNAL_RELATION_FUNCTIONS, ids=lambda f: f.__name__)
def test_internal_returns_name_the_relation(func):
    returns = _numpy_section(inspect.getdoc(func), 'Returns')
    assert 'DuckDBPyRelation' in returns, (
        f'{func.__name__} documents its return as {returns.split()[:1]}'
    )


@pytest.mark.unit_conversion
@pytest.mark.parametrize('func', RETURN_FORMAT_FUNCTIONS, ids=lambda f: f.__name__)
def test_public_returns_follow_return_format(func):
    returns = _numpy_section(inspect.getdoc(func), 'Returns')
    assert 'return_format' in returns and re.search(r'\bpolars\b|\bpl\.DataFrame\b', returns), (
        f'{func.__name__} Returns does not say the result follows return_format '
        f'(polars by default): {returns.split()[:1]}'
    )


@pytest.mark.unit_conversion
def test_return_rel_documented_as_deprecated():
    entry = _parameter_entry(uc.convert_dose_units_by_med_category, 'return_rel')
    assert 'deprecated' in entry.lower() and "return_format='duckdb'" in entry, (
        f"return_rel is not documented as a deprecated alias of return_format='duckdb': {entry!r}"
    )
    forwarded = _parameter_entry(uc.standardize_med_dose_units, 'kwargs')
    assert '`return_rel`' not in forwarded, (
        'standardize_med_dose_units still lists return_rel among the forwarded kwargs'
    )


# Each listing of the `_unit_class` values: from a mention of `_unit_class` to
# the next 'unrecognized' in the same paragraph, never running on into a blank
# line or a later `_unit_class`.
_UNIT_CLASS_VALUES = re.compile(
    r'_unit_class(?:(?!_unit_class|\n[ \t]*\n).)*?unrecognized', re.S,
)


@pytest.mark.unit_conversion
@pytest.mark.parametrize('func', (
    uc.standardize_dose_to_base_units,
    uc._convert_clean_units_to_base_units,
    uc._convert_base_units_to_preferred_units,
    uc._create_unit_conversion_counts_table,
), ids=lambda f: f.__name__)
def test_unit_class_docstrings_list_countable(func):
    listings = _UNIT_CLASS_VALUES.findall(inspect.getdoc(func))
    assert listings, f'{func.__name__} docstring no longer lists the _unit_class values'
    stale = [' '.join(text.split()) for text in listings if 'countable' not in text]
    assert not stale, f'{func.__name__} lists _unit_class values without countable: {stale}'


_PANDAS_TYPE = re.compile(r'\bpd\.|\bpandas\b')
_POLARS_TYPE = re.compile(r'\bpl\.|\bpolars\b')


def _pandas_only_types(func) -> list:
    """Parameter types naming pandas but not polars, and a Returns section that
    names pandas without tying it to `return_format` (D-261022-pandas-input)."""
    doc = inspect.getdoc(func) or ''
    stale = [
        line.strip()
        for line in _numpy_section(doc, 'Parameters').splitlines()
        if line and not line[0].isspace() and ' : ' in line
        and _PANDAS_TYPE.search(line) and not _POLARS_TYPE.search(line)
    ]
    returns = _numpy_section(doc, 'Returns')
    if _PANDAS_TYPE.search(returns) and 'return_format' not in returns:
        stale.append('Returns: ' + returns.strip().splitlines()[0])
    return stale


@pytest.mark.unit_conversion
@pytest.mark.parametrize('func', FRAME_FUNCTIONS, ids=lambda f: f.__name__)
def test_docstrings_type_frames_polars_first(func):
    stale = _pandas_only_types(func)
    assert not stale, f'{func.__name__} types frames as pandas only: {stale}'


@pytest.mark.unit_conversion
def test_package_docstring_names_every_submodule():
    modules = sorted(p.stem for p in PACKAGE_DIR.glob('_*.py') if p.stem != '__init__')
    missing = [m for m in modules if f'`{m}`' not in (uc.__doc__ or '')]
    assert not missing, f'package docstring does not mention {missing}'


# ===========================================
# API reference page
# ===========================================

_API_ENTRY = re.compile(r'^::: (clifpy\.utils\.unit_converter\.[\w.]+)\s*$', re.M)
# Both fence styles Sybil runs; the closing fence must match the opening one.
_PY_FENCE = re.compile(
    r'^[ \t]*(?P<fence>```|~~~)python[ \t]*\n(?P<code>.*?)^[ \t]*(?P=fence)', re.S | re.M,
)
_INVISIBLE_PY = re.compile(r'<!--\s*invisible-code-block:\s*python\n(.*?)-->', re.S)


def _runnable_snippets(page: Path) -> list:
    """Every snippet Sybil runs on `page`: python fences and invisible blocks."""
    text = page.read_text(encoding='utf-8')
    fenced = [m.group('code') for m in _PY_FENCE.finditer(text)]
    return fenced + _INVISIBLE_PY.findall(text)


# Anything that points a runnable example at something other than the bundled
# open demo: a site or example config, a config_path argument, the network,
# Box, or a restricted build or site.
_OFFLINE_RISK = re.compile(
    r'''config/[^'"\s]*\.ya?ml|config_path\s*=|https?://|Box-Box|CloudStorage'''
    r'|CLIF-MIMIC/output|output_phi|intermediate_phi|northwestern|rush_config|ucmc',
    re.I,
)


def _offline_offenders(page: Path) -> list:
    """First line of every snippet Sybil would run that matches `_OFFLINE_RISK`
    (a config yaml, a `config_path=` argument, a URL, Box or CloudStorage, a
    restricted CLIF-MIMIC, PHI-tier or site-named path)."""
    return [
        (code.strip().splitlines() or [''])[0]
        for code in _runnable_snippets(page)
        if _OFFLINE_RISK.search(code)
    ]


@pytest.mark.unit_conversion
def test_api_page_code_runs_offline_on_open_data():
    offenders = _offline_offenders(API_PAGE)
    assert not offenders, f'snippets reading a config, restricted data or the network: {offenders}'


_RISKY_LINES = (
    "path = 'config/ucmc_config.yaml'",
    "path = 'northwestern_config.yaml'",
    "path = '/Users/x/Library/Box-Box/data'",
    "path = '/Users/x/Library/CloudStorage/data'",
    "path = '../CLIF-MIMIC/output/x'",
    "path = 'config/config.yaml'",
    "url = 'https://example.org/data.csv'",
)


def _page_with(tmp_path: Path, fence: str, body: str, lang: str = 'python') -> Path:
    page = tmp_path / 'page.md'
    page.write_text(f'text\n\n{fence}{lang}\n{body}\n{fence}\n', encoding='utf-8')
    return page


@pytest.mark.unit_conversion
@pytest.mark.parametrize('line', _RISKY_LINES)
def test_offline_guard_flags_each_risk(tmp_path, line):
    assert _offline_offenders(_page_with(tmp_path, '```', line)) == [line]


@pytest.mark.unit_conversion
def test_offline_guard_sees_tilde_fences(tmp_path):
    line = "path = 'config/config.yaml'"
    assert _offline_offenders(_page_with(tmp_path, '~~~', line)) == [line]


@pytest.mark.unit_conversion
def test_offline_guard_ignores_demo_and_non_python_fences(tmp_path):
    demo = "path = 'clifpy/data/clif_demo/3.0'"
    assert _offline_offenders(_page_with(tmp_path, '```', demo)) == []
    assert _offline_offenders(
        _page_with(tmp_path, '```', "path = 'config/config.yaml'", lang='text'),
    ) == []


@pytest.mark.unit_conversion
def test_api_page_documents_the_public_surface():
    entries = _API_ENTRY.findall(API_PAGE.read_text(encoding='utf-8'))
    unresolved = [
        dotted for dotted in entries
        if not hasattr(
            importlib.import_module(dotted.rpartition('.')[0]), dotted.rpartition('.')[2],
        )
    ]
    documented = {dotted.rpartition('.')[2] for dotted in entries}
    undocumented = sorted(set(uc.__all__) - documented)
    assert not unresolved, f'API entries that do not resolve: {unresolved}'
    assert not undocumented, f'public names missing from {API_PAGE.name}: {undocumented}'


# ===========================================
# User guide
# ===========================================

ACCEPTABLE_BEGIN = '<!-- acceptable-units:begin -->'
ACCEPTABLE_END = '<!-- acceptable-units:end -->'
NOT_CONVERTED = '*(not converted)*'


def _code(tokens) -> str:
    return ', '.join(f'`{t}`' for t in tokens)


def _shown(token: str) -> str:
    """A grammar token as users see it: the unit family spelled out."""
    return uc.CANONICAL_UNIT_SPELLING.get(token, token)


def _acceptable_units_markdown() -> str:
    """The guide's accepted-unit block, rendered from the grammar constants."""
    def family_lines(standalone: bool) -> list:
        return [
            line
            for name, spec in uc.SUBCLASS_SPEC.items()
            if (name in uc.STANDALONE_SUBCLASSES) == standalone
            for line in (
                f"    - {name}: {_code(_shown(t) for t in spec['tokens'])} "
                f"(base `{_shown(spec['base'])}`)", '')
        ]

    base_time = [t for t in uc.ACCEPTABLE_TIME_UNITS if t not in uc.TIME_TO_BASE_FACTOR]
    weights = [w for w in uc.ACCEPTABLE_WEIGHT_UNITS if w]
    lines = [
        '- **amount**, combined with an optional weight and time unit:', '',
        *family_lines(standalone=False),
        '- **standalone amount**, never combined with a weight or time unit:', '',
        *family_lines(standalone=True),
        f'- **weight:** {_code(weights)}, or none', '',
        f'- **time:** {_code(uc.ACCEPTABLE_TIME_UNITS)} (base {_code(base_time)})', '',
        f'That makes {len(uc.ALL_ACCEPTABLE_UNITS)} accepted units: '
        f'{len(uc.ACCEPTABLE_AMOUNT_UNITS)} amounts and '
        f'{len(uc.ACCEPTABLE_RATE_UNITS)} rates.',
    ]
    return '\n'.join(lines)


def _between(text: str, begin: str, end: str) -> str:
    assert begin in text and end in text, f'{begin} / {end} markers missing from the guide'
    return text.split(begin, 1)[1].split(end, 1)[0].strip()


def _section(text: str, heading: str) -> str:
    """Body under `heading`, up to the next markdown heading of any level."""
    assert f'\n{heading}\n' in text, f'{heading!r} not found in the guide'
    body = text.split(f'\n{heading}\n', 1)[1]
    nxt = re.search(r'^#{1,6} ', body, flags=re.M)
    return body[:nxt.start()] if nxt else body


def _reference_examples() -> list:
    """(raw spelling, unit class, _clean_unit, _base_unit) for every table example."""
    examples = []
    for line in _section(GUIDE.read_text(encoding='utf-8'), '### Reference Table').splitlines():
        cells = [c.strip() for c in line.strip().strip('|').split('|')]
        if len(cells) != 5 or cells[0] == 'unit class' or set(cells[0]) <= {'-', ':'}:
            continue
        unit_class, _subclass, clean, raw_list, base = cells
        for raw in raw_list.split(', '):
            examples.append((raw, unit_class, clean, clean if base == NOT_CONVERTED else base))
    return examples


@pytest.mark.unit_conversion
def test_acceptable_units_block_matches_the_grammar():
    expected = _acceptable_units_markdown()
    actual = _between(GUIDE.read_text(encoding='utf-8'), ACCEPTABLE_BEGIN, ACCEPTABLE_END)
    assert actual == expected, f'regenerate the block between the markers:\n\n{expected}'


@pytest.mark.unit_conversion
def test_reference_table_matches_the_converter():
    examples = _reference_examples()
    assert len(examples) > 80, 'reference table not found or truncated'
    frame = pl.DataFrame({
        'med_dose_unit': [raw for raw, *_ in examples],
        'med_dose': [1.0] * len(examples),
    })
    base, _ = uc.standardize_dose_to_base_units(frame)
    got = {
        raw: (unit_class, clean, base_unit)
        for raw, unit_class, clean, base_unit in base.select(
            'med_dose_unit', '_unit_class', '_clean_unit', '_base_unit',
        ).iter_rows()
    }
    wrong = [
        f'{raw!r}: page says {(cls, clean, base_unit)}, converter gives {got[raw]}'
        for raw, cls, clean, base_unit in examples
        if got[raw] != (cls, clean, base_unit)
    ]
    assert not wrong, 'reference table disagrees with the converter:\n' + '\n'.join(wrong)


@pytest.mark.unit_conversion
def test_countable_tokens_in_guide_match_the_default_set():
    m = re.search(
        r'`DEFAULT_COUNTABLE_UNITS` holds (\d+) tokens:(.*?)\.(?:\s|$)',
        GUIDE.read_text(encoding='utf-8'), flags=re.S,
    )
    assert m, 'the guide no longer lists the countable tokens'
    assert int(m.group(1)) == len(uc.DEFAULT_COUNTABLE_UNITS)
    assert set(re.findall(r'`([^`]+)`', m.group(2))) == set(uc.DEFAULT_COUNTABLE_UNITS)


@pytest.mark.unit_conversion
def test_guide_quotes_every_status_value():
    from clifpy.utils.unit_converter._preferred import AMBIGUOUS_MU_STATUS

    expected = (
        'original unit is missing',
        'is a countable dosage form; not convertible',
        'is not recognized',
        'cannot convert weighted to unweighted: weight_kg is missing',
        'cannot convert unweighted to weighted: weight_kg is missing',
        AMBIGUOUS_MU_STATUS,
    )
    text = GUIDE.read_text(encoding='utf-8')
    missing = [s for s in expected if s not in text]
    assert not missing, f'the guide does not mention {missing}'


@pytest.mark.unit_conversion
def test_guide_code_runs_offline_on_open_data():
    offenders = _offline_offenders(GUIDE)
    assert not offenders, f'snippets reading a config, restricted data or the network: {offenders}'


@pytest.mark.unit_conversion
def test_guide_documents_return_formats():
    from clifpy.utils.unit_converter._outputs import RETURN_FORMATS

    expected = [f"'{fmt}'" for fmt in RETURN_FORMATS] + [
        'return_rel=True',
        'DeprecationWarning',
        '_clean_dose_unit_formats',
        '_clean_dose_unit_names',
    ]
    text = GUIDE.read_text(encoding='utf-8')
    missing = [s for s in expected if s not in text]
    assert not missing, f'the guide does not mention {missing}'


# pandas in a runnable doc example (D-261022-pandas-free-backend): an import,
# a `pd.` call, or a pandas-only method on a result.
_PANDAS_IDIOMS = re.compile(
    r'^\s*(?:import|from) pandas\b|\bpd\.|\.(?:loc|iloc)\[|\.groupby\(|\.df\(\)'
    r'|\.tz_convert\(|\.tz_localize\(|\.to_pandas\(\)',
    re.M,
)


@pytest.mark.unit_conversion
def test_doc_examples_are_polars():
    offenders = [
        f'{page.name}: {match.group(0).strip()}'
        for page in (GUIDE, API_PAGE)
        for code in _runnable_snippets(page)
        for match in _PANDAS_IDIOMS.finditer(code)
    ]
    assert not offenders, f'pandas idioms in runnable doc examples: {offenders}'


@pytest.mark.unit_conversion
def test_guide_quotes_every_status_details_value():
    text = GUIDE.read_text(encoding='utf-8')
    missing = [s for s in uc.CONVERT_STATUS_DETAILS if s not in text]
    assert not missing, f'the guide does not quote {missing}'


RETIRED_NAMES = (
    'target_schema', 'schema_unit_col', 'schema_category_col',
    '_target_source', 'base_fallback', 'TARGET_SOURCE',
    '_apply_schema_provenance', '_refuse_stale_target_source',
)


@pytest.mark.unit_conversion
def test_docs_and_code_drop_the_retired_target_names():
    """The schema-provenance names left with `_convert_status_details`
    (D-261119-status-details, D-261119-rename)."""
    root = GUIDE.parents[2]
    pages = [GUIDE, API_PAGE, root / 'clifpy' / 'clif_orchestrator.py',
             root / 'examples' / 'README.md', root / 'examples' / 'med_unit_converter_demo.py',
             *sorted((root / 'clifpy' / 'utils' / 'unit_converter').glob('*.py'))]
    stale = {
        page.name: found for page in pages
        if (found := [n for n in RETIRED_NAMES if n in page.read_text(encoding='utf-8')])
    }
    assert not stale, f'retired names still present: {stale}'


@pytest.mark.unit_conversion
def test_guide_documents_the_weight_source_labels():
    """D-261119-weight-labels: 'prefilled' (was 'provided') and 'not_needed',
    and `_weight_age_hours` in the lean output."""
    text = GUIDE.read_text(encoding='utf-8')
    for label in ("'prefilled'", "'not_needed'", '`_weight_age_hours`'):
        assert label in text, label
    assert "`'provided'`" not in text and "'provided'" not in text


def _guide_section(start: str, end: str) -> str:
    text = GUIDE.read_text(encoding='utf-8')
    return text[text.index(start):text.index(end, text.index(start))]


@pytest.mark.unit_conversion
def test_guide_migration_names_the_changed_statuses():
    """Against main, _convert_status values did change (the weight reason split
    in two, countable forms and the ambiguous MU got their own reasons), and
    only the first eight parameters are positional, not "everything since main"."""
    text = GUIDE.read_text(encoding='utf-8')
    migration = _guide_section('## Migration from main\n', '## Reference\n')
    assert 'cannot convert to a weighted unit if weight_kg is missing' in migration
    assert 'countable dosage form' in migration
    for claim in ('values are unchanged', 'unchanged from earlier releases',
                  'keeps its released values', 'Every parameter added since `main`'):
        assert claim not in text, claim


@pytest.mark.unit_conversion
def test_guide_flowchart_checks_a_prefilled_weight_first():
    """A pre-filled weight_kg labels every row 'prefilled' or NULL, crossing the
    weight axis or not, so the chart must test it before the axis."""
    chart = _guide_section('```mermaid\n', '\n```\n')
    assert chart.index('weight_kg already in med_df') < chart.index('cross the weight axis')


@pytest.mark.unit_conversion
def test_guide_names_every_strategy_that_serves_an_undated_dose():
    from datetime import datetime

    dose = pl.DataFrame({'hospitalization_id': ['H_NOW'], 'admin_dttm': [None]},
                        schema={'hospitalization_id': pl.Utf8, 'admin_dttm': pl.Datetime})
    weights = pl.DataFrame({
        'hospitalization_id': ['H_OLD', 'H_NOW', 'H_NEW'],
        'recorded_dttm': [datetime(2024, 1, 2), datetime(2024, 3, 9), datetime(2024, 5, 2)],
        'vital_category': ['weight_kg'] * 3,
        'vital_value': [60.0, 61.0, 62.0],
    })
    stays = pl.DataFrame({
        'hospitalization_id': ['H_OLD', 'H_NOW', 'H_NEW'],
        'patient_id': ['P'] * 3,
        'admission_dttm': [datetime(2024, 1, 1), datetime(2024, 3, 9), datetime(2024, 5, 1)],
    })
    serving = {
        name for name in uc.WEIGHT_FALLBACKS
        if uc.attach_weight(dose, weights, fallbacks=(name,), hospitalization_df=stays)
        ['_weight_source'][0] == name
    }
    assert serving == {'same_hosp_earliest', 'prior_hosp_latest', 'later_hosp_earliest'}
    text = GUIDE.read_text(encoding='utf-8')
    paragraph = next(p for p in text.split('\n\n') if 'has no "before"' in p)
    for name in serving:
        assert f'`{name}`' in paragraph, name


@pytest.mark.unit_conversion
def test_guide_does_not_mention_the_orchestrator():
    """The guide teaches the converter functions only: whether to recommend
    the orchestrator is undecided, so the page neither mentions nor uses it."""
    text = GUIDE.read_text(encoding='utf-8')
    found = sorted({
        m.group(0) for m in re.finditer(
            r'(?i)orchestrator|\bco\.\w+|_dose_units_for_\w+|df_converted|conversion_counts|save_to_table',
            text,
        )
    })
    assert not found, f'orchestrator references in the guide: {found}'

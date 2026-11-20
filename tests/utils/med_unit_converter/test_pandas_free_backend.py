"""The converter computes only in DuckDB or polars (decision D-261022-pandas-free-backend).

pandas may appear in exactly two places:

- the input boundary in ``_inputs.py``: ``pandas_to_arrow`` hands a pandas
  DataFrame to Arrow untouched (falling back to DuckDB's own pandas scanner
  for a mixed-type object column), and ``pandas_index`` keeps the caller's
  index for the deprecated pandas output;
- the deprecated ``return_format='pandas'`` shim, ``_outputs.to_pandas``,
  which converts the finished polars result at the very end.

This test parses every module of the package and the orchestrator's
converter methods and fails on any other pandas use: an ``import pandas``, a
``pd`` / ``pandas`` name, or a pandas bridge (``.df``, ``.to_df()``,
``.fetchdf()``, ``.fetch_df()``, ``.to_pandas()``, ``from_pandas``,
``from_df``). Annotations and ``if TYPE_CHECKING:`` blocks are type-only and
skipped.
"""
import ast
import re
from pathlib import Path

import pytest

import clifpy
import clifpy.utils.unit_converter as unit_converter_pkg

PACKAGE_DIR = Path(unit_converter_pkg.__file__).parent
ORCHESTRATOR = Path(clifpy.__file__).parent / 'clif_orchestrator.py'

# (module file, top-level function) pairs where pandas is allowed.
PANDAS_ALLOWED = {
    ('_inputs.py', 'pandas_to_arrow'),
    ('_inputs.py', 'pandas_index'),
    ('_outputs.py', 'to_pandas'),
}
PANDAS_NAMES = frozenset({'pd', 'pandas'})
PANDAS_BRIDGES = frozenset({
    'df', 'to_df', 'fetchdf', 'fetch_df', 'fetch_df_chunk',
    'to_pandas', 'from_pandas', 'from_df',
})
# The orchestrator's converter wrappers, and the weight loader Task 13 adds.
ORCHESTRATOR_METHODS = re.compile(
    r'^(convert_dose_units_for_|standardize_dose_units_for_'
    r'|_standardize_dose_units$|_load_weight_vitals$)'
)
KNOWN_ORCHESTRATOR_METHODS = {
    'convert_dose_units_for_continuous_meds',
    'convert_dose_units_for_intermittent_meds',
    'standardize_dose_units_for_continuous_meds',
    'standardize_dose_units_for_intermittent_meds',
    '_standardize_dose_units',
}


def _is_type_checking(test: ast.expr) -> bool:
    return (isinstance(test, ast.Name) and test.id == 'TYPE_CHECKING') or (
        isinstance(test, ast.Attribute) and test.attr == 'TYPE_CHECKING')


class _PandasFinder(ast.NodeVisitor):
    """Collect `label:line what` for every pandas use outside `allowed` functions."""

    def __init__(self, label: str, allowed=frozenset()):
        self.label, self.allowed, self.func, self.hits = label, allowed, None, []

    def _hit(self, node, what):
        if self.func not in self.allowed:
            self.hits.append(f'{self.label}:{node.lineno} {what}')

    def visit_FunctionDef(self, node):
        outer = self.func
        self.func = outer or node.name   # nested functions share the outer allowance
        for decorator in node.decorator_list:
            self.visit(decorator)
        for default in [*node.args.defaults, *node.args.kw_defaults]:
            if default is not None:
                self.visit(default)
        for statement in node.body:   # annotations are type-only: not visited
            self.visit(statement)
        self.func = outer

    visit_AsyncFunctionDef = visit_FunctionDef

    def visit_AnnAssign(self, node):
        self.visit(node.target)
        if node.value is not None:
            self.visit(node.value)

    def visit_If(self, node):
        if _is_type_checking(node.test):
            for statement in node.orelse:
                self.visit(statement)
            return
        self.generic_visit(node)

    def visit_Import(self, node):
        for alias in node.names:
            if alias.name.split('.')[0] == 'pandas':
                self._hit(node, f'import {alias.name}')

    def visit_ImportFrom(self, node):
        if (node.module or '').split('.')[0] == 'pandas':
            self._hit(node, f'from {node.module} import ...')

    def visit_Name(self, node):
        if node.id in PANDAS_NAMES:
            self._hit(node, f'name {node.id}')

    def visit_Attribute(self, node):
        if node.attr in PANDAS_BRIDGES:
            self._hit(node, f'.{node.attr}')
        self.generic_visit(node)


def _scan(source: str, label: str, allowed=frozenset()) -> list:
    finder = _PandasFinder(label, allowed)
    finder.visit(ast.parse(source))
    return finder.hits


def _orchestrator_methods() -> dict:
    tree = ast.parse(ORCHESTRATOR.read_text())
    (cls,) = [n for n in tree.body if isinstance(n, ast.ClassDef) and n.name == 'ClifOrchestrator']
    return {n.name: n for n in cls.body
            if isinstance(n, ast.FunctionDef) and ORCHESTRATOR_METHODS.match(n.name)}


@pytest.mark.unit_conversion
def test_backend_is_pandas_free():
    """No pandas in the computation: only the Arrow boundary and the deprecated shim."""
    hits = []
    for path in sorted(PACKAGE_DIR.glob('*.py')):
        allowed = frozenset(func for file, func in PANDAS_ALLOWED if file == path.name)
        hits += _scan(path.read_text(), f'unit_converter/{path.name}', allowed)
    for name, method in sorted(_orchestrator_methods().items()):
        finder = _PandasFinder(f'clif_orchestrator.py::{name}')
        finder.visit(method)
        hits += finder.hits
    assert hits == [], (
        'pandas used outside the input boundary (_inputs.pandas_to_arrow / '
        'pandas_index) and the deprecated shim (_outputs.to_pandas):\n'
        + '\n'.join(hits)
    )


@pytest.mark.unit_conversion
def test_pandas_guard_covers_the_known_entry_points():
    """The allow-list names real functions and the scan finds every converter
    wrapper, so a rename cannot silently widen or empty the guard."""
    for file, func in PANDAS_ALLOWED:
        tree = ast.parse((PACKAGE_DIR / file).read_text())
        assert func in {n.name for n in tree.body if isinstance(n, ast.FunctionDef)}
    assert KNOWN_ORCHESTRATOR_METHODS <= set(_orchestrator_methods())


@pytest.mark.unit_conversion
@pytest.mark.parametrize('snippet, flagged', [
    ('import pandas as pd', True),
    ('from pandas import DataFrame', True),
    ('x = pd.DataFrame()', True),
    ('x = rel.df()', True),
    ('x = rel.to_df()', True),
    ('x = frame.to_pandas()', True),
    ('x = duckdb.from_df(frame)', True),
    ('x = self.vitals.df', True),
    ('x = self.vitals.data', False),
    ('x = rel.pl()', False),
    ('def f(a: pd.DataFrame) -> pd.DataFrame:\n    return a', False),
    ('if TYPE_CHECKING:\n    import pandas', False),
    ("FrameLike = Union['pandas.DataFrame', int]", False),
    ('def to_pandas(frame):\n    return frame.to_pandas()', False),
])
def test_pandas_guard_flags_each_kind_of_use(snippet, flagged):
    """The finder is not vacuous: every bridge it promises to catch is caught."""
    hits = _scan(snippet, 'snippet', allowed=frozenset({'to_pandas'}))
    assert bool(hits) is flagged, hits

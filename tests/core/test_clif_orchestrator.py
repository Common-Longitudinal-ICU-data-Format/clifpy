"""Tests for ClifOrchestrator dose-unit conversion: timezone handling (#144) and the weight path.

Issue #144
----------
A timezone specified on the orchestrator (``ClifOrchestrator(timezone=X)``) was not
honored in ``df_converted['admin_dttm']`` after dose-unit conversion: the converted
timestamps came back in the *machine's* local zone instead of ``X``.

Root cause (documentation + future warning)
-------------------------------------------
Load and convert use *different DuckDB connections with different TimeZone settings*:

- The **load** path (``load_parquet_with_tz``) reads on a fresh **isolated** connection
  (``duckdb.connect()``), pins ``SET timezone='UTC'`` on that con only, ``fetchdf()``s and
  ``close()``s it, then applies the site timezone in pandas (``tz_convert``) -> tz-aware,
  correct.
- The **converter** (``convert_dose_units_by_med_category``) runs bare ``duckdb.sql(...)`` /
  ``.to_df()`` on the process-wide **default** connection, which is never pinned and so
  renders ``TIMESTAMPTZ`` in the machine's OS zone.

The bug is a *label-only* error (the instant is preserved) and is visible only when the
configured ``timezone`` differs from the machine/default-connection zone -- which is why
these tests force that difference via ``hostile_default_tz`` and parametrize the timezone
(catching the subtle 1-hour case, not just the glaring UTC one).

Prevention: any code materializing a ``TIMESTAMPTZ`` to pandas on the default connection
must pin that connection's TimeZone first (or carry timestamps tz-naive/UTC and attach the
zone in pandas) -- never rely on the ambient default-connection zone.

Weight path (audit F2)
----------------------
The four dose methods read patient weights through ``_load_weight_vitals``, which never
assigns ``co.vitals``, and work on the tables' polars ``data`` only, never the pandas
``df`` view; see the tests after the #144 section.
"""
import ast
import inspect
import textwrap
from pathlib import Path

import duckdb
import polars as pl
import pytest
from polars.testing import assert_frame_equal

from clifpy import ClifOrchestrator
import clifpy.utils.unit_converter as unit_converter
from clifpy.utils.outlier_handler import apply_outlier_handling

# The open CLIF 3.0 demo (tz Etc/GMT+5). Pass clif_version='3.0' with it:
# config/demo_data_config.yaml targets the 2.1 root, clifpy/data/clif_demo.
_DEMO_3_DIR = str(Path(__file__).parents[2] / "clifpy" / "data" / "clif_demo" / "3.0")


@pytest.fixture
def hostile_default_tz():
    """Pin DuckDB's *default* connection to a non-UTC zone (Los Angeles).

    Distinct from every parametrized ``timezone`` below, so configured != machine in
    every case -- the condition under which #144 manifests. Restores UTC on teardown.
    """
    duckdb.sql("SET TimeZone = 'America/Los_Angeles'")
    try:
        yield
    finally:
        duckdb.sql("SET TimeZone = 'UTC'")


def _assert_admin_dttm_unchanged(converted_df, src_series):
    """The converted ``admin_dttm`` must match the loaded one in tz-label and instants.

    Both sides are polars: the stored ``df_converted`` and the table's ``data``.
    """
    assert isinstance(converted_df, pl.DataFrame), type(converted_df)
    out = converted_df["admin_dttm"]
    assert out.dtype.time_zone == src_series.dtype.time_zone, (
        f"conversion changed admin_dttm tz {src_series.dtype.time_zone} -> "
        f"{out.dtype.time_zone} (issue #144)"
    )
    assert sorted(out.drop_nulls().to_list()) == sorted(src_series.drop_nulls().to_list())


@pytest.mark.parametrize("timezone", ["UTC", "US/Eastern", "US/Central"])
def test_orchestrator_continuous_conversion_respects_timezone(
    timezone, hostile_default_tz, tmp_path
):
    """Configured ``timezone`` must survive continuous-med dose conversion end-to-end.

    Reproduces the #144 flow: ``ClifOrchestrator(timezone=X)`` ->
    ``convert_dose_units_for_continuous_meds`` -> assert the saved
    ``df_converted['admin_dttm']`` is still in ``X`` (equal to the loaded ``data``), not the
    hostile Los Angeles default-connection zone. Both results are stored as polars.

    No ``override=True``: the demo's sodium_bicarbonate runs in mEq/min, which made the
    pre-#153 converter abort a fentanyl-only request. The default ``override=False``
    therefore also guards #153 end to end.
    """
    co = ClifOrchestrator(
        data_directory=_DEMO_3_DIR,
        filetype="parquet",
        timezone=timezone,
        output_directory=str(tmp_path),
        clif_version="3.0",
    )
    co.convert_dose_units_for_continuous_meds(
        preferred_units={"fentanyl": "mcg/min"}, save_to_table=True
    )
    table = co.medication_admin_continuous
    assert isinstance(table.conversion_counts, pl.DataFrame)
    _assert_admin_dttm_unchanged(table.df_converted, table.data["admin_dttm"])


@pytest.mark.parametrize("timezone", ["UTC", "US/Eastern", "US/Central"])
def test_orchestrator_intermittent_conversion_respects_timezone(
    timezone, hostile_default_tz, tmp_path
):
    """Same as the continuous case for intermittent meds (same underlying converter).

    No ``override=True`` here either: the demo's intermittent antibiotics are charted
    in 'dose', which the pre-#153 converter refused for a fentanyl-only request.
    """
    co = ClifOrchestrator(
        data_directory=_DEMO_3_DIR,
        filetype="parquet",
        timezone=timezone,
        output_directory=str(tmp_path),
        clif_version="3.0",
    )
    co.convert_dose_units_for_intermittent_meds(
        preferred_units={"fentanyl": "mg"}, save_to_table=True
    )
    table = co.medication_admin_intermittent
    assert isinstance(table.conversion_counts, pl.DataFrame)
    _assert_admin_dttm_unchanged(table.df_converted, table.data["admin_dttm"])


def test_wide_dataset_pivots_polars_df_converted(tmp_path):
    """``create_wide_dataset`` still pivots the converted doses now that
    ``df_converted`` is polars.

    wide_dataset.py filtered ``df_converted`` with pandas' ``.isin``, which a
    polars Series lacks (AttributeError). It now filters in polars and passes
    the rows on as pandas, so the converted dose becomes a
    ``<category>_<unit>`` column as before.
    """
    co = _demo_co(tmp_path)
    co.convert_dose_units_for_continuous_meds(
        preferred_units={"fentanyl": "mcg/min"}, override=True, save_to_table=True
    )
    assert isinstance(co.medication_admin_continuous.df_converted, pl.DataFrame)

    co.create_wide_dataset(
        tables_to_load=["medication_admin_continuous"],
        category_filters={"medication_admin_continuous": ["fentanyl"]},
        show_progress=False,
    )
    assert "fentanyl_mcg_min" in co.wide_df.columns
    assert co.wide_df["fentanyl_mcg_min"].notna().any()


def test_wide_dataset_names_unit_family_fallback_units_with_units(tmp_path):
    """A unit-family category left out of ``preferred_units`` falls back to the
    family base spelled ``units/...``, so it pivots as ``<category>_units_<t>``
    (main-3.0 gave ``<category>_u_<t>``). Documented in the guide's "Unit family
    spelling" bullet; this pins the spelling."""
    co = _demo_co(tmp_path)
    co.convert_dose_units_for_continuous_meds(
        preferred_units={"fentanyl": "mcg/min"}, override=True, save_to_table=True
    )
    co.create_wide_dataset(
        tables_to_load=["medication_admin_continuous"],
        category_filters={"medication_admin_continuous": ["heparin"]},
        show_progress=False,
    )
    heparin_cols = [c for c in co.wide_df.columns if c.startswith("heparin_")]
    assert heparin_cols, "the demo carries heparin doses"
    assert all(c.startswith("heparin_units_") for c in heparin_cols), heparin_cols
    assert not any(c.startswith("heparin_u_") for c in heparin_cols)
    assert not any("milli" in c for c in heparin_cols)


def test_conversion_warns_when_the_pandas_view_was_edited_in_place(tmp_path):
    """The wrappers read ``.data``; a shape-changing edit of the cached pandas
    view (``.df``) never reaches it, and the conversion says so."""
    import logging

    co = _demo_co(tmp_path)
    co.load_table("medication_admin_continuous")
    co.medication_admin_continuous.df["site"] = "A"  # edits the pandas view only

    seen = []

    class _Capture(logging.Handler):
        def emit(self, record):
            seen.append(record.getMessage())

    table_logger = logging.getLogger("clifpy.tables.medication_admin_continuous")
    handler = _Capture(level=logging.WARNING)
    table_logger.addHandler(handler)
    try:
        co.convert_dose_units_for_continuous_meds(
            preferred_units={"fentanyl": "mcg/min"}, override=True
        )
    finally:
        table_logger.removeHandler(handler)
    assert any("modified in place" in m for m in seen)
    assert "site" not in co.medication_admin_continuous.df_converted.columns


# ---------------------------------------------------------------------------
# Dose methods: one weight path (audit F2)
# ---------------------------------------------------------------------------
# All four dose methods read patient weights through
# ClifOrchestrator._load_weight_vitals, which never assigns co.vitals, and work
# on the tables' polars data only. These tests run on the open 3.0 demo and
# never pass override.

# Each request converts its drugs across the weight axis (mcg/kg/min -> mcg/min
# and mg/hr; mg -> mg/kg), so every call has rows that need a patient weight.
_TARGETS = {
    "medication_admin_continuous": {"norepinephrine": "mcg/min", "propofol": "mg/hr"},
    "medication_admin_intermittent": {"propofol": "mg/kg", "fentanyl": "mcg"},
}


def _demo_co(tmp_path) -> ClifOrchestrator:
    """Orchestrator on the open CLIF 3.0 demo; its logs go under ``tmp_path``."""
    return ClifOrchestrator(
        data_directory=_DEMO_3_DIR,
        filetype="parquet",
        timezone="Etc/GMT+5",
        output_directory=str(tmp_path),
        clif_version="3.0",
    )


def _stays_with_targets(med_df, table_name, n=3):
    """Sorted stays carrying every target drug of ``table_name``; the first
    ``n`` of them, or all with ``n=None``. ``med_df`` is a table's polars ``data``."""
    drugs = sorted(_TARGETS[table_name])
    ids = (
        med_df.filter(pl.col("med_category").is_in(drugs))
        .group_by("hospitalization_id")
        .agg(pl.col("med_category").n_unique().alias("n_drugs"))
        .filter(pl.col("n_drugs") == len(drugs))
        .get_column("hospitalization_id")
        .sort()
        .to_list()
    )
    assert len(ids) >= (n or 1), f"demo has only {len(ids)} stays with {drugs}"
    return ids if n is None else ids[:n]


# method -> (med table, the converter entry point it calls)
_DOSE_METHODS = {
    "convert_dose_units_for_continuous_meds": (
        "medication_admin_continuous", "convert_dose_units_by_med_category"),
    "convert_dose_units_for_intermittent_meds": (
        "medication_admin_intermittent", "convert_dose_units_by_med_category"),
    "standardize_dose_units_for_continuous_meds": (
        "medication_admin_continuous", "standardize_med_dose_units"),
    "standardize_dose_units_for_intermittent_meds": (
        "medication_admin_intermittent", "standardize_med_dose_units"),
}

_CROSS_STAY_SOURCES = {"prior_hosp_latest", "later_hosp_earliest"}

# Every method the dose path runs (D-261022-pandas-free-backend). Task 6's package
# guard names only the public four, _standardize_dose_units and _load_weight_vitals.
_DOSE_PATH = (
    *_DOSE_METHODS,
    "_standardize_dose_units",
    "_convert_dose_units",
    "_med_frame",
    "_weight_inputs",
    "_load_weight_vitals",
    "_is_loaded",
    "_warn_if_load_scope_narrows",
    "_applied_load_scope",
)


def _call_dose_method(co, method, **kwargs):
    """Run ``method`` on its demo targets; return ``(converted, counts)``.

    convert_* get the targets as ``preferred_units``. standardize_* get them as
    an in-memory ``targets`` (no network) plus one category the demo
    lacks, as a real schema would.
    """
    table_name, _ = _DOSE_METHODS[method]
    targets = _TARGETS[table_name]
    if method.startswith("standardize"):
        request = {"targets": {**targets, "angiotensin": "ng/kg/min"}}
    else:
        request = {"preferred_units": targets}
    return getattr(co, method)(**request, save_to_table=False, **kwargs)


def _spy_converter(monkeypatch, method) -> dict:
    """Record what ``method`` hands its converter entry point.

    Keyword arguments land under their names, the positional ones under
    ``"args"`` (``args[0]`` is ``med_df``). The orchestrator imports the entry
    point from ``clifpy.utils.unit_converter`` at call time, so patching the
    package attribute intercepts it. The spy still runs the real conversion.
    """
    _, entry = _DOSE_METHODS[method]
    real = getattr(unit_converter, entry)
    seen = {}

    def spy(*args, **kwargs):
        seen.update(kwargs)
        seen["args"] = args
        return real(*args, **kwargs)

    monkeypatch.setattr(unit_converter, entry, spy)
    return seen


def _assert_targets_converted(converted, table_name):
    """Every target-drug row converted, so each one that needed a weight got one."""
    assert isinstance(converted, pl.DataFrame), type(converted)
    rows = converted.filter(pl.col("med_category").is_in(sorted(_TARGETS[table_name])))
    assert rows.height > 0
    assert (rows["_convert_status"] == "success").all(), (
        rows["_convert_status"].value_counts().rows()
    )


def _pandas_uses(func) -> list:
    """``'<line>: <use>'`` for each pandas use in the body of ``func``.

    The signature is not scanned, only the statements that run. Flags the
    cached pandas view ``.df`` (also DuckDB's ``rel.df()``), the exports
    ``.to_pandas()`` / ``.to_df()`` / ``.fetchdf()``, the names ``pd`` /
    ``pandas`` and any pandas import.
    """
    node = ast.parse(textwrap.dedent(inspect.getsource(func))).body[0]
    uses = []
    for stmt in node.body:
        for sub in ast.walk(stmt):
            if isinstance(sub, ast.Attribute) and sub.attr in {"df", "to_pandas", "to_df", "fetchdf"}:
                uses.append(f"{sub.lineno}: .{sub.attr}")
            elif isinstance(sub, ast.Name) and sub.id in {"pd", "pandas"}:
                uses.append(f"{sub.lineno}: {sub.id}")
            elif isinstance(sub, ast.Import) and any(
                alias.name.split(".")[0] == "pandas" for alias in sub.names
            ):
                uses.append(f"{sub.lineno}: import pandas")
            elif isinstance(sub, ast.ImportFrom) and (sub.module or "").split(".")[0] == "pandas":
                uses.append(f"{sub.lineno}: from pandas import")
    return uses


def test_load_weight_vitals_reads_weights_without_assigning_vitals(tmp_path):
    co = _demo_co(tmp_path)
    table_name = "medication_admin_continuous"
    ids = _stays_with_targets(co.load_table(table_name).data, table_name)

    weights = co._load_weight_vitals(ids)

    assert co.vitals is None
    assert isinstance(weights, pl.DataFrame)
    assert weights["vital_category"].unique().to_list() == ["weight_kg"]
    assert set(weights["hospitalization_id"].to_list()) == set(ids)


def test_load_weight_vitals_filters_a_loaded_vitals_table_locally(tmp_path):
    co = _demo_co(tmp_path)
    table_name = "medication_admin_continuous"
    ids = _stays_with_targets(co.load_table(table_name).data, table_name)
    vitals = co.load_table("vitals")
    loaded = vitals.data
    n_loaded = loaded.height

    weights = co._load_weight_vitals(ids)

    assert co.vitals is vitals and co.vitals.data is loaded
    assert loaded.height == n_loaded and loaded["vital_category"].n_unique() > 1
    expected = loaded.filter(
        (pl.col("vital_category") == "weight_kg") & pl.col("hospitalization_id").is_in(ids)
    )
    assert_frame_equal(weights, expected)
    # BaseTable caches its pandas view in _df_pandas; the weight read never builds it
    assert vitals._df_pandas is None


def test_load_weight_vitals_cross_stay_reads_every_stay(tmp_path):
    co = _demo_co(tmp_path)
    table_name = "medication_admin_continuous"
    ids = _stays_with_targets(co.load_table(table_name).data, table_name)

    weights = co._load_weight_vitals(ids, cross_stay=True)

    assert co.vitals is None
    assert weights["vital_category"].unique().to_list() == ["weight_kg"]
    assert weights["hospitalization_id"].n_unique() > len(ids)


@pytest.mark.parametrize("vitals_state", ["absent", "empty", "loaded"])
@pytest.mark.parametrize("method", list(_DOSE_METHODS))
def test_dose_methods_leave_co_vitals_alone(method, vitals_state, tmp_path, monkeypatch):
    """No dose method assigns co.vitals, whatever state it is in.

    absent: never loaded, still None afterwards. empty: a table object whose
    data is None counts as not loaded (one check everywhere), so weights are
    read from disk and the object stays empty. loaded: the full table is
    filtered locally, so the object and its frame are unchanged and the
    converter sees only weight_kg rows.
    """
    co = _demo_co(tmp_path)
    table_name, _ = _DOSE_METHODS[method]
    if vitals_state != "absent":
        co.load_table("vitals")
    if vitals_state == "empty":
        co.vitals.data = None
    vitals_before = co.vitals
    data_before = None if vitals_before is None else vitals_before.data
    n_before = None if data_before is None else data_before.height
    seen = _spy_converter(monkeypatch, method)

    converted, _ = _call_dose_method(co, method)

    assert co.vitals is vitals_before
    if vitals_before is not None:
        assert co.vitals.data is data_before
        assert (None if data_before is None else data_before.height) == n_before
    assert seen["vitals_df"]["vital_category"].unique().to_list() == ["weight_kg"]
    _assert_targets_converted(converted, table_name)


@pytest.mark.parametrize("method", list(_DOSE_METHODS))
def test_dose_methods_honour_hospitalization_ids_when_meds_are_loaded(
    method, tmp_path, monkeypatch
):
    co = _demo_co(tmp_path)
    table_name, _ = _DOSE_METHODS[method]
    table = co.load_table(table_name)
    meds = table.data
    ids = _stays_with_targets(meds, table_name)
    seen = _spy_converter(monkeypatch, method)

    converted, _ = _call_dose_method(co, method, hospitalization_ids=ids)

    assert set(converted["hospitalization_id"].to_list()) == set(ids)
    assert converted.height == meds.filter(pl.col("hospitalization_id").is_in(ids)).height
    assert set(seen["vitals_df"]["hospitalization_id"].to_list()) == set(ids)
    assert table.data is meds  # the loaded table is not narrowed
    # polars in, polars out; the table's cached pandas view is never built
    assert isinstance(seen["args"][0], pl.DataFrame)
    assert seen["return_format"] == "polars"
    assert table._df_pandas is None
    _assert_targets_converted(converted, table_name)


@pytest.mark.parametrize("method", list(_DOSE_METHODS))
def test_dose_methods_forward_weight_arguments(method, tmp_path, monkeypatch):
    co = _demo_co(tmp_path)
    table_name, _ = _DOSE_METHODS[method]
    ids = _stays_with_targets(co.load_table(table_name).data, table_name)
    hosp = co.load_table("hospitalization").data
    seen = _spy_converter(monkeypatch, method)

    converted, _ = _call_dose_method(
        co,
        method,
        hospitalization_ids=ids,
        weight_fallbacks=("asof", "same_hosp_earliest"),
        weight_max_staleness_days=30,
        hospitalization_df=hosp,
    )

    assert seen["weight_fallbacks"] == ("asof", "same_hosp_earliest")
    assert seen["weight_max_staleness_days"] == 30
    assert seen["hospitalization_df"] is hosp
    # a same-stay chain keeps the weight read to the requested stays
    assert set(seen["vitals_df"]["hospitalization_id"].to_list()) == set(ids)
    _assert_targets_converted(converted, table_name)


@pytest.mark.parametrize("method", list(_DOSE_METHODS))
def test_cross_stay_fallbacks_default_hospitalization_and_read_every_stay(
    method, tmp_path, monkeypatch
):
    co = _demo_co(tmp_path)
    table_name, _ = _DOSE_METHODS[method]
    ids = _stays_with_targets(co.load_table(table_name).data, table_name)
    seen = _spy_converter(monkeypatch, method)

    _call_dose_method(
        co, method, hospitalization_ids=ids,
        weight_fallbacks=("asof", "prior_hosp_latest"),
    )

    assert co.hospitalization is not None
    assert seen["hospitalization_df"] is co.hospitalization.data
    assert seen["vitals_df"]["hospitalization_id"].n_unique() > len(ids)
    assert co.vitals is None


@pytest.mark.parametrize("method", list(_DOSE_METHODS))
def test_cross_stay_weight_reaches_a_stay_without_its_own(method, tmp_path):
    """End to end: with the stay's own weights gone from the loaded vitals, its
    weight comes from the same patient's other stay."""
    co = _demo_co(tmp_path)
    table_name, _ = _DOSE_METHODS[method]
    meds = co.load_table(table_name).data
    stays = co.load_table("hospitalization").data
    vitals = co.load_table("vitals")
    weights = vitals.data.filter(pl.col("vital_category") == "weight_kg")
    weighted = set(weights["hospitalization_id"].to_list())
    patient_of = dict(zip(stays["hospitalization_id"].to_list(), stays["patient_id"].to_list()))

    def siblings(stay):
        same_patient = stays.filter(pl.col("patient_id") == patient_of[stay])
        return set(same_patient["hospitalization_id"].to_list()) - {stay}

    stay = next(
        s for s in _stays_with_targets(meds, table_name, n=None) if siblings(s) & weighted
    )
    vitals.data = weights.filter(pl.col("hospitalization_id").is_in(sorted(siblings(stay))))

    converted, _ = _call_dose_method(
        co,
        method,
        hospitalization_ids=[stay],
        weight_fallbacks=("asof", "same_hosp_earliest", "prior_hosp_latest", "later_hosp_earliest"),
        show_intermediate=True,
    )

    rows = converted.filter(pl.col("med_category").is_in(sorted(_TARGETS[table_name])))
    # rows that needed no weight read 'not_needed'; the weighed ones name a strategy
    sources = set(rows.filter(pl.col("weight_kg").is_not_null())["_weight_source"].to_list())
    assert sources and sources <= _CROSS_STAY_SOURCES
    _assert_targets_converted(converted, table_name)


@pytest.mark.parametrize("name", _DOSE_PATH)
def test_dose_path_never_touches_pandas(name):
    """D-261022-pandas-free-backend: each dose-path method reads the tables'
    polars ``data`` and runs no pandas code."""
    uses = _pandas_uses(getattr(ClifOrchestrator, name))
    assert not uses, f"ClifOrchestrator.{name} uses pandas: {uses}"


@pytest.mark.parametrize(
    "refused",
    [
        {"return_rel": True},
        {"return_format": "duckdb"},
        {"return_format": "duckdb", "return_rel": True},
    ],
    ids=lambda kw: "+".join(kw),
)
@pytest.mark.parametrize(
    "method",
    [
        "standardize_dose_units_for_continuous_meds",
        "standardize_dose_units_for_intermittent_meds",
    ],
)
def test_standardize_methods_refuse_return_rel_and_return_format(method, refused, tmp_path):
    """**kwargs reach standardize_med_dose_units, where return_rel=True would
    turn the results (and df_converted) into DuckDB relations and return_format
    collides with the fixed 'polars'. These methods always return polars, so
    both are refused, naming the public method, before any table is read."""
    co = _demo_co(tmp_path)
    table_name, _ = _DOSE_METHODS[method]
    noun = "an unexpected keyword argument" if len(refused) == 1 else "unexpected keyword arguments"
    names = ", ".join(repr(k) for k in sorted(refused))

    with pytest.raises(TypeError) as exc:
        _call_dose_method(co, method, **refused)

    assert str(exc.value).startswith(f"{method}() got {noun} {names}: it always returns polars")
    assert "unit_converter._convert" not in str(exc.value)  # not the private module
    assert getattr(co, table_name) is None


# ---------------------------------------------------------------------------
# A filtered load narrows later dose calls: the orchestrator warns (task 17.10)
# ---------------------------------------------------------------------------


class _Warnings:
    """Collect the WARNING messages the orchestrator logger emits."""

    def __init__(self):
        import logging

        self.messages = []
        outer = self

        class _Capture(logging.Handler):
            def emit(self, record):
                outer.messages.append(record.getMessage())

        self._logger = logging.getLogger("clifpy.ClifOrchestrator")
        self._handler = _Capture(level=logging.WARNING)

    def __enter__(self):
        self._logger.addHandler(self._handler)
        return self

    def __exit__(self, *exc):
        self._logger.removeHandler(self._handler)

    def scope_warnings(self):
        return [m for m in self.messages if "loaded for" in m]


def _stays(tmp_path, table_name, n):
    probe = _demo_co(tmp_path)
    return _stays_with_targets(probe.load_table(table_name).data, table_name, n=n)


@pytest.mark.parametrize(
    "method",
    [
        "convert_dose_units_for_continuous_meds",
        "standardize_dose_units_for_intermittent_meds",
    ],
)
def test_batching_second_call_warns_with_count_of_stays_out_of_scope(method, tmp_path):
    table_name, _ = _DOSE_METHODS[method]
    a, b = _stays(tmp_path, table_name, 2)
    co = _demo_co(tmp_path)
    _call_dose_method(co, method, hospitalization_ids=[a])

    with _Warnings() as w:
        converted, _ = _call_dose_method(co, method, hospitalization_ids=[a, b])

    found = w.scope_warnings()
    assert len(found) == 1, w.messages
    assert "1 of the 2 requested" in found[0]
    assert "load_table" in found[0]
    assert a not in found[0] and b not in found[0]  # counts only, never ids
    assert set(converted["hospitalization_id"].to_list()) == {a}


def test_call_without_ids_after_a_filtered_load_warns_with_loaded_count(tmp_path):
    table_name = "medication_admin_continuous"
    ids = _stays(tmp_path, table_name, 3)
    co = _demo_co(tmp_path)
    co.convert_dose_units_for_continuous_meds(
        preferred_units=_TARGETS[table_name], hospitalization_ids=ids, save_to_table=False
    )

    with _Warnings() as w:
        co.convert_dose_units_for_continuous_meds(
            preferred_units=_TARGETS[table_name], save_to_table=False
        )

    found = w.scope_warnings()
    assert len(found) == 1, w.messages
    assert "3 hospitalization(s) only" in found[0]


def test_user_filtered_load_then_wider_call_warns(tmp_path):
    table_name = "medication_admin_continuous"
    a, b = _stays(tmp_path, table_name, 2)
    co = _demo_co(tmp_path)
    co.load_table(table_name, filters={"hospitalization_id": [a]})

    with _Warnings() as w:
        co.convert_dose_units_for_continuous_meds(
            preferred_units=_TARGETS[table_name],
            hospitalization_ids=[a, b],
            save_to_table=False,
        )

    assert len(w.scope_warnings()) == 1, w.messages
    assert "1 of the 2 requested" in w.scope_warnings()[0]


def test_no_scope_warning_when_scope_covers_request_or_load_was_unfiltered(tmp_path):
    table_name = "medication_admin_continuous"
    a, b = _stays(tmp_path, table_name, 2)
    kw = dict(preferred_units=_TARGETS[table_name], save_to_table=False)

    # the filtered scope covers the request
    co = _demo_co(tmp_path)
    co.load_table(table_name, filters={"hospitalization_id": [a, b]})
    with _Warnings() as w:
        co.convert_dose_units_for_continuous_meds(hospitalization_ids=[a], **kw)
    assert w.scope_warnings() == []

    # an unfiltered load
    co = _demo_co(tmp_path)
    co.load_table(table_name)
    with _Warnings() as w:
        co.convert_dose_units_for_continuous_meds(hospitalization_ids=[a, b], **kw)
        co.convert_dose_units_for_continuous_meds(**kw)
    assert w.scope_warnings() == []
    # A new frame assigned to table.data does not end the scope by itself; see
    # the task 17.18 tests below for the rule (the stays it holds decide).


def test_str_filter_loads_one_stay_and_a_wider_call_warns(tmp_path):
    """io loads a bare str as an ``=`` filter: one stay, not its characters."""
    table_name = "medication_admin_continuous"
    a, b = _stays(tmp_path, table_name, 2)
    co = _demo_co(tmp_path)
    co.load_table(table_name, filters={"hospitalization_id": a})

    with _Warnings() as w:
        co.convert_dose_units_for_continuous_meds(
            preferred_units=_TARGETS[table_name],
            hospitalization_ids=[a, b],
            save_to_table=False,
        )

    assert len(w.scope_warnings()) == 1, w.messages
    assert "loaded for 1 hospitalization(s)" in w.scope_warnings()[0]
    assert "1 of the 2 requested" in w.scope_warnings()[0]


def test_int_looking_filter_ids_match_str_request_ids(tmp_path):
    """io compares str(v), so an int filter covers the same stay as its str."""
    table_name = "medication_admin_continuous"
    a, b = _stays(tmp_path, table_name, 2)
    co = _demo_co(tmp_path)
    co.load_table(table_name, filters={"hospitalization_id": [int(a), int(b)]})

    with _Warnings() as w:
        co.convert_dose_units_for_continuous_meds(
            preferred_units=_TARGETS[table_name],
            hospitalization_ids=[a, b],
            save_to_table=False,
        )

    assert w.scope_warnings() == [], w.messages


def test_non_list_iterable_filter_records_no_scope(tmp_path):
    """io stringifies a tuple into one ``=`` comparison, so no scope is knowable."""
    table_name = "medication_admin_continuous"
    a, _ = _stays(tmp_path, table_name, 2)
    co = _demo_co(tmp_path)
    table = co.load_table(table_name, filters={"hospitalization_id": (a,)})
    assert table._loaded_id_scope is None


# ---------------------------------------------------------------------------
# The scope follows the stays in table.data, and covers vitals (task 17.18).
# Rule: a recorded scope applies while every hospitalization_id in table.data
# lies inside it. A new frame with the same stays keeps it; a frame holding any
# stay outside it was widened by the caller, and the scope no longer applies.
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("reassign", ["apply_outlier_handling", "df_converted", "clone"])
def test_scope_warning_survives_a_new_frame_with_the_same_stays(reassign, tmp_path):
    """clifpy's own apply_outlier_handling (``table.df = ...``), task 17.13's
    remedy (``table.data = table.df_converted``) and any copy all assign a new
    frame that holds the filtered load's stays, so the narrowing and its
    warning stay."""
    table_name = "medication_admin_continuous"
    a, b = _stays(tmp_path, table_name, 2)
    co = _demo_co(tmp_path)
    table = co.load_table(table_name, filters={"hospitalization_id": [a]})
    loaded = table.data
    wider_call = co.convert_dose_units_for_continuous_meds
    request = {"preferred_units": _TARGETS[table_name]}
    if reassign == "apply_outlier_handling":
        apply_outlier_handling(table)
    elif reassign == "df_converted":
        # 17.13's remedy between two column pairs: the dose pass, keep its
        # result, then the volume pass on it
        _with_volume_columns(table)
        co.convert_dose_units_for_continuous_meds(hospitalization_ids=[a], **request)
        table.data = table.df_converted
        wider_call = co.standardize_dose_units_for_continuous_meds
        request = {
            "targets": {"propofol": "ml/hr"},
            "dose_col": "volume_infusion_rate",
            "unit_col": "volume_infusion_rate_unit",
        }
    else:
        table.data = table.data.clone()
    assert table.data is not loaded  # a new frame, with the same stay

    with _Warnings() as w:
        converted, _ = wider_call(hospitalization_ids=[a, b], save_to_table=False, **request)

    found = w.scope_warnings()
    assert len(found) == 1, w.messages
    assert found[0].startswith(f"{table_name} was loaded for 1 hospitalization(s) only")
    assert "1 of the 2 requested" in found[0]
    assert set(converted["hospitalization_id"].to_list()) == {a}


def test_no_scope_warning_once_table_data_holds_stays_outside_the_scope(tmp_path):
    """A frame with stays outside the scope means the caller widened the table.
    The scope is then cleared, so later calls skip the scan and a frame the
    caller narrows again does not revive the old scope."""
    table_name = "medication_admin_continuous"
    a, b = _stays(tmp_path, table_name, 2)
    kw = dict(preferred_units=_TARGETS[table_name], save_to_table=False)
    co = _demo_co(tmp_path)
    table = co.load_table(table_name, filters={"hospitalization_id": [a]})
    table.data = _demo_co(tmp_path).load_table(table_name).data  # every stay

    with _Warnings() as w:
        converted, _ = co.convert_dose_units_for_continuous_meds(
            hospitalization_ids=[a, b], **kw
        )

    assert w.scope_warnings() == [], w.messages
    assert set(converted["hospitalization_id"].to_list()) == {a, b}
    assert table._loaded_id_scope is None

    table.data = table.data.filter(pl.col("hospitalization_id") == a)  # the caller's own narrowing
    with _Warnings() as w:
        co.convert_dose_units_for_continuous_meds(hospitalization_ids=[a, b], **kw)
    assert w.scope_warnings() == [], w.messages
    assert table._loaded_id_scope is None


@pytest.mark.parametrize(
    "method",
    [
        "convert_dose_units_for_continuous_meds",
        "standardize_dose_units_for_intermittent_meds",
    ],
)
def test_vitals_loaded_for_some_stays_warns_with_counts(method, tmp_path):
    """The med table is unloaded, so it is loaded for [a, b] and covers the
    call; only the vitals load is narrower."""
    table_name, _ = _DOSE_METHODS[method]
    a, b = _stays(tmp_path, table_name, 2)
    co = _demo_co(tmp_path)
    co.load_table("vitals", filters={"hospitalization_id": [a]})

    with _Warnings() as w:
        _call_dose_method(co, method, hospitalization_ids=[a, b])

    found = w.scope_warnings()
    assert len(found) == 1, w.messages
    assert found[0].startswith("vitals was loaded for 1 hospitalization(s) only")
    assert "1 of the 2 hospitalization(s) this call converts" in found[0]
    assert "no weight from it where a conversion needs one" in found[0]
    assert "co.load_table('vitals')" in found[0] and "vitals_df" in found[0]
    assert a not in found[0] and b not in found[0]  # counts only, never ids


def test_vitals_warning_counts_only_the_stays_with_med_rows(tmp_path):
    """A requested stay with no rows in the med table is not converted, so it
    is not counted: K and M come from the med rows, not the request."""
    table_name = "medication_admin_continuous"
    a, b = _stays(tmp_path, table_name, 2)
    probe = _demo_co(tmp_path)
    med_stays = set(probe.load_table(table_name).data["hospitalization_id"].to_list())
    no_meds = sorted(
        set(probe.load_table("hospitalization").data["hospitalization_id"].to_list())
        - med_stays
    )
    assert no_meds, "demo has no stay without continuous-med rows"
    co = _demo_co(tmp_path)
    co.load_table("vitals", filters={"hospitalization_id": [a]})

    with _Warnings() as w:
        co.convert_dose_units_for_continuous_meds(
            preferred_units=_TARGETS[table_name],
            hospitalization_ids=[a, b, no_meds[0]],
            save_to_table=False,
        )

    found = w.scope_warnings()
    assert len(found) == 1, w.messages
    assert "1 of the 2 hospitalization(s) this call converts" in found[0]


def test_no_vitals_warning_when_its_scope_covers_the_call_or_weights_are_attached(tmp_path):
    table_name = "medication_admin_continuous"
    a, b = _stays(tmp_path, table_name, 2)
    kw = dict(preferred_units=_TARGETS[table_name], save_to_table=False)

    # the vitals scope covers the call
    co = _demo_co(tmp_path)
    co.load_table("vitals", filters={"hospitalization_id": [a, b]})
    with _Warnings() as w:
        co.convert_dose_units_for_continuous_meds(hospitalization_ids=[a], **kw)
    assert w.scope_warnings() == [], w.messages

    # the med rows already carry weight_kg, so the converter never reads vitals
    co = _demo_co(tmp_path)
    co.load_table("vitals", filters={"hospitalization_id": [a]})
    meds = co.load_table(table_name)
    meds.data = meds.data.with_columns(weight_kg=pl.lit(70.0))
    with _Warnings() as w:
        co.convert_dose_units_for_continuous_meds(hospitalization_ids=[a, b], **kw)
    assert w.scope_warnings() == [], w.messages


def test_cross_stay_call_over_a_filtered_vitals_load_warns_fallbacks_reach_only_its_stays(
    tmp_path,
):
    """A cross-stay fallback reads the patients' other stays, which the vitals
    scope may leave out even when the call's own stays lie inside it. The
    warning says the fallbacks reach only the stays inside the scope, without
    claiming the others are missing (the scope may already hold them)."""
    table_name = "medication_admin_continuous"
    a, _ = _stays(tmp_path, table_name, 2)
    co = _demo_co(tmp_path)
    co.load_table("vitals", filters={"hospitalization_id": [a]})

    with _Warnings() as w:
        co.convert_dose_units_for_continuous_meds(
            preferred_units=_TARGETS[table_name],
            hospitalization_ids=[a],
            weight_fallbacks=("asof", "prior_hosp_latest"),
            save_to_table=False,
        )

    found = w.scope_warnings()
    assert len(found) == 1, w.messages
    assert found[0].startswith("vitals was loaded for 1 hospitalization(s) only")
    assert "reach only the stays inside it" in found[0]
    assert "cannot reach" not in found[0] and "out of reach" not in found[0]
    assert "every stay of these patients" in found[0]
    assert "this call converts" not in found[0]  # its own stay lies inside the scope
    assert "co.load_table('vitals')" in found[0] and "vitals_df" in found[0]
    assert a not in found[0]


def test_cross_stay_call_counts_its_own_stays_outside_the_vitals_scope(tmp_path):
    """When some of the call's own stays lie outside the vitals scope, the
    cross-stay WARNING leads with their count, then the reach of the fallbacks,
    and its remedy covers both. It states only what is certain (none of their
    own weights are in it): a fallback may still borrow a sibling's weight."""
    table_name = "medication_admin_continuous"
    a, b = _stays(tmp_path, table_name, 2)
    co = _demo_co(tmp_path)
    co.load_table("vitals", filters={"hospitalization_id": [a]})

    with _Warnings() as w:
        co.convert_dose_units_for_continuous_meds(
            preferred_units=_TARGETS[table_name],
            hospitalization_ids=[a, b],
            weight_fallbacks=("asof", "prior_hosp_latest"),
            save_to_table=False,
        )

    found = w.scope_warnings()
    assert len(found) == 1, w.messages
    msg = found[0]
    assert msg.startswith(
        "vitals was loaded for 1 hospitalization(s) only, so 1 of the 2 "
        "hospitalization(s) this call converts are outside it, so none of their "
        "own weights are in it, and the cross-stay weight fallbacks reach only "
        "the stays inside it"
    )
    assert "get no weight" not in msg
    assert "their weights and those of the patients' other stays" in msg
    assert "every stay of these patients" in msg and "co.load_table('vitals')" in msg
    assert "vitals_df" in msg
    assert a not in msg and b not in msg  # counts only, never ids


def test_cross_stay_warning_does_not_claim_a_loss_a_sibling_weight_fills(tmp_path):
    """Two stays of one patient, vitals loaded for the one with weights. The
    other stay lies outside the scope, yet its rows that need a weight borrow
    one from the stay inside it through a cross-stay fallback. So the WARNING
    says none of its own weights are in the scope, never that it gets none."""
    table_name = "medication_admin_continuous"
    probe = _demo_co(tmp_path)
    meds = probe.load_table(table_name).data
    stays = probe.load_table("hospitalization").data
    weighted = set(
        probe.load_table("vitals").data
        .filter(pl.col("vital_category") == "weight_kg")["hospitalization_id"].to_list()
    )
    needs_weight = set(
        meds.filter(
            pl.col("med_category").is_in(sorted(_TARGETS[table_name]))
            & pl.col("med_dose_unit").str.to_lowercase().str.contains("/kg")
        )["hospitalization_id"].to_list()
    )
    patient_of = dict(zip(stays["hospitalization_id"].to_list(), stays["patient_id"].to_list()))
    pairs = [
        (inside, outside)
        for outside in sorted(needs_weight)
        for inside in sorted(weighted)
        if inside != outside and patient_of.get(inside) == patient_of.get(outside)
    ]
    assert pairs, "demo has no patient whose weighted stay could lend a weight"
    inside, outside = pairs[0]
    co = _demo_co(tmp_path)
    co.load_table("vitals", filters={"hospitalization_id": [inside]})

    with _Warnings() as w:
        converted, _ = co.convert_dose_units_for_continuous_meds(
            preferred_units=_TARGETS[table_name],
            hospitalization_ids=[inside, outside],
            weight_fallbacks=("asof", "same_hosp_earliest", "prior_hosp_latest", "later_hosp_earliest"),
            show_intermediate=True,
            save_to_table=False,
        )

    # the stay outside the scope did get weights, from its sibling
    borrowed = converted.filter(
        (pl.col("hospitalization_id") == outside) & pl.col("weight_kg").is_not_null()
    )
    assert borrowed.height > 0
    assert set(borrowed["_weight_source"].to_list()) <= _CROSS_STAY_SOURCES
    assert (borrowed["_convert_status"] == "success").all()
    found = w.scope_warnings()
    assert len(found) == 1, w.messages
    assert "1 of the 2 hospitalization(s) this call converts" in found[0]
    assert "none of their own weights are in it" in found[0]
    assert "get no weight" not in found[0]
    assert inside not in found[0] and outside not in found[0]  # counts only, never ids


def _with_volume_columns(table):
    """Add a synthetic ``volume_infusion_rate`` pair to the table's ``data``."""
    table.data = table.data.with_columns(
        volume_infusion_rate=pl.lit(5.0),
        volume_infusion_rate_unit=pl.lit("mL/hr"),
    )


def test_wide_dataset_explains_a_df_converted_without_the_dose_pair(tmp_path):
    """A volume-only ``df_converted`` (the volume pass replaced the dose pass)
    must raise a ValueError naming the pair it holds and the remedy, not a bare
    ``KeyError: '_convert_status'`` and not a silent pivot of the raw dose."""
    co = _demo_co(tmp_path)
    co.load_table("medication_admin_continuous")
    table = co.medication_admin_continuous
    _with_volume_columns(table)
    co.standardize_dose_units_for_continuous_meds(
        {"fentanyl": "ml/hr"},
        dose_col="volume_infusion_rate",
        unit_col="volume_infusion_rate_unit",
    )
    with pytest.raises(ValueError, match=r"table\.data = table\.df_converted") as exc:
        co.create_wide_dataset(
            tables_to_load=["medication_admin_continuous"],
            category_filters={"medication_admin_continuous": ["fentanyl"]},
            show_progress=False,
        )
    assert "volume_infusion_rate" in str(exc.value)
    # `_convert_status_details__volume_infusion_rate` is not read as a pair
    # named 'details__volume_infusion_rate': the held list names one pair
    assert "_convert_status_details__volume_infusion_rate" in table.df_converted.columns
    assert "pair(s) 'volume_infusion_rate' instead" in str(exc.value)


@pytest.mark.parametrize("method", sorted(_DOSE_METHODS))
def test_dose_methods_return_the_lean_output(tmp_path, method):
    """Every orchestrator dose method returns the lean output: no
    `_clean_unit` / `_unit_class` unless show_intermediate=True."""
    co = _demo_co(tmp_path)
    table_name, _ = _DOSE_METHODS[method]
    co.load_table(table_name)
    out, counts = _call_dose_method(co, method)
    assert "_clean_unit" not in out.columns and "_unit_class" not in out.columns
    assert {"_convert_status", "_convert_status_details", "_weight_source"} <= set(out.columns)
    assert {"_clean_unit", "_unit_class", "_weight_source"} <= set(counts.columns)
    full, _ = _call_dose_method(co, method, show_intermediate=True)
    assert {"_clean_unit", "_unit_class"} <= set(full.columns)


@pytest.mark.parametrize(
    "method",
    ["standardize_dose_units_for_continuous_meds", "standardize_dose_units_for_intermittent_meds"])
def test_standardize_weight_variant_reads_no_vitals(method, tmp_path, monkeypatch):
    """allow_weight_variant=True reaches the converter through **kwargs, and
    since no row then crosses the weight axis, no weight vitals are read."""
    co = _demo_co(tmp_path)
    table_name, _ = _DOSE_METHODS[method]
    co.load_table(table_name)

    def _no_vitals(*args, **kwargs):
        raise AssertionError("weight vitals were read")

    monkeypatch.setattr(type(co), "_load_weight_vitals", _no_vitals)
    seen = _spy_converter(monkeypatch, method)
    out, _ = _call_dose_method(co, method, allow_weight_variant=True)
    assert seen["allow_weight_variant"] is True
    assert seen["vitals_df"] is None
    assert not out["_convert_status"].str.contains("weight_kg is missing").any()
    assert (out["_weight_source"] == "not_needed").all()


def test_standardize_methods_keep_unlisted_categories_as_charted(tmp_path):
    """The orchestrator's standardize_* inherit `unlisted='keep'`: a category
    the targets omit keeps its dose, and `unlisted='base'` passes through."""
    co = _demo_co(tmp_path)
    co.load_table("medication_admin_continuous")
    out, _ = co.standardize_dose_units_for_continuous_meds(
        {"fentanyl": "mcg/hr"}, save_to_table=False)
    kept = out.filter(
        pl.col("_convert_status_details") == "success: no target listed; unit cleaned only")
    assert kept.height > 0
    assert kept["med_category"].unique().to_list() != ["fentanyl"]
    assert kept.select(
        pl.col("med_dose_converted").eq_missing(pl.col("med_dose")).all()).item()
    base, _ = co.standardize_dose_units_for_continuous_meds(
        {"fentanyl": "mcg/hr"}, save_to_table=False, unlisted="base")
    assert base.filter(pl.col("_convert_status_details").str.starts_with(
        "success: no target listed; ") & ~pl.col("_convert_status_details").str.ends_with(
        "unit cleaned only")).height == kept.height


def test_wide_dataset_keeps_both_pairs_when_data_is_reassigned(tmp_path):
    """``table.data = table.df_converted`` between the two passes keeps both
    pairs, and ``create_wide_dataset`` then pivots the dose pair."""
    co = _demo_co(tmp_path)
    co.load_table("medication_admin_continuous")
    table = co.medication_admin_continuous
    _with_volume_columns(table)
    co.convert_dose_units_for_continuous_meds(
        preferred_units={"fentanyl": "mcg/min"}, override=True
    )
    table.data = table.df_converted
    co.standardize_dose_units_for_continuous_meds(
        {"fentanyl": "ml/hr"},
        dose_col="volume_infusion_rate",
        unit_col="volume_infusion_rate_unit",
    )
    cols = table.df_converted.columns
    assert "med_dose_converted" in cols
    assert "volume_infusion_rate_converted" in cols

    co.create_wide_dataset(
        tables_to_load=["medication_admin_continuous"],
        category_filters={"medication_admin_continuous": ["fentanyl"]},
        show_progress=False,
    )
    assert "fentanyl_mcg_min" in co.wide_df.columns


def test_wide_dataset_names_renamed_default_pair_not_a_second_pair(tmp_path):
    """A dose pass written under other output names leaves ``_convert_status``
    but no ``_convert_status__*`` column. The error must say the default pair
    was renamed or overwritten, not suggest a second column pair."""
    co = _demo_co(tmp_path)
    co.load_table("medication_admin_continuous")
    table = co.medication_admin_continuous
    # The orchestrator's methods never rename the output; the converter does.
    table.df_converted, _ = unit_converter.convert_dose_units_by_med_category(
        table.data,
        preferred_units={"fentanyl": "mcg/min"},
        converted_dose_col="dose_renamed",
        converted_unit_col="unit_renamed",
    )
    cols = table.df_converted.columns
    assert "dose_renamed" in cols and not any(
        c.startswith("_convert_status__") for c in cols
    )
    with pytest.raises(ValueError, match="renamed or overwritten") as exc:
        co.create_wide_dataset(
            tables_to_load=["medication_admin_continuous"],
            category_filters={"medication_admin_continuous": ["fentanyl"]},
            show_progress=False,
        )
    msg = str(exc.value)
    assert "med_dose_converted" in msg and "med_dose_unit_converted" in msg
    assert "second column pair" not in msg


def test_wide_dataset_omits_held_pairs_clause_when_no_pair_status_exists(tmp_path):
    """A df_converted with neither ``_convert_status`` nor any
    ``_convert_status__*`` column holds no pair: the message must not claim
    it 'holds the converted pair(s)' with an empty list."""
    co = _demo_co(tmp_path)
    co.load_table("medication_admin_continuous")
    table = co.medication_admin_continuous
    table.df_converted = table.data
    with pytest.raises(ValueError, match="lacks the configured") as exc:
        co.create_wide_dataset(
            tables_to_load=["medication_admin_continuous"],
            category_filters={"medication_admin_continuous": ["fentanyl"]},
            show_progress=False,
        )
    assert "It holds the converted pair(s)" not in str(exc.value)

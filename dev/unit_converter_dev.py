import marimo

__generated_with = "0.23.3"
app = marimo.App(width="columns")


@app.cell
def _():
    import time
    from pathlib import Path

    import marimo as mo
    import polars as pl

    import clifpy
    from clifpy.utils.config import load_config
    from clifpy.utils.io import load_data
    from clifpy.utils.unit_converter import (
        load_dose_unit_targets,
        standardize_med_dose_units,
    )

    return (
        Path,
        clifpy,
        load_config,
        load_data,
        load_dose_unit_targets,
        mo,
        pl,
        standardize_med_dose_units,
        time,
    )


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    # Standardize every medication against the CLIF mCIDE (stress test)

    Converts **every** continuous and intermittent medication row of one site
    to the CLIF 3.0 mCIDE target units. The mCIDE CSVs in
    `dev/med-unit-converter/` go straight into
    `standardize_med_dose_units(targets=<path>)`, which parses them itself; no
    dict is built by hand.

    Each call returns two frames, and they are the point of this notebook:

    1. **the converted table**: the input rows with `med_dose_converted`,
       `med_dose_unit_converted`, `_convert_status`,
       `_convert_status_details`, `weight_kg`, `_weight_source` and
       `_weight_age_hours` added;
    2. **the conversion status breakdown** (`counts`): one row per category,
       source unit and outcome, with how many rows took that path.

    The summaries at the end are extras built from `counts`; users do not get
    them from the function.

    To run: pick the site and choose whether to accept each target's weight
    variant. Each site's `data_directory`, `filetype` and `timezone` come from
    its CLIF config in `CONFIG_DIR` (`../../../../clif/config` from the
    worktree root), read with clifpy's `load_config`; the dropdown lists every
    config found there, `ucmc3`, `mimic`, `nw` and `rush` first. No path lives
    in this notebook. `demo` runs as is, on the bundled open CLIF 3.0 demo.
    Run it with `uv run marimo edit dev/unit_converter_dev.py` from the
    worktree root. Restricted sites are run by you, never by Claude.
    The converted table is row-level: it is for your own review, not for
    sharing.
    """)
    return


@app.cell
def _(Path, clifpy, mo):
    # The CLIF mCIDE target files, fed straight to standardize_med_dose_units.
    # git-ignored local copies; refresh them from the CLIF repository's mCIDE/ folder.
    TARGETS_DIR = Path("dev/med-unit-converter")
    CONT_TARGETS = TARGETS_DIR / "clif_medication_admin_continuous_med_categories.csv"
    INTM_TARGETS = TARGETS_DIR / "clif_medication_admin_intermittent_med_categories.csv"

    # Site configs (data_directory, filetype, timezone), one per site, read with
    # clifpy's load_config: ../../../../clif/config from the worktree root, the
    # notebook's parent folder, so the location does not depend on the cwd.
    # TODO(lead): keep one config per site in CONFIG_DIR; the dropdown lists every
    # .yaml / .yml / .json file there.
    CONFIG_DIR = (mo.notebook_dir().parent / "../../../../clif/config").resolve()
    # Listed first in the dropdown when their config exists.
    SITE_ORDER = ("ucmc3", "mimic", "nw", "rush")
    # The bundled open CLIF 3.0 demo, in load_config's shape.
    DEMO_CONFIG = dict(
        data_directory=str(Path(clifpy.__file__).parent / "data" / "clif_demo" / "3.0"),
        timezone="Etc/GMT+5",
        filetype="parquet",
    )

    # Weight lookup for the rows whose conversion crosses the weight axis.
    WEIGHT_CHAIN = ("asof", "same_hosp_earliest", "prior_hosp_latest", "later_hosp_earliest")
    MAX_STALENESS_DAYS = None  # e.g. 7
    # Optional: write the extra summaries (counts under 10 masked) to SAVE_DIR / <site>/.
    SAVE_DIR = None  # e.g. Path("output/med_unit_stress")
    return (
        CONFIG_DIR,
        CONT_TARGETS,
        DEMO_CONFIG,
        INTM_TARGETS,
        MAX_STALENESS_DAYS,
        SAVE_DIR,
        SITE_ORDER,
        TARGETS_DIR,
        WEIGHT_CHAIN,
    )


@app.cell
def _(CONT_TARGETS, INTM_TARGETS, load_dose_unit_targets, mo):
    # What standardize_med_dose_units reads from each file. The calls below pass
    # the paths themselves; this cell only shows the parsed targets.
    cont_targets = load_dose_unit_targets(CONT_TARGETS)
    cont_volume_targets = load_dose_unit_targets(
        CONT_TARGETS, unit_col="volume_infusion_rate_units"
    )
    intm_targets = load_dose_unit_targets(INTM_TARGETS)
    mo.md(
        f"Targets read: **{len(cont_targets)}** continuous dose, "
        f"**{len(cont_volume_targets)}** continuous volume and "
        f"**{len(intm_targets)}** intermittent dose categories "
        f"(e.g. norepinephrine → `{cont_targets.get('norepinephrine')}`)."
    )
    return


@app.cell
def _(CONFIG_DIR, SITE_ORDER, mo):
    def _label(path):
        """`config_ucmc3.yaml` / `ucmc3_config.yaml` / `ucmc3.yaml` -> 'ucmc3'."""
        return path.stem.removeprefix("config_").removesuffix("_config")

    _found = {
        _label(p): p for p in sorted(CONFIG_DIR.glob("*"))
        if p.suffix.lower() in {".yaml", ".yml", ".json"}
    } if CONFIG_DIR.is_dir() else {}
    # demo first, then SITE_ORDER, then any other config in the folder
    site_configs = (
        {"demo": None}
        | {s: _found[s] for s in SITE_ORDER if s in _found}
        | {k: v for k, v in _found.items() if k not in SITE_ORDER}
    )
    _missing = [s for s in SITE_ORDER if s not in _found]
    site_picker = mo.ui.dropdown(options=list(site_configs), value="demo", label="site")
    # allow_weight_variant: off converts to the exact target (crossing the
    # weight axis with a looked-up weight); on accepts the target's weight-
    # adjusted or weight-naive variant (target mcg/hr: mcg/kg/hr stays,
    # mg/kg/hr becomes mcg/kg/hr), so no weight is needed.
    variant_switch = mo.ui.switch(
        value=False, label="accept each target's weight variant (allow_weight_variant)"
    )
    mo.vstack([
        site_picker,
        variant_switch,
        *([mo.md(f"_No config in `CONFIG_DIR` for: {', '.join(_missing)}._")] if _missing else []),
    ])
    return site_configs, site_picker, variant_switch


@app.cell
def _(DEMO_CONFIG, load_config, site_configs, site_picker):
    site = site_picker.value
    # load_config checks the required keys and that data_directory exists.
    site_cfg = (
        DEMO_CONFIG if site_configs[site] is None
        else load_config(str(site_configs[site]))
    )
    return site, site_cfg


@app.cell
def _(Path, load_data, pl, site_cfg, time):
    def _available(table, wanted):
        """The `wanted` columns this site's file has (3.0 adds the volume pair)."""
        path = Path(site_cfg["data_directory"]) / f"clif_{table}.{site_cfg['filetype']}"
        schema = (
            pl.read_parquet_schema(path)
            if site_cfg["filetype"] == "parquet"
            else pl.scan_csv(path).collect_schema()
        )
        return [c for c in wanted if c in schema]

    def _load(table, columns, filters=None):
        t0 = time.perf_counter()
        frame = load_data(
            table, site_cfg["data_directory"], site_cfg["filetype"],
            site_tz=site_cfg["timezone"], columns=columns, filters=filters,
        )
        load_times[table] = time.perf_counter() - t0
        return frame

    _med_cols = [
        "hospitalization_id", "admin_dttm", "med_category", "med_dose", "med_dose_unit",
        "volume_infusion_rate", "volume_infusion_rate_unit",
    ]
    load_times = {}
    mac = _load(
        "medication_admin_continuous",
        _available("medication_admin_continuous", _med_cols),
    )
    mai = _load(
        "medication_admin_intermittent",
        _available("medication_admin_intermittent", _med_cols[:5]),
    )
    weights = _load(
        "vitals",
        ["hospitalization_id", "recorded_dttm", "vital_category", "vital_value"],
        filters={"vital_category": ["weight_kg"]},
    )
    hosp = _load("hospitalization", ["hospitalization_id", "patient_id", "admission_dttm"])
    return hosp, load_times, mac, mai, weights


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## Continuous medications

    `standardize_med_dose_units(mac, targets=CONT_TARGETS, vitals_df=weights, ...)`,
    against the continuous mCIDE file's `med_dose_unit` column.
    """)
    return


@app.cell
def _(
    MAX_STALENESS_DAYS,
    TARGETS_DIR,
    WEIGHT_CHAIN,
    hosp,
    mac,
    standardize_med_dose_units,
    time,
    variant_switch,
    weights,
):
    _t0 = time.perf_counter()
    cont, cont_counts = standardize_med_dose_units(
        mac,
        targets=TARGETS_DIR / "clif_medication_admin_continuous_med_categories.csv",
        vitals_df=weights,
        weight_fallbacks=WEIGHT_CHAIN,
        hospitalization_df=hosp,
        weight_max_staleness_days=MAX_STALENESS_DAYS,
        allow_weight_variant=variant_switch.value,
    )
    cont_seconds = time.perf_counter() - _t0
    return cont, cont_counts, cont_seconds


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ### Output 1: the converted table (`cont`)

    Every input row, in input order, with the lean output columns added:

    - `med_dose_converted`, `med_dose_unit_converted`: the dose in its target
      unit; on a failed row, the original dose and the cleaned source unit;
    - `_convert_status`: `'success'`, or why the row did not convert;
    - `_convert_status_details`: how a successful row got its unit
      (converted, already the target, the target's weight variant, or no
      target listed), or the failure reason again;
    - `weight_kg`, `_weight_source`, `_weight_age_hours`: the weight used, how
      it was settled (a strategy, `'prefilled'`, `'not_needed'`, or NULL when
      a weight was needed and none was found) and its age at the dose.

    Row-level: for your own review, not for sharing.
    """)
    return


@app.cell
def _(cont):
    cont
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ### Output 2: the conversion status breakdown (`cont_counts`)

    The key output to review. One row per `med_category` × source unit ×
    outcome, sorted by category and then by count. The review columns come
    first:

    - `med_category`, `med_dose_unit` (as charted);
    - `_preferred_unit` (the target) and `med_dose_unit_converted` (what came
      out);
    - `_convert_status_details` (how the row got there, or why not) and
      `_weight_source`;
    - `count`, the rows that took that path.

    Then the QA detail: `_clean_unit` (cleaned), `_base_unit`, `_unit_class`
    (rate, amount, countable, unrecognized) and `_convert_status`. Units are
    spelled out (`units/hr`, `milli-units/min`) except where a target is echoed
    as written.

    Filter `_convert_status != 'success'` for what did not convert, and
    `_convert_status_details` starting with `success: no target listed` for
    the categories the mCIDE file does not list (often CLIF 2.1 names or
    spelling drift), which are kept as charted.
    """)
    return


@app.cell
def _(cont_counts):
    cont_counts
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## Continuous medications: the volume pair (CLIF 3.0 data only)

    A second call on `cont` converts `volume_infusion_rate` /
    `volume_infusion_rate_unit`. `targets_unit_col` is picked automatically:
    the same file's `volume_infusion_rate_units` column. The pass sets the
    dose pass's columns aside and returns them unchanged; its own columns end
    in `__volume_infusion_rate`.
    """)
    return


@app.cell
def _(
    CONT_TARGETS,
    cont,
    standardize_med_dose_units,
    time,
    variant_switch,
    weights,
):
    if "volume_infusion_rate" in cont.columns:
        _t0 = time.perf_counter()
        cont_vol, cont_vol_counts = standardize_med_dose_units(
            cont,
            targets=CONT_TARGETS,
            dose_col="volume_infusion_rate",
            unit_col="volume_infusion_rate_unit",
            vitals_df=weights,
            allow_weight_variant=variant_switch.value,
        )
        cont_vol_seconds = time.perf_counter() - _t0
    else:
        cont_vol, cont_vol_counts, cont_vol_seconds = None, None, None
    return cont_vol, cont_vol_counts, cont_vol_seconds


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ### Output 1: the converted table (`cont_vol`)

    `cont` with `volume_infusion_rate_converted`,
    `volume_infusion_rate_unit_converted`,
    `_convert_status__volume_infusion_rate` and
    `_convert_status_details__volume_infusion_rate` added. Row-level.
    """)
    return


@app.cell
def _(cont_vol, mo):
    cont_vol if cont_vol is not None else mo.md("_No volume pair in this site's table (CLIF 2.1 data)._")
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ### Output 2: the conversion status breakdown (`cont_vol_counts`)

    As above, for the volume pair: its QA columns end in
    `__volume_infusion_rate`, and the unit column is
    `volume_infusion_rate_unit`.
    """)
    return


@app.cell
def _(cont_vol_counts, mo):
    cont_vol_counts if cont_vol_counts is not None else mo.md("_No volume pair in this site's table (CLIF 2.1 data)._")
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## Intermittent medications

    `standardize_med_dose_units(mai, targets=INTM_TARGETS, vitals_df=weights, ...)`,
    against the intermittent mCIDE file.
    """)
    return


@app.cell
def _(
    INTM_TARGETS,
    MAX_STALENESS_DAYS,
    WEIGHT_CHAIN,
    hosp,
    mai,
    standardize_med_dose_units,
    time,
    variant_switch,
    weights,
):
    _t0 = time.perf_counter()
    intm, intm_counts = standardize_med_dose_units(
        mai,
        targets=INTM_TARGETS,
        vitals_df=weights,
        weight_fallbacks=WEIGHT_CHAIN,
        hospitalization_df=hosp,
        weight_max_staleness_days=MAX_STALENESS_DAYS,
        allow_weight_variant=variant_switch.value,
    )
    intm_seconds = time.perf_counter() - _t0
    return intm, intm_counts, intm_seconds


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ### Output 1: the converted table (`intm`)

    Every intermittent row with the same lean output columns as `cont`.
    Countable dosage forms (`tablet`, `puff`, ...) carry no dose without the
    product strength, so they report `... is a countable dosage form; not
    convertible`, unless the target is that same form. Row-level.
    """)
    return


@app.cell
def _(intm):
    intm
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ### Output 2: the conversion status breakdown (`intm_counts`)

    The key output to review for intermittent doses; same columns as
    `cont_counts`.
    """)
    return


@app.cell
def _(intm_counts):
    intm_counts
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## Extras (not part of the function's output)

    Built from the tables above for the stress test: wall time per step, and
    the counts tables of every pass summarized side by side.
    """)
    return


@app.cell
def _(
    cont,
    cont_seconds,
    cont_vol,
    cont_vol_seconds,
    intm,
    intm_seconds,
    load_times,
    mac,
    mai,
    pl,
):
    # Wall time per step (rows in = rows out for every conversion)
    _steps = [(f"load {t}", None, s) for t, s in load_times.items()]
    _steps += [
        ("standardize continuous dose", mac.height, cont_seconds),
        ("standardize intermittent dose", mai.height, intm_seconds),
    ]
    if cont_vol is not None:
        _steps.append(("standardize continuous volume", mac.height, cont_vol_seconds))
    assert cont.height == mac.height and intm.height == mai.height
    timings = pl.DataFrame(
        _steps, schema={"step": pl.String, "rows": pl.Int64, "seconds": pl.Float64},
        orient="row",
    ).with_columns(
        seconds=pl.col("seconds").round(2),
        rows_per_second=(pl.col("rows") / pl.col("seconds")).round(0),
    )
    timings
    return (timings,)


@app.cell
def _(cont_counts, cont_vol_counts, intm_counts, pl):
    # Each pass's counts table, with its per-pair column names mapped back to
    # the default pair's, so every summary below reads them alike.
    def _normalized(counts, dose_col, unit_col):
        if dose_col == "med_dose":
            return counts
        return counts.rename({
            c: c.removesuffix(f"__{dose_col}") for c in counts.columns
            if c.endswith(f"__{dose_col}")
        } | {unit_col: "med_dose_unit"})

    passes = {
        "continuous dose": _normalized(cont_counts, "med_dose", "med_dose_unit"),
        "intermittent dose": _normalized(intm_counts, "med_dose", "med_dose_unit"),
    }
    if cont_vol_counts is not None:
        passes["continuous volume"] = _normalized(
            cont_vol_counts, "volume_infusion_rate", "volume_infusion_rate_unit")
    all_counts = pl.concat(
        [c.select(
            "med_category", "med_dose_unit", "_convert_status",
            "_convert_status_details", "_weight_source", "count",
        ).with_columns(pass_name=pl.lit(name)) for name, c in passes.items()],
        how="vertical_relaxed",
    )
    return (all_counts,)


@app.cell
def _(all_counts, mo):
    outcomes = mo.sql(
        f"""
        -- Rows per outcome and pass, with each outcome's share of the pass
        FROM all_counts
        SELECT
            pass_name
            , _convert_status_details
            , n_rows: SUM(count)::BIGINT
            , pct: ROUND(100.0 * SUM(count) / SUM(SUM(count)) OVER (PARTITION BY pass_name), 2)
        GROUP BY pass_name, _convert_status_details
        ORDER BY pass_name, n_rows DESC
        """
    )
    return (outcomes,)


@app.cell
def _(all_counts, mo):
    weight_sources = mo.sql(
        f"""
        -- How each row's weight was settled: a strategy, 'prefilled',
        -- 'not_needed', or NULL (needed, none found)
        FROM all_counts
        SELECT
            pass_name
            , _weight_source
            , n_rows: SUM(count)::BIGINT
        GROUP BY ALL
        ORDER BY pass_name, n_rows DESC
        """
    )
    return (weight_sources,)


@app.cell
def _(all_counts, mo):
    failures = mo.sql(
        f"""
        -- The largest groups of rows that did not convert, by category, unit and reason
        FROM all_counts
        SELECT
            pass_name
            , med_category
            , med_dose_unit
            , _convert_status
            , n_rows: SUM(count)::BIGINT
        WHERE _convert_status != 'success'
        GROUP BY ALL
        ORDER BY n_rows DESC
        LIMIT 100
        """
    )
    return (failures,)


@app.cell
def _(all_counts, mo):
    unlisted = mo.sql(
        f"""
        -- Categories the mCIDE file does not list (kept as charted): often
        -- CLIF 2.1 names or spelling drift
        FROM all_counts
        SELECT
            pass_name
            , med_category
            , n_rows: SUM(count)::BIGINT
        WHERE _convert_status_details LIKE 'success: no target listed%'
        GROUP BY ALL
        ORDER BY pass_name, n_rows DESC
        """
    )
    return (unlisted,)


@app.cell
def _(
    Path,
    SAVE_DIR,
    failures,
    outcomes,
    pl,
    site,
    timings,
    unlisted,
    weight_sources,
):
    # Aggregates only, with every count under 10 masked, so the files can be shared.
    if SAVE_DIR is not None:
        _out = Path(SAVE_DIR) / site
        _out.mkdir(parents=True, exist_ok=True)
        timings.write_csv(_out / "timings.csv")
        for _name, _frame in {
            "outcomes": outcomes, "weight_sources": weight_sources,
            "failures": failures, "unlisted": unlisted,
        }.items():
            _small = pl.col("n_rows") < 10
            _frame.with_columns(
                n_rows=pl.when(_small).then(pl.lit("<10")).otherwise(pl.col("n_rows").cast(pl.String)),
                *([pl.when(_small).then(None).otherwise(pl.col("pct")).alias("pct")]
                  if "pct" in _frame.columns else []),
            ).write_csv(_out / f"{_name}.csv")
    return


if __name__ == "__main__":
    app.run()

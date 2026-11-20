import marimo

__generated_with = "0.23.3"
app = marimo.App(width="medium", sql_output="polars")


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    # Medication dose-unit converter (CLIF 3.0)

    A tour of `clifpy.utils.unit_converter` on the bundled open demo, the
    100-patient CLIF-MIMIC demo in the CLIF 3.0 layout
    (`clifpy/data/clif_demo/3.0/`). Every number comes from that demo or from a
    small synthetic frame, so the notebook needs no config file and no network.

    1. Load the four tables the converter uses.
    2. Standardize continuous doses against mCIDE target units, exactly or
       accepting each target's weight variant.
    3. Ask for your own units, including one that cannot be met.
    4. Attach patient weights through a fallback chain with a staleness cap.
    5. Convert a second column pair on the same frame, and extend the countable units.
    6. Run step 2 through `ClifOrchestrator`.
    7. Calling it fast on a full table.
    """)
    return


@app.cell
def _():
    import tempfile
    from pathlib import Path

    import marimo as mo
    import polars as pl

    import clifpy
    from clifpy import ClifOrchestrator, load_data
    from clifpy.utils.unit_converter import (
        DEFAULT_COUNTABLE_UNITS,
        attach_weight,
        convert_dose_units_by_med_category,
        standardize_med_dose_units,
    )

    return (
        ClifOrchestrator,
        DEFAULT_COUNTABLE_UNITS,
        Path,
        attach_weight,
        clifpy,
        convert_dose_units_by_med_category,
        load_data,
        mo,
        pl,
        standardize_med_dose_units,
        tempfile,
    )


@app.cell
def _(Path, clifpy):
    # The bundled open demo in the CLIF 3.0 layout. Passed explicitly, because
    # config/demo_data_config.yaml points at the 2.1 root.
    demo_dir = Path(clifpy.__file__).parent / "data" / "clif_demo" / "3.0"
    site_tz = "Etc/GMT+5"
    clif_version = "3.0"
    # The weight chain every weight lookup below uses: the latest weight at or
    # before the dose, else the stay's earliest weight, else the latest weight
    # from one of the patient's earlier stays.
    weight_chain = ("asof", "same_hosp_earliest", "prior_hosp_latest")
    return clif_version, demo_dir, site_tz, weight_chain


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## 1. Load the tables

    `load_data` returns polars by default, and so does the converter. It takes
    a polars frame (eager or lazy), a DuckDB relation or a pandas frame,
    computes in DuckDB, and hands back polars unless `return_format` asks for
    `'polars_lazy'`, `'duckdb'` or the deprecated `'pandas'`, the same choices
    `load_data` offers. Only `weight_kg` rows of `vitals` are needed, and only
    three columns of `hospitalization` (they link a patient's stays for the
    cross-stay weight fallback).
    """)
    return


@app.cell
def _(demo_dir, load_data, site_tz):
    _src = dict(table_path=str(demo_dir), table_format_type="parquet", site_tz=site_tz)
    mac = load_data("medication_admin_continuous", **_src)
    mai = load_data("medication_admin_intermittent", **_src)
    weights = load_data(
        "vitals",
        columns=["hospitalization_id", "recorded_dttm", "vital_category", "vital_value"],
        filters={"vital_category": ["weight_kg"]},
        **_src,
    )
    hosp = load_data(
        "hospitalization",
        columns=["patient_id", "hospitalization_id", "admission_dttm"],
        **_src,
    )
    return hosp, mac, mai, weights


@app.cell(hide_code=True)
def _(hosp, mac, mai, mo, weights):
    mo.md(f"""
    | table | rows | hospitalizations |
    |---|---:|---:|
    | `medication_admin_continuous` | {mac.height:,} | {mac["hospitalization_id"].n_unique()} |
    | `medication_admin_intermittent` | {mai.height:,} | {mai["hospitalization_id"].n_unique()} |
    | `vitals` (`weight_kg` only) | {weights.height:,} | {weights["hospitalization_id"].n_unique()} |
    | `hospitalization` | {hosp.height:,} | {hosp["hospitalization_id"].n_unique()} |
    """)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## 2. Standardize against mCIDE target units

    CLIF 3.0 names a target unit for every `med_category` in its mCIDE files.
    `standardize_med_dose_units` takes those `targets` as a file path, a URL
    or a `{med_category: unit}` dict, and converts every row to its category's
    target. In practice you pass the file itself, e.g.
    `standardize_med_dose_units(mac, targets=csv_path_or_url, vitals_df=weights)`
    with
    `mCIDE/medication_admin_continuous/clif_medication_admin_continuous_med_categories.csv`
    from the [CLIF repository](https://github.com/Common-Longitudinal-ICU-data-Format/CLIF).

    This notebook stays offline, so the cell below copies the targets for the
    categories the demo carries. The output is lean: the converted dose and
    unit, `_convert_status` (`'success'` or why not), `_convert_status_details`
    (how a successful row got there: converted, already in the target unit,
    or no target listed), `weight_kg`, `_weight_source` (which strategy
    supplied the weight, `'prefilled'`, or `'not_needed'` where none is
    needed) and `_weight_age_hours` (how old that weight was at the dose).
    The QA columns (`_clean_unit`, `_unit_class`, ...)
    are in the counts table, or in the rows with `show_intermediate=True`.

    A category the targets do not list is kept as charted: its dose is copied
    and only its unit spelling is cleaned (`success: no target listed; unit
    cleaned only`). The demo was crosswalked from CLIF 2.1, so a few of its
    categories use 2.1 names (`dextrose_in_water_d5w` rather than
    `dextrose_5_water`, `magnesium` rather than `magnesium_sulfate`). They are
    unlisted, and the converter logs a warning naming them.
    `sodium_bicarbonate` fails the other way: its 3.0 target is a volume
    (`ml/hr`) but the demo charts milliequivalents (`mEq/min`), so its rows keep
    their original unit and `n_success` is 0.
    """)
    return


@app.cell
def _():
    # The CLIF 3.0 mCIDE target units (med_dose_unit column) of
    # mCIDE/medication_admin_continuous/clif_medication_admin_continuous_med_categories.csv
    # at Common-Longitudinal-ICU-data-Format/CLIF@7bd9282, for the 29
    # categories the demo carries that the file lists.
    mcide_cont_targets = {
        "amiodarone": "mg/min",
        "bumetanide": "mg/hr",
        "dexmedetomidine": "mcg/kg/hr",
        "diltiazem": "mg/hr",
        "dobutamine": "mcg/kg/min",
        "dopamine": "mcg/kg/min",
        "epinephrine": "mcg/kg/min",
        "esmolol": "mcg/kg/min",
        "fentanyl": "mcg/hr",
        "furosemide": "mg/hr",
        "heparin": "units/kg/hr",
        "hydromorphone": "mg/hr",
        "insulin": "units/hr",
        "labetalol": "mg/min",
        "midazolam": "mg/hr",
        "milrinone": "mcg/kg/min",
        "morphine": "mg/hr",
        "nicardipine": "mg/hr",
        "nitroglycerin": "mcg/min",
        "norepinephrine": "mcg/kg/min",
        "octreotide": "mcg/hr",
        "pantoprazole": "mg/hr",
        "phenylephrine": "mcg/kg/min",
        "propofol": "mcg/kg/min",
        "rocuronium": "mcg/kg/min",
        "sodium_bicarbonate": "ml/hr",
        "sodium_chloride": "ml/hr",
        "tpn": "ml/hr",
        "vasopressin": "units/min",
    }
    return (mcide_cont_targets,)


@app.cell
def _(
    hosp,
    mac,
    mcide_cont_targets,
    standardize_med_dose_units,
    weight_chain,
    weights,
):
    std_cont, std_cont_counts = standardize_med_dose_units(
        mac,
        mcide_cont_targets,
        vitals_df=weights,
        weight_fallbacks=weight_chain,
        hospitalization_df=hosp,
    )
    return std_cont, std_cont_counts


@app.cell
def _(std_cont):
    std_cont
    return


@app.cell
def _(mo, std_cont_counts):
    _df = mo.sql(
        f"""
        FROM std_cont_counts
        ORDER by med_category
        """
    )
    return


@app.cell
def _(mo, std_cont_counts):
    status_details_summary = mo.sql(
        f"""
        -- One row per category, outcome and resulting unit. Unlisted
        -- categories read 'success: no target listed; unit cleaned only'.
        FROM std_cont_counts
        SELECT
            med_category
            , _convert_status_details
            , med_dose_unit_converted
            , n_rows: SUM(count)::BIGINT
            , n_success: COALESCE(SUM(count) FILTER (WHERE _convert_status = 'success'), 0)::BIGINT
        GROUP BY ALL
        ORDER BY _convert_status_details, med_category, n_rows DESC
        """
    )
    return


@app.cell
def _(mo, std_cont):
    heparin_rows = mo.sql(
        f"""
        -- Heparin is charted in units/hour; the 3.0 target is units/kg/hr, so
        -- each row needs the patient's weight.
        FROM std_cont
        SELECT
            med_dose
            , med_dose_unit
            , weight_kg
            , med_dose_converted
            , med_dose_unit_converted
            , _convert_status
        WHERE med_category = 'heparin'
        ORDER BY hospitalization_id, admin_dttm
        LIMIT 5
        """
    )
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ### Accept the target's weight variant

    Converting to the exact target crosses the weight axis where the two
    differ in weight basis: heparin charted in `units/hr` against the
    `units/kg/hr` target is divided by the patient's weight, and needs one.
    `allow_weight_variant=True` accepts the target's weight-adjusted or
    weight-naive variant instead: with target `units/kg/hr`, a `units/hr` row
    stays `units/hr`, and with target `mcg/hr`, `mg/kg/hr` becomes `mcg/kg/hr`
    (not `mcg/hr`). No row then needs a weight, so the call below passes no
    vitals at all, and `_weight_source` reads `'not_needed'`. The comparison counts each
    outcome in both runs.
    """)
    return


@app.cell
def _(mac, mcide_cont_targets, standardize_med_dose_units):
    std_variant, std_variant_counts = standardize_med_dose_units(
        mac,
        mcide_cont_targets,
        allow_weight_variant=True,
    )
    assert (std_variant["_weight_source"] == "not_needed").all()
    return (std_variant_counts,)


@app.cell
def _(mo, std_cont_counts, std_variant_counts):
    variant_vs_exact = mo.sql(
        f"""
        -- Rows per outcome: the exact target (section 2 above) against the
        -- weight variant. 'already the weight variant' rows were copied.
        FROM std_cont_counts
        SELECT run: 'exact target', _convert_status_details, n_rows: SUM(count)::BIGINT
        GROUP BY ALL
        UNION ALL
        FROM std_variant_counts
        SELECT run: 'weight variant', _convert_status_details, n_rows: SUM(count)::BIGINT
        GROUP BY ALL
        ORDER BY _convert_status_details, run
        """
    )
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## 3. Your own units, including an impossible one

    `convert_dose_units_by_med_category` takes the targets as
    `preferred_units`. Categories you leave out are converted to their base
    unit. A row that cannot be converted keeps its original dose and its
    cleaned unit, and `_convert_status` says why. Here `midazolam` asks for a rate
    (`mg/hr`) from an intermittent table charted in amounts (`mg`), which no
    factor can bridge. No requested unit crosses the weight axis, so no vitals
    are needed.
    """)
    return


@app.cell
def _():
    preferred_intm = {
        "acetaminophen": "g",
        "fentanyl": "mcg",
        "hydromorphone": "mg",
        "insulin": "units",
        "magnesium": "g",
        "vancomycin": "mg/kg",
        # impossible: an amount cannot become a rate
        "midazolam": "mg/hr",
    }
    return (preferred_intm,)


@app.cell
def _(convert_dose_units_by_med_category, mai, preferred_intm, weights):
    intm_converted, intm_counts = convert_dose_units_by_med_category(
        mai,
        preferred_units=preferred_intm,
        vitals_df=weights
    )
    return intm_converted, intm_counts


@app.cell
def _(intm_converted):
    intm_converted
    return


@app.cell
def _(intm_counts):
    intm_counts
    return


@app.cell
def _(intm_converted, mo):
    status_breakdown = mo.sql(
        f"""
        -- Every intermittent row by outcome. 'dose' is a countable dosage
        -- form (section 5), so those rows can never convert.
        FROM intm_converted
        SELECT
            _convert_status
            , n_rows: COUNT(*)
            , pct_rows: ROUND(100 * COUNT(*) / SUM(COUNT(*)) OVER (), 1)
        GROUP BY ALL
        ORDER BY n_rows DESC
        """
    )
    return


@app.cell
def _(intm_counts, mo, preferred_intm):
    requested_breakdown = mo.sql(
        f"""
        -- The requested categories only: source unit, requested unit, result.
        FROM intm_counts
        SELECT
            med_category
            , med_dose_unit
            , _preferred_unit
            , med_dose_unit_converted
            , _convert_status
            , n_rows: SUM(count)::BIGINT
        WHERE med_category IN {tuple(sorted(preferred_intm))}
        GROUP BY ALL
        ORDER BY med_category, n_rows DESC
        """
    )
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## 4. Patient weight: a fallback chain with a staleness cap

    Converting between weighted (`mcg/kg/min`) and unweighted (`mcg/min`) units
    needs a weight per row. `attach_weight` tries the strategies in order and
    records the winner in `_weight_source`. `max_staleness_days` makes a weight
    charted more than that many days before the dose ineligible, so the row
    falls through to the next strategy. Move the slider to see rows shift down
    the chain; the uncapped column is the same chain with no cap. Every demo
    stay with a medication has a charted weight, so `prior_hosp_latest` (a
    weight from an earlier stay) never wins here; it serves stays with no
    weight of their own.
    """)
    return


@app.cell
def _(mo):
    staleness_days = mo.ui.slider(
        steps=[1, 2, 3, 5, 7, 14, 30, 90, 365],
        value=3,
        label="max_staleness_days",
        show_value=True,
    )
    staleness_days
    return (staleness_days,)


@app.cell
def _(attach_weight, hosp, mac, staleness_days, weight_chain, weights):
    weighted = attach_weight(
        mac,
        weights,
        fallbacks=weight_chain,
        hospitalization_df=hosp,
        max_staleness_days=staleness_days.value,
    )
    # exactly one output row per input row, whichever strategy wins
    assert weighted.height == mac.height
    return (weighted,)


@app.cell
def _(attach_weight, hosp, mac, weight_chain, weights):
    weighted_uncapped = attach_weight(
        mac,
        weights,
        fallbacks=weight_chain,
        hospitalization_df=hosp,
    )
    return (weighted_uncapped,)


@app.cell
def _(weighted_uncapped):
    weighted_uncapped
    return


@app.cell
def _(mo, weighted):
    provenance_capped = mo.sql(
        f"""
        -- Winning strategy per row under the slider's cap; '(none)' = no weight.
        FROM weighted
        SELECT
            weight_source: COALESCE(_weight_source, '(none)')
            , n_capped: COUNT(*)
        GROUP BY ALL
        """,
        output=False
    )
    return (provenance_capped,)


@app.cell
def _(mo, weighted_uncapped):
    provenance_uncapped = mo.sql(
        f"""
        -- The same chain with no cap.
        FROM weighted_uncapped
        SELECT
            weight_source: COALESCE(_weight_source, '(none)')
            , n_uncapped: COUNT(*)
        GROUP BY ALL
        """,
        output=False
    )
    return (provenance_uncapped,)


@app.cell
def _(mo, provenance_capped, provenance_uncapped):
    provenance = mo.sql(
        f"""
        -- Uncapped next to capped, in chain order.
        FROM provenance_uncapped
        FULL JOIN provenance_capped USING (weight_source)
        SELECT
            weight_source
            , n_uncapped: COALESCE(n_uncapped, 0)
            , n_capped: COALESCE(n_capped, 0)
        ORDER BY list_position(
            ['asof', 'same_hosp_earliest', 'prior_hosp_latest', '(none)'],
            weight_source
        )
        """
    )
    return


@app.cell(hide_code=True)
def _(mo, staleness_days, weighted, weighted_uncapped):
    _capped = weighted["weight_kg"].null_count() / weighted.height
    _uncapped = weighted_uncapped["weight_kg"].null_count() / weighted_uncapped.height
    mo.md(
        f"Continuous rows left without a weight: **{_capped:.2%}** at "
        f"`max_staleness_days={staleness_days.value}`, against {_uncapped:.2%} "
        f"uncapped."
    )
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## 5. A second column pair, and local countable units

    The converter works on any dose/unit column pair. CLIF 3.0 adds
    `volume_infusion_rate` / `volume_infusion_rate_unit` to
    `medication_admin_continuous`, but the demo was crosswalked from 2.1 and
    has neither, so this section uses a five-row synthetic polars frame that
    carries both pairs. It converts the dose pair, then the volume pair of
    that result, without naming a single output column. Each pass names its
    columns after its pair: the dose pass writes `med_dose_converted`,
    `_convert_status` and `_convert_status_details`, and the volume pass
    `volume_infusion_rate_converted`, `_convert_status__volume_infusion_rate`
    and `_convert_status_details__volume_infusion_rate`. The volume pass sets the dose
    pass's columns aside while it runs and returns them unchanged, so the two
    results sit side by side, and the rows come back in input order.

    Countable dosage forms (`tablet`, `dose`, `puff`, ...) carry no dose without
    the product strength, so they are classed `countable` instead of
    unrecognized. `countable_units` replaces the default set, so extend it with
    a union; the synthetic `neb` row shows the difference.
    """)
    return


@app.cell
def _(pl, site_tz):
    vol_frame = pl.DataFrame({
        "hospitalization_id": ["h1", "h1", "h2", "h2", "h3"],
        "admin_dttm": [
            "2026-01-01 08:00", "2026-01-01 09:00", "2026-01-02 10:00",
            "2026-01-02 11:00", "2026-01-03 12:00",
        ],
        "med_category": [
            "norepinephrine", "norepinephrine", "sodium_chloride",
            "sodium_chloride", "albuterol",
        ],
        "med_dose": [0.05, 0.1, 100.0, 1.0, 2.5],
        "med_dose_unit": ["mcg/kg/min", "mcg/kg/min", "mL/hour", "L/hr", "mg"],
        "volume_infusion_rate": [7.5, 0.015, 100.0, 1.0, 1.0],
        "volume_infusion_rate_unit": ["mL/hr", "L/hr", "mL/hour", "L/hr", "neb"],
    }).with_columns(
        # wall-clock strings, labelled in the site's zone
        pl.col("admin_dttm")
        .str.to_datetime("%Y-%m-%d %H:%M")
        .dt.replace_time_zone(site_tz)
    )
    # the dose pair's targets: none crosses the weight axis, so no vitals
    dose_targets = {
        "norepinephrine": "mcg/kg/min",
        "sodium_chloride": "ml/hr",
        "albuterol": "mg",
    }
    vol_targets = {
        "norepinephrine": "ml/hr",
        "sodium_chloride": "ml/hr",
        "albuterol": "ml/hr",
    }
    # the source pair only: the outputs are named after it
    vol_cols = dict(
        dose_col="volume_infusion_rate",
        unit_col="volume_infusion_rate_unit",
    )
    return dose_targets, vol_cols, vol_frame, vol_targets


@app.cell
def _(convert_dose_units_by_med_category, dose_targets, vol_frame):
    # pass 1: the dose pair, under its default names
    dose_pass, _ = convert_dose_units_by_med_category(
        vol_frame,
        preferred_units=dose_targets,
    )
    return (dose_pass,)


@app.cell
def _(
    convert_dose_units_by_med_category,
    dose_pass,
    vol_cols,
    vol_frame,
    vol_targets,
):
    # pass 2: the volume pair of pass 1's result, with no output names given
    vol_converted, _ = convert_dose_units_by_med_category(
        dose_pass,
        preferred_units=vol_targets,
        **vol_cols,
    )
    # rows in input order, and med_dose passed through untouched
    assert vol_converted["med_dose"].to_list() == vol_frame["med_dose"].to_list()
    vol_converted.select(
        "med_category",
        "med_dose", "med_dose_unit", "med_dose_converted", "med_dose_unit_converted",
        "_convert_status",
        "volume_infusion_rate", "volume_infusion_rate_unit",
        "volume_infusion_rate_converted", "volume_infusion_rate_unit_converted",
        "_convert_status__volume_infusion_rate",
        "_convert_status_details__volume_infusion_rate",
    )
    return (vol_converted,)


@app.cell
def _(
    DEFAULT_COUNTABLE_UNITS,
    convert_dose_units_by_med_category,
    dose_pass,
    vol_cols,
    vol_targets,
):
    # pass 2 again, with 'neb' added to the countable units
    vol_countable, _ = convert_dose_units_by_med_category(
        dose_pass,
        preferred_units=vol_targets,
        countable_units=DEFAULT_COUNTABLE_UNITS | {"neb"},
        **vol_cols,
    )
    return (vol_countable,)


@app.cell
def _(mo, vol_converted, vol_countable):
    neb_compare = mo.sql(
        f"""
        -- The volume pass's 'neb' row with the default countable units, then
        -- with 'neb' added. Its status columns carry the volume pair's suffix.
        FROM vol_converted
        SELECT
            countable_units: 'default'
            , volume_infusion_rate_unit
            , _convert_status__volume_infusion_rate
            , _convert_status_details__volume_infusion_rate
        WHERE volume_infusion_rate_unit = 'neb'
        UNION ALL
        FROM vol_countable
        SELECT
            countable_units: 'default + neb'
            , volume_infusion_rate_unit
            , _convert_status__volume_infusion_rate
            , _convert_status_details__volume_infusion_rate
        WHERE volume_infusion_rate_unit = 'neb'
        """
    )
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## 6. The same through `ClifOrchestrator`

    The orchestrator loads `medication_admin_continuous` and the `weight_kg`
    vitals itself, forwards the weight arguments, and stores the result on the
    table as two polars frames, `df_converted` and `conversion_counts`. The
    cross-stay fallback gets `co.hospitalization.data`, the table's polars
    frame (`.df` would build a pandas copy). Its logs go to a scratch
    directory here, not `./output`. With the same data, targets and weight
    chain it must agree with section 2 row for row.
    """)
    return


@app.cell
def _(
    ClifOrchestrator,
    clif_version,
    demo_dir,
    mcide_cont_targets,
    site_tz,
    tempfile,
    weight_chain,
):
    co = ClifOrchestrator(
        data_directory=str(demo_dir),
        filetype="parquet",
        timezone=site_tz,
        output_directory=tempfile.mkdtemp(prefix="clifpy_med_unit_demo_"),
        clif_version=clif_version,
    )
    co.load_table("hospitalization")
    co.standardize_dose_units_for_continuous_meds(
        mcide_cont_targets,
        weight_fallbacks=weight_chain,
        hospitalization_df=co.hospitalization.data,
    )
    orch_counts = co.medication_admin_continuous.conversion_counts
    return co, orch_counts


@app.cell
def _(mo, orch_counts, std_cont_counts):
    only_in_orch = mo.sql(
        f"""
        -- Counts rows the orchestrator produced that section 2 did not; expected: none.
        FROM orch_counts
        EXCEPT ALL
        FROM std_cont_counts
        """,
        output=False
    )
    return (only_in_orch,)


@app.cell
def _(mo, orch_counts, std_cont_counts):
    only_in_direct = mo.sql(
        f"""
        -- Counts rows section 2 produced that the orchestrator did not; expected: none.
        FROM std_cont_counts
        EXCEPT ALL
        FROM orch_counts
        """,
        output=False
    )
    return (only_in_direct,)


@app.cell(hide_code=True)
def _(co, mo, only_in_direct, only_in_orch, orch_counts):
    assert only_in_orch.is_empty() and only_in_direct.is_empty()
    # the weights were loaded for this call only; co.vitals is never replaced
    assert co.vitals is None
    mo.md(
        f"The orchestrator's `conversion_counts` ({orch_counts.height} rows) match "
        f"section 2 exactly, and `co.vitals` is still `None`: the weights were "
        f"loaded for the call, not stored on the orchestrator."
    )
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## 7. Calling it fast

    The demo is small, so every call above returns at once. On a full site
    table the same calls scale with the rows they read; these patterns keep
    that number down. They are guidance, not run here.

    **Load only the columns and categories you convert.** `load_data` takes
    `columns=` and `filters=`, and pushes both into the parquet scan; vitals
    need only the `weight_kg` rows.

    ```python
    mac = load_data(
        "medication_admin_continuous", data_dir, "parquet", site_tz=site_tz,
        columns=["hospitalization_id", "admin_dttm", "med_category",
                 "med_dose", "med_dose_unit"],
        filters={"med_category": list(targets)},
    )
    weights = load_data(
        "vitals", data_dir, "parquet", site_tz=site_tz,
        columns=["hospitalization_id", "recorded_dttm", "vital_category", "vital_value"],
        filters={"vital_category": ["weight_kg"]},
    )
    ```

    **Stay in polars** (the default `return_format`). `'pandas'` is deprecated
    and copies every column; call `.to_pandas()` only where your own code
    needs it.

    **Attach weights once, and reuse them.** `attach_weight` runs the lookup;
    converting its output reads the attached `weight_kg` instead of looking
    it up again, so the continuous and intermittent tables share one lookup
    rule and the lookup runs once per table.

    ```python
    mac_w = attach_weight(mac, weights, fallbacks=weight_chain, hospitalization_df=hosp)
    mai_w = attach_weight(mai, weights, fallbacks=weight_chain, hospitalization_df=hosp)
    cont, cont_counts = standardize_med_dose_units(mac_w, cont_targets)
    intm, intm_counts = standardize_med_dose_units(mai_w, intm_targets)
    ```

    **Leave `show_intermediate` off** outside QA: it keeps a dozen extra
    columns per row. The counts table already carries the QA columns.

    **Skip the weight lookup when a per-kg variant is acceptable.**
    `allow_weight_variant=True` makes no row cross the weight axis, so no
    weight is read at all: no vitals to load, and no lookup to run (the
    orchestrator then reads no weight vitals, unless you pass `vitals_df` or
    `weight_fallbacks`):

    ```python
    cont, cont_counts = standardize_med_dose_units(mac, cont_targets, allow_weight_variant=True)
    ```

    **Stay lazy for a SQL step downstream.** `return_format="duckdb"` returns
    relations that run when you collect them, so a filter or aggregate you
    add is planned together with the conversion. (`standardize_med_dose_units`
    still evaluates the small counts table once inside the call, for its
    warnings; `convert_dose_units_by_med_category` stays fully lazy.)

    ```python
    cont_rel, _ = standardize_med_dose_units(mac_w, cont_targets, return_format="duckdb")
    hourly = cont_rel.filter("_convert_status = 'success'").aggregate(
        "med_category, n: count(*)"
    ).pl()
    ```
    """)
    return


if __name__ == "__main__":
    app.run()

"""Load CLIF data in each of the four return formats.

    uv run marimo edit examples/io_return_formats_demo.py     # interactive
    uv run examples/io_return_formats_demo.py                 # runs top to bottom

One section per format, written the way you would write it in real code, using
`columns=` and `filters=` as you normally would. Each section loads the table
twice -- once without `site_tz`, once with it -- so you can see `recorded_dttm`
shift from UTC to the site zone.

Note: this runs against the packaged synthetic demo data. Against real CLIF data,
avoid displaying rows that carry an ID column.
"""

import marimo

__generated_with = "0.23.3"
app = marimo.App(width="medium")


@app.cell
def _():
    import marimo as mo

    return (mo,)


@app.cell
def _(mo):
    mo.md(r"""
    # `load_data()` return formats

    | `return_format` | you get | use it when |
    |---|---|---|
    | `'polars'` *(default)* | `pl.DataFrame` | you want the data now |
    | `'polars_lazy'` | `pl.LazyFrame` | you want to chain filters and collect once |
    | `'duckdb'` | `DuckDBPyRelation` | you want to keep composing in SQL |
    | `'pandas'` | `pd.DataFrame` | legacy code — **deprecated** |

    `columns=`, `filters=`, `sample_size=` and `site_tz=` work the same in all four.
    """)
    return


@app.cell
def _():
    from pathlib import Path

    import polars as pl

    import clifpy
    from clifpy.utils.io import load_data, new_duckdb_con

    DATA_DIR = str(Path(clifpy.__file__).parent / "data" / "clif_demo")
    SITE_TZ = "US/Eastern"

    # Typical narrowing: a few columns, a couple of categories.
    VITAL_COLS = ["hospitalization_id", "recorded_dttm", "vital_category", "vital_value"]
    VITAL_KINDS = ["heart_rate", "sbp"]
    return (
        DATA_DIR,
        SITE_TZ,
        VITAL_COLS,
        VITAL_KINDS,
        load_data,
        new_duckdb_con,
        pl,
    )


@app.cell
def _(mo):
    mo.md(r"""
    ## 1. `'polars'` — the default

    Nothing to pass. You get a materialized `pl.DataFrame`.
    """)
    return


@app.cell
def _(DATA_DIR, VITAL_COLS, VITAL_KINDS, load_data):
    # No site_tz -> recorded_dttm comes back UTC.
    vitals_utc = load_data(
        "vitals", DATA_DIR, "parquet",
        columns=VITAL_COLS,
        filters={"vital_category": VITAL_KINDS},
    )
    vitals_utc
    return


@app.cell
def _(DATA_DIR, SITE_TZ, VITAL_COLS, VITAL_KINDS, load_data):
    # Same call with site_tz -> same instants, labelled US/Eastern.
    vitals = load_data(
        "vitals", DATA_DIR, "parquet",
        columns=VITAL_COLS,
        filters={"vital_category": VITAL_KINDS},
        site_tz=SITE_TZ,
    )
    vitals
    return (vitals,)


@app.cell
def _(pl, vitals):
    # `columns=` and `filters=` really did narrow the data.
    vitals.select(
        n_rows=pl.len(),
        n_cols=pl.lit(vitals.width),
        categories=pl.col("vital_category").n_unique(),
    )
    return


@app.cell
def _(pl, vitals):
    # Ordinary polars from here on.
    vitals.group_by("vital_category").agg(
        n=pl.len(), median=pl.col("vital_value").median()
    ).sort("n", descending=True)
    return


@app.cell
def _(mo):
    mo.md(r"""
    ## 2. `'polars_lazy'` — defer the work

    Returns a `pl.LazyFrame`. Nothing runs until `.collect()`.

    `filters=` is applied by the loader; anything you chain afterwards is pushed
    into the parquet reader too, so only the rows you asked for are ever read.
    """)
    return


@app.cell
def _(DATA_DIR, VITAL_COLS, VITAL_KINDS, load_data):
    vitals_lazy_utc = load_data(
        "vitals", DATA_DIR, "parquet",
        columns=VITAL_COLS,
        filters={"vital_category": VITAL_KINDS},
        return_format="polars_lazy",
    )
    vitals_lazy_utc.collect()
    return


@app.cell
def _(DATA_DIR, SITE_TZ, VITAL_COLS, VITAL_KINDS, load_data):
    vitals_lazy = load_data(
        "vitals", DATA_DIR, "parquet",
        columns=VITAL_COLS,
        filters={"vital_category": VITAL_KINDS},
        site_tz=SITE_TZ,
        return_format="polars_lazy",
    )
    vitals_lazy.collect()
    return (vitals_lazy,)


@app.cell
def _(pl, vitals_lazy):
    # Chain more work on top; still nothing has executed.
    heart_rate = vitals_lazy.filter(pl.col("vital_category") == "heart_rate").select(
        "recorded_dttm", "vital_value"
    )
    return (heart_rate,)


@app.cell
def _(heart_rate, mo):
    # `Parquet SCAN` with the predicates pushed into it -- both the loader's
    # filters= and the .filter() added above.
    mo.md(f"```\n{heart_rate.explain()}\n```")
    return


@app.cell
def _(heart_rate, pl):
    heart_rate.select(
        n=pl.len(),
        median_bpm=pl.col("vital_value").median(),
        earliest=pl.col("recorded_dttm").min(),
    ).collect()
    return


@app.cell
def _(mo):
    mo.md(r"""
    ## 3. `'duckdb'` — keep composing in SQL

    Returns a `DuckDBPyRelation` you can join against other relations.

    Two rules, both from the same fact — **a relation holds no values yet**:

    - `site_tz` cannot reach it (there is no column to label), so passing one
      warns. Pin the zone on a connection instead.
    - Relations cannot cross connections. Pass the **same** connection to every
      load you intend to join.
    """)
    return


@app.cell
def _(DATA_DIR, VITAL_COLS, VITAL_KINDS, load_data):
    # Default connection, which load_data pins to UTC.
    vitals_rel_utc = load_data(
        "vitals", DATA_DIR, "parquet",
        columns=VITAL_COLS,
        filters={"vital_category": VITAL_KINDS},
        return_format="duckdb",
    )
    vitals_rel_utc.df()
    return


@app.cell
def _(DATA_DIR, SITE_TZ, VITAL_COLS, VITAL_KINDS, load_data, new_duckdb_con):
    con = new_duckdb_con(site_tz=SITE_TZ)

    vitals_rel = load_data(
        "vitals", DATA_DIR, "parquet",
        columns=VITAL_COLS,
        filters={"vital_category": VITAL_KINDS},
        return_format="duckdb", duckdb_con=con,
    )
    labs_rel = load_data(
        "labs", DATA_DIR, "parquet",
        columns=["hospitalization_id", "lab_category", "lab_value_numeric"],
        filters={"lab_category": "creatinine"},
        return_format="duckdb", duckdb_con=con,
    )

    # Built on a connection pinned to US/Eastern -> renders Eastern.
    vitals_rel.df()
    return labs_rel, vitals_rel


@app.cell
def _(labs_rel, vitals_rel):
    # Both relations live on the same connection, so they compose.
    # Using the relation API rather than a SQL string: marimo can see the
    # dependency on these two names, which it cannot do inside a query string.
    paired = (
        vitals_rel.join(labs_rel, "hospitalization_id")
        .aggregate("vital_category, count(*) AS n")
        .order("n DESC")
    )
    paired.df()
    return


@app.cell
def _(mo):
    mo.md(r"""
    ## 4. `'pandas'` — deprecated

    Still works and warns. Kept only so existing code keeps running; use
    `'polars'` in anything new.
    """)
    return


@app.cell
def _(DATA_DIR, VITAL_COLS, VITAL_KINDS, load_data):
    import warnings

    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        vitals_pd_utc = load_data(
            "vitals", DATA_DIR, "parquet",
            columns=VITAL_COLS,
            filters={"vital_category": VITAL_KINDS},
            return_format="pandas",
        )

    # Filter to ours -- pandas emits unrelated deprecations of its own.
    _ours = [c for c in caught if "return_format" in str(c.message)]
    print(f"{_ours[0].category.__name__}: {_ours[0].message}")
    vitals_pd_utc
    return


@app.cell
def _(DATA_DIR, SITE_TZ, VITAL_COLS, VITAL_KINDS, load_data):
    vitals_pd = load_data(
        "vitals", DATA_DIR, "parquet",
        columns=VITAL_COLS,
        filters={"vital_category": VITAL_KINDS},
        site_tz=SITE_TZ,
        return_format="pandas",
    )
    vitals_pd
    return (vitals_pd,)


@app.cell
def _(mo):
    mo.md(r"""
    ## The arguments behave identically across formats

    `'polars'` / `'polars_lazy'` scan with polars; `'duckdb'` / `'pandas'` go
    through DuckDB SQL. Two engines, so it is worth checking they agree on what
    `columns=` and `filters=` mean.
    """)
    return


@app.cell
def _(DATA_DIR, SITE_TZ, VITAL_COLS, VITAL_KINDS, load_data, pl):
    def _rows_and_cols(fmt):
        _out = load_data(
            "vitals", DATA_DIR, "parquet",
            columns=VITAL_COLS,
            filters={"vital_category": VITAL_KINDS},
            sample_size=500,
            **({} if fmt == "duckdb" else {"site_tz": SITE_TZ}),
            return_format=fmt,
        )
        if isinstance(_out, pl.LazyFrame):
            _out = _out.collect()
        if not isinstance(_out, pl.DataFrame):
            _out = _out.pl() if hasattr(_out, "pl") else pl.from_pandas(_out)
        return _out.height, _out.columns, sorted(_out["vital_category"].unique().to_list())

    args_check = pl.DataFrame(
        [
            {"return_format": _f, "n_rows": _n, "n_cols": len(_c),
             "columns_as_asked": _c == VITAL_COLS,
             "only_requested_categories": _k == sorted(VITAL_KINDS)}
            for _f in ("polars", "polars_lazy", "duckdb", "pandas")
            for _n, _c, _k in [_rows_and_cols(_f)]
        ]
    )
    args_check
    return


@app.cell
def _(mo):
    mo.md(r"""
    ## Timezones agree too

    Two different questions: is the column **labelled** with the zone you asked
    for, and do all four describe the same **instants**?
    """)
    return


@app.cell
def _(pl, vitals, vitals_lazy, vitals_pd, vitals_rel):
    def _utc(series):
        return set(series.dt.convert_time_zone("UTC").to_list())

    _rel_df = vitals_rel.pl()
    _lazy_df = vitals_lazy.collect()
    _pd_utc = set(
        pl.from_pandas(vitals_pd["recorded_dttm"]).dt.convert_time_zone("UTC").to_list()
    )

    timezone_check = pl.DataFrame({
        "return_format": ["polars", "polars_lazy", "duckdb", "pandas"],
        "tz_label": [
            str(vitals.schema["recorded_dttm"].time_zone),
            str(_lazy_df.schema["recorded_dttm"].time_zone),
            str(_rel_df.schema["recorded_dttm"].time_zone),
            str(vitals_pd["recorded_dttm"].dt.tz),
        ],
        "instants_match_pandas": [
            _utc(vitals["recorded_dttm"]) == _pd_utc,
            _utc(_lazy_df["recorded_dttm"]) == _pd_utc,
            _utc(_rel_df["recorded_dttm"]) == _pd_utc,
            True,
        ],
    })
    timezone_check
    return


@app.cell
def _(mo):
    mo.md(r"""
    ### One gotcha, and this demo data triggers it

    The demo tables are CLIF-MIMIC-shaped: dates are shifted ~150 years forward for
    de-identification, which puts them past 2037 — where pytz's DST table stops and
    Python's `zoneinfo` keeps projecting the rule.

    polars' own engine freezes DST the same way pytz does, so the tables above agree.
    But pulling a **single value out into Python** takes a different route:

    ```python
    df["recorded_dttm"].dt.hour()[0]   # 12  -- polars engine, agrees with pandas
    df["recorded_dttm"][0].hour        # 13  -- Python datetime, via zoneinfo
    ```

    Work in columns and you are fine. If you must extract a scalar, take the hour
    with `.dt.hour()` rather than reading it off a `datetime` object.
    """)
    return


@app.cell
def _(mo):
    mo.md(r"""
    `'duckdb'` shows the zone of the connection it was built on — which is the
    zone we pinned, so it lines up here. Had we used the default connection it
    would read `UTC`, and `.df()` would give UTC wall-clock times.

    To relabel a relation you did not pin, convert after materializing:

    ```python
    from clifpy.utils.io import convert_datetime_columns_to_site_tz

    df = convert_datetime_columns_to_site_tz(rel.df(), "US/Eastern")
    ```
    """)
    return


if __name__ == "__main__":
    app.run()

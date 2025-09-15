import pandas as pd
import numpy as np
from tqdm import tqdm
from typing import Union
import duckdb 


def process_resp_support_waterfall(
    resp_support: pd.DataFrame,
    *,
    id_col: str = "hospitalization_id",
    bfill: bool = False,                
    verbose: bool = True,
) -> pd.DataFrame:
    """
    Clean + waterfall-fill the CLIF **`resp_support`** table
    (Python port of Nick's reference R pipeline).

    Parameters
    ----------
    resp_support : pd.DataFrame
        Raw CLIF respiratory-support table **already in UTC**.
    id_col : str, default ``"hospitalization_id"``
        Encounter-level identifier column.
    bfill : bool, default ``False``
        If *True*, numeric setters are back-filled after forward-fill.
        If *False* (default) only forward-fill is used.
    verbose : bool, default ``True``
        Prints progress banners when *True*.

    Returns
    -------
    pd.DataFrame
        Fully processed table with

        * hourly scaffold rows (``HH:59:59``) inserted,
        * device / mode heuristics applied,
        * hierarchical episode IDs (``device_cat_id → …``),
        * numeric waterfall fill inside each ``mode_name_id`` block
          (forward-only or bi-directional per *bfill*),
        * tracheostomy flag forward-filled,
        * one unique row per ``(id_col, recorded_dttm)`` in
          chronological order.

    Notes
    -----
    The function **does not** change time-zones; convert before
    calling if needed.
    """

    p = print if verbose else (lambda *_, **__: None)

    # ------------------------------------------------------------------ #
    # Helper: forward-fill only or forward + back depending on flag      #
    # ------------------------------------------------------------------ #
    def fb(obj):
        if isinstance(obj, (pd.DataFrame, pd.Series)):
            return obj.ffill().bfill() if bfill else obj.ffill()
        raise TypeError("obj must be a pandas DataFrame or Series")

    # ------------------------------------------------------------------ #
    # Small helper to build the hourly scaffold                          #
    #   - tries DuckDB (fast), falls back to pandas                      #
    # ------------------------------------------------------------------ #
    def _build_hourly_scaffold(rs: pd.DataFrame) -> pd.DataFrame:
        # Try DuckDB first
        try:
            # local import so package doesn't hard-depend on it
            if verbose:
                p("  • Building hourly scaffold via DuckDB")

            con = duckdb.connect()
            # Only need id + timestamps for bounds
            con.register("rs", rs[[id_col, "recorded_dttm"]].dropna(subset=["recorded_dttm"]))

            # Generate hourly series from floor(min) to floor(max), then add :59:59
            sql = f"""
            WITH bounds AS (
              SELECT
                {id_col} AS id,
                date_trunc('hour', MIN(recorded_dttm)) AS tmin_h,
                date_trunc('hour', MAX(recorded_dttm)) AS tmax_h
              FROM rs
              GROUP BY 1
            ),
            hour_sequence AS (
              SELECT
                b.id AS {id_col},
                gs.ts + INTERVAL '59 minutes 59 seconds' AS recorded_dttm
              FROM bounds b,
                   LATERAL generate_series(b.tmin_h, b.tmax_h, INTERVAL 1 HOUR) AS gs(ts)
            )
            SELECT {id_col}, recorded_dttm
            FROM hour_sequence
            ORDER BY {id_col}, recorded_dttm
            """
            scaffold = con.execute(sql).df()
            con.close()

            # Ensure pandas datetime with UTC if input was tz-aware
            # (function contract says already UTC; this keeps dtype consistent)
            scaffold["recorded_dttm"] = pd.to_datetime(scaffold["recorded_dttm"], utc=True, errors="coerce")
            scaffold["recorded_date"] = scaffold["recorded_dttm"].dt.date
            scaffold["recorded_hour"] = scaffold["recorded_dttm"].dt.hour
            scaffold["is_scaffold"]   = True
            return scaffold

        except Exception as e:
            if verbose:
                p(f"  • DuckDB scaffold unavailable ({type(e).__name__}: {e}). Falling back to pandas...")
            # ---- Original pandas scaffold (ground truth) ----
            rs_copy = rs.copy()
            rs_copy["recorded_date"] = rs_copy["recorded_dttm"].dt.date
            rs_copy["recorded_hour"] = rs_copy["recorded_dttm"].dt.hour

            min_max = rs_copy.groupby(id_col)["recorded_dttm"].agg(["min", "max"]).reset_index()
            tqdm.pandas(disable=not verbose, desc="Creating hourly scaffolds")
            scaffold = (
                min_max.progress_apply(
                    lambda r: pd.date_range(
                        r["min"].floor("h"),
                        r["max"].floor("h"),
                        freq="1h", tz="UTC"
                    ),
                    axis=1,
                )
                .explode()
                .rename("recorded_dttm")
            )
            scaffold = (
                min_max[[id_col]].join(scaffold)
                .assign(recorded_dttm=lambda d: d["recorded_dttm"].dt.floor("h")
                                               + pd.Timedelta(minutes=59, seconds=59))
            )
            scaffold["recorded_date"] = scaffold["recorded_dttm"].dt.date
            scaffold["recorded_hour"] = scaffold["recorded_dttm"].dt.hour
            scaffold["is_scaffold"]   = True
            return scaffold

    # ------------------------------------------------------------------ #
    # Phase 0 – set-up & hourly scaffold                                 #
    # ------------------------------------------------------------------ #
    p("✦ Phase 0: initialise & create hourly scaffold")
    rs = resp_support.copy()

    # Lower-case categorical strings
    for c in ["device_category", "device_name", "mode_category", "mode_name"]:
        if c in rs.columns:
            rs[c] = rs[c].str.lower()

    # Numeric coercion
    num_cols = [
        "tracheostomy", "fio2_set", "lpm_set", "peep_set",
        "tidal_volume_set", "resp_rate_set", "resp_rate_obs",
        "pressure_support_set", "peak_inspiratory_pressure_set",
    ]
    num_cols = [c for c in num_cols if c in rs.columns]
    if num_cols:
        rs[num_cols] = rs[num_cols].apply(pd.to_numeric, errors="coerce")

    # FiO₂ scaling if documented 40 → 0.40
    if "fio2_set" in rs.columns:
        fio2_mean = rs["fio2_set"].mean(skipna=True)
        if pd.notna(fio2_mean) and fio2_mean > 1.0:
            rs.loc[rs["fio2_set"] > 1, "fio2_set"] /= 100
            p("  • Scaled FiO₂ values > 1 down by /100")

    # Build hourly scaffold (DuckDB if available, else pandas)
    scaffold = _build_hourly_scaffold(rs)
    if verbose:
        p(f"  • Scaffold rows created: {len(scaffold):,}")

    # We keep recorded_date/hour on rs only for temporary ops below
    rs["recorded_date"] = rs["recorded_dttm"].dt.date
    rs["recorded_hour"] = rs["recorded_dttm"].dt.hour

    # ------------------------------------------------------------------ #
    # Phase 1 – heuristic device / mode inference                        #
    # ------------------------------------------------------------------ #
    p("✦ Phase 1: heuristic inference of device & mode")

    # Most-frequent fall-back labels
    device_counts = rs[["device_name", "device_category"]].value_counts().reset_index()

    imv_devices = device_counts.loc[device_counts["device_category"] == "imv", "device_name"]
    most_common_imv_name = imv_devices.iloc[0] if len(imv_devices) > 0 else "ventilator"

    nippv_devices = device_counts.loc[device_counts["device_category"] == "nippv", "device_name"]
    most_common_nippv_name = nippv_devices.iloc[0] if len(nippv_devices) > 0 else "bipap"

    mode_counts = rs[["mode_name", "mode_category"]].value_counts().reset_index()
    cmv_modes = mode_counts.loc[
        mode_counts["mode_category"] == "assist control-volume control", "mode_name"
    ]
    most_common_cmv_name = cmv_modes.iloc[0] if len(cmv_modes) > 0 else "AC/VC"

    # --- 1-a IMV from mode_category
    mask = (
        rs["device_category"].isna() & rs["device_name"].isna()
        & rs["mode_category"].str.contains(
            r"(?:assist control-volume control|simv|pressure control)", na=False, regex=True
            )
    )
    rs.loc[mask, ["device_category", "device_name"]] = ["imv", most_common_imv_name]

    # --- 1-b IMV look-behind/ahead
    rs = rs.sort_values([id_col, "recorded_dttm"])
    prev_cat = rs.groupby(id_col)["device_category"].shift()
    next_cat = rs.groupby(id_col)["device_category"].shift(-1)
    imv_like = (
        rs["device_category"].isna()
        & ((prev_cat == "imv") | (next_cat == "imv"))
        & rs["peep_set"].gt(1) & rs["resp_rate_set"].gt(1) & rs["tidal_volume_set"].gt(1)
    )
    rs.loc[imv_like, ["device_category", "device_name"]] = ["imv", most_common_imv_name]

    # --- 1-c NIPPV heuristics
    prev_cat = rs.groupby(id_col)["device_category"].shift()
    next_cat = rs.groupby(id_col)["device_category"].shift(-1)
    nippv_like = (
        rs["device_category"].isna()
        & ((prev_cat == "nippv") | (next_cat == "nippv"))
        & rs["peak_inspiratory_pressure_set"].gt(1)
        & rs["pressure_support_set"].gt(1)
    )
    rs.loc[nippv_like, "device_category"] = "nippv"
    rs.loc[nippv_like & rs["device_name"].isna(), "device_name"] = most_common_nippv_name

    # --- 1-d Clean duplicates & empty rows
    rs = rs.sort_values([id_col, "recorded_dttm"])
    rs["dup_count"] = rs.groupby([id_col, "recorded_dttm"])["recorded_dttm"].transform("size")
    rs = rs[~((rs["dup_count"] > 1) & (rs["device_category"] == "nippv"))]
    rs["dup_count"] = rs.groupby([id_col, "recorded_dttm"])["recorded_dttm"].transform("size")
    rs = rs[~((rs["dup_count"] > 1) & rs["device_category"].isna())].drop(columns="dup_count")

    # --- 1-e Guard: nasal-cannula rows must never carry PEEP
    if "peep_set" in rs.columns:
        mask_bad_nc = (rs["device_category"] == "nasal cannula") & rs["peep_set"].gt(0)
        if mask_bad_nc.any():
            rs.loc[mask_bad_nc, "device_category"] = np.nan
            p(f"{mask_bad_nc.sum():,} rows had PEEP>0 on nasal cannula device_category reset")

    # Drop rows with nothing useful
    all_na_cols = [
        "device_category", "device_name", "mode_category", "mode_name",
        "tracheostomy", "fio2_set", "lpm_set", "peep_set", "tidal_volume_set",
        "resp_rate_set", "resp_rate_obs", "pressure_support_set",
        "peak_inspiratory_pressure_set",
    ]
    rs = rs.dropna(subset=[c for c in all_na_cols if c in rs.columns], how="all")

    # Unique per timestamp
    rs = rs.drop_duplicates(subset=[id_col, "recorded_dttm"], keep="first")

    # Merge scaffold (exactly like original)
    rs["is_scaffold"] = False
    rs = pd.concat([rs, scaffold], ignore_index=True).sort_values(
        [id_col, "recorded_dttm", "recorded_date", "recorded_hour"]
    )

    # ------------------------------------------------------------------ #
    # Phase 2 – hierarchical IDs                                         #
    # ------------------------------------------------------------------ #
    p("✦ Phase 2: build hierarchical IDs")

    def change_id(col: pd.Series, by: pd.Series) -> pd.Series:
        return (
            col.fillna("missing")
            .groupby(by)
            .transform(lambda s: s.ne(s.shift()).cumsum())
            .astype("int32")
        )

    rs["device_category"] = rs.groupby(id_col)["device_category"].ffill()
    rs["device_cat_id"]   = change_id(rs["device_category"], rs[id_col])

    rs["device_name"] = (
        rs.sort_values("recorded_dttm")
          .groupby([id_col, "device_cat_id"])["device_name"]
          .transform(fb).infer_objects(copy=False)
    )
    rs["device_id"] = change_id(rs["device_name"], rs[id_col])

    rs = rs.sort_values([id_col, "recorded_dttm"])
    rs["mode_category"] = (
        rs.groupby([id_col, "device_id"])["mode_category"]
          .transform(fb).infer_objects(copy=False)
    )
    rs["mode_cat_id"] = change_id(
        rs["mode_category"].fillna("missing"), rs[id_col]
    )

    rs["mode_name"] = (
        rs.groupby([id_col, "mode_cat_id"])["mode_name"]
          .transform(fb).infer_objects(copy=False)
    )
    rs["mode_name_id"] = change_id(
        rs["mode_name"].fillna("missing"), rs[id_col]
    )

    # ------------------------------------------------------------------ #
    # Phase 3 – numeric waterfall                                        #
    # ------------------------------------------------------------------ #
    fill_type = "bi-directional" if bfill else "forward-only"
    p(f"✦ Phase 3: {fill_type} numeric fill inside mode_name_id blocks")

    # FiO₂ default for room-air
    if "fio2_set" in rs.columns:
        rs.loc[(rs["device_category"] == "room air") & rs["fio2_set"].isna(), "fio2_set"] = 0.21

    # Tidal-volume clean-up
    if "tidal_volume_set" in rs.columns:
        bad_tv = (
            ((rs["mode_category"] == "pressure support/cpap") & rs.get("pressure_support_set").notna())
            | (rs["mode_category"].isna() & rs.get("device_name").str.contains("trach", na=False))
            | ((rs["mode_category"] == "pressure support/cpap") & rs.get("device_name").str.contains("trach", na=False))
        )
        rs.loc[bad_tv, "tidal_volume_set"] = np.nan

    num_cols_fill = [
        c for c in [
            "fio2_set", "lpm_set", "peep_set", "tidal_volume_set",
            "pressure_support_set", "resp_rate_set", "resp_rate_obs",
            "peak_inspiratory_pressure_set",
        ] if c in rs.columns
    ]

    def fill_block(g: pd.DataFrame) -> pd.DataFrame:
        if (g["device_category"] == "trach collar").any():
            breaker = (g["device_category"] == "trach collar").cumsum()
            return g.groupby(breaker)[num_cols_fill].apply(fb)
        return fb(g[num_cols_fill])

    p(f"  • applying waterfall fill to {rs[id_col].nunique():,} encounters")
    tqdm.pandas(disable=not verbose, desc="Waterfall fill by mode_name_id")
    rs[num_cols_fill] = (
        rs.groupby([id_col, "mode_name_id"], group_keys=False, sort=False)
          .progress_apply(fill_block)
    )

    # “T-piece” → classify as blow-by
    tpiece = rs["mode_category"].isna() & rs.get("device_name").str.contains("t-piece", na=False)
    rs.loc[tpiece, "mode_category"] = "blow by"

    # Tracheostomy flag forward-fill per encounter
    if "tracheostomy" in rs.columns:
        rs["tracheostomy"] = rs.groupby(id_col)["tracheostomy"].ffill()

    # ------------------------------------------------------------------ #
    # Phase 4 – final tidy-up                                            #
    # ------------------------------------------------------------------ #
    p("✦ Phase 4: final dedup & ordering")
    rs = (
        rs.drop_duplicates()
          .sort_values([id_col, "recorded_dttm"])
          .reset_index(drop=True)
    )

    # Drop helper cols
    rs = rs.drop(columns=[c for c in ["recorded_date", "recorded_hour"] if c in rs.columns])

    p("[OK] Respiratory-support waterfall complete.")
    return rs

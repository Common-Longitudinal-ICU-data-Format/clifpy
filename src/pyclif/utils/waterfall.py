
"""waterfall.py
Module containing utilities for processing and filling missing values in time-series data
using waterfall (forward/backward fill) methods

Functions
---------
process_resp_support_waterfall : Process respiratory support data using waterfall fill
    Takes raw CLIF respiratory support data and applies cleaning and filling methods
    to create a complete time-series dataset.
process_crrt_waterfall: Process the clif crrt therapy table using clinically defined ffill logic.

Notes
-----
All datetime inputs should be in UTC timezone. Convert timezones before using these functions.

Authors: Nick Inghram, Kaveri Chhikara
"""


import pandas as pd
import numpy as np
from tqdm import tqdm
from typing import Union


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
    rs[num_cols] = rs[num_cols].apply(pd.to_numeric, errors="coerce")

    # FiO₂ scaling if documented 40 → 0.40
    fio2_mean = rs["fio2_set"].mean(skipna=True)
    if pd.notna(fio2_mean) and fio2_mean > 1.0:
        rs.loc[rs["fio2_set"] > 1, "fio2_set"] /= 100
        p("  • Scaled FiO₂ values > 1 down by /100")

    rs["recorded_date"]  = rs["recorded_dttm"].dt.date
    rs["recorded_hour"]  = rs["recorded_dttm"].dt.hour

    # Build hourly scaffold
    min_max = rs.groupby(id_col)["recorded_dttm"].agg(["min", "max"]).reset_index()
    tqdm.pandas(disable=not verbose, desc="Creating hourly scaffolds")
    scaffold = (
        min_max.progress_apply(
            lambda r: pd.date_range(r["min"].floor("h"),
                                    r["max"].floor("h"),
                                    freq="1h", tz="UTC"),
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
    scaffold["recorded_date"]  = scaffold["recorded_dttm"].dt.date
    scaffold["recorded_hour"]  = scaffold["recorded_dttm"].dt.hour
    scaffold["is_scaffold"]    = True

    # ------------------------------------------------------------------ #
    # Phase 1 – heuristic device / mode inference                        #
    # ------------------------------------------------------------------ #
    p("✦ Phase 1: heuristic inference of device & mode")

    # Most-frequent fall-back labels
    device_counts = rs[["device_name", "device_category"]].value_counts().reset_index()
    most_common_imv_name   = device_counts.loc[device_counts["device_category"] == "imv",
                                               "device_name"].iloc[0]
    most_common_nippv_name = device_counts.loc[device_counts["device_category"] == "nippv",
                                               "device_name"].iloc[0]

    mode_counts = rs[["mode_name", "mode_category"]].value_counts().reset_index()
    most_common_cmv_name = mode_counts.loc[
        mode_counts["mode_category"] == "assist control-volume control",
        "mode_name"
    ].iloc[0]

    # --- 1-a IMV from mode_category
    mask = (
        rs["device_category"].isna() & rs["device_name"].isna()
        & rs["mode_category"].str.contains(
            r"(assist control-volume control|simv|pressure control)", na=False
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

    # --- 1-d Clean duplicates & empty rows (unchanged logic)
    rs = rs.sort_values([id_col, "recorded_dttm"])
    rs["dup_count"] = rs.groupby([id_col, "recorded_dttm"])["recorded_dttm"].transform("size")
    rs = rs[~((rs["dup_count"] > 1) & (rs["device_category"] == "nippv"))]
    rs["dup_count"] = rs.groupby([id_col, "recorded_dttm"])["recorded_dttm"].transform("size")
    rs = rs[~((rs["dup_count"] > 1) & rs["device_category"].isna())].drop(columns="dup_count")

    # --- 1-e Guard: nasal-cannula rows must never carry PEEP
    mask_bad_nc = (rs["device_category"] == "nasal cannula") & rs["peep_set"].gt(0)

    if mask_bad_nc.any():
        # let the later heuristics try again
        rs.loc[mask_bad_nc, "device_category"] = np.nan
        # emit a debug message
        p(f"{mask_bad_nc.sum():,} rows had PEEP>0 on nasal cannula "
          f"device_category reset")

    # Drop rows with nothing useful
    all_na_cols = [
        "device_category", "device_name", "mode_category", "mode_name",
        "tracheostomy", "fio2_set", "lpm_set", "peep_set", "tidal_volume_set",
        "resp_rate_set", "resp_rate_obs", "pressure_support_set",
        "peak_inspiratory_pressure_set",
    ]
    rs = rs.dropna(subset=all_na_cols, how="all")

    # Unique per timestamp
    rs = rs.drop_duplicates(subset=[id_col, "recorded_dttm"], keep="first")

    # Merge scaffold
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
    rs.loc[(rs["device_category"] == "room air") & rs["fio2_set"].isna(), "fio2_set"] = 0.21

    # Tidal-volume clean-up
    bad_tv = (
        ((rs["mode_category"] == "pressure support/cpap") & rs["pressure_support_set"].notna())
        | (rs["mode_category"].isna() & rs["device_name"].str.contains("trach", na=False))
        | ((rs["mode_category"] == "pressure support/cpap") &
           rs["device_name"].str.contains("trach", na=False))
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
    tpiece = rs["mode_category"].isna() & rs["device_name"].str.contains("t-piece", na=False)
    rs.loc[tpiece, "mode_category"] = "blow by"

    # Tracheostomy flag forward-fill per encounter
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

def process_crrt_waterfall(
    crrt: pd.DataFrame,
    *,
    id_col: str = "hospitalization_id",
    gap_thresh: Union[str, pd.Timedelta] = "2h",
    infer_modes: bool = True,          # infer missing mode from numeric pattern
    flag_missing_bfr: bool = True,     # add QC flag if blood-flow still NaN
    wipe_unused: bool = True,          # null parameters not used by the mode
    fix_islands: bool = True,          # relabel single-row SCUF islands
    verbose: bool = True,
) -> pd.DataFrame:
    """
    Clean + episode-aware forward-fill for the CLIF `crrt_therapy` table.
    Episode-aware clean-up and forward-fill of the CLIF `crrt_therapy` table.

    The function mirrors the respiratory-support “waterfall” logic but adapts it to
    the quirks of Continuous Renal Replacement Therapy (CRRT):

    1. **Episode detection** - a new `crrt_episode_id` starts whenever  
       • `crrt_mode_category` changes **OR**  
       • the gap between successive rows exceeds *gap_thresh* (default 2 h).
    2. **Numeric forward-fill inside an episode** - fills *only* the parameters
       that are clinically relevant for the active mode.
    3. **Mode-specific wiping** after filling, parameters that are **not used**
       in the current mode (e.g. `dialysate_flow_rate` in SCUF) are nulled so
       stale data never bleed across modes.
    4. **Deduplication & ordering** guarantees exactly **one row per
       `(id_col, recorded_dttm)`**, chronologically ordered.

    Parameters
    ----------
    crrt : pd.DataFrame
        Raw `crrt_therapy` table **in UTC**. Must contain the schema columns
        defined on the CLIF website (see docstring footer).
    id_col : str, default ``"hospitalization_id"``
        Encounter-level identifier.
    gap_thresh : str or pd.Timedelta, default ``"2h"``
        Maximum tolerated gap **inside** an episode before a new episode is
        forced. Accepts any pandas-parsable offset string (``"90min"``, ``"3h"``,
        …) or a ``pd.Timedelta``.
    verbose : bool, default ``True``
        If *True* prints progress banners.

    Returns
    -------
    pd.DataFrame
        Processed CRRT DataFrame with

        * ``crrt_episode_id`` (int32) - sequential per encounter,
        * forward-filled numeric parameters **within** each episode,
        * unused parameters blanked per mode,
        * unique, ordered rows ``id_col, recorded_dttm``.

    Add-ons v2.0
    ------------
    • Optional numeric-pattern inference of `crrt_mode_category`.
    • Flags rows that *should* have blood-flow but don't.
    • Optional fix for single-row modality islands (sandwiched rows).
    • Optional wipe vs. keep of parameters not used by the active mode.

    Key steps
    ----------
    0.  Lower-case strings, coerce numerics, **infer** mode when blank.
    1.  **Relabel single-row SCUF islands** (if *fix_islands*).
    2.  Detect `crrt_episode_id` (mode change or >gap_thresh).
    3.  Forward-fill numeric parameters *within* an episode.
    4.  QC flag → `blood_flow_missing_after_ffill` (optional).
    5.  Wipe / flag parameters not valid for the mode (configurable).
    6.  Deduplicate & order ⇒ one row per ``(id_col, recorded_dttm)``.
    """
    p = print if verbose else (lambda *_, **__: None)
    gap_thresh = pd.Timedelta(gap_thresh)

    # ───────────── Phase 0 — prep, numeric coercion, optional inference
    p("✦ Phase 0: prep & numeric coercion (+optional mode inference)")
    df = crrt.copy()

    df["crrt_mode_category"] = df["crrt_mode_category"].str.lower()
    # save original dialysate_flow_rate values
    df["_orig_df"] = df["dialysate_flow_rate"]

    # 0a) RAW SCUF DF‐OUT sanity check
    # look for rows that are already labeled “scuf”
    # and that have a non‐zero dialysate_flow_rate in the raw data
    raw_scuf = df["crrt_mode_category"].str.lower() == "scuf"
    raw_df_positive = df["_orig_df"].fillna(0) > 0

    n_bad = (raw_scuf & raw_df_positive).sum()
    if n_bad:
        print(f"!!!  Found {n_bad} raw SCUF rows with dialysate_flow_rate > 0 (should be 0 or NA)")
        print(" Converting these mode category to NA, keep recorded numerical values as the ground truth")
        df.loc[raw_df_positive, "crrt_mode_category"] = np.nan
    else:
        print("!!! No raw SCUF rows had dialysate_flow_rate > 0")

    NUM_COLS = [
        "blood_flow_rate",
        "pre_filter_replacement_fluid_rate",
        "post_filter_replacement_fluid_rate",
        "dialysate_flow_rate",
        "ultrafiltration_out",
    ]
    NUM_COLS = [c for c in NUM_COLS if c in df.columns]
    df[NUM_COLS] = df[NUM_COLS].apply(pd.to_numeric, errors="coerce")

    #  any row whose original ultrafiltration_out was >0 must never be SCUF
    def drop_scuf_on_positive_df(df, p):
        bad_df  = df["_orig_df"].fillna(0) > 0
        scuf_now = df["crrt_mode_category"] == "scuf"
        n = (bad_df & scuf_now).sum()
        if n:
            p(f"→ Removing {n:,} SCUF labels on rows with DF>0")
            df.loc[bad_df & scuf_now, "crrt_mode_category"] = np.nan
            

    if infer_modes:
        miss = df["crrt_mode_category"].isna()
        pre  = df["pre_filter_replacement_fluid_rate"].notna()
        post = df["post_filter_replacement_fluid_rate"].notna()
        dial = df["dialysate_flow_rate"].notna()
        bf   = df["blood_flow_rate"].notna()
        uf   = df["ultrafiltration_out"].notna()
        all_num_present = df[NUM_COLS].notna().all(axis=1)

        df.loc[miss & all_num_present,                       "crrt_mode_category"] = "cvvhdf"
        df.loc[miss & (~dial) & pre & post,                  "crrt_mode_category"] = "cvvh"
        df.loc[miss & dial & (~pre) & (~post),               "crrt_mode_category"] = "cvvhd"
        df.loc[miss & (~dial) & (~pre) & (~post) & bf & uf,  "crrt_mode_category"] = "scuf"

        filled = (miss & df["crrt_mode_category"].notna()).sum()
        p(f"  • numeric-pattern inference filled {filled:,} missing modes")
        drop_scuf_on_positive_df(df, p)

    # ───────────── Phase 1 — sort and *fix islands before episodes*
    p("✦ Phase 1: sort + SCUF-island fix")
    df = df.sort_values([id_col, "recorded_dttm"]).reset_index(drop=True)

    if fix_islands:
        # after sorting, BEFORE episode detection
        prev_mode = df.groupby(id_col)["crrt_mode_category"].shift()
        next_mode = df.groupby(id_col)["crrt_mode_category"].shift(-1)

        scuf_island = (
            (df["crrt_mode_category"] == "scuf") &
            (prev_mode.notna()) & (next_mode.notna()) &     # ensure we have neighbours
            (prev_mode == next_mode)                        # both neighbours agree
        )

        df.loc[scuf_island, "crrt_mode_category"] = prev_mode[scuf_island]
        n_fixed = scuf_island.sum()
        p(f"  • relabelled {n_fixed:,} SCUF-island rows")
        drop_scuf_on_positive_df(df, p)


    # ───────────── Phase 2 — episode detection (now with fixed modes)
    p("✦ Phase 2: derive `crrt_episode_id`")
    mode_change = (
        df.groupby(id_col)["crrt_mode_category"]
          .apply(lambda s: s != s.shift())
          .reset_index(level=0, drop=True)
    )
    time_gap = df.groupby(id_col)["recorded_dttm"].diff().gt(gap_thresh).fillna(False)
    df["crrt_episode_id"] = ((mode_change | time_gap)
                              .groupby(df[id_col]).cumsum()
                              .astype("int32"))

    # ───────────── Phase 3 — forward-fill numerics inside episodes
    p("✦ Phase 3: forward-fill numeric vars inside episodes")
    tqdm.pandas(disable=not verbose, desc="ffill per episode")
    df[NUM_COLS] = (
        df.groupby([id_col, "crrt_episode_id"], sort=False, group_keys=False)[NUM_COLS]
          .progress_apply(lambda g: g.ffill())
    )

    # QC: blood-flow still missing?
    if flag_missing_bfr and "blood_flow_rate" in NUM_COLS:
        need_bfr = df["crrt_mode_category"].isin(["scuf", "cvvh", "cvvhd", "cvvhdf"])
        df["blood_flow_missing_after_ffill"] = need_bfr & df["blood_flow_rate"].isna()
        p(f"  • blood-flow still missing where required: "
          f"{df['blood_flow_missing_after_ffill'].mean():.1%}")
        
    # Bridge tiny episodes
    
    single_row_ep = (
        df.groupby([id_col, "crrt_episode_id"]).size() == 1
    ).reset_index(name="n").query("n == 1")
    print("Bridging single row episodes")

    rows_to_bridge = df.merge(single_row_ep[[id_col, "crrt_episode_id"]],
                            on=[id_col, "crrt_episode_id"]).index
    
    CAT_COLS = [c for c in ["crrt_mode_category"] if c in df.columns]

    # Combine with the numeric columns we already had
    BRIDGE_COLS = NUM_COLS + CAT_COLS

    # Forward-fill (and back-fill just in case the island is the first row of the encounter)
    df.loc[rows_to_bridge, BRIDGE_COLS] = (
        df.loc[rows_to_bridge, BRIDGE_COLS]
        .groupby(df.loc[rows_to_bridge, id_col])      # keep encounter boundaries
        .apply(lambda g: g.ffill())          
        .reset_index(level=0, drop=True)
    )
    drop_scuf_on_positive_df(df, p)
    # ───────────── Phase 4 — wipe / flag unused parameters
    p("✦ Phase 4: handle parameters not valid for the mode")
    MODE_PARAM_MAP = {
        "scuf":   {"blood_flow_rate", "ultrafiltration_out"},
        "cvvh":   {"blood_flow_rate", "pre_filter_replacement_fluid_rate",
                   "post_filter_replacement_fluid_rate", "ultrafiltration_out"},
        "cvvhd":  {"blood_flow_rate", "dialysate_flow_rate", "ultrafiltration_out"},
        "cvvhdf": {"blood_flow_rate", "pre_filter_replacement_fluid_rate","post_filter_replacement_fluid_rate",
                   "dialysate_flow_rate", "ultrafiltration_out"},
    }

    wiped_totals = {c: 0 for c in NUM_COLS}
    for mode, keep in MODE_PARAM_MAP.items():
        mask = df["crrt_mode_category"] == mode
        drop_cols = list(set(NUM_COLS) - keep)
        if wipe_unused:
            for col in drop_cols:
                wiped_totals[col] += df.loc[mask, col].notna().sum()
            df.loc[mask, drop_cols] = np.nan
        else:
            for col in drop_cols:
                df.loc[mask & df[col].notna(), f"{col}_unexpected"] = True

    if verbose and wipe_unused:
        p("  • cells set → NA by wipe:")
        for col, n in wiped_totals.items():
            p(f"    {col:<35} {n:>8,}")
    # ───────────── Phase 4a — SCUF‐specific sanity check
    if "dialysate_flow_rate" in df.columns:
        # only consider rows that were originally SCUF mode
        # and whose original _orig_df was non‐zero/non‐NA
        scuf_rows = df["crrt_mode_category"] == "scuf"
        orig_bad = df["_orig_df"].fillna(0) > 0

        # these are rows where the *original* data had UF>0 despite SCUF
        bad_orig_scuf = scuf_rows & orig_bad

        n_bad_orig = bad_orig_scuf.sum()
        if n_bad_orig:
            p(f"!!! {n_bad_orig} rows originally labeled SCUF had DF>0 (raw data); forcing DF→NA for those")
            df.loc[bad_orig_scuf, "dialysate_flow_rate"] = np.nan
        else:
            p("!!! No SCUF rows with DF>0")

    # then drop the helper column
    df = df.drop(columns="_orig_df")

    # ───────────── Phase 5 — deduplicate & order
    p("✦ Phase 5: deduplicate & order")
    pre = len(df)
    df = (
        df.drop_duplicates(subset=[id_col, "recorded_dttm"])
          .sort_values([id_col, "recorded_dttm"])
          .reset_index(drop=True)
    )
    p(f"  • dropped {pre - len(df):,} duplicate rows")

    if verbose:
        sparse = df[NUM_COLS].isna().all(axis=1).mean()
        p(f"  • rows with all NUM_COLS missing: {sparse:.1%}")

    p("[OK] CRRT waterfall complete.")
    return df

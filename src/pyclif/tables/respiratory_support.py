from datetime import datetime
from enum import Enum
from typing import List, Dict, Optional
from tqdm import tqdm
import pandas as pd
from ..utils.io import load_data
from ..utils.validator import validate_table


class respiratory_support:
    """Respiratory support table wrapper using lightweight JSON-spec validation."""

    def __init__(self, data: Optional[pd.DataFrame] = None):
        self.df: Optional[pd.DataFrame] = data
        self.errors: List[dict] = []

        if self.df is not None:
            self.validate()

    # ------------------------------------------------------------------
    # Constructors
    # ------------------------------------------------------------------
    @classmethod
    def from_file(cls, table_path: str, table_format_type: str = "parquet"):
        """Load the respiratory support table from *table_path* and build a :class:`respiratory_support`."""
        data = load_data("respiratory_support", table_path, table_format_type)
        return cls(data)

    # ------------------------------------------------------------------
    # Public helpers
    # ------------------------------------------------------------------
    def isvalid(self) -> bool:
        """Return ``True`` if the last :pyfunc:`validate` finished without errors."""
        return not self.errors

    def validate(self):
        """Validate ``self.df`` against the mCIDE *RespiratorySupportModel.json* spec."""
        if self.df is None:
            print("No dataframe to validate.")
            return

        # Run shared validation utility
        self.errors = validate_table(self.df, "respiratory_support")

        # User-friendly status output
        if not self.errors:
            print("Validation completed successfully.")
        else:
            print(f"Validation completed with {len(self.errors)} error(s). See `errors` attribute.")

    # ------------------------------------------------------------------
    # Waterfall Processing
    # ------------------------------------------------------------------
    def waterfall(self, *, id_col: str = "hospitalization_id", verbose: bool = True) -> pd.DataFrame:
        """
        Clean + water-fall-fill the respiratory support data exactly like Nick's reference R pipeline.

        Parameters
        ----------
        id_col : str, default "hospitalization_id"
            Encounter-level identifier.
        verbose : bool, default True
            If True prints progress banners for every phase.

        Returns
        -------
        pd.DataFrame
            Fully processed respiratory-support DataFrame with:
            - hourly scaffold rows inserted (at HH:59:59),
            - device / mode heuristics applied,
            - hierarchical IDs: device_cat_id → device_id → mode_cat_id → mode_name_id,
            - bidirectional numeric waterfall fill inside the most granular ID,
            - tracheostomy forward-filled,
            - one row per timestamp (duplicate-safe), ordered id_col, recorded_dttm.

        Notes
        -----
        - This function does not change timezones; convert before calling if needed.
        - No column re-ordering trick is used (avoids duplicated column error).
        """
        if self.df is None:
            raise ValueError("No data available. Load data first using from_file() or provide data in constructor.")
        
        return self._process_resp_support_waterfall(
            self.df, 
            id_col=id_col, 
            verbose=verbose
        )

    def _process_resp_support_waterfall(
        self,
        resp_support: pd.DataFrame,
        *,
        id_col: str = "hospitalization_id",
        verbose: bool = True,
    ) -> pd.DataFrame:
        """
        Internal method that implements the waterfall processing logic.
        """
        p = print if verbose else (lambda *a, **k: None)

        # ------------------------------------------------------------------ #
        # Phase 0 – set-up & hourly scaffold                                 #
        # ------------------------------------------------------------------ #
        p("✦ Phase 0 – initialise & create hourly scaffold")

        rs = resp_support.copy()

        # Normalise categorical strings
        for c in ["device_category", "device_name", "mode_category", "mode_name"]:
            if c in rs.columns:
                rs[c] = rs[c].str.lower()

        # Numeric coercion
        num_cols = [
            "tracheostomy",
            "fio2_set",
            "lpm_set",
            "peep_set",
            "tidal_volume_set",
            "resp_rate_set",
            "resp_rate_obs",
            "pressure_support_set",
            "peak_inspiratory_pressure_set",
        ]
        num_cols = [c for c in num_cols if c in rs.columns]
        rs[num_cols] = rs[num_cols].apply(pd.to_numeric, errors="coerce")

        rs['fio2_set'] = pd.to_numeric(rs['fio2_set'], errors='coerce')
        # (Optional) If FiO2 is >1 on average => scale by /100
        fio2_mean = rs['fio2_set'].mean(skipna=True)
        # If the mean is greater than 1, divide 'fio2_set' by 100
        if fio2_mean and fio2_mean > 1.0:
            # Only divide values greater than 1 to avoid re-dividing already correct values
            rs.loc[rs['fio2_set'] > 1, 'fio2_set'] = \
                rs.loc[rs['fio2_set'] > 1, 'fio2_set'] / 100
            p("Updated fio2_set to be between 0.21 and 1")
        else:
            p(f"FIO2_SET mean={fio2_mean} is within the required range")

        # Helpers
        rs["recorded_date"] = rs["recorded_dttm"].dt.date
        rs["recorded_hour"] = rs["recorded_dttm"].dt.hour

        # Hourly scaffold rows (HH:59:59)
        min_max = (
            rs.groupby(id_col)["recorded_dttm"]
            .agg(["min", "max"])
            .reset_index()
        )
        scaffold = (
            min_max.apply(
                lambda r: pd.date_range(
                    r["min"].floor("h"),
                    r["max"].floor("h"),
                    freq="1h",
                    tz="UTC",
                ),
                axis=1,
            )
            .explode()
            .rename("recorded_dttm")
        )
        scaffold = (
            min_max[[id_col]]
            .join(scaffold)
            .assign(
                recorded_dttm=lambda d: d["recorded_dttm"].dt.floor("h")
                + pd.Timedelta(minutes=59, seconds=59)
            )
        )
        scaffold["recorded_date"] = scaffold["recorded_dttm"].dt.date
        scaffold["recorded_hour"] = scaffold["recorded_dttm"].dt.hour
        scaffold["is_scaffold"] = True

        # ------------------------------------------------------------------ #
        # Phase 1 – heuristics to infer / clean device & mode                #
        # ------------------------------------------------------------------ #
        p("✦ Phase 1 – heuristic inference of device / mode")

        # Most-common names (used as fall-back labels)
        device_counts = (
            rs[["device_name", "device_category"]]
            .value_counts()
            .to_frame("count")
            .reset_index()
        )
        most_common_imv_name = device_counts.loc[
            device_counts["device_category"] == "imv", "device_name"
        ].iloc[0] if len(device_counts.loc[device_counts["device_category"] == "imv"]) > 0 else "unknown_imv"
        
        most_common_nippv_name = device_counts.loc[
            device_counts["device_category"] == "nippv", "device_name"
        ].iloc[0] if len(device_counts.loc[device_counts["device_category"] == "nippv"]) > 0 else "unknown_nippv"

        mode_counts = (
            rs[["mode_name", "mode_category"]]
            .value_counts()
            .to_frame("count")
            .reset_index()
        )
        most_common_cmv_name = mode_counts.loc[
            mode_counts["mode_category"] == "assist control-volume control",
            "mode_name",
        ].iloc[0] if len(mode_counts.loc[mode_counts["mode_category"] == "assist control-volume control"]) > 0 else "unknown_cmv"

        # 1-a  fill IMV from mode_category
        mask = (
            rs["device_category"].isna()
            & rs["device_name"].isna()
            & rs["mode_category"].str.contains(
                r"(?:assist control-volume control|simv|pressure control)", na=False
            )
        )
        rs.loc[mask, ["device_category", "device_name"]] = ["imv", most_common_imv_name]

        # missing IMV name
        rs.loc[
            (rs["device_category"] == "imv") & rs["device_name"].isna(),
            "device_name",
        ] = most_common_imv_name

        # 1-b  IMV heuristics (look-behind / look-ahead)
        rs = rs.sort_values([id_col, "recorded_dttm"])
        prev_cat = rs.groupby(id_col)["device_category"].shift()
        next_cat = rs.groupby(id_col)["device_category"].shift(-1)

        imv_like = (
            rs["device_category"].isna()
            & ((prev_cat == "imv") | (next_cat == "imv"))
            & rs["peep_set"].gt(1)
            & rs["resp_rate_set"].gt(1)
            & rs["tidal_volume_set"].gt(1)
        )
        rs.loc[imv_like, ["device_category", "device_name"]] = ["imv", most_common_imv_name]

        # 1-c  NIPPV heuristics
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

        # 1-d  clearly IMV again
        prev_cat = rs.groupby(id_col)["device_category"].shift()
        next_cat = rs.groupby(id_col)["device_category"].shift(-1)

        back_to_imv = (
            rs["device_category"].isna()
            & ~rs["device_name"].str.contains("trach", na=False)
            & ((prev_cat == "imv") | (next_cat == "imv"))
            & rs["tidal_volume_set"].gt(0)
            & rs["resp_rate_set"].gt(0)
        )
        rs.loc[back_to_imv, ["device_category", "device_name"]] = [
            "imv",
            most_common_imv_name,
        ]

        fill_mode_mask = back_to_imv & rs["mode_category"].isna()
        rs.loc[
            fill_mode_mask, ["mode_category", "mode_name"]
        ] = ["assist control-volume control", most_common_cmv_name]

        # 1-e  duplicate timestamp handling
        rs["dup_count"] = rs.groupby([id_col, "recorded_dttm"])["recorded_dttm"].transform(
            "size"
        )
        rs = rs[~((rs["dup_count"] > 1) & (rs["device_category"] == "nippv"))]
        rs["dup_count"] = rs.groupby([id_col, "recorded_dttm"])["recorded_dttm"].transform(
            "size"
        )
        rs = rs[~((rs["dup_count"] > 1) & rs["device_category"].isna())].drop(
            columns="dup_count"
        )

        # 1-f  random carried-over BiPAP before trach-collar
        lead_dev = rs.groupby(id_col)["device_category"].shift(-1)
        lag_dev = rs.groupby(id_col)["device_category"].shift()
        drop_bipap = (
            (rs["device_category"] == "nippv")
            & (lead_dev == "trach collar")
            & (lag_dev != "nippv")
        )
        rs = rs[~drop_bipap]

        # 1-g  rows with nothing useful
        all_na_cols = [
            "device_category",
            "device_name",
            "mode_category",
            "mode_name",
            "tracheostomy",
            "fio2_set",
            "lpm_set",
            "peep_set",
            "tidal_volume_set",
            "resp_rate_set",
            "resp_rate_obs",
            "pressure_support_set",
            "peak_inspiratory_pressure_set",
        ]
        rs = rs.dropna(subset=all_na_cols, how="all")

        # unique per timestamp
        rs = rs.drop_duplicates(subset=[id_col, "recorded_dttm"], keep="first")

        rs["is_scaffold"] = False 
        rs = pd.concat([rs, scaffold], ignore_index=True).sort_values(
            [id_col, "recorded_dttm", "recorded_date", "recorded_hour"]
        )

        # ------------------------------------------------------------------ #
        # Phase 2 – hierarchical IDs                                         #
        # ------------------------------------------------------------------ #
        p("✦ Phase 2 – build device / mode hierarchical IDs")

        def change_id(col: pd.Series, by: pd.Series) -> pd.Series:
            return (
                col.fillna("missing")
                .groupby(by)
                .transform(lambda s: s.ne(s.shift()).cumsum())
                .astype("int32")
            )

        # 2-A  device_cat_id
        rs["device_category"] = rs.groupby(id_col)["device_category"].ffill()
        rs["device_cat_id"] = change_id(rs["device_category"], rs[id_col])

        # 2-B  device_id
        rs["device_name"] = (
            rs.sort_values("recorded_dttm")
            .groupby([id_col, "device_cat_id"])["device_name"]
            .transform(lambda s: s.ffill().bfill()).infer_objects(copy=False)
        )
        rs["device_id"] = change_id(rs["device_name"], rs[id_col])

        # 2-C  mode_cat_id
        rs = rs.sort_values([id_col, "recorded_dttm"])
        rs["mode_category"] = (
            rs.groupby([id_col, "device_id"])["mode_category"]
            .transform(lambda s: s.ffill().bfill()).infer_objects(copy=False)
        )
        dev_curr = rs["device_id"]
        dev_prev = rs.groupby(id_col)["device_id"].shift()
        mode_curr = rs["mode_category"].fillna("missing")
        mode_prev = rs.groupby(id_col)["mode_category"].shift().fillna("missing")
        mode_cat_bump = ((dev_curr != dev_prev) | (mode_curr != mode_prev)).astype(int)
        rs["mode_cat_id"] = mode_cat_bump.groupby(rs[id_col]).cumsum().astype("int32")

        # 2-D  mode_name_id
        rs["mode_name"] = (
            rs.groupby([id_col, "mode_cat_id"])["mode_name"]
            .transform(lambda s: s.ffill().bfill()).infer_objects(copy=False)
        )
        cat_curr = rs["mode_cat_id"]
        cat_prev = rs.groupby(id_col)["mode_cat_id"].shift()
        name_curr = rs["mode_name"].fillna("missing")
        name_prev = rs.groupby(id_col)["mode_name"].shift().fillna("missing")
        mode_name_bump = ((cat_curr != cat_prev) | (name_curr != name_prev)).astype(int)
        rs["mode_name_id"] = mode_name_bump.groupby(rs[id_col]).cumsum().astype("int32")

        # ------------------------------------------------------------------ #
        # Phase 3 – numeric waterfall inside mode_name_id                    #
        # ------------------------------------------------------------------ #
        p("✦ Phase 3 – numeric down/up-fill inside mode_name_id blocks")

        # FiO2 default for room-air
        ra_mask = (rs["device_category"] == "room air") & rs["fio2_set"].isna()
        rs.loc[ra_mask, "fio2_set"] = 0.21

        # Bad tidal-volume rows → NA
        tv_bad = (
            ((rs["mode_category"] == "pressure support/cpap") & rs["pressure_support_set"].notna())
            | (rs["mode_category"].isna() & rs["device_name"].str.contains("trach", na=False))
            | (
                (rs["mode_category"] == "pressure support/cpap")
                & rs["device_name"].str.contains("trach", na=False)
            )
        )
        rs.loc[tv_bad, "tidal_volume_set"] = np.nan

        num_cols_fill = [
            c
            for c in [
                "fio2_set",
                "lpm_set",
                "peep_set",
                "tidal_volume_set",
                "pressure_support_set",
                "resp_rate_set",
                "resp_rate_obs",
                "peak_inspiratory_pressure_set",
            ]
            if c in rs.columns
        ]

        def fill_block(g: pd.DataFrame) -> pd.DataFrame:
            if (g["device_category"] == "trach collar").any():
                breaker = (g["device_category"] == "trach collar").cumsum()
                return (
                    g.groupby(breaker)[num_cols_fill]
                    .apply(lambda x: x.ffill().bfill()).infer_objects(copy=False)
                )
            return g[num_cols_fill].ffill().bfill()

        rs[num_cols_fill] = (
            rs.groupby([id_col, "mode_name_id"], group_keys=False, sort=False)
            .apply(fill_block)
        )

        # "t-piece" rows with blank mode_category → classify as blow-by
        tpiece_mask = rs['mode_category'].isna() & rs['device_name'].str.contains('t-piece', na=False)
        rs.loc[tpiece_mask, 'mode_category'] = 'blow by'

        # Tracheostomy forward-fill only
        rs["tracheostomy"] = rs.groupby(id_col)["tracheostomy"].ffill()

        # ------------------------------------------------------------------ #
        # Phase 4 – final tidy-up                                            #
        # ------------------------------------------------------------------ #
        p("✦ Phase 4 – final deduplication & ordering")
        rs = (
            rs.drop_duplicates()
            .sort_values([id_col, "recorded_dttm"])
            .reset_index(drop=True)
        )

        # Final columns to drop
        drop_cols = [
            "recorded_date",
            "recorded_hour"
        ]
        drop_cols = [c for c in drop_cols if c in rs.columns]
        rs = rs.drop(columns=drop_cols)

        p("✅ Respiratory-support waterfall complete.")
        return rs
# Usage

## Main Functions

```python
from clifpy import calculate_sofa2, calculate_sofa2_daily

# this is the generic function that takes in a cohort_df defined by [hospitalization_id, start_dttm, end_dttm] where hospitalization_id can be repeated across rows but windows for each hospitalization_id must be non-overlapping.
sofa2_results = calculate_sofa2(
  cohort_df = cohort_df,
  clif_config_path=CONFIG_PATH, # path to CLIF config file for data loading.
  return_rel=False, # If True, return DuckDB relation for lazy evaluation.
  sofa2_config=SOFA2Config(), # Configuration object with calculation parameters. If None, uses default values define below.
)

# this is the higher-level function that takes in a daily_cohort_df defined by [hospitalization_id, start_dttm, end_dttm] where the hospitalization_id must be unique but windows can be any duration >= 24 hours and will be broken into consecutive 24-hour chunks (with partial days dropped). The output will have an additional column 'nth_day' = 1, 2, 3, ...
# Example: 47h window → 1 row (nth_day=1); 49h window → 2 rows (nth_day=1, 2)
daily_sofa2_results = calculate_sofa2_daily(
  cohort_df = daily_cohort_df,
  clif_config_path=CONFIG_PATH,
  return_rel=False,
  sofa2_config=SOFA2Config(),
)
```

## Alternative Identity Column (`id_name`)

Both functions accept `id_name` and `id_mapping` for scoring by an alternative grouping (e.g. `encounter_block` from `stitch_encounters()`). CLIF tables only have `hospitalization_id`, so when using an alternative ID, the pipeline remaps all CLIF tables internally via `id_mapping` so that ASOF lookbacks work across hospitalization boundaries.

```python
from clifpy.utils.stitching_encounters import stitch_encounters

hosp_stitched, adt_stitched, encounter_mapping = stitch_encounters(hospitalization, adt)

# Option A: pass id_mapping directly (recommended — skips SELECT DISTINCT)
sofa2_results = calculate_sofa2(
  cohort_df=cohort_df,               # [encounter_block, start_dttm, end_dttm]
  clif_config_path=CONFIG_PATH,
  id_name='encounter_block',
  id_mapping=encounter_mapping,       # [hospitalization_id, encounter_block]
)

# Option B: omit id_mapping — cohort must have both columns
sofa2_results = calculate_sofa2(
  cohort_df=cohort_df,               # [hospitalization_id, encounter_block, start_dttm, end_dttm]
  clif_config_path=CONFIG_PATH,
  id_name='encounter_block',
  # id_mapping extracted internally via SELECT DISTINCT hospitalization_id, encounter_block
)
```

## `SOFA2Config` (dataclass)

```python
@dataclass
class SOFA2Config:
  # DISCUSSION
  # Pre-window lookback (hours) - per subscore type
  resp_lookback_hours: float = 6.0
  liver_lookback_hours: float = 24.0
  kidney_lookback_hours: float = 12.0
  hemo_lookback_hours: float = 24.0 # DONE: changed from 12 to 24

  # CV subscore
  pressor_min_duration_minutes: int = 60

  # Resp subscore
  pf_sf_tolerance_hours: float = 4.0

  # Brain subscore (footnote c)
  post_sedation_gcs_invalidate_hours: float = 12.0

  # Kidney subscore (urine output)
  uo_first_measurement_baseline: str = 'window_start'  # 'window_start' | 'first_measurement'
```

## `DuckDBResourceConfig` (dataclass)

Controls DuckDB resource limits (memory, threads, disk) during SOFA-2 computation. When all fields are None (the default on macOS/Linux), DuckDB uses system defaults.

```python
from clifpy.utils._duckdb_config import DuckDBResourceConfig

@dataclass
class DuckDBResourceConfig:
  memory_limit: str | None = None              # e.g., '8GB'. Spills to disk when exceeded.
  temp_directory: str | None = None            # Where spill files go. Default: .tmp in CWD.
  max_temp_directory_size: str | None = None   # e.g., '10GB'. Clean error if exceeded.
  batch_size: int | None = None                # Process cohort in chunks of this size.
  threads: int | None = None                   # Parallel execution thread count.
```

### Auto-detect from system

```python
config = DuckDBResourceConfig.from_system()
```

Detects available RAM and disk, then sets conservative limits. Platform-aware:


| Setting                   | macOS/Linux                        | Windows                    |
| ------------------------- | ---------------------------------- | -------------------------- |
| `memory_limit`            | 70% of available RAM               | 50% of available RAM       |
| `threads`                 | system default (all logical cores) | physical cores only        |
| `temp_directory`          | system default (.tmp in CWD)       | `%TEMP%` (system temp dir) |
| `max_temp_directory_size` | 50% of free disk                   | 50% of free disk           |


### Windows auto-tuning

On Windows, when no `duckdb_config` is passed to `calculate_sofa2()` or `calculate_sofa2_daily()`, `from_system()` is automatically applied. This prevents the default DuckDB behavior (all logical cores, ~80% RAM) from causing excessive context switching and swapping on Windows. The applied settings are logged.

To override on Windows:

```python
# Use explicit config
sofa2_results = calculate_sofa2(
  cohort_df=cohort_df,
  clif_config_path=CONFIG_PATH,
  duckdb_config=DuckDBResourceConfig(memory_limit='4GB', threads=4),
)

# Or use auto-detect with custom memory fraction
sofa2_results = calculate_sofa2(
  cohort_df=cohort_df,
  clif_config_path=CONFIG_PATH,
  duckdb_config=DuckDBResourceConfig.from_system(memory_fraction=0.6),
)

# Use system defaults (skip auto-tuning) by passing empty config
sofa2_results = calculate_sofa2(
  cohort_df=cohort_df,
  clif_config_path=CONFIG_PATH,
  duckdb_config=DuckDBResourceConfig(),  # all None = DuckDB defaults
)
```

---

# Specs


| additional specs                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                               | windowed status | rolling status |
| ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | --------------- | -------------- |
| lab_collect_dttm are used instead of lab_result_dttm for all labs.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                             | DONE            |                |
| in addition to the subscores and total SOFA-2 score, the final output df will also return all the deciding values (e.g. the worst creatinine that decides the kidney score) as well as all the `*_dttm_offset` columns, defined as `measure_dttm - start_dttm` for all the deciding values. Thus, a positive `creatinine_dttm_offset` would show how long after the start of the target time window was the creatinine value measured, while a negative `creatinine_dttm_offset` would mean no creatineine value is found within the target window, and we had to fall back to before the start of the time window for the most recent value -- up to a certain hours defined in details below | DONE            |                |


## Brain (footnote c, d)

- score 0 - gcs_total = 15 OR gcs_motor = 6 'thumbs-up, fist, or peace sign'
- score 1 - gcs_total = 13–14 OR gcs_motor = 5 'localizing to pain' OR need for drugs to treat delirium (footnote e)
- score 2 - gcs_total = 9–12 OR gcs_motor = 4 'withdrawal to pain'
- score 3 - gcs_total = 6–8 OR gcs_motor = 3 'flexion to pain'
- score 4 - gcs_total = 3–5 OR gcs_motor = 1-2 'extension to pain, no response to pain, generalized myoclonus'


| footnote                                                                                                                                                                                                                  | windowed status                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                   | rolling status |
| ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | -------------- |
| c. For sedated patients, use the last GCS before sedation; if unknown, score 0.                                                                                                                                           | DONE: uses episode-based sedation detection (`_detect_sedation_episodes()`) from continuous meds only. GCS measurements falling within any sedation episode (from `sedation_start` to `sedation_end_extended`, where extended = `sedation_end + post_sedation_gcs_invalidate_hours`) are invalidated. If no valid GCS and sedation present, score 0. Sedation drugs: `['propofol', 'dexmedetomidine', 'ketamine', 'midazolam', 'fentanyl', 'hydromorphone', 'morphine', 'remifentanil', 'pentobarbital', 'lorazepam']` DISCUSSION |                |
| d. If full GCS cannot be assessed, use the best motor response score only.                                                                                                                                                | DONE: Two modes controlled by `deprioritize_gcs_motor` (default `False`). **Default (False):** both gcs_total and gcs_motor scored independently, worst (highest) subscore used. **deprioritize_gcs_motor=True:** gcs_motor only used as fallback when no valid gcs_total available (legacy behavior).                                                                                                                                                                                                                                                                                                                                                                                                                   |                |
| e. If the patient is receiving drug therapy for delirium, score 1 point even if GCS is 15. For relevant drugs, see the International Management of Pain, Agitation, and Delirium in Adult Patients in the ICU Guidelines. | DONE: continuous delirium drugs (`['dexmedetomidine']`) use pre-window ASOF + in-window from `medication_admin_continuous`; intermittent delirium drugs (`['haloperidol', 'quetiapine', 'ziprasidone', 'olanzapine']`) use in-window only from `medication_admin_intermittent` with `med_dose > 0 AND mar_action_category != 'not_given'`.                                                                                                                                                                                        |                |


delirium drugs:

- Mentioned in the PADIS guideline and already in MCIDE: dexmedetomidine
- Mentioned in the PADIS guideline but NOT already in MCIDE and we are proposing to add to MCIDE: haloperidol, quetiapine, ziprasidone, olanzapine. DONE: pending mCIDE updates, placeholder codes are added to implementation.
- Mentioned in the PADIS guideline but NOT already in MCIDE and we are NOT proposing to add to MCIDE: statin (rosuvastatin)
- Screened in the Fei et al. study: Dexmedetomidine, Haloperidol, Olanzapine, Quetiapine, and Ziprasidone (all covered in the MCIDE expansion)

### NOTE: Dexmedetomidine dual-role (footnotes c + e interaction)

Dexmedetomidine appears in both `SEDATION_DRUGS` (footnote c) and `CONT_DELIRIUM_DRUGS` (footnote e). This is clinically intentional — dex is genuinely used for both sedation and delirium treatment. When dex is administered:

- **Sedation (c)**: creates sedation episodes, invalidating GCS measured during the infusion (and up to `post_sedation_gcs_invalidate_hours` after)
- **Delirium (e)**: flags `has_delirium_drug=1`, forcing a minimum brain score of 1 when valid GCS = 15

Consequence: GCS recorded **before** dex starts remains valid (not within a sedation episode), and the delirium minimum-score-1 rule still applies. When valid GCS < 13, the delirium flag has no effect (scores 2-4 already exceed the minimum of 1). Test cases 107-108 exercise this interaction.

## Respiratory (footnote f)

- score 0 = PaO₂:FiO₂ ratio >300 mm Hg (>40 kPa) (or, when applicable, SpO₂:FiO₂ ratio >300 mm Hg)
- score 1 = PaO₂:FiO₂ ratio ≤300 mm Hg (≤40 kPa) (or, when applicable, SpO₂:FiO₂ ratio ≤300 mm Hg)
- score 2 = PaO₂:FiO₂ ratio ≤225 mm Hg (≤30 kPa) (or, when applicable, SpO₂:FiO₂ ratio ≤250 mm Hg)
- score 3 = PaO₂:FiO₂ ratio ≤150 mm Hg (≤20 kPa) (or, when applicable, SpO₂:FiO₂ ratio ≤200 mm Hg) and advanced ventilatory support (footnote g, h)
- score 4 = PaO₂:FiO₂ ratio ≤75 mm Hg (≤10 kPa) (or, when applicable, SpO₂:FiO₂ ratio ≤120 mm Hg) and advanced ventilatory support (footnote g, h) or ECMO (footnote i)


| footnote                                                                                                                                                                                                                                                                                                             | windowed status                                                                                                                                                                                                                                                                                                                          | rolling status |
| -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | -------------- |
| f. Use the SpO₂:FiO₂ ratio only when PaO₂:FiO₂ ratio is unavailable and SpO₂ <98%.                                                                                                                                                                                                                                   | DONE                                                                                                                                                                                                                                                                                                                                     |                |
| g. Advanced ventilatory support includes HFNC, CPAP, BiPAP, noninvasive or invasive mechanical ventilation, or long-term home ventilation. Scores of 3-4 require both an appropriate PaO₂:FiO₂ or SpO₂:FiO₂ ratio and advanced ventilatory support; ignore transient changes within 1 hour (e.g., after suctioning). | DONE except for 'ignore transient changes within 1 hour' which is FUTURE                                                                                                                                                                                                                                                                 |                |
| h. Patients without advanced respiratory support can score at most 2 points unless ventilatory support is (1) unavailable or (2) limited by ceiling of treatment; if so, scored by PaO₂:FiO₂ or SpO₂:FiO₂ alone.                                                                                                     | NOT_APPLICABLE cannot operationalize                                                                                                                                                                                                                                                                                                     |                |
| i. If ECMO is used for respiratory failure, assign 4 points in the respiratory system (regardless of PaO₂:FiO₂) and do not score it in the cardiovascular system. If ECMO is used for cardiovascular indications, score it in both cardiovascular and respiratory systems.                                           | DONE: resp ecmo_flag filters to actual ECMO devices only (`mcs_group = 'ecmo' OR ecmo_configuration_category IS NOT NULL`); non-VV ECMO (`ecmo_configuration_category IN ('va','va_v', 'vv_a')`) scores 4 in both resp and CV via `_flag_mechanical_cv_support()`. VV-ECMO (`ecmo_configuration_category = 'vv'`) scores 4 in resp only. |                |



| additional specs                                                                                                                                                                             | windowed status                                                                                     | rolling status |
| -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------- | -------------- |
| impute fio2 from lpm_set and room air (see details below)                                                                                                                                    | DONE                                                                                                |                |
| fio2 are matched to their most recent pao2 or spo2 measurements up to `pf_sf_tolerance_hours` (default = 4 hrs)                                                                              | DONE; `pf_sf_dttm_offset` column added showing the time gap between PaO2/SpO2 and FiO2 measurements |                |
| if no pao2, spo2, or fio2 is found within the target window, the most recent values from before the `start_dttm` of the target window are used, up to `resp_lookback_hours` (default = 6 hr) | DONE                                                                                                |                |
| infer `device_category = 'imv'` when NULL but `mode_category` suggests IMV (Step 0 in `_resp.py`)                                                                                            | DONE                                                                                                |                |
| forward-fill `fio2_set` within contiguous device_category episodes before imputation (Step 0b via `_forward_fill_fio2()` in `_resp.py`)                                                      | DONE                                                                                                |                |


### Respiratory preprocessing (Steps 0, 0b)

Before FiO2 imputation, two preprocessing steps run on the raw `resp_rel`:

**Step 0 — Device heuristic (mode_category → IMV)**: If `device_category IS NULL` and `mode_category` matches `assist control-volume control`, `simv`, or `pressure control` (case-insensitive), infer `device_category = 'imv'`. This ensures the `is_advanced_support` flag is correctly set for scores 3-4. Adapted from the waterfall pipeline (`waterfall.py` Phase 1a).

**Step 0b — FiO2 forward-fill within device episodes** (`_forward_fill_fio2()`): Forward-fills `fio2_set` within contiguous runs of the same `device_category` per hospitalization. Does NOT cross device boundaries. Uses `IS DISTINCT FROM` for NULL-safe episode detection. This is an isolated function that can be added/removed from the pipeline independently. See `.dev/sofa2_waterfall.md` for comparison with the original waterfall and SOFA v1 Polars approaches.

### fio2_set imputation

```sql
CASE
    WHEN t.fio2_set IS NOT NULL AND t.fio2_set > 0 THEN t.fio2_set
    WHEN LOWER(t.device_category) = 'room air' THEN 0.21
    WHEN LOWER(t.device_category) = 'nasal cannula' THEN
        CASE WHEN t.lpm_set <= 1 THEN 0.24
              WHEN t.lpm_set <= 2 THEN 0.28
              WHEN t.lpm_set <= 3 THEN 0.32
              WHEN t.lpm_set <= 4 THEN 0.36
              WHEN t.lpm_set <= 5 THEN 0.40
              WHEN t.lpm_set <= 6 THEN 0.44
              ELSE 0.50 END
    ELSE t.fio2_set
END AS fio2_imputed
```

# Cardiovascular (footnotes j,k,l,m)


| score | spec                                                                                                                                                                                                                                                | windowed status | rolling status |
| ----- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | --------------- | -------------- |
| 0     | MAP ≥70 mm Hg, no vasopressor or inotrope use                                                                                                                                                                                                       | DONE            |                |
| 1     | MAP <70 mm Hg, no vasopressor or inotrope use                                                                                                                                                                                                       | DONE            |                |
| 2     | Low-dose vasopressor (sum of norepinephrine and epinephrine ≤0.2 μg/kg/min) or any dose of other vasopressor or inotrope                                                                                                                            | DONE            |                |
| 3     | Medium-dose vasopressor (sum of norepinephrine and epinephrine >0.2 to ≤0.4 μg/kg/min) or low-dose vasopressor (sum of norepinephrine and epinephrine ≤0.2 μg/kg/min) with any other vasopressor or inotrope                                        | DONE            |                |
| 4     | High-dose vasopressor (sum of norepinephrine and epinephrine >0.4 μg/kg/min) or medium-dose vasopressor (sum of norepinephrine and epinephrine >0.2 to ≤0.4 μg/kg/min) with any other vasopressor or inotrope or mechanical support (footnote i, n) | DONE            |                |



| footnotes                                                                                                                                                                                                              | windowed status                                                                                                                                                                                                                                                                                                                                     | rolling status |
| ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | -------------- |
| j. Count vasopressors only if given as a continuous IV infusion for ≥1 hour.                                                                                                                                           | DONE. applied to ALL vasopressors (norepi, epi, dopamine, dobutamine, vasopressin, phenylephrine, milrinone, angiotensin, isoproterenol).                                                                                                                                                                                                           |                |
| k. Norepinephrine dosing should be expressed as the base: 1 mg norepinephrine base ≈ 2 mg norepinephrine bitartrate monohydrate ≈ 1.89 mg anhydrous bitartrate (hydrogen/acid/tartrate) ≈ 1.22 mg hydrochloride.       | NOT_APPLICABLE                                                                                                                                                                                                                                                                                                                                      |                |
| l. If dopamine is used as a single vasopressor, use these cutoffs: 2 points, ≤20 μg/kg/min; 3 points, >20 to ≤40 μg/kg/min; 4 points, >40 μg/kg/min.                                                                   | DONE. `is_dopamine_only: dopamine_max > 0 AND norepi_epi_sum = 0 AND has_other_non_dopa = 0`                                                                                                                                                                                                                                                        |                |
| m. When vasoactive drugs are unavailable or limited by ceiling of treatment, use MAP cutoffs for scoring: 0 point, ≥70 mm Hg; 1 point, 60-69 mm Hg; 2 points, 50-59 mm Hg; 3 points, 40-49 mm Hg; 4 points, <40 mm Hg. | NOT_APPLICABLE cannot operationalize                                                                                                                                                                                                                                                                                                                |                |
| n. Mechanical cardiovascular support includes venoarterial ECMO, IABP, LV assist device, microaxial flow pump.                                                                                                         | DONE: `_flag_mechanical_cv_support()` detects non-VV ECMO (`ecmo_configuration_category IN ('va','va_v', 'vv_a')`), IABP (`mcs_group = 'iabp'`), LV assist/microaxial (`mcs_group IN ('impella_lvad', 'temporary_lvad', 'durable_lvad')`), RV assist (`mcs_group = 'temporary_rvad'`). Mechanical CV support → 4 points (highest priority in CASE). |                |


# Liver

- score 0 = Total bilirubin ≤1.20 mg/dL (≤20.6 μmol/L)
- score 1 = Total bilirubin ≤3.0 mg/dL (≤51.3 μmol/L)
- score 2 = Total bilirubin ≤6.0 mg/dL (≤102.6 μmol/L)
- score 3 = Total bilirubin ≤12.0 mg/dL (≤205 μmol/L)
- score 4 = Total bilirubin >12 mg/dL (>205 μmol/L)


| additional specs                                                                                                                                                                                    | windowed status | rolling status |
| --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | --------------- | -------------- |
| if no bilirubin_total is found within the target window, the most recent value from before the `start_dttm` of the target window is used, up to `liver_lookback_hours` (default = 24 hr) DISCUSSION | DONE            |                |


# Kidney


| score | spec                                                                                                     | windowed status                                                                                                                                                                                                                     | rolling status |
| ----- | -------------------------------------------------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | -------------- |
| 0     | Creatinine ≤1.20 mg/dL (≤110 μmol/L)                                                                     | DONE                                                                                                                                                                                                                                |                |
| 1     | Creatinine ≤2.0 mg/dL (≤170 μmol/L) OR urine output <0.5 mL/kg/h for 6–12 h                              | DONE: creatinine + UO rate via MIMIC-style trailing self-join. Net UO = output (urine) - input (flush_irrigation_urine). Rate = vol / weight / obs_hours, valid when obs_hours >= 6h. Score 1 iff 6h rate < 0.5 AND 12h rate ≥ 0.5. |                |
| 2     | Creatinine ≤3.50 mg/dL (≤300 μmol/L) OR urine output <0.5 mL/kg/h for ≥12 h                              | DONE: 12h trailing rate < 0.5, valid when obs_hours >= 12h                                                                                                                                                                          |                |
| 3     | Creatinine >3.50 mg/dL (>300 μmol/L) OR urine output <0.3 mL/kg/h for ≥24 h OR anuria (0 mL) for ≥12 h | DONE: 24h rate < 0.3 (obs >= 24h) OR trailing 12h volume ≤ 0 (obs >= 12h)                                                                                                                                                           |                |
| 4     | Receiving or fulfills criteria for RRT (footnotes o,p,q) (includes chronic use)                          | DONE: carry forward score 4 from CRRT for 3 days (configurable via `rrt_carryforward_days`)                                                                                                                                         |                |



| footnote                                                                                                                                                                   | windowed status                                                                         | rolling status |
| -------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | --------------------------------------------------------------------------------------- | -------------- |
| o. Excludes patients receiving RRT exclusively for nonrenal causes (e.g., toxin, bacterial toxin, or cytokine removal).                                                    | NOT_APPLICABLE cannot operationalize                                                    |                |
| p. For patients not receiving RRT (eg, ceiling of treatment, machine unavailability, or delayed start), score 4 points if they otherwise meet RRT criteria (defined below) | DONE: now includes oliguria path (`has_uo_oliguria`) as alternative to creatinine > 1.2 |                |
| q. For patients on intermittent RRT, score 4 points on days not receiving RRT until RRT use is terminated.                                                                 | FUTURE - we dont have intermittent RRT for now                                          |                |


RRT criteria in footnote p:

- creatinine >1.2 mg/dL (>110 μmol/L) OR oliguria (<0.3 mL/kg/h) for >6 hours
- AND 
  - serum potassium >= 6.0 mmol/L OR 
  - metabolic acidosis (pH <= 7.20 and serum bicarbonate <= 12 mmol/L)


| additional specs                                                                                                                                                                                   | windowed status | rolling status |
| -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | --------------- | -------------- |
| if no creatinine, potassium, pH, bicarbonate is found within the target window, values from before the `start_dttm` of the target window are used, up to `kidney_lookback_hours` (default = 12 hr) | DONE            |                |


### Urine Output Rate Calculation

Adapted from MIMIC reference SQL (`urine_output_rate.sql`). Data sources:

- `clif_output` filtered to `output_group = 'urine'` (positive volume)
- `clif_input` filtered to `input_category = 'flush_irrigation_urine'` (subtracted from UO)
- Weight from `clif_vitals` where `vital_category = 'weight_kg'` (ASOF matched)

Pipeline:

1. UNION ALL output + negated irrigation → net UO timeline (24h pre-window + in-window)
2. Aggregate net volume per `(id_name, start_dttm, recorded_dttm)`
3. LAG-based `tm_since_last_uo` — first measurement baseline controlled by `SOFA2Config.uo_first_measurement_baseline` (`'window_start'` default, `'first_measurement'` for conservative)
4. Self-join trailing accumulation: sum volumes + observation times over 6h/12h/24h (uses N-1 hour DATEDIFF following MIMIC convention)
5. Rate = net_vol / weight / obs_hours (valid only when obs_hours >= 6/12/24)
6. Per-window: MIN(rate) → worst rate → UO score (0-3)

Final kidney score = `GREATEST(creatinine_score, COALESCE(uo_score, 0))` with RRT override to 4.

New output columns: `uo_score`, `uo_rate_6hr`, `uo_rate_12hr`, `uo_rate_24hr`, `has_uo_oliguria`, `weight_at_uo`

# Hemostasis

- score 0 = Platelets >150 × 10³/μL
- score 1 = Platelets ≤150 × 10³/μL
- score 2 = Platelets ≤100 × 10³/μL
- score 3 = Platelets ≤80 × 10³/μL
- score 4 = Platelets ≤50 × 10³/μL


| additional specs                                                                                                                                                         | windowed status | rolling status |
| ------------------------------------------------------------------------------------------------------------------------------------------------------------------------ | --------------- | -------------- |
| if no platelet_count is found within the target window, values from before the `start_dttm` of the target window are used, up to `hemo_lookback_hours` (default = 24 hr) | DONE            | DONE (POC)     |


## Other footnotes


| footnote                                                                                                                                                                                                                                                                                                                       | windowed status                                | rolling status |
| ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ | ---------------------------------------------- | -------------- |
| a. The final score is obtained by summing the maximum points from each of the 6 organ systems individually within a 24-hour period, ranging from 0 to 24.                                                                                                                                                                      | DONE                                           |                |
| b. For missing values at day 1, the general recommendation is to score these as 0 points. This may vary for specific purposes (eg, bedside use, research, etc). For sequential scoring, for missing data after day 1, it is to carry forward the last observation, the rationale being that nonmeasurement suggests stability. | DONE: implemented in `calculate_sofa2_daily()` |                |


# Implementation Details

## Cohort Definition

- **Multi-window support**: One row per `({id_name}, start_dttm, end_dttm)` where `id_name` defaults to `hospitalization_id` (or `encounter_block` when using stitched encounters)
- Allows multiple scoring windows per hospitalization (e.g., rolling 24h periods)
- **Non-overlapping windows assumption**: Windows for the same `{id_name}` cannot overlap
- This enables INNER JOIN without data explosion (each measurement belongs to at most one window)

## Window Identity

- `({id_name}, start_dttm)` uniquely identifies each scoring window (defaults to `hospitalization_id`)
- Carried through all intermediate tables via GROUP BY and SELECT
- Final `sofa_scores` output: one row per cohort row (LEFT JOIN preserves all)
- Windows without any data → NULL scores (expected behavior)

## Pre-window Temporal Patterns

Two fundamentally different approaches for handling data around window boundaries, plus three medication-specific scenarios:

### Labs/Vitals Fallback

For labs and vitals (FiO2, PaO2, SpO2, creatinine, bilirubin, platelets, etc.), pre-window values are used **only as fallback** when no in-window data exists. This avoids scoring on stale data when fresh measurements are available.

```sql
-- ASOF JOIN for pre-window (closest measurement before start_dttm)
FROM cohort_rel c
ASOF LEFT JOIN labs_rel t
    ON c.hospitalization_id = t.hospitalization_id
    AND c.start_dttm > t.lab_collect_dttm
SELECT ...
    , t.lab_collect_dttm - c.start_dttm AS creatinine_dttm_offset  -- negative for pre-window
WHERE creatinine_dttm_offset >= -INTERVAL '{lookback_hours} hours'

-- ANTI JOIN fallback (use pre-window only if no in-window data)
WITH windows_with_data AS (
    SELECT DISTINCT hospitalization_id, start_dttm
    FROM in_window_measurements
),
pre_window_fallback AS (
    FROM pre_window_measurements p
    ANTI JOIN windows_with_data w USING (hospitalization_id, start_dttm)
    SELECT *
)
FROM in_window_measurements SELECT *
UNION ALL
FROM pre_window_fallback SELECT *
```

### Medication Administration Window Boundaries

All medication patterns use ASOF JOINs to capture the continuous state of drug infusions across window boundaries. Three scenarios differ in what post-window information is needed:


| Scenario                           | Pre-window                      | In-window                                                                                        | Post-window                                | Used by                                                                 |
| ---------------------------------- | ------------------------------- | ------------------------------------------------------------------------------------------------ | ------------------------------------------ | ----------------------------------------------------------------------- |
| **Window-bounded episodes** (cont) | ASOF: `start_dttm > admin_dttm` | `admin_dttm >= start_dttm AND <= end_dttm`                                                       | none                                       | Sedation (cont only)                                                    |
| **Flag only** (cont)               | ASOF: `start_dttm > admin_dttm` | `admin_dttm >= start_dttm AND <= end_dttm`                                                       | none                                       | Delirium drugs (cont: dexmedetomidine)                                  |
| **Flag only** (intm)               | none                            | `admin_dttm >= start_dttm AND <= end_dttm`, `med_dose > 0`, `mar_action_category != 'not_given'` | none                                       | Delirium drugs (intm: haloperidol, quetiapine, ziprasidone, olanzapine) |
| **Exact duration episodes**        | ASOF: `start_dttm > admin_dttm` | `admin_dttm >= start_dttm AND <= end_dttm`                                                       | First event after: `end_dttm < admin_dttm` | Vasopressors                                                            |


**Why these scenarios?**

- **Window-bounded episodes**: Detect sedation episodes from continuous meds only using the episode detection pipeline (dedup → collapse → LAG → cumulative SUM → episode boundaries). GCS within any episode's `[sedation_start, sedation_end + post_sedation_gcs_invalidate_hours]` range are invalidated. Captures intermittent sedation gaps (e.g., drug stopped and restarted).
- **Flag only**: Just need boolean "was drug active?" — no duration needed. Continuous drugs (dexmedetomidine) use pre-window ASOF + in-window; intermittent drugs only need in-window check.
- **Exact duration**: Need precise episode length for ≥60 min validation (footnote j). Episodes can spill past `end_dttm`, so we fetch the first MAR event *after* the window to measure the full duration.

**Episode detection pattern** (used by exact duration only):

1. MAR dedup via `QUALIFY ROW_NUMBER()` (priority-based, see [dedup section](#deduplication-of-mar_action_category))
2. Collapse to binary on/off (`is_on`) per timestamp
3. `LAG(is_on)` to detect transitions (off → on)
4. Cumulative `SUM` of transitions → episode IDs
5. `FIRST_VALUE` / `LAST_VALUE` over episode window → episode start/end
6. For pressors: validate `DATEDIFF >= 60 min`

## Pre-Window Lookback by Subscore


| Module | Lookback                                                                     | Data                                   | Pattern                         |
| ------ | ---------------------------------------------------------------------------- | -------------------------------------- | ------------------------------- |
| Resp   | 4h                                                                           | FiO2, PaO2, SpO2                       | Labs/vitals fallback            |
| Liver  | 24h                                                                          | Bilirubin                              | Labs/vitals fallback            |
| Kidney | 12h                                                                          | Creatinine, potassium, pH, bicarbonate | Labs/vitals fallback            |
| Hemo   | 24h                                                                          | Platelets                              | Labs/vitals fallback            |
| Brain  | GCS in-window only; sedation cont pre+in only; delirium cont pre+in, intm in | GCS, sedation drugs, delirium drugs    | Mixed                           |
| CV     | Vasopressors pre+in+post; mechanical CV support in-window only               | Vasopressors, ECMO/MCS                 | Exact duration + in-window flag |


## Optional Tables

Some CLIF tables may not be available at all sites. The pipeline handles these gracefully via **empty sentinel relations** — a DuckDB relation with the correct schema but zero rows. The downstream LEFT JOIN + COALESCE pattern converts missing data to safe defaults (0 or NULL).


| Table                           | Required columns                                                                      | Impact when missing                                                                                         | Default behavior                                 |
| ------------------------------- | ------------------------------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------- | ------------------------------------------------ |
| `ecmo_mcs`                      | `hospitalization_id`, `recorded_dttm`, `mcs_group`, `ecmo_configuration_category`     | `has_ecmo` → 0 (resp), `has_mechanical_cv_support` → 0 (CV)                                                 | Warning logged, ECMO scoring skipped             |
| `medication_admin_intermittent` | `hospitalization_id`, `admin_dttm`, `med_category`, `med_dose`, `mar_action_category` | Intermittent delirium drugs and intermittent sedation drugs not detected; only continuous drug sources used | Warning logged, intermittent med scoring skipped |
| `output`                        | `hospitalization_id`, `recorded_dttm`, `output_volume`, `output_group`                | UO scoring skipped; kidney uses creatinine only                                                             | Warning logged, UO scoring skipped               |
| `input`                         | `hospitalization_id`, `recorded_dttm`, `input_volume`, `input_category`               | Irrigation not subtracted from UO; gross output used                                                        | Warning logged, irrigation subtraction skipped   |


**Implementation**: `_load_ecmo_optional()`, `_load_intm_meds_optional()`, `_load_output_optional()`, and `_load_input_optional()` in `_core.py` handle two failure modes:

1. **File not found**: `load_data()` raises exception → caught → empty sentinel + warning
2. **Missing columns**: column validation after load → empty sentinel + warning

## SQL Patterns

> All SQL below shows `hospitalization_id` for readability; in practice, the code uses `{id_name}` via f-strings, which resolves to `encounter_block` (or any alternative) when configured.


| Pattern        | Purpose                                                        | Example                                                                                                                                 |
| -------------- | -------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------- |
| **INNER JOIN** | Carry window identity from `cohort_df`                         | `JOIN cohort_df c ON t.hospitalization_id = c.hospitalization_id AND t.recorded_dttm >= c.start_dttm AND t.recorded_dttm <= c.end_dttm` |
| **ASOF JOIN**  | Match most recent measurement before a timestamp               | `ASOF LEFT JOIN meds t ON c.start_dttm > t.admin_dttm`                                                                                  |
| **ANTI JOIN**  | Implement fallback logic (exclude windows with in-window data) | `ANTI JOIN windows_with_data USING (hospitalization_id, start_dttm)`                                                                    |
| **LEFT JOIN**  | Preserve all cohort rows in final aggregation                  | `FROM cohort_df c LEFT JOIN resp_score USING (hospitalization_id, start_dttm)`                                                          |


## deduplication of mar_action_category

Adapt the following query to deduplicate the mar_action_category column which is necessary for accurately measuring the continuous duration of medication administration (and avoid cases where we mistake the admin as stopped while it was stopped and started again instantly within the same minute.) the permissible values for mar_action_category are: 'dose_change', 'going', 'start', 'stop', 'verify', 'other'.

```sql
SELECT *
FROM meds_df
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY hospitalization_id, admin_dttm, med_category
    ORDER BY 
        -- apply mar action dedup logic
        CASE WHEN mar_action_category IS NULL THEN 10
            WHEN mar_action_category in ('verify', 'not_given') THEN 9
            WHEN mar_action_category = 'stop' THEN 8
            WHEN mar_action_category = 'going' THEN 7
            ELSE 1 END,
        -- if tied at the same mar action, deprioritize zero or null doses
        CASE WHEN med_dose > 0 THEN 1
            ELSE 2 END,
        -- prioritize larger doses
        med_dose DESC 
) = 1
ORDER BY hospitalization_id, med_category, admin_dttm;
```

## File Structure

```
clifpy/utils/sofa2/
  __init__.py    # Public exports: calculate_sofa2, calculate_sofa2_daily, SOFA2Config
  _utils.py      # SOFA2Config + shared aggregation/flag queries + shared scoring SQL helpers
  _brain.py      # Brain subscore (windowed)
  _resp.py       # Respiratory subscore (windowed)
  _cv.py         # Cardiovascular subscore (windowed, most complex)
  _liver.py      # Liver subscore (windowed)
  _kidney.py     # Kidney subscore (windowed)
  _hemo.py       # Hemostasis subscore (windowed)
  _core.py       # Main orchestration functions (windowed)
  _perf.py       # Performance profiling utilities
  rolling/
    __init__.py  # Public exports: calculate_rolling_hemo, RollingSOFA2Config
    _config.py   # RollingSOFA2Config dataclass
    _hemo.py     # Hemostasis subscore (rolling, POC)
```


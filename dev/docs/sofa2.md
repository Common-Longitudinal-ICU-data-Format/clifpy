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

## `SOFA2Config` (dataclass)

```python
@dataclass
class SOFA2Config:
  # Pre-window lookback (hours) - per subscore type
  resp_lookback_hours: float = 4.0
  liver_lookback_hours: float = 24.0
  kidney_lookback_hours: float = 12.0
  hemo_lookback_hours: float = 12.0

  # CV subscore
  pressor_min_duration_minutes: int = 60

  # Resp subscore
  pf_sf_tolerance_hours: float = 4.0

  # Brain subscore (footnote c)
  post_sedation_gcs_invalidate_hours: float = 1.0
```
---

# Specs

| additional specs | status |
|----------|----------|
| lab_collect_dttm are used instead of lab_result_dttm for all labs. | DONE |
| in addition to the subscores and total SOFA-2 score, the final output df will also return all the deciding values (e.g. the worst creatinine that decides the kidney score) as well as all the `*_dttm_offset` columns, defined as `measure_dttm - start_dttm` for all the deciding values. Thus, a positive `creatinine_dttm_offset` would show how long after the start of the target time window was the creatinine value measured, while a negative `creatinine_dttm_offset` would mean no creatineine value is found within the target window, and we had to fall back to before the start of the time window for the most recent value -- up to a certain hours defined in details below | DONE |


## Brain (footnote c, d)
- score 0 = GCS 15 (or, when gcs_total unavailable, gcs_motor = 6 'thumbs-up, fist, or peace sign')
- score 1 = GCS 13–14 (or, when gcs_total unavailable, gcs_motor = 5 'localizing to pain') or need for drugs to treat delirium (footnote e)
- score 2 = GCS 9–12 (or, when gcs_total unavailable, gcs_motor = 4 'withdrawal to pain')
- score 3 = GCS 6–8 (or, when gcs_total unavailable, gcs_motor = 3 'flexion to pain')
- score 4 = GCS 3–5 (or, when gcs_total unavailable, gcs_motor in [0, 1, 2] 'extension to pain, no response to pain, generalized myoclonus')

| footnote | status |
|----------|----------|
| c. For sedated patients, use the last GCS before sedation; if unknown, score 0. | DONE: sedation admin episodes are calculated and only GCS recorded outside of sedation episodes are considered valid and used for scoring. Specifically, the end of sedation episodes can be extended with the `post_sedation_gcs_invalidate_hours` parameter (default = 1 hr) to account for post-sedation lingering effects that could still invalidate GCS. Sedation drugs include `['propofol', 'dexmedetomidine', 'ketamine', 'midazolam', 'fentanyl', 'hydromorphone', 'morphine', 'remifentanil', 'pentobarbital', 'lorazepam']`|
| d. If full GCS cannot be assessed, use the best motor response score only. | DONE: gcs_motor is only used as fallback when no valid (i.e. outside of extended sedation episodes) gcs_total is available.   
| e. If the patient is receiving drug therapy for delirium, score 1 point even if GCS is 15. For relevant drugs, see the International Management of Pain, Agitation, and Delirium in Adult Patients in the ICU Guidelines. | DONE: added relevant drugs (cover only dexmedetomidine for now) |

delirium drugs:
- Mentioned in the PADIS guideline and already in MCIDE: dexmedetomidine
- Mentioned in the PADIS guideline but NOT already in MCIDE and we are proposing to add to MCIDE: haloperidol, quetiapine, ziprasidone, olanzapine, zyprexa TODO: add them into the current script as if they are already available in the same syntax as dexmedetomidine.
- Mentioned in the PADIS guideline but NOT already in MCIDE and we are NOT proposing to add to MCIDE: statin (rosuvastatin)
- Screened in the Fei et al. study: Dexmedetomidine, Haloperidol, Olanzapine, Quetiapine, and Ziprasidone (all covered in the MCIDE expansion)

## Respiratory (footnote f) 
- score 0 = PaO₂:FiO₂ ratio >300 mm Hg (>40 kPa) (or, when applicable, SpO₂:FiO₂ ratio >300 mm Hg)
- score 1 = PaO₂:FiO₂ ratio ≤300 mm Hg (≤40 kPa) (or, when applicable, SpO₂:FiO₂ ratio ≤300 mm Hg)
- score 2 = PaO₂:FiO₂ ratio ≤225 mm Hg (≤30 kPa) (or, when applicable, SpO₂:FiO₂ ratio ≤250 mm Hg)
- score 3 = PaO₂:FiO₂ ratio ≤150 mm Hg (≤20 kPa) (or, when applicable, SpO₂:FiO₂ ratio ≤200 mm Hg) and advanced ventilatory support (footnote g, h)
- score 4 = PaO₂:FiO₂ ratio ≤75 mm Hg (≤10 kPa) (or, when applicable, SpO₂:FiO₂ ratio ≤120 mm Hg) and advanced ventilatory support (footnote g, h) or ECMO (footnote i)

| footnote | status |
|------|----------|
| f. Use the SpO₂:FiO₂ ratio only when PaO₂:FiO₂ ratio is unavailable and SpO₂ <98%. | DONE |
| g. Advanced ventilatory support includes HFNC, CPAP, BiPAP, noninvasive or invasive mechanical ventilation, or long-term home ventilation. Scores of 3-4 require both an appropriate PaO₂:FiO₂ or SpO₂:FiO₂ ratio and advanced ventilatory support; ignore transient changes within 1 hour (e.g., after suctioning). | DONE except for 'ignore transient changes within 1 hour' which is FUTURE |
| h. Patients without advanced respiratory support can score at most 2 points unless ventilatory support is (1) unavailable or (2) limited by ceiling of treatment; if so, scored by PaO₂:FiO₂ or SpO₂:FiO₂ alone. | NOT_APPLICABLE cannot operationalize |
| i. If ECMO is used for respiratory failure, assign 4 points in the respiratory system (regardless of PaO₂:FiO₂) and do not score it in the cardiovascular system. If ECMO is used for cardiovascular indications, score it in both cardiovascular and respiratory systems. | TODO: if `ecmo_mcs.ecmo_configuration_category = 'vv'` then ECMO used for respiratory failure and always score 4 in resp; elif `ecmo_mcs.ecmo_configuration_category in ('va','va_v', 'vv_a')`, then ECMO used for cardiovascular indications and score 4 in both resp and CV.

| additional specs | status |
|----------|----------| 
| impute fio2 from lpm_set and room air (see detailsbelow) | DONE |
| fio2 are matched to their most recent pao2 or spo2 measurements up to `pf_sf_tolerance_hours` (default = 4 hrs) | DONE; `pf_sf_dttm_offset` column added showing the time gap between PaO2/SpO2 and FiO2 measurements |
| if no pao2, spo2, or fio2 is found within the target window, the most recent values from before the `start_dttm` of the target window are used, up to `resp_lookback_hours` (default = 4 hr) | DONE |

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
| score | spec | status |
|----------|----------|----------|
| 0 | MAP ≥70 mm Hg, no vasopressor or inotrope use | DONE |
| 1 | MAP <70 mm Hg, no vasopressor or inotrope use | DONE |
| 2 | Low-dose vasopressor (sum of norepinephrine and epinephrine ≤0.2 μg/kg/min) or any dose of other vasopressor or inotrope | DONE|
| 3 | Medium-dose vasopressor (sum of norepinephrine and epinephrine >0.2 to ≤0.4 μg/kg/min) or low-dose vasopressor (sum of norepinephrine and epinephrine ≤0.2 μg/kg/min) with any other vasopressor or inotrope | DONE|
| 4 | High-dose vasopressor (sum of norepinephrine and epinephrine >0.4 μg/kg/min)  or medium-dose vasopressor (sum of norepinephrine and epinephrine >0.2 to ≤0.4 μg/kg/min) with any other vasopressor or inotrope or mechanical support (footnote i, n) | all DONE except TODO for the 'mechanical support' part, in connection with footnote n. |

| footnotes | status |
|----------|----------|
| j. Count vasopressors only if given as a continuous IV infusion for ≥1 hour. | DONE. applied to ALL vasopressors (norepi, epi, dopamine, dobutamine, vasopressin, phenylephrine, milrinone, angiotensin, isoproterenol). |
| k. Norepinephrine dosing should be expressed as the base: 1 mg norepinephrine base ≈ 2 mg norepinephrine bitartrate monohydrate ≈ 1.89 mg anhydrous bitartrate (hydrogen/acid/tartrate) ≈ 1.22 mg hydrochloride. | NOT_APPLICABLE |
| l. If dopamine is used as a single vasopressor, use these cutoffs: 2 points, ≤20 μg/kg/min; 3 points, >20 to ≤40 μg/kg/min; 4 points, >40 μg/kg/min. | DONE. `is_dopamine_only: dopamine_max > 0 AND norepi_epi_sum = 0 AND has_other_non_dopa = 0`|
| m. When vasoactive drugs are unavailable or limited by ceiling of treatment, use MAP cutoffs for scoring: 0 point, ≥70 mm Hg; 1 point, 60-69 mm Hg; 2 points, 50-59 mm Hg; 3 points, 40-49 mm Hg; 4 points, <40 mm Hg. | NOT_APPLICABLE cannot operationalize |
| n. Mechanical cardiovascular support includes venoarterial ECMO, IABP, LV assist device, microaxial flow pump. | TODO: venoarterial ECMO => `ecmo_mcs.ecmo_configuration_category in ('va','va_v', 'vv_a')`; IABP => `ecmo_mcs.mcs_group = 'iabp'`; LV assist device, microaxial flow pump => `ecmo_mcs.mcs_group IN ('impella_lvad', 'temporary_lvad', 'durable_lvad', 'temporary_rvad')` 


# Liver
- score 0 = Total bilirubin ≤1.20 mg/dL (≤20.6 μmol/L)
- score 1 = Total bilirubin ≤3.0 mg/dL (≤51.3 μmol/L)
- score 2 = Total bilirubin ≤6.0 mg/dL (≤102.6 μmol/L)
- score 3 = Total bilirubin ≤12.0 mg/dL (≤205 μmol/L)
- score 4 = Total bilirubin >12 mg/dL (>205 μmol/L)

| additional specs | status |
|----------|----------| 
| if no bilirubin_total is found within the target window, values from before the `start_dttm` of the target window are used, up to `liver_lookback_hours` (default = 24 hr) | DONE |

# Kidney
| score | spec | status |
|----------|----------|----------|
| 0 | Creatinine ≤1.20 mg/dL (≤110 μmol/L) | DONE |
| 1 | Creatinine ≤2.0 mg/dL (≤170 μmol/L) or urine output <0.5 mL/kg/h for 6–12 h | DONE (UO is not yet in CLIF and thus FUTURE) |
| 2 | Creatinine ≤3.50 mg/dL (≤300 μmol/L) or urine output <0.5 mL/kg/h for ≥12 h | DONE (UO is not yet in CLIF and thus FUTURE) |
| 3 | Creatinine >3.50 mg/dL (>300 μmol/L)  OR urine output <0.3 mL/kg/h for ≥24 h OR anuria (0 mL) for ≥12 h | DONE (UO is not yet in CLIF and thus FUTURE) |
| 4 | Receiving or fulfills criteria for RRT (footnotes o,p,q) (includes chronic use) | DONE |

| footnote | status |
|----------|----------|
| o. Excludes patients receiving RRT exclusively for nonrenal causes (e.g., toxin, bacterial toxin, or cytokine removal). | NOT_APPLICABLE cannot operationalize |
| p. For patients not receiving RRT (eg, ceiling of treatment, machine unavailability, or delayed start), score 4 points if they otherwise meet RRT criteria (defined below) | DONE |
| q. For patients on intermittent RRT, score 4 points on days not receiving RRT until RRT use is terminated. | FUTURE - we dont have intermittent RRT for now |

RRT criteria in footnote p:
- creatinine >1.2 mg/dL (>110 μmol/L) OR oliguria (<0.3 mL/kg/h) for >6 hours
- AND 
  - serum potassium >= 6.0 mmol/L OR 
  - metabolic acidosis (pH <= 7.20 and serum bicarbonate <= 12 mmol/L)

| additional specs | status |
|----------|----------| 
| if no creatinine, potassium, pH, bicarbonate is found within the target window, values from before the `start_dttm` of the target window are used, up to `kidney_lookback_hours` (default = 12 hr) | DONE 

# Hemostasis
- score 0 = Platelets >150 × 10³/μL
- score 1 = Platelets ≤150 × 10³/μL
- score 2 = Platelets ≤100 × 10³/μL
- score 3 = Platelets ≤80 × 10³/μL
- score 4 = Platelets ≤50 × 10³/μL

| additional specs | status |
|----------|----------| 
| if no platelet_count is found within the target window, values from before the `start_dttm` of the target window are used, up to `hemo_lookback_hours` (default = 12 hr) | DONE |

##  Other footnotes
| footnote | status |
|----------|----------|
| a. The final score is obtained by summing the maximum points from each of the 6 organ systems individually within a 24-hour period, ranging from 0 to 24. | DONE |
| b. For missing values at day 1, the general recommendation is to score these as 0 points. This may vary for specific purposes (eg, bedside use, research, etc). For sequential scoring, for missing data after day 1, it is to carry forward the last observation, the rationale being that nonmeasurement suggests stability. | DONE: implemented in `calculate_sofa2_daily()` |



# Implementation Details


## Cohort Definition

- **Multi-window support**: One row per `(hospitalization_id, start_dttm, end_dttm)`

- Allows multiple scoring windows per hospitalization (e.g., rolling 24h periods)

- **Non-overlapping windows assumption**: Windows for the same `hospitalization_id` cannot overlap

- This enables INNER JOIN without data explosion (each measurement belongs to at most one window)

## Window Identity

- `(hospitalization_id, start_dttm)` uniquely identifies each scoring window

- Carried through all intermediate tables via GROUP BY and SELECT

- Final `sofa_scores` output: one row per cohort row (LEFT JOIN preserves all)

- Windows without any data → NULL scores (expected behavior)

## Pre-window Initial State Patterns

Two different patterns for handling measurements before the window start:

| Type | Pattern | Rationale |
|------|---------|-----------|
| **Medications (vaso)** | Always forward-fill | Dose continues until changed; pre-window dose is always relevant |
| **Labs/vitals (FiO2, PaO2, SpO2)** | Fallback only | Don't score on stale data if fresh in-window measurements exist |

### Fallback Logic (labs/vitals)

```sql
WITH windows_with_data AS (
    SELECT DISTINCT hospitalization_id, start_dttm
    FROM in_window_measurements
),
pre_window_fallback AS (
    -- Only for windows WITHOUT in-window data
    FROM pre_window_measurements p
    ANTI JOIN windows_with_data w USING (hospitalization_id, start_dttm)
    SELECT *
)
FROM in_window_measurements SELECT *
UNION ALL
FROM pre_window_fallback SELECT *
```

## Pre-Window Lookback by Subscore

| Module | Lookback | Data |
|----------|----------|------|
| Resp | 4h | FiO2, PaO2, SpO2 |
| Liver | 24h | Bilirubin |
| Kidney | 12h | Creatinine, potassium, pH, bicarbonate |
| Hemo | 12h | Platelets |
| Medication | Always forward-fill | Vasopressors via ASOF JOIN |
| Brain | In-window only | GCS, delirium drug |

## Pre-Window Fallback Pattern

Two patterns for handling measurements before window start:

| Type | Pattern | Rationale |
|------|---------|-----------|
| **Medications (vaso)** | Always forward-fill | Dose continues until changed |
| **Labs/vitals** | Fallback only | Don't score on stale data if fresh in-window data exists |

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
WITH windows_with_data AS (SELECT DISTINCT hospitalization_id, start_dttm FROM in_window),
pre_window_fallback AS (
    FROM pre_window p
    ANTI JOIN windows_with_data USING (hospitalization_id, start_dttm)
)
FROM in_window UNION ALL FROM pre_window_fallback
```


## Medication admin episode duration

**Episode detection pattern:**

1. Identify episode boundaries (dose transitions from 0 → non-zero)

2. Assign episode IDs using cumulative sum

3. Get episode start time with FIRST_VALUE

4. Validate: `DATEDIFF(episode_start, current_event) >= 60 min`

**Window boundary handling:**

- In-window events: `admin_dttm > start_dttm AND admin_dttm < end_dttm` (strict inequality)

- Forward-filled at start: ASOF JOIN with `start_dttm >= admin_dttm`

- Forward-filled at end: ASOF JOIN with `end_dttm >= admin_dttm`

**Rationale:** ASOF JOINs create forward-filled events at window boundaries to capture infusions that span past start_dttm or end_dttm.

## SQL Patterns

| Pattern | Purpose | Example |
|---------|---------|---------|
| **INNER JOIN** | Carry window identity from `cohort_df` | `JOIN cohort_df c ON t.hospitalization_id = c.hospitalization_id AND t.recorded_dttm >= c.start_dttm AND t.recorded_dttm <= c.end_dttm` |
| **ASOF JOIN** | Match most recent measurement before a timestamp | `ASOF LEFT JOIN meds t ON c.start_dttm > t.admin_dttm` |
| **ANTI JOIN** | Implement fallback logic (exclude windows with in-window data) | `ANTI JOIN windows_with_data USING (hospitalization_id, start_dttm)` |
| **LEFT JOIN** | Preserve all cohort rows in final aggregation | `FROM cohort_df c LEFT JOIN resp_score USING (hospitalization_id, start_dttm)` |


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
  _utils.py      # SOFA2Config + shared aggregation/flag queries
  _brain.py      # Brain subscore
  _resp.py       # Respiratory subscore
  _cv.py         # Cardiovascular subscore (most complex)
  _liver.py      # Liver subscore
  _kidney.py     # Kidney subscore
  _hemo.py       # Hemostasis subscore
  _core.py       # Main orchestration functions
```



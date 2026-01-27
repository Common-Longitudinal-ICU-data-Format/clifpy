PLANNED: use ARGMAX or ARGMIN to add timestamps for all the determining, extremal measurements in the final output for QA purpose.

# Brain (footnote c, d)
- score 0 = GCS 15 (or thumbs-up, fist, or peace sign)
- score 1 = GCS 13–14 (or localizing to pain) (footnote d) or need for drugs to treat delirium (footnote e)
- score 2 = GCS 9–12 (or withdrawal to pain)
- score 3 = GCS 6–8 (or flexion to pain)
- score 4 = GCS 3–5 (or extension to pain, no response to pain, generalized myoclonus)

| footnote | status |
|----------|----------|
| c. For sedated patients, use the last GCS before sedation; if unknown, score 0. | TODO |
| d. If full GCS cannot be assessed, use the best motor response score only. | TODO |
| e. If the patient is receiving drug therapy for delirium, score 1 point even if GCS is 15. For relevant drugs, see the International Management of Pain, Agitation, and Delirium in Adult Patients in the ICU Guidelines. | DONE: added relevant drugs (cover only dexmedetomidine for now) |

delirium drugs:
- Mentioned in the PADIS guideline and already in MCIDE: dexmedetomidine
- Mentioned in the PADIS guideline but NOT already in MCIDE and we are proposing to add to MCIDE: haloperidol, quetiapine (atypical antipsychotics), ziprasidone, olanzapine, zyprexa
- Mentioned in the PADIS guideline but NOT already in MCIDE and we are NOT proposing to add to MCIDE: statin (rosuvastatin)
- Screened in the Fei et al. study: Dexmedetomidine, Haloperidol, Olanzapine, Quetiapine, and Ziprasidone (allcovered in the MCIDE expansion)


# Respiratory (footnote f) 
- score 0 = PaO₂:FiO₂ ratio >300 mm Hg (>40 kPa) (or, when applicable, SpO₂:FiO₂ ratio >300 mm Hg)
- score 1 = PaO₂:FiO₂ ratio ≤300 mm Hg (≤40 kPa) (or, when applicable, SpO₂:FiO₂ ratio ≤300 mm Hg)
- score 2 = PaO₂:FiO₂ ratio ≤225 mm Hg (≤30 kPa) (or, when applicable, SpO₂:FiO₂ ratio ≤250 mm Hg)
- score 3 = PaO₂:FiO₂ ratio ≤150 mm Hg (≤20 kPa) (or, when applicable, SpO₂:FiO₂ ratio ≤200 mm Hg) and advanced ventilatory support (footnote g, h)
- score 4 = PaO₂:FiO₂ ratio ≤75 mm Hg (≤10 kPa) (or, when applicable, SpO₂:FiO₂ ratio ≤120 mm Hg) and advanced ventilatory support (footnote g, h) or ECMO (footnote i)

| footnote | status |
|------|----------|
| f. Use the SpO₂:FiO₂ ratio only when PaO₂:FiO₂ ratio is unavailable and SpO₂ <98%. | DONE |
| g. Advanced ventilatory support includes HFNC, CPAP, BiPAP, noninvasive or invasive mechanical ventilation, or long-term home ventilation. Scores of 3-4 require both an appropriate PaO₂:FiO₂ or SpO₂:FiO₂ ratio and advanced ventilatory support; ignore transient changes within 1 hour (e.g., after suctioning). | SHELVED TODO: review others implementation |
| h. Patients without advanced respiratory support can score at most 2 points unless ventilatory support is (1) unavailable or (2) limited by ceiling of treatment; if so, scored by PaO₂:FiO₂ or SpO₂:FiO₂ alone. | DONE |
| i. If ECMO is used for respiratory failure, assign 4 points in the respiratory system (regardless of PaO₂:FiO₂) and do not score it in the cardiovascular system. If ECMO is used for cardiovascular indications, score it in both cardiovascular and respiratory systems. | PENDING ecmo table (VV = 4 in resp, non-VV = 4 in both resp and CV) settings don't matter, can just score based on device |

implementation details:
- added imputation of fio2_set from lpm_set for nasal cannula 
- tracking transient changes within 1 hr?
- current implementation of tolerance is 4 hrs between PaO2 or SpO2 and FiO2 measurements.
- TODO: add pre-window lookback cutoff

# Cardiovascular (footnotes j,k,l,m)
| score | spec | status |
|----------|----------|----------|
| 0 | MAP ≥70 mm Hg, no vasopressor or inotrope use | DONE |
| 1 | MAP <70 mm Hg, no vasopressor or inotrope use | DONE |
| 2 | Low-dose vasopressor (sum of norepinephrine and epinephrine ≤0.2 μg/kg/min) or any dose of other vasopressor or inotrope | DONE|
| 3 | Medium-dose vasopressor (sum of norepinephrine and epinephrine >0.2 to ≤0.4 μg/kg/min) or low-dose vasopressor (sum of norepinephrine and epinephrine ≤0.2 μg/kg/min) with any other vasopressor or inotrope | DONE|
| 4 | High-dose vasopressor (sum of norepinephrine and epinephrine >0.4 μg/kg/min)  or medium-dose vasopressor (sum of norepinephrine and epinephrine >0.2 to ≤0.4 μg/kg/min) with any other vasopressor or inotrope or mechanical support (footnote i, n) | PENDING: Need ECMO table |

| footnotes | status |
|----------|----------|
| j. Count vasopressors only if given as a continuous IV infusion for ≥1 hour. | DONE |
| k. Norepinephrine dosing should be expressed as the base: 1 mg norepinephrine base ≈ 2 mg norepinephrine bitartrate monohydrate ≈ 1.89 mg anhydrous bitartrate (hydrogen/acid/tartrate) ≈ 1.22 mg hydrochloride. | QUESTION: NOT_APPLICABLE ? |
| l. If dopamine is used as a single vasopressor, use these cutoffs: 2 points, ≤20 μg/kg/min; 3 points, >20 to ≤40 μg/kg/min; 4 points, >40 μg/kg/min. | DONE |
| m. When vasoactive drugs are unavailable or limited by ceiling of treatment, use MAP cutoffs for scoring: 0 point, ≥70 mm Hg; 1 point, 60-69 mm Hg; 2 points, 50-59 mm Hg; 3 points, 40-49 mm Hg; 4 points, <40 mm Hg. | QUESTION: NOT_APPLICABLE ? |
| n. Mechanical cardiovascular support includes venoarterial ECMO, IABP, LV assist device, microaxial flow pump. | PENDING need ECMO table |

### footnote j implementation: ≥60min duration validation

Applied to ALL vasopressors (norepi, epi, dopamine, dobutamine, vasopressin, phenylephrine, milrinone, angiotensin, isoproterenol).

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

### footnote l implementation: dopamine-only scoring

**Dopamine tracked separately** from other non-epi/norepi vasopressors to enable dose-based scoring.

**Dopamine-only detection:**

```sql
is_dopamine_only: dopamine_max > 0 AND norepi_epi_sum = 0 AND has_other_non_dopa = 0
```

**Dose-based cutoffs (μg/kg/min):**

- ≤20 → 2 points

- >20 to ≤40 → 3 points

- >40 → 4 points

## deduplication of mar_action_category
Adapt the following query to deduplicate the mar_action_category column which is necessary for accurately measuring the continuous duration of medication administration (and avoid cases where we mistake the admin as stopped while it was stopped and started again instantly.) the permissible values for mar_action_category are: 'dose_change', 'going', 'start', 'stop', 'verify', 'other'.
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

# Liver
ALL DONE:
- score 0 = Total bilirubin ≤1.20 mg/dL (≤20.6 μmol/L)
- score 1 = Total bilirubin ≤3.0 mg/dL (≤51.3 μmol/L)
- score 2 = Total bilirubin ≤6.0 mg/dL (≤102.6 μmol/L)
- score 3 = Total bilirubin ≤12.0 mg/dL (≤205 μmol/L)
- score 4 = Total bilirubin >12 mg/dL (>205 μmol/L)

24 hr pre-window lookback

# Kidney
- score 0 = Creatinine ≤1.20 mg/dL (≤110 μmol/L)
- score 1 = Creatinine ≤2.0 mg/dL (≤170 μmol/L)  
  or urine output <0.5 mL/kg/h for 6–12 h
- score 2 = Creatinine ≤3.50 mg/dL (≤300 μmol/L)  
  or urine output <0.5 mL/kg/h for ≥12 h
- score 3 = Creatinine >3.50 mg/dL (>300 μmol/L)  
  or urine output <0.3 mL/kg/h for ≥24 h  
  or anuria (0 mL) for ≥12 h
- score 4 = Receiving or fulfills criteria for RRT (footnotes o,p,q) (includes chronic use)

| footnote | status |
|----------|----------|
| o. Excludes patients receiving RRT exclusively for nonrenal causes (e.g., toxin, bacterial toxin, or cytokine removal). | QUESTION - NOT_APPLICABLE |
| p. For patients not receiving RRT (eg, ceiling of treatment, machine unavailability, or delayed start), score 4 points if they otherwise meet RRT criteria (defined below) | TODO |
| q. For patients on intermittent RRT, score 4 points on days not receiving RRT until RRT use is terminated. | QUESTION - we dont have intermittent RRT yet, no? |

RRT criteria: 
- creatinine >1.2 mg/dL (>110 μmol/L) OR oliguria (<0.3 mL/kg/h) for >6 hours
- AND 
  - serum potassium >= 6.0 mmol/L OR 
  - metabolic acidosis (pH <= 7.20 and serum bicarbonate <= 12 mmol/L)

12 hr pre-window lookback

# Hemostasis
ALL DONE:
- score 0 = Platelets >150 × 10³/μLf
- score 1 = Platelets ≤150 × 10³/μL
- score 2 = Platelets ≤100 × 10³/μL
- score 3 = Platelets ≤80 × 10³/μL
- score 4 = Platelets ≤50 × 10³/μL

12 hr pre-window lookback

# Other footnotes
| footnote | status |
|----------|----------|
| a. The final score is obtained by summing the maximum points from each of the 6 organ systems individually within a 24-hour period, ranging from 0 to 24. | TODO |
| b. For missing values at day 1, the general recommendation is to score these as 0 points. This may vary for specific purposes (eg, bedside use, research, etc). For sequential scoring, for missing data after day 1, it is to carry forward the last observation, the rationale being that nonmeasurement suggests stability. | TODO |



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

## SQL Patterns

| Pattern | Purpose | Example |
|---------|---------|---------|
| **INNER JOIN** | Carry window identity from `cohort_df` | `JOIN cohort_df c ON t.hospitalization_id = c.hospitalization_id AND t.recorded_dttm >= c.start_dttm AND t.recorded_dttm <= c.end_dttm` |
| **ASOF JOIN** | Match most recent measurement before a timestamp | `ASOF LEFT JOIN meds t ON c.start_dttm > t.admin_dttm` |
| **ANTI JOIN** | Implement fallback logic (exclude windows with in-window data) | `ANTI JOIN windows_with_data USING (hospitalization_id, start_dttm)` |
| **LEFT JOIN** | Preserve all cohort rows in final aggregation | `FROM cohort_df c LEFT JOIN resp_score USING (hospitalization_id, start_dttm)` |

## FiO2 Imputation

For nasal cannula without explicit FiO2, impute from `lpm_set`:

| LPM | Imputed FiO2 |
|-----|--------------|
| ≤1 | 0.24 |
| ≤2 | 0.28 |
| ≤3 | 0.32 |
| ≤4 | 0.36 |
| ≤5 | 0.40 |
| ≤6 | 0.44 |
| >6 | 0.50 |

Room air = 0.21
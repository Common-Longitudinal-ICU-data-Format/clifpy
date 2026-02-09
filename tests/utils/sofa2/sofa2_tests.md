# SOFA-2 Test Plan

## Architecture: Hybrid Approach

| Approach | Pros | Cons |
|----------|------|------|
| **All-in-One** | Single cohort, tests orchestration | Hard to isolate failures, combinatorial explosion |
| **Individual** | Focused, easy to debug | Doesn't test integration |
| **Hybrid** | Best of both | Slightly more files |

Each subscore has its **own cohort + data** for test isolation. A small integration test covers `_core.py` orchestration.

---

## Directory Structure

```
tests/utils/sofa2/
├── sofa2_tests.md          # This file
├── resp/
│   ├── clif_cohort.csv
│   ├── clif_respiratory_support.csv
│   ├── clif_vitals.csv
│   ├── clif_labs.csv
│   ├── clif_ecmo_mcs.csv
│   ├── resp_expected.csv
│   └── test_sofa2_resp.py
├── brain/
│   ├── clif_cohort.csv
│   ├── clif_patient_assessments.csv
│   ├── clif_medication_admin_continuous.csv
│   ├── brain_expected.csv
│   └── test_sofa2_brain.py
├── cv/
│   ├── clif_cohort.csv
│   ├── clif_vitals.csv
│   ├── clif_medication_admin_continuous.csv
│   ├── cv_expected.csv
│   └── test_sofa2_cv.py
├── liver/
│   ├── clif_cohort.csv
│   ├── clif_labs.csv
│   ├── liver_expected.csv
│   └── test_sofa2_liver.py
├── kidney/
│   ├── clif_cohort.csv
│   ├── clif_labs.csv
│   ├── clif_crrt_therapy.csv
│   ├── kidney_expected.csv
│   └── test_sofa2_kidney.py
├── hemo/
│   ├── clif_cohort.csv
│   ├── clif_labs.csv
│   ├── hemo_expected.csv
│   └── test_sofa2_hemo.py
└── integration/
    ├── clif_cohort.csv
    ├── (all CLIF tables)
    ├── sofa2_expected.csv
    └── test_sofa2_integration.py
```

---

## Data Model Reference

### Input Tables (CLIF Format)

| Table | Key Columns | Notes |
|-------|-------------|-------|
| `respiratory_support` | hospitalization_id, recorded_dttm, device_category, fio2_set, lpm_set, mode_category | |
| `labs` | hospitalization_id, **lab_collect_dttm**, lab_category, lab_value_numeric | `lab_collect_dttm` not `lab_result_dttm` |
| `vitals` | hospitalization_id, recorded_dttm, vital_category, vital_value | |
| `ecmo_mcs` | hospitalization_id, recorded_dttm, ecmo_configuration_category, mcs_group | |
| `patient_assessments` | hospitalization_id, recorded_dttm, assessment_category, assessment_value | |
| `medication_admin_continuous` | hospitalization_id, admin_dttm, med_category, med_dose, med_dose_unit, action_category | |
| `crrt_therapy` | hospitalization_id, recorded_dttm, crrt_mode_category | |

### Subscore Input Requirements

| Subscore | CLIF Tables | Lab Categories | Vital Categories | Med Categories | Other |
|----------|-------------|----------------|------------------|----------------|-------|
| **Resp** | respiratory_support, labs, vitals, ecmo_mcs | po2_arterial | spo2 | - | device_category |
| **Brain** | patient_assessments, medication_admin_continuous | - | - | dexmedetomidine | gcs_total |
| **CV** | medication_admin_continuous, vitals | - | map | norepinephrine, epinephrine, dopamine, dobutamine, vasopressin, phenylephrine, milrinone, angiotensin_ii, isoproterenol | |
| **Liver** | labs | bilirubin_total | - | - | |
| **Kidney** | labs, crrt_therapy | creatinine, potassium, ph_arterial, ph_venous, bicarbonate | - | - | crrt_mode_category |
| **Hemo** | labs | platelet_count | - | - | |

### Resp Output Columns

```
hospitalization_id, start_dttm, pf_ratio, sf_ratio, has_advanced_support,
device_category, pao2_at_worst, pao2_dttm_offset, spo2_at_worst,
spo2_dttm_offset, fio2_at_worst, fio2_dttm_offset, has_ecmo, resp
```

---

## Test Case Outlines (Future Subscores)

### Resp (hosp_id 1-22)

| hosp_id | pf_ratio | sf_ratio | has_advanced | has_ecmo | resp | notes |
|---------|----------|----------|--------------|----------|------|-------|
| 1 | 380.95 | NULL | 0 | 0 | 0 | P/F > 300 |
| 2 | NULL | 333.33 | 0 | 0 | 0 | S/F > 300, no PaO2 |
| 3 | 280.00 | NULL | 0 | 0 | 1 | P/F <= 300 |
| 4 | NULL | 290.00 | 0 | 0 | 1 | S/F <= 300 |
| 5 | 200.00 | NULL | 1 | 0 | 2 | P/F <= 225 |
| 6 | NULL | 240.00 | 1 | 0 | 2 | S/F <= 250 |
| 7 | 120.00 | NULL | 1 | 0 | 3 | P/F <= 150 + vent |
| 8 | NULL | 180.00 | 1 | 0 | 3 | S/F <= 200 + vent |
| 9 | 120.00 | NULL | 0 | 0 | 2 | P/F <= 150 no vent -> capped at 2 |
| 10 | 60.00 | NULL | 1 | 0 | 4 | P/F <= 75 + vent |
| 11 | NULL | 100.00 | 1 | 0 | 4 | S/F <= 120 + vent |
| 12 | 400.00 | NULL | 1 | 1 | 4 | ECMO override |
| 13 | 380.95 | NULL | 0 | 0 | 0 | FiO2 imputed (room air) |
| 14 | 222.22 | NULL | 0 | 0 | 2 | FiO2 imputed (4 LPM) |
| 15 | 400.00 | NULL | 0 | 0 | 0 | Pre-window FiO2 fallback |
| 16 | 200.00 | NULL | 1 | 0 | 2 | Pre-window ignored |
| 17 | NULL | NULL | NULL | 0 | NULL | >4hr tolerance exceeded |
| 18 | NULL | 285.00 | 0 | 0 | 1 | S/F fallback (no PaO2) |
| 19 | NULL | NULL | NULL | 0 | NULL | SpO2 >=98% filtered |
| 20-w1 | 200.00 | NULL | 1 | 0 | 2 | Multi-window: deterioration w1 |
| 20-w2 | 60.00 | NULL | 1 | 0 | 4 | Multi-window: deterioration w2 |
| 21-w1 | 60.00 | NULL | 1 | 0 | 4 | Multi-window: recovery w1 |
| 21-w2 | 350.00 | NULL | 0 | 0 | 0 | Multi-window: recovery w2 |
| 22 | NULL | NULL | NULL | 0 | NULL | No data |

### Brain (hosp_id 101-107)

| hosp_id | gcs_min | has_delirium_drug | brain | notes |
|---------|---------|-------------------|-------|-------|
| 101 | 15 | 0 | 0 | GCS >= 15 |
| 102 | 15 | 1 | 1 | GCS 15 + dexmed -> forced score 1 |
| 103 | 14 | 0 | 1 | GCS 13-14 |
| 104 | 10 | 0 | 2 | GCS 9-12 |
| 105 | 7 | 0 | 3 | GCS 6-8 |
| 106 | 4 | 0 | 4 | GCS 3-5 |
| 107 | NULL | 0 | NULL | No GCS data |

### CV (hosp_id 201-213)

| hosp_id | map_min | ne+epi | dopamine | other_vaso | cv | notes |
|---------|---------|--------|----------|------------|-----|-------|
| 201 | 75 | 0 | 0 | 0 | 0 | MAP >= 70, no pressors |
| 202 | 65 | 0 | 0 | 0 | 1 | MAP < 70, no pressors |
| 203 | 60 | 0.1 | 0 | 0 | 2 | Low-dose ne+epi (<=0.2) |
| 204 | 55 | 0.3 | 0 | 0 | 3 | Medium-dose (0.2-0.4) |
| 205 | 50 | 0.5 | 0 | 0 | 4 | High-dose (>0.4) |
| 206 | 60 | 0 | 15 | 0 | 2 | Dopamine <=20 |
| 207 | 55 | 0 | 30 | 0 | 3 | Dopamine 20-40 |
| 208 | 50 | 0 | 50 | 0 | 4 | Dopamine >40 |
| 209 | 60 | 0 | 0 | 1 | 2 | Other vasopressor only |
| 210 | 55 | 0.15 | 0 | 1 | 3 | Low ne+epi + other |
| 211 | NULL | 0 | 0 | 0 | NULL | No MAP data |
| 212 | 60 | 0.2 | 0 | 0 | 2 | Pressor <60 min -> ignored |
| 213 | 60 | 0.2 | 0 | 0 | 2 | Pressor >=60 min -> counted |

### Liver (hosp_id 301-306)

| hosp_id | bilirubin_total | liver | notes |
|---------|-----------------|-------|-------|
| 301 | 1.0 | 0 | <= 1.2 |
| 302 | 2.5 | 1 | <= 3.0 |
| 303 | 5.0 | 2 | <= 6.0 |
| 304 | 10.0 | 3 | <= 12.0 |
| 305 | 15.0 | 4 | > 12.0 |
| 306 | NULL | NULL | No data |

### Kidney (hosp_id 401-408)

| hosp_id | creatinine | has_rrt | rrt_criteria | kidney | notes |
|---------|------------|---------|--------------|--------|-------|
| 401 | 1.0 | 0 | 0 | 0 | <= 1.2 |
| 402 | 1.5 | 0 | 0 | 1 | > 1.2 |
| 403 | 2.5 | 0 | 0 | 2 | > 2.0 |
| 404 | 4.0 | 0 | 0 | 3 | > 3.5 |
| 405 | 2.0 | 1 | 0 | 4 | RRT present |
| 406 | 1.5 | 0 | 1 | 4 | RRT criteria met (K>=6) |
| 407 | 1.5 | 0 | 1 | 4 | RRT criteria met (pH<=7.2, HCO3<=12) |
| 408 | NULL | 0 | 0 | NULL | No data |

---

## Future Phases

### Phase 2: Respiratory Fixtures

- Create under `tests/utils/sofa2/resp/`

- Use `lab_collect_dttm` (not `lab_result_dttm`)

### Phase 3: Other Subscores

- Brain, CV, Liver, Kidney

### Phase 4: Integration Test

- Small cohort testing `calculate_sofa2()` end-to-end

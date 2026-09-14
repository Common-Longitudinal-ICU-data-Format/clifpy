# Which medications need more than one permissible dose unit?

**Status:** analysis complete, awaiting a schema decision.
**Inputs:** `medication_admin_continuous_dose_by_category_and_unit.csv` (consortium DQA export,
11 sites) and `meds unit standardization.xlsx - 09112026_med_admin_cont.csv` (the target schema).
**Reproduce:** `python dev/med-unit-converter/analyze_multi_unit.py`
**Outputs:** `multi_unit_recommendations.csv`, `target_vs_practice.csv`

> `*.csv` is gitignored repo-wide, so neither the DQA inputs nor the generated outputs are checked
> in — this document is the durable record. Re-running the script needs the two DQA exports present
> in this directory, and regenerates both CSVs in seconds.

---

## The question

The standardization spreadsheet assigns **exactly one** `med_dose_unit_category` per
`med_category`. Sites whose charting convention differs must convert to it.

For most conversions that is free. For some it is not: converting `mcg/min` to `mcg/kg/min`
requires the patient's weight, and `dev/weighted-unit-impact/FINDINGS.md` showed that when weight
is missing the resulting row loss is **biased** — it falls precisely on the encounters that never
got weighed, which are not a random sample of patients.

So: **for which medications do sites genuinely disagree, in a way the converter cannot silently
fix?** Those are the rows that should be allowed a second permissible unit, so that a site charting
unweighted doses is not forced to invent a weight it does not have.

## What does *not* need a second unit

A drug charted in several units does not automatically need several targets. Most disagreements are
pure arithmetic that `clifpy.utils.unit_converter` already performs losslessly:


| kind of difference | example                  | second target needed?                    |
| ------------------ | ------------------------ | ---------------------------------------- |
| mass prefix        | `mcg` ↔ `mg` ↔ `ng`      | **no** — ×1000                           |
| time base          | `/hr` ↔ `/min` ↔ `/day`  | **no** — ×60                             |
| weight base        | `/kg` ↔ `/lb`            | **no** — ×2.205                          |
| **weight axis**    | `mcg/min` ↔ `mcg/kg/min` | **yes** — needs the patient's weight     |
| **rate vs amount** | `mg` ↔ `mg/hr`           | **yes** — needs the infusion duration    |
| **family**         | `ml/hr` ↔ `mg/hr`        | **yes** — needs concentration or potency |


Only the bottom three need data that may not exist. Everything below is filtered to those.

## Method

Each unit string is decomposed into three axes — **family** (mass / volume / unit / equivalent /
substance / cell-count / gas-fraction) × **weight** (`/kg`, `/lb`, none) × **time** (`/min`, `/hr`,
`/day`, none). Two units are *irreconcilable* if they differ on the weight, rate-vs-amount, or
family axis. Unit strings are normalised first through clifpy's own
`_clean_dose_unit_formats_duckdb` → `_clean_dose_unit_names_duckdb` chain, so `MCG/KG/MIN` ,
`mcg/kg/min` and `microgram/kg/minute` are one token.

For each `(med_category, site)` we take that site's **dominant unit** — the one with the most
observations, among sites charting the drug at least 100 times. This matters: counting every site
with ≥1 row made nitroglycerin look like a 10-site weight-indexed drug, contradicting the known
result that 10 of 11 sites chart it unweighted. A site's *primary convention* is the question;
stray rows are not.

**Scale:** 130,401,044 recognised observations across 256 `med_category` values; 154 of those use
more than one unit. After the irreconcilability filter and the dominant-unit restriction:
**36 candidate pairs across 29 categories**, tiered below.

> **Separate data-quality finding.** 165,006 observations carry a valid dose unit but **no
> `med_category` at all** — JHU 163,416 (mostly `mL/hr`) and NU 1,590. They are excluded here
> because they cannot inform a per-category decision, but they are unmapped rows worth routing back
> to those two sites.

---

## Tier A — add a second permissible unit

The alternative is the **primary convention at 3 or more sites**. These are real practice
differences, not noise, and forcing them onto the single target is what produces biased loss.


| `med_category`   | `med_group`     | keep              | **add**            | why            | sites on the added unit                        | obs       |
| ---------------- | --------------- | ----------------- | ------------------ | -------------- | ---------------------------------------------- | --------- |
| `isoproterenol`  | vasoactives     | `mcg/kg/min` (3)  | `**mcg/min`** (7)  | weight         | Emory, NU, RUSH, Sunnybrook, UCMC, UCSF, UPenn | 49,182    |
| `phenylephrine`  | vasoactives     | `mcg/kg/min` (5)  | `**mcg/min**` (6)  | weight         | NU, OHSU, RUSH, Sunnybrook, UCSF, UPenn        | 2,095,677 |
| `albuterol`      | inhaled         | `mg` (1)          | `**mg/hr**` (6)    | rate vs amount | Emory, OHSU, UCMC, UCSF, UMN, UPenn            | 38,264    |
| `naloxone`       | others          | `mcg/kg/hr` (3)   | `**mg/hr**` (6)    | weight         | Emory, MIMIC, RUSH, Sunnybrook, UMN, UPenn     | 12,836    |
| `norepinephrine` | vasoactives     | `mcg/kg/min` (7)  | `**mcg/min**` (4)  | weight         | NU, RUSH, Sunnybrook, UPenn                    | 7,508,447 |
| `heparin`        | anticoagulation | `units/kg/hr` (7) | `**units/hr**` (4) | weight         | MIMIC, OHSU, Sunnybrook, UMN                   | 2,735,616 |
| `epinephrine`    | vasoactives     | `mcg/kg/min` (7)  | `**mcg/min**` (4)  | weight         | NU, RUSH, Sunnybrook, UPenn                    | 1,376,496 |
| `ketamine`       | sedation        | `mg/kg/hr` (6)    | `**mg/hr**` (3)    | weight         | OHSU, RUSH, UMN                                | 198,082   |


Parenthesised numbers are the count of sites where that unit is the site's primary convention.

Notes:

- `**heparin**` — the existing `chad_notes` entry reads *"Occasionally run at low fixed
units/hour."* The data says it is not occasional: 4 of 11 sites chart `units/hr` as their primary
convention, 2.7 M observations.
- **Four of these have the target in the minority.** For `isoproterenol`, `phenylephrine`,
`albuterol` and `naloxone`, *more* sites use the proposed addition than the current target. Worth
deciding whether the pair should be listed in the other order — it determines which sites bear
the conversion burden.
- `**naloxone*`* — NU's `mcg/hr` (1,745 obs) is already covered by adding `mg/hr`; those differ
only by a mass prefix.
- `**ketamine**` — NU and UCSF chart `mcg/kg/min` (93,567 obs), which is lossless against
`mg/kg/hr`. No third target is needed.

## Tier B — the listed target is used by no site

For these five, **zero sites** chart the current target as their dominant unit. Adding a second
value does not fix that; the primary should change.


| `med_category`              | `med_group`               | current target | what sites actually chart                                           | suggested                    |
| --------------------------- | ------------------------- | -------------- | ------------------------------------------------------------------- | ---------------------------- |
| `albumin`                   | fluids_electrolytes       | *(blank)*      | `ml/hr` NU, Sunnybrook (13,049) · `g/kg` OHSU (341)                 | `ml/hr` + `g`                |
| `alprostadil`               | pulmonary_vasodilators_iv | `mcg/hr`       | `mcg/kg/min` Emory, UCMC, UMN (3,254) · `mcg/kg/hr` NU (12,159)     | `mcg/kg/min` + `mcg/hr`      |
| `ipratropium`               | inhaled                   | `ml`           | `mg/hr` OHSU, UPenn (12,310) · `mg` UMN (271)                       | `mg/hr` + `mg`               |
| `tacrolimus`                | others                    | `mcg/kg/day`   | `mcg/hr` UMN (121,356) · `mg/day` UCSF (4,084) · `mg` Emory (2,233) | `mcg/hr` + `mcg/kg/day`      |
| `terbutaline` (inhaled row) | inhaled                   | `mg`           | `mcg/kg/min` JHU (125) only                                         | too little data — see tier D |


- `**albumin`** already carries the note *"5% (mL) and 25% (g) have different units"* and its target
cell is blank. `ml/hr` and `g` cannot be reconciled without the product concentration, so both are
needed regardless of what else is decided.
- `**alprostadil*`* and `**ipratropium**` are plain misses — no site charts the listed unit at all.
- `**tacrolimus**` has three sites on three mutually irreconcilable units and nobody on the target.
This row needs a decision, not a mechanical addition.

## Tier C — looks wrong, needs no change

Listed so they are not mistaken for problems. The target is an unusual prefix choice, but the
conversion is exact and the converter handles it.


| `med_category`               | target       | what most sites chart                | relationship |
| ---------------------------- | ------------ | ------------------------------------ | ------------ |
| `bivalirudin`                | `mcg/kg/hr`  | `mg/kg/hr` at 8 sites (617,174 obs)  | ×1000        |
| `epoprostenol` (inhaled row) | `mcg/kg/min` | `ng/kg/min` at 7 sites (335,297 obs) | ×1000        |


Flipping the prefix would reduce per-site transformation but loses nothing either way.

## Tier D — single-site differences: ask the site, don't change the schema

These appear at exactly one or two sites. Adding them would enshrine one site's local practice — or
one site's defect — as a consortium-wide permissible unit.

### D1 — plausible local clinical convention


| `med_category`  | target       | site convention | site  | obs     |
| --------------- | ------------ | --------------- | ----- | ------- |
| `nitroglycerin` | `mcg/min`    | `mcg/kg/min`    | MIMIC | 109,782 |
| `nicardipine`   | `mg/hr`      | `mcg/kg/min`    | MIMIC | 70,403  |
| `lidocaine`     | `mg/min`     | `mg/kg/hr`      | UMN   | 15,549  |
| `rocuronium`    | `mcg/kg/min` | `mg/hr`         | OHSU  | 8,016   |
| `morphine`      | `mg/hr`      | `mg`            | UMN   | 7,129   |
| `vecuronium`    | `mcg/kg/min` | `mg/hr`         | OHSU  | 340     |


MIMIC weight-indexing nitroglycerin and nicardipine is a genuine institutional convention rather
than an error — but it is one site of eleven, and MIMIC has near-complete weight coverage, so
converting *toward* the unweighted target is the direction that loses nothing.

### D2 — RUSH charts bolus volumes as bare `ml` in the continuous table

One site-level pattern across five categories, not five findings:


| `med_category`              | target  | RUSH also uses | obs     |
| --------------------------- | ------- | -------------- | ------- |
| `sodium_chloride`           | `ml/hr` | `ml`           | 352,467 |
| `lactated_ringers_solution` | `ml/hr` | `ml`           | 76,488  |
| `plasma_lyte`               | `ml/hr` | `ml`           | 41,122  |
| `dextrose_10_water`         | `ml/hr` | `ml`           | 16,548  |
| `bupivacaine`               | `ml/hr` | `ml`           | 9,907   |


Almost certainly boluses that belong in `medication_admin_intermittent`. Worth raising with RUSH as
a table-routing question rather than a unit question.

### D3 — pump rate charted instead of drug dose


| `med_category`             | target                     | site unit | site  | obs    |
| -------------------------- | -------------------------- | --------- | ----- | ------ |
| `epoprostenol` (both rows) | `ng/kg/min` / `mcg/kg/min` | `ml/hr`   | Emory | 27,102 |
| `papaverine`               | `mg/hr`                    | `ml/hr`   | UCSF  | 755    |


`ml/hr` here is the infusion *volume* rate — which is what the `volume_referenence_unit` column
exists for. Converting it to a dose needs the concentration, which the table does not carry.

### D4 — probable `med_category` mapping errors


| `med_category`          | target     | site unit          | site         | obs           | reading                                                                     |
| ----------------------- | ---------- | ------------------ | ------------ | ------------- | --------------------------------------------------------------------------- |
| `dextrose_5_water`      | `ml/hr`    | `mcg/kg/min`       | RUSH         | 79,903        | D5W has no mcg/kg/min dose — likely a drug mapped into the diluent category |
| `dextrose_other`        | `ml/hr`    | `mg/min` · `g/min` | RUSH · MIMIC | 9,846 · 7,689 | plausibly glucose infusion rate — confirm before discarding                 |
| `tpn`                   | `ml/hr`    | `g`                | Emory        | 74            | a macronutrient amount, not a TPN rate                                      |
| `zidovudine`            | `mg/kg/hr` | `mg`               | UMN          | 95            | below any threshold for a schema change                                     |
| `terbutaline` (inhaled) | `mg`       | `mcg/kg/min`       | JHU          | 125           | weight-indexed *inhaled* terbutaline is implausible                         |


`terbutaline` deserves a specific look: the inhaled and IV rows are distinguished only by
`med_group`, and JHU's 125 weight-indexed observations look like they belong to the IV row.

---

## Summary


| tier  | action                                   | rows                        |
| ----- | ---------------------------------------- | --------------------------- |
| **A** | add a second `med_dose_unit_category`    | 8 categories                |
| **B** | change the primary target first          | 5 categories                |
| **C** | no change — lossless despite appearances | 2 categories                |
| **D** | route back to the contributing site      | ~16 single-site differences |


Tier A, ready to paste:

```
isoproterenol      mcg/kg/min   +  mcg/min
phenylephrine      mcg/kg/min   +  mcg/min
albuterol          mg           +  mg/hr
naloxone           mcg/kg/hr    +  mg/hr
norepinephrine     mcg/kg/min   +  mcg/min
heparin            units/kg/hr  +  units/hr
epinephrine        mcg/kg/min   +  mcg/min
ketamine           mg/kg/hr     +  mg/hr
```

Unit spellings follow the CLIF 3.0 output vocabulary — the unit family is spelled out (`units/hr`,
`milli-units/min`, `million-units`) while mass and volume stay abbreviated (`mcg`, `ml`). The script
applies this through `CANONICAL_UNIT_SPELLING` in `clifpy/utils/unit_converter/_grammar.py`, so the
document and the converter cannot drift apart.

## Caveats

- The DQA export has no `med_group`, so the two categories the spreadsheet splits by route —
`epoprostenol` (IV / inhaled) and `terbutaline` (IV / inhaled) — receive the same observed split
on both of their rows. Read those four rows with that in mind.
- A site is counted once per drug, on its dominant unit. A site that genuinely runs two conventions
in parallel therefore shows only the larger one.
- The 100-observation floor removes sites that barely chart a drug. Lowering it adds noise, not
signal; raising it starts dropping real small-volume drugs such as `alprostadil`.
- Everything here is aggregate. No row-level or ID-linked data was read at any point.


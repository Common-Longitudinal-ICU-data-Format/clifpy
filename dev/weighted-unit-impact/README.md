# Weighted dose unit impact analysis

**CLIF version:** 2.1 or 3.0
**Runtime:** roughly 3–6 minutes per site

## What this answers

CLIF 3.0 mCIDE targets a weight-indexed unit for 29 continuous medications
(`norepinephrine → mcg/kg/min`, `heparin → units/kg/hr`, …). Sites disagree on
whether they chart weighted or unweighted units, so standardizing everyone onto
the mCIDE target forces an unweighted → weighted conversion that consumes
patient weight.

The consortium has to choose:

- **Option A — one unit.** Everyone converts to the single mCIDE target. Uniform and directly poolable, but any dose whose weight is unavailable is lost or silently left unconverted.
- **Option B — two units.** Sites that natively chart unweighted keep an unweighted standard alongside the weighted one. Nothing is lost, but downstream analyses must handle two units.

This analysis measures what option A would actually cost **at your site**, so
the decision rests on data rather than assumption.

## Quickstart

```bash
git clone https://github.com/Common-Longitudinal-ICU-data-Format/clifpy
cd clifpy && git checkout weighted-unit-impact
uv sync

cp dev/weighted-unit-impact/config_template.yaml my_site_config.yaml
# edit the five required fields

make weight-impact CONFIG=my_site_config.yaml
```

Then send back the folder it names: `output/weighted_unit_impact/<site_name>/`.

If it crashes partway, **send what it produced anyway** — each step is
independent, and `run_manifest.json` records which ones completed.

## Required CLIF tables and fields

| table | fields | used for |
|---|---|---|
| `medication_admin_continuous` | `hospitalization_id`, `admin_dttm`, `med_category`, `med_dose`, `med_dose_unit` | the doses under study |
| `vitals` | `hospitalization_id`, `recorded_dttm`, `vital_category`, `vital_value` | `vital_category = 'weight_kg'` |
| `hospitalization` | `hospitalization_id`, `patient_id`, `admission_dttm` | weights from a patient's *other* admissions |
| `patient` | not required | — |

`hospitalization` is optional: without it the analysis still runs, and the
scenarios needing patient-level lookup collapse into one bucket with a logged
warning.

A drug your site does not chart is reported as absent, not as an error.

## Configuration

Five required fields. `clif_version` in particular has **no safe default** —
clifpy falls back to `"3.0"`, so a 2.1 dataset left unlabelled validates against
the wrong schema without warning. The analysis refuses to start without it.

```yaml
site_name: "Your_Site_Name"
data_directory: "/path/to/clif/tables/"
filetype: "parquet"
timezone: "US/Central"
clif_version: "2.1"
```

Optional, with defaults: `weight_staleness_days: 7`, `min_cell_size: 11`,
`drug_subset: null` (all 29 drugs).

## What gets written

One folder, `output/weighted_unit_impact/<site_name>/`:

| file | contents |
|---|---|
| `site_summary.md` | **read this first** — your results in prose |
| `unit_inventory.csv` | every `(med_category, med_dose_unit)` pair, cleaned and classified |
| `unit_inventory_by_drug.csv` | per-drug rollup with forced-conversion volume |
| `weight_scenarios.csv` | nine weight-availability scenarios per drug |
| `weight_convertibility_by_drug.csv` | recoverability summary |
| `impact_m1_loss.csv` … `impact_m6_silent_failure.csv` | the six impact metrics |
| `decision_table.csv` | per-drug A-vs-B recommendation |
| `run_manifest.json` | clifpy version, commit, row counts, steps skipped |
| `run.log` | full log |

## Privacy

**Nothing patient-level is written.** There is deliberately no second output
tier, because there is nothing to keep back:

- Every figure is a count, percentage, or distribution statistic over a group.
- Identifiers are join keys in memory only. `assert_no_identifiers` runs on every file before it is written and refuses anything ID-shaped, including columns added later.
- Any figure describing fewer than `min_cell_size` hospitalizations is nulled and flagged `suppressed=True` — kept rather than dropped, since a dropped row is indistinguishable from a true zero when pooling.

You can inspect the whole folder before sending it.

## Definitions worth knowing before quoting a number

**Stale** (scenario 6): the most recent weight before the first dose is more
than `weight_staleness_days` old. Not missing — the conversion succeeds. The
concern is that ICU weight moves with resuscitation and diuresis.

**High spread** (scenario 9): `max(weight) / min(weight)` across the whole
hospitalization exceeds 1.20. These rows convert fine; the point is that a
different, equally defensible weight from the same stay would give a materially
different dose.

**Scenario ordering** is 8 → 7 → 6 → 9 → 1 → 2 → 3 → 4 → 5, so a hospitalization
with both a clean pre-dose weight *and* high spread is reported as high spread.
"Clean" therefore understates how many have *some* usable pre-dose weight — that
figure is scenarios 1 + 6 + 9.

## For the coordinating centre

```bash
make weight-impact-pool INPUTS="output/weighted_unit_impact/*"
```

Reads every returned folder and writes `_pooled/decision_memo.md` plus pooled
CSVs. Each file carries its own `site_name`, so adding a site means dropping its
folder in — no configuration.

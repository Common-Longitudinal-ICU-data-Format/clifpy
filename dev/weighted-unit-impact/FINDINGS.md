# Should CLIF standardize medication doses to a single weight-based unit?

Sites disagree on whether they chart doses as `mcg/min` or `mcg/kg/min`. CLIF
3.0 proposes standardizing everyone onto a single weight-based unit. This
document sets out which drugs and sites are affected, what that standardization
would cost, and what we recommend.

**Where we landed, so far:** a single unit is fine for most medications, but for
a handful — the vasopressors, plus ketamine, isoproterenol and naloxone — sites
should be allowed to report an unweighted unit alongside the weighted one. The
reason is not how much data is lost. It is that the loss is **undetectable** by
any check currently in place.

Written for someone who has heard that dose units vary across sites but has not
looked into it. It assumes you know what `medication_admin_continuous` is and
nothing more.

**What this is based on.** Section 3 covers all 11 CLIF sites, from the
consortium DQA inventory. Sections 5 onward are a deep analysis of **two sites
only** — RUSH and Northwestern, 14.6 million administrations — and those two
happen to share a convention, which biases which drugs get exercised. Section 3
says who should run this next and why. The analysis is reproducible at any site
in about five minutes; see section 9.

---

## 1. The background: two ways to write the same dose

A norepinephrine infusion can be charted two ways:

| | example | meaning |
|---|---|---|
| **unweighted** | `5 mcg/min` | 5 micrograms every minute |
| **weighted** | `0.0625 mcg/kg/min` | 0.0625 micrograms per kilogram of patient, every minute |

They describe the same infusion for an 80 kg patient. Which one appears in your
data is an institutional convention — usually whatever the infusion pumps and
order sets were configured to display, sometimes decades ago.

Across the 11 CLIF sites, that convention splits roughly down the middle. For
norepinephrine, four sites (NU, RUSH, Sunnybrook, UPenn) chart essentially 100%
unweighted; the other seven chart essentially 100% weighted.

**Converting between them requires the patient's weight.** That is the entire
source of the problem.

## 2. Why this is coming up now

CLIF 3.0's mCIDE specification names a target unit for each medication —
`norepinephrine → mcg/kg/min`, `heparin → units/kg/hr`, and so on. For **29
continuous medications the target is weight-based**.

If every site converts to those targets, sites that chart unweighted must divide
every dose by a patient weight. clifpy's unit converter does this automatically
and correctly *when a weight exists*.

Consortium-wide, that is **16,995,264 medication administrations** that would
need an unweighted → weighted conversion.

## 3. Which drugs and which sites actually disagree

Before any of the deeper analysis, the basic question: where is there a split at
all? Below, a site is counted as "unweighted" for a drug when the majority of its
observations for that drug use an unweighted unit. Spelling variants
(`mcg/kg/minute` vs `mcg/kg/min`) are normalised first, so they are correctly not
counted as disagreement.

### Drugs where sites disagree

| medication | sites | weighted | unweighted | observations | % obs weighted | the unweighted sites |
|---|---:|---:|---:|---:|---:|---|
| norepinephrine | 11 | 7 | 4 | 18,163,487 | 53% | NU, RUSH, Sunnybrook, UPenn |
| heparin | 11 | 7 | 4 | 11,996,890 | 69% | MIMIC, OHSU, Sunnybrook, UMN |
| phenylephrine | 11 | 5 | 6 | 3,862,040 | 40% | NU, OHSU, RUSH, Sunnybrook, UCSF, UPenn |
| epinephrine | 11 | 7 | 4 | 3,344,544 | 58% | NU, RUSH, Sunnybrook, UPenn |
| nicardipine | 11 | 1 | 10 | 2,589,665 | 3% | all but MIMIC |
| ketamine | 11 | 8 | 3 | 1,207,867 | 83% | OHSU, RUSH, UMN |
| nitroglycerin | 11 | 1 | 10 | 1,025,314 | 25% | all but MIMIC |
| epoprostenol | 10 | 9 | 1 | 377,494 | 92% | Emory |
| vecuronium | 9 | 8 | 1 | 214,047 | 100% | OHSU |
| tacrolimus | 4 | 1 | 3 | 143,528 | 10% | Emory, UCSF, UMN |
| rocuronium | 10 | 9 | 1 | 102,736 | 92% | OHSU |
| isoproterenol | 10 | 3 | 7 | 97,608 | 44% | Emory, NU, RUSH, Sunnybrook, UCMC, UCSF, UPenn |
| naloxone | 10 | 3 | 7 | 50,920 | 57% | Emory, MIMIC, NU, RUSH, Sunnybrook, UMN, UPenn |
| albumin | 6 | 1 | 5 | 13,761 | 3% | Emory, NU, RUSH, Sunnybrook, UCSF |
| adenosine | 6 | 5 | 1 | 2,082 | 41% | OHSU |
| zidovudine | 6 | 5 | 1 | 1,342 | 93% | UMN |
| etomidate | 3 | 1 | 2 | 1,293 | 12% | OHSU, UMN |

Two of these need no action at all: **nicardipine and nitroglycerin** are charted
unweighted by 10 of 11 sites, and mCIDE also targets them unweighted (`mg/hr`,
`mcg/min`). The lone weighted site is the outlier, and no forced conversion
occurs. They appear here only because the sites disagree.

### Which sites sit on the unweighted side, and for how many drugs

| site | contested drugs it charts unweighted | examples |
|---|---:|---|
| NU | 9 | albumin, epinephrine, isoproterenol, naloxone, nicardipine, nitroglycerin |
| OHSU | 9 | adenosine, etomidate, heparin, ketamine, nicardipine, nitroglycerin |
| RUSH | 9 | albumin, epinephrine, isoproterenol, ketamine, naloxone, nicardipine |
| Sunnybrook | 9 | albumin, epinephrine, heparin, isoproterenol, naloxone, nicardipine |
| UMN | 9 | etomidate, heparin, ketamine, naloxone, nicardipine, nitroglycerin |
| Emory | 7 | albumin, epoprostenol, isoproterenol, naloxone, nicardipine, nitroglycerin |
| UPenn | 7 | epinephrine, isoproterenol, naloxone, nicardipine, norepinephrine |
| UCSF | 6 | albumin, isoproterenol, nicardipine, nitroglycerin, phenylephrine, tacrolimus |
| UCMC | 3 | isoproterenol, nicardipine, nitroglycerin |
| JHU | 2 | nicardipine, nitroglycerin |
| MIMIC | 2 | heparin, naloxone |

**No site is uniformly unweighted.** Every one of the 11 uses weight-based units
for some drugs — from 23.5% of observations at Sunnybrook to 66.3% at UCMC. So
the concern that a site might simply not record weight is not supported at the
site level; it has to be tested per drug, which is what the rest of this document
does.

### Three distinct patterns, which need different responses

- **A convention block.** NU, RUSH, Sunnybrook and UPenn are ~0% weighted on epinephrine, norepinephrine *and* phenylephrine — the same four sites, the same three drugs. That is an institutional convention, not a data gap.
- **An isolated anomaly.** OHSU is 20% and 16% weighted on vecuronium and rocuronium where every peer is ~100%. This is the shape that suggests a data problem rather than a convention, and is worth a separate look.
- **A benign majority.** nicardipine and nitroglycerin, described above — no action needed.

### Why the rest of this document is mostly about vasopressors

The two sites available for deep analysis, **RUSH and Northwestern, are both
members of the same convention block**. Both are ~100% unweighted on the
vasopressors and already weighted on nearly everything else. So the drugs that
would actually be forced *at these two sites* are almost entirely norepinephrine,
epinephrine, phenylephrine and ketamine.

That is a property of the sample, not of the consortium. A site with a different
profile would surface different drugs:

| if this site runs it | expect the pressure on |
|---|---|
| MIMIC, OHSU, Sunnybrook, UMN | **heparin** — 11,996,890 obs, target `units/kg/hr` |
| Emory, UCSF, UMN | **tacrolimus** — target `mcg/kg/day`, 90% of its volume forced |
| OHSU | **ketamine, vecuronium, rocuronium** |
| Emory | **epoprostenol** |

**Heparin in particular is the second-largest contested drug consortium-wide and
is barely exercised here** — RUSH is only 2.1% forced on it and NU 0.1%, because
both already chart `units/kg/hr`. Its behaviour under forced conversion is
essentially untested by this analysis, and one of the four heparin-unweighted
sites should run it before the consortium decides anything about heparin.

## 4. The two options

> ### Option A — one unit
> Every site converts to the single mCIDE target. Norepinephrine is always
> `mcg/kg/min`, everywhere.
>
> **Upside:** uniform and directly poolable. An analyst never thinks about units.
> **Downside:** any dose whose patient weight is unavailable cannot be converted.

> ### Option B — two units
> Sites that natively chart unweighted keep an unweighted standard *alongside*
> the weighted one. Norepinephrine is `mcg/kg/min` where weight allows, and
> `mcg/min` otherwise, with both explicitly labelled.
>
> **Upside:** nothing is lost. Every dose is usable for something.
> **Downside:** downstream analyses must handle two units.

**A is cheaper to consume. B is lossless.** The question this analysis answers is
whether A loses enough, or loses it badly enough, to be worth B's extra
complexity.

## 5. What we expected to find, and what we actually found

The worry going in was: *sites that don't use weight-based units may not record
patient weight at all, so forcing the conversion would silently drop those
patients.*

**That worry turned out to be small.** Only **0.9–3.8%** of hospitalizations have
no recorded weight anywhere. Every site records weight for most patients,
including the sites that chart unweighted units.

But two other problems turned up that nobody was looking for, and one of them is
decisive.

---

## Finding 1 — weight is usually there, but often not *before* the drug starts

For each hospitalization that received a drug, we asked: is there a usable weight
recorded before the first dose?

| | norepi RUSH | norepi NU | epi RUSH | epi NU | phenyl RUSH | phenyl NU |
|---|---:|---:|---:|---:|---:|---:|
| hospitalizations | 13,672 | 34,714 | 3,072 | 8,430 | 4,477 | 23,176 |
| **clean weight before first dose** | 65.7% | 65.5% | 63.1% | 63.1% | 72.4% | 71.2% |
| weight before, but ambiguous (see Finding 2) | 16.5% | 15.8% | 20.8% | 19.9% | 13.4% | 12.4% |
| weight only *after* the first dose | 12.5% | 12.2% | 7.4% | 10.5% | 6.9% | 8.5% |
| weight before, but >7 days old | 3.0% | 1.6% | 6.5% | 1.4% | 4.9% | 2.2% |
| weight only from another admission | 1.2% | 1.6% | 1.0% | 1.6% | 1.2% | 1.9% |
| **no weight anywhere** | **0.9%** | **3.3%** | **1.0%** | **3.5%** | **1.2%** | **3.8%** |

The striking thing is how closely the two sites agree — 65.7% vs 65.5% for
norepinephrine, 63.1% vs 63.1% for epinephrine. Two independent hospitals,
cohorts differing 2.5× in size, landing within a fraction of a percent.

That suggests this is **not a data-quality problem any one site could fix**. It
looks structural to how vasopressor patients arrive: through the ED or OR, where
the drug is started before anyone weighs the patient.

**The good news:** ~12% of the gap is closed by a clifpy option that already
exists. `fallback_on_earliest=True` uses the first weight recorded during the
stay when none precedes the dose. It is **off by default**.

| drug | convertible today | with that flag on |
|---|---:|---:|
| norepinephrine | 84.1% | 96.4% |
| epinephrine | 87.4% | 96.3% |
| phenylephrine | 88.3% | 96.0% |
| naloxone | 64.8% | 95.3% |

## Finding 2 — "has a weight" is not the same as "has *the* weight"

For 12–21% of hospitalizations, a weight exists before the first dose *and*
the patient's weight varies substantially during the stay — max/min across the
admission exceeding 1.20, with a median ratio of **1.3**.

These convert without complaint. The concern is that a different, equally
defensible weight from the same admission would have produced a dose about **30%
different**. ICU weight genuinely moves — fluid resuscitation, diuresis,
third-spacing — so this is not a measurement error to be cleaned up.

This matters for the decision only indirectly: it means the weighted unit is
itself somewhat imprecise, which weakens Option A's central claim to being the
more comparable representation. It does not, by itself, argue for B.

## Finding 3 — the failure is invisible, and this is what decides it

When clifpy cannot convert a dose, it does not crash or drop the row. It keeps
the original value, keeps the original unit label, and records the reason in a
`_convert_status` column.

**Everything about that is correct.** The row honestly says `mcg/min`. Nothing is
mislabelled.

The problem is what the *column* becomes. After standardizing norepinephrine at
RUSH, `med_dose_converted` is:

- **97.2%** in `mcg/kg/min` — the target
- **2.8%** in `mcg/min` — the original

with **no error, no warning, and no failed run**. Option A promises one unit; what
it delivers is a column that looks standardized and is not.

Now consider the safety nets:

| | what it does | why it does not help |
|---|---|---|
| outlier range check | flags implausible doses per `(medication, unit)` | `5 mcg/min` is correctly checked against the `mcg/min` range (0–200) and correctly passes. It has no way to notice the column is mixed. |
| the analyst | reads the standardized column | re-checking the unit on every row defeats the purpose of standardizing |

Measured across both sites:

| drug | site | unconvertible rows | caught by any current check | would have been caught if labelled as the target |
|---|---|---:|---:|---:|
| norepinephrine | RUSH | 16,047 | **0%** | 78% |
| norepinephrine | NU | 124,313 | **0%** | 67% |
| phenylephrine | RUSH | 2,384 | **0%** | 88% |
| phenylephrine | NU | 43,766 | **0%** | 82% |
| epinephrine | RUSH | 1,786 | **0%** | 71% |
| epinephrine | NU | 19,801 | **0%** | 43% |

**Not one row is caught.** Had these carried the target label, 43–88% would have
been rejected as implausible.

### This is not hypothetical

`clifpy/utils/sofa2/_cv.py` is exactly the trusting consumer described above. It
converts vasopressor doses to `mcg/kg/min`, then reads the **unconverted**
column. At the four unweighted sites, every vasopressor dose therefore scores
maximum cardiovascular support on SOFA-2. Written up separately in
`dev/docs/sofa2_cv_uses_unconverted_dose.md`.

---

## 6. The recommendation

**Option B for six medications. Option A for everything else.**

Both sites independently flagged the same six, for the same reasons:

| medication | why |
|---|---|
| norepinephrine | mixed output column, 0% of the mixture detectable |
| phenylephrine | same |
| epinephrine | same |
| ketamine | the doses that fail to convert are systematically different from those that succeed |
| isoproterenol | >5% of hospitalizations lose the drug entirely |
| naloxone | same, plus biased loss |

Every other medication — including heparin, propofol and dexmedetomidine, which
carry far more volume — comes out as **Option A, loss negligible**.

### The reasoning is about safety, not volume

Only 2.8–8.6% of vasopressor rows fail to convert, and we checked whether those
rows differ from the ones that succeed: they do not (median dose ratios
0.80–1.07). Statistically the loss is benign. Losing 3% of rows at random costs
a little power and nothing else.

What makes it unacceptable is that **Option A does not lose them visibly.** It
produces a column that claims uniformity it does not have, and no existing check
can detect the difference.

Under Option B the two units are explicit and expected, so consumers are built to
handle them. Under Option A the mixture is unexpected, so they are not.

### What to do today, regardless of the decision

1. **Turn on `fallback_on_earliest`** when converting. It recovers ~12 points of convertibility and already exists.
2. **Never read `med_dose_converted` without also checking `_convert_status`**, or filter to `_convert_status == 'success'` first.
3. **Fix `sofa2/_cv.py`** before relying on SOFA-2 cardiovascular scores at an unweighted site.

---

## 7. How things were measured

Every threshold below is configurable; these are the defaults that produced the
numbers above.

**"Clean weight before first dose"** — a weight recorded before the first
administration of that drug in that hospitalization, that is not stale, not an
outlier, not zero, and not ambiguous.

**"Stale"** — the most recent pre-dose weight is more than **7 days** old.
Observed median age in that bucket was 10–11 days, so the finding is not an
artefact of choosing 7.

**"Ambiguous" / high spread** — `max(weight) / min(weight)` across the whole
hospitalization exceeds **1.20**. Computed over the whole stay, not just before
the dose, because the question is how well-defined *this patient's weight* is
during this admission.

**Category ordering.** Each hospitalization is placed in exactly one category, in
the order: zero weight → outlier → stale → ambiguous → clean → after-only →
other-admission → none. A hospitalization with both a clean pre-dose weight and
high spread is reported as ambiguous, deliberately, so the ambiguity is not
hidden inside a success count.

That means **"clean" understates availability.** The share with *any* usable
pre-dose weight is clean + stale + ambiguous:

| | norepi RUSH | norepi NU | epi RUSH | epi NU |
|---|---:|---:|---:|---:|
| any pre-dose weight | 85.3% | 82.9% | 90.4% | 84.4% |
| of which unambiguous | 65.7% | 65.5% | 63.1% | 63.1% |

**Small cells.** Any figure describing fewer than 11 hospitalizations is
suppressed. Suppressed rows are kept and flagged rather than deleted, so that a
suppressed cell is never mistaken for a true zero when results are pooled.

**No patient-level data leaves a site.** Every output is a count, percentage or
distribution statistic over a group. Identifier columns are refused at write
time, not by convention.

## 8. Caveats

- **Two sites, both adult ICU, both CLIF 2.1, both US/Central.** Their agreement is encouraging but is not evidence of generality. Other sites should run this before the consortium decides.
- **The 1.20 spread threshold is a judgement call**, chosen as roughly the point where a dose difference crosses a clinically meaningful vasopressor band. The full distribution is reported alongside it.
- **`spread_ratio` uses whole-stay extremes**, so a long admission with genuine clinical weight change looks the same as a stay with inconsistent measurement.
- **Northwestern's live volume is 23% below the consortium DQA inventory** (2,949,730 vs 3,852,109); RUSH agrees within 1%. Most likely a different extract date, but it should be explained before pooling.
- **Two clifpy issues found along the way** do not affect these results but are worth fixing: the weight conversion has no `weight_kg > 0` guard (would yield `inf`; zero occurrences here), and the outlier config's 30 kg weight floor would discard pediatric weights (not a concern for adult ICU cohorts).

## 9. Running this at your site

```bash
git clone https://github.com/Common-Longitudinal-ICU-data-Format/clifpy
cd clifpy && git checkout weighted-unit-impact
uv sync
cp dev/weighted-unit-impact/config_template.yaml my_site_config.yaml   # 5 fields
make weight-impact CONFIG=my_site_config.yaml
```

Takes 3–6 minutes. Send back `output/weighted_unit_impact/<your_site>/` — read
`site_summary.md` in there first; it is your own numbers in the same shape as
this document.

Full detail in `README.md`.

"""
SOFA Score Calculation - Comparing ClifOrchestrator vs compute_sofa_polars

This script compares two methods of SOFA score calculation:
1. ClifOrchestrator (DuckDB-based)
2. compute_sofa_polars (Polars-based)

Structure:
- STEP 1: Run Both Methods
- STEP 2: Compare Results
- STEP 3: Generate Plots
"""

import pandas as pd
import polars as pl
import warnings
from pathlib import Path
import time
import matplotlib.pyplot as plt
import numpy as np

from clifpy.clif_orchestrator import ClifOrchestrator
from clifpy.utils.sofa import REQUIRED_SOFA_CATEGORIES_BY_TABLE
from clifpy import compute_sofa_polars

warnings.filterwarnings('ignore')

# =============================================================================
# CONFIGURATION
# =============================================================================
CONFIG_PATH = '../config/config.json'
DATA_DIRECTORY =  "clifpy/data/clif_demo"
TIME_WINDOW_HOURS = 24
TIMEZONE = 'US/Eastern'
FILETYPE = 'parquet'

# Create output directory
output_path = Path('output')
output_path.mkdir(exist_ok=True)


# =============================================================================
# HELPER FUNCTIONS
# =============================================================================
def find_sofa_columns(df):
    """Find SOFA component columns in a DataFrame."""
    cols = {}
    for col in df.columns:
        col_lower = col.lower()
        if 'total' in col_lower and 'sofa' in col_lower: cols['total'] = col
        elif 'resp' in col_lower: cols['respiratory'] = col
        elif 'coag' in col_lower: cols['coagulation'] = col
        elif 'liver' in col_lower or 'hepatic' in col_lower: cols['liver'] = col
        elif 'cv' in col_lower: cols['cv'] = col
        elif 'cns' in col_lower: cols['cns'] = col
        elif 'renal' in col_lower: cols['renal'] = col
    return cols


# =============================================================================
#                           STEP 1: RUN BOTH METHODS
# =============================================================================
print("=" * 80)
print("STEP 1: COMPUTE SOFA SCORES WITH BOTH METHODS")
print("=" * 80)

# -----------------------------------------------------------------------------
# 1.1 ClifOrchestrator Method
# -----------------------------------------------------------------------------
print("\n" + "-" * 40)
print("1.1 ClifOrchestrator (DuckDB-based)")
print("-" * 40)

method1_start = time.time()

co = ClifOrchestrator(config_path=CONFIG_PATH)
co.load_table('hospitalization')
hosp_df = co.hospitalization.df

# Create cohort
cohort_df = pd.DataFrame({
    'hospitalization_id': hosp_df['hospitalization_id'],
    'start_time': pd.to_datetime(hosp_df['admission_dttm']),
    'end_time': pd.to_datetime(hosp_df['admission_dttm']) + pd.Timedelta(hours=TIME_WINDOW_HOURS)
})
hosp_ids = cohort_df['hospitalization_id'].astype(str).unique().tolist()

print(f"   Cohort: {len(cohort_df)} hospitalizations")
print(f"   Time window: First {TIME_WINDOW_HOURS} hours from admission")

# Load required tables
print("\n   Loading tables...")
tables_loaded = {}

try:
    co.load_table('labs', filters={'hospitalization_id': hosp_ids,
        'lab_category': ['creatinine', 'platelet_count', 'po2_arterial', 'bilirubin_total']},
        columns=['hospitalization_id', 'lab_result_dttm', 'lab_category', 'lab_value_numeric'])
    tables_loaded['labs'] = len(co.labs.df)
except Exception as e:
    tables_loaded['labs'] = f"Error: {e}"

try:
    co.load_table('vitals', filters={'hospitalization_id': hosp_ids,
        'vital_category': ['map', 'spo2', 'weight_kg', 'height_cm']},
        columns=['hospitalization_id', 'recorded_dttm', 'vital_category', 'vital_value'])
    tables_loaded['vitals'] = len(co.vitals.df)
except Exception as e:
    tables_loaded['vitals'] = f"Error: {e}"

try:
    co.load_table('patient_assessments', filters={'hospitalization_id': hosp_ids,
        'assessment_category': ['gcs_total']},
        columns=['hospitalization_id', 'recorded_dttm', 'assessment_category', 'numerical_value', 'categorical_value'])
    tables_loaded['patient_assessments'] = len(co.patient_assessments.df)
except Exception as e:
    tables_loaded['patient_assessments'] = f"Error: {e}"

try:
    co.load_table('medication_admin_continuous', filters={'hospitalization_id': hosp_ids,
        'med_category': ['norepinephrine', 'epinephrine', 'dopamine', 'dobutamine']})
    tables_loaded['medication_admin_continuous'] = len(co.medication_admin_continuous.df)
    
    if len(co.medication_admin_continuous.df) > 0:
        med_df = co.medication_admin_continuous.df.copy()
        med_df = med_df[med_df['med_dose'].notna()]
        if 'med_dose_unit' in med_df.columns:
            med_df = med_df[med_df['med_dose_unit'].notna()]
        co.medication_admin_continuous.df = med_df
        
        preferred_units = {'norepinephrine': 'mcg/kg/min', 'epinephrine': 'mcg/kg/min',
                          'dopamine': 'mcg/kg/min', 'dobutamine': 'mcg/kg/min'}
        co.convert_dose_units_for_continuous_meds(preferred_units=preferred_units, override=True)
        
        if hasattr(co.medication_admin_continuous, 'df_converted'):
            med_df_converted = co.medication_admin_continuous.df_converted.copy()
            if '_convert_status' in med_df_converted.columns:
                co.medication_admin_continuous.df_converted = med_df_converted[
                    med_df_converted['_convert_status'] == 'success'].copy()
except Exception as e:
    tables_loaded['medication_admin_continuous'] = f"Error: {e}"

try:
    co.load_table('respiratory_support', filters={'hospitalization_id': hosp_ids},
        columns=['hospitalization_id', 'recorded_dttm', 'device_category', 'fio2_set'])
    tables_loaded['respiratory_support'] = len(co.respiratory_support.df)
except Exception as e:
    tables_loaded['respiratory_support'] = f"Error: {e}"

print("   Tables loaded:")
for table, count in tables_loaded.items():
    print(f"      {table}: {count}")

# Create wide dataset and compute SOFA
co.create_wide_dataset(category_filters=REQUIRED_SOFA_CATEGORIES_BY_TABLE,
                       cohort_df=cohort_df, return_dataframe=True)

required_med_cols = ['norepinephrine_mcg_kg_min', 'epinephrine_mcg_kg_min',
                     'dopamine_mcg_kg_min', 'dobutamine_mcg_kg_min']
for col in required_med_cols:
    if col not in co.wide_df.columns:
        co.wide_df[col] = None

sofa_orchestrator = co.compute_sofa_scores(wide_df=co.wide_df, id_name='hospitalization_id',
    fill_na_scores_with_zero=True, remove_outliers=True, create_new_wide_df=False)

method1_time = time.time() - method1_start
print(f"\n   ✓ Orchestrator SOFA computed: {sofa_orchestrator.shape}")
print(f"   ✓ Time: {method1_time:.2f}s")

# -----------------------------------------------------------------------------
# 1.2 compute_sofa_polars Method
# -----------------------------------------------------------------------------
print("\n" + "-" * 40)
print("1.2 compute_sofa_polars (Polars-based)")
print("-" * 40)

method2_start = time.time()

cohort_polars = pl.DataFrame({
    'patient_id': co.hospitalization.df['patient_id'].astype(str),
    'hospitalization_id': co.hospitalization.df['hospitalization_id'].astype(str),
    'start_dttm': pl.Series(pd.to_datetime(co.hospitalization.df['admission_dttm'])),
    'end_dttm': pl.Series(pd.to_datetime(co.hospitalization.df['admission_dttm']) + pd.Timedelta(hours=TIME_WINDOW_HOURS))
})

try:
    sofa_polars = compute_sofa_polars(data_directory=DATA_DIRECTORY, cohort_df=cohort_polars,
        filetype=FILETYPE, timezone=TIMEZONE, profile=False)
    sofa_polars_pd = sofa_polars.to_pandas()
    method2_time = time.time() - method2_start
    print(f"   ✓ Polars SOFA computed: {sofa_polars_pd.shape}")
    print(f"   ✓ Time: {method2_time:.2f}s")
except Exception as e:
    print(f"   ✗ compute_sofa_polars failed: {e}")
    sofa_polars_pd = None
    method2_time = None

# Save raw results
sofa_orchestrator.to_csv(output_path / 'sofa_orchestrator.csv', index=False)
if sofa_polars_pd is not None:
    sofa_polars_pd.to_csv(output_path / 'sofa_polars.csv', index=False)


# =============================================================================
#                           STEP 2: COMPARE RESULTS
# =============================================================================
print("\n\n" + "=" * 80)
print("STEP 2: COMPARE RESULTS")
print("=" * 80)

if sofa_polars_pd is None:
    print("   ✗ Cannot compare - Polars method failed")
    exit(1)

# Prepare for comparison
sofa_cols_orch = find_sofa_columns(sofa_orchestrator)
sofa_cols_polars = find_sofa_columns(sofa_polars_pd)

df1 = sofa_orchestrator.copy()
df1['hospitalization_id'] = df1['hospitalization_id'].astype(str)
df2 = sofa_polars_pd.copy()
df2['hospitalization_id'] = df2['hospitalization_id'].astype(str)

hosp_ids_orch = set(df1['hospitalization_id'].unique())
hosp_ids_polars = set(df2['hospitalization_id'].unique())

only_in_orch = hosp_ids_orch - hosp_ids_polars
only_in_polars = hosp_ids_polars - hosp_ids_orch
in_both = hosp_ids_orch & hosp_ids_polars

# -----------------------------------------------------------------------------
# 2.1 Patient Coverage
# -----------------------------------------------------------------------------
print("\n" + "-" * 40)
print("2.1 Patient Coverage")
print("-" * 40)

print(f"\n   Orchestrator: {len(hosp_ids_orch)} patients")
print(f"   Polars:       {len(hosp_ids_polars)} patients")
print(f"   In both:      {len(in_both)} patients")
print(f"   Only in Orchestrator: {len(only_in_orch)}")
print(f"   Only in Polars: {len(only_in_polars)}")

# -----------------------------------------------------------------------------
# 2.2 Total SOFA Comparison
# -----------------------------------------------------------------------------
print("\n" + "-" * 40)
print("2.2 Total SOFA Score Comparison")
print("-" * 40)

merged = df1.merge(df2, on='hospitalization_id', suffixes=('_orch', '_polars'), how='inner')
merged['sofa_diff'] = merged['sofa_total_polars'] - merged['sofa_total_orch']

print(f"\n   Matched patients: {len(merged)}")
print(f"\n   ┌─────────────────────────────────────────────────┐")
print(f"   │              TOTAL SOFA SCORES                  │")
print(f"   ├─────────────────┬───────────────┬───────────────┤")
print(f"   │ Metric          │ Orchestrator  │ Polars        │")
print(f"   ├─────────────────┼───────────────┼───────────────┤")
print(f"   │ Mean            │ {sofa_orchestrator['sofa_total'].mean():>13.2f} │ {sofa_polars_pd['sofa_total'].mean():>13.2f} │")
print(f"   │ Median          │ {sofa_orchestrator['sofa_total'].median():>13.1f} │ {sofa_polars_pd['sofa_total'].median():>13.1f} │")
print(f"   │ Std             │ {sofa_orchestrator['sofa_total'].std():>13.2f} │ {sofa_polars_pd['sofa_total'].std():>13.2f} │")
print(f"   └─────────────────┴───────────────┴───────────────┘")

print(f"\n   Agreement:")
print(f"      Mean difference (Polars - Orch): {merged['sofa_diff'].mean():.3f}")
print(f"      Exact matches: {(merged['sofa_diff'] == 0).sum()} / {len(merged)} ({(merged['sofa_diff'] == 0).sum()/len(merged)*100:.1f}%)")

# -----------------------------------------------------------------------------
# 2.3 Component-Level Comparison
# -----------------------------------------------------------------------------
print("\n" + "-" * 40)
print("2.3 Component-Level Comparison")
print("-" * 40)

components = ['respiratory', 'coagulation', 'liver', 'cv', 'cns', 'renal']
component_results = {}

print(f"\n   {'Component':<15} | {'Orch':>6} | {'Polars':>6} | {'Match':>15}")
print("   " + "-" * 55)

for comp in components:
    col_orch = [c for c in merged.columns if comp[:4] in c.lower() and '_orch' in c]
    col_polars = [c for c in merged.columns if comp[:4] in c.lower() and '_polars' in c]
    
    if col_orch and col_polars:
        mean_orch = merged[col_orch[0]].mean()
        mean_polars = merged[col_polars[0]].mean()
        exact = (merged[col_polars[0]] == merged[col_orch[0]]).sum()
        pct = exact / len(merged) * 100
        
        component_results[comp] = {
            'mean_orch': mean_orch, 
            'mean_polars': mean_polars,
            'exact_matches': exact,
            'match_pct': pct
        }
        
        # Highlight mismatches
        status = "✓" if pct == 100 else "⚠️" if pct >= 90 else "❌"
        print(f"   {comp.capitalize():<15} | {mean_orch:>6.2f} | {mean_polars:>6.2f} | {exact:>4}/{len(merged)} ({pct:>5.1f}%) {status}")
    else:
        print(f"   {comp.capitalize():<15} | Column not found")

# Save comparison
merged.to_csv(output_path / 'sofa_comparison.csv', index=False)

# -----------------------------------------------------------------------------
# 2.4 Investigate Discrepancies
# -----------------------------------------------------------------------------
print("\n" + "-" * 40)
print("2.4 Discrepancy Analysis")
print("-" * 40)

# Find components with mismatches
mismatched_components = [comp for comp, stats in component_results.items() if stats['match_pct'] < 100]

if not mismatched_components:
    print("\n   ✓ All components match 100%!")
else:
    print(f"\n   Components with discrepancies: {mismatched_components}")
    
    for comp in mismatched_components:
        col_orch = [c for c in merged.columns if comp[:4] in c.lower() and '_orch' in c][0]
        col_polars = [c for c in merged.columns if comp[:4] in c.lower() and '_polars' in c][0]
        
        diff = merged[col_polars] - merged[col_orch]
        mismatches = merged[diff != 0]
        
        print(f"\n   {comp.upper()} Discrepancies ({len(mismatches)} patients):")
        print(f"      Difference distribution:")
        print(f"      {diff.value_counts().sort_index().to_dict()}")


# =============================================================================
#                           STEP 3: GENERATE PLOTS
# =============================================================================
print("\n\n" + "=" * 80)
print("STEP 3: GENERATE COMPARISON PLOTS")
print("=" * 80)

fig, axes = plt.subplots(2, 3, figsize=(15, 10))
fig.suptitle('SOFA Score Comparison: Orchestrator vs Polars', fontsize=14, fontweight='bold')

# Plot 1: Total SOFA Distribution
ax = axes[0, 0]
bins = range(0, 21)
ax.hist(sofa_orchestrator['sofa_total'].dropna(), bins=bins, alpha=0.5, 
        label=f'Orchestrator (mean={sofa_orchestrator["sofa_total"].mean():.2f})', color='blue')
ax.hist(sofa_polars_pd['sofa_total'].dropna(), bins=bins, alpha=0.5, 
        label=f'Polars (mean={sofa_polars_pd["sofa_total"].mean():.2f})', color='red')
ax.set_xlabel('Total SOFA Score')
ax.set_ylabel('Count')
ax.set_title('Total SOFA Distribution')
ax.legend()
ax.grid(axis='y', alpha=0.3)

# Plot 2: Scatter Plot (Orchestrator vs Polars)
ax = axes[0, 1]
ax.scatter(merged['sofa_total_orch'], merged['sofa_total_polars'], alpha=0.5, c='purple', s=40)
ax.plot([0, 20], [0, 20], 'r--', linewidth=2, label='Perfect agreement')
ax.set_xlabel('Orchestrator SOFA')
ax.set_ylabel('Polars SOFA')
ax.set_title('Score Agreement')
corr = merged['sofa_total_orch'].corr(merged['sofa_total_polars'])
ax.text(0.05, 0.95, f'r = {corr:.3f}', transform=ax.transAxes, fontsize=10,
        verticalalignment='top', bbox=dict(boxstyle='round', facecolor='white', alpha=0.8))
ax.legend()
ax.grid(alpha=0.3)

# Plot 3: Difference Distribution
ax = axes[0, 2]
diff_counts = merged['sofa_diff'].value_counts().sort_index()
colors = ['green' if x == 0 else ('orange' if abs(x) <= 1 else 'red') for x in diff_counts.index]
ax.bar(diff_counts.index, diff_counts.values, color=colors, alpha=0.7, edgecolor='black')
ax.axvline(x=0, color='black', linestyle='--', linewidth=2)
ax.set_xlabel('Score Difference (Polars - Orchestrator)')
ax.set_ylabel('Count')
ax.set_title('Distribution of Differences')
ax.grid(axis='y', alpha=0.3)

# Plot 4: Component Means Comparison
ax = axes[1, 0]
labels = []
means_orch = []
means_polars = []

for comp in components:
    if comp in component_results:
        labels.append(comp[:4].capitalize())
        means_orch.append(component_results[comp]['mean_orch'])
        means_polars.append(component_results[comp]['mean_polars'])

x = np.arange(len(labels))
width = 0.35
ax.bar(x - width/2, means_orch, width, label='Orchestrator', color='blue', alpha=0.7)
ax.bar(x + width/2, means_polars, width, label='Polars', color='red', alpha=0.7)
ax.set_xlabel('SOFA Component')
ax.set_ylabel('Mean Score')
ax.set_title('Mean Component Scores')
ax.set_xticks(x)
ax.set_xticklabels(labels)
ax.legend()
ax.grid(axis='y', alpha=0.3)

# Plot 5: Match Rates by Component
ax = axes[1, 1]
match_rates = [component_results[comp]['match_pct'] for comp in components if comp in component_results]
colors = ['green' if r == 100 else 'orange' if r >= 90 else 'red' for r in match_rates]
bars = ax.bar(labels, match_rates, color=colors, alpha=0.7, edgecolor='black')
ax.axhline(y=100, color='green', linestyle='--', linewidth=2, alpha=0.5)
ax.axhline(y=90, color='orange', linestyle='--', linewidth=2, alpha=0.5)
ax.set_xlabel('SOFA Component')
ax.set_ylabel('Match Rate (%)')
ax.set_title('Component Match Rates')
ax.set_ylim(0, 105)
ax.grid(axis='y', alpha=0.3)

# Add percentage labels on bars
for bar, rate in zip(bars, match_rates):
    ax.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 1, 
            f'{rate:.1f}%', ha='center', va='bottom', fontsize=9)

# Plot 6: Summary
ax = axes[1, 2]
ax.axis('off')

exact_match_pct = (merged['sofa_diff'] == 0).sum() / len(merged) * 100

summary_text = f"""
════════════════════════════════════════
         COMPARISON SUMMARY
════════════════════════════════════════

SAMPLE SIZE:
  Orchestrator: {len(sofa_orchestrator)} patients
  Polars:       {len(sofa_polars_pd)} patients
  Matched:      {len(merged)} patients

TOTAL SOFA:
  Orchestrator mean: {sofa_orchestrator['sofa_total'].mean():.2f}
  Polars mean:       {sofa_polars_pd['sofa_total'].mean():.2f}
  Correlation:       {corr:.4f}
  Exact match:       {exact_match_pct:.1f}%

TIMING:
  Orchestrator: {method1_time:.2f}s
  Polars:       {method2_time:.2f}s

DISCREPANCIES:
  {', '.join(mismatched_components) if mismatched_components else 'None - All components match!'}

════════════════════════════════════════
"""

ax.text(0.05, 0.5, summary_text, transform=ax.transAxes, fontsize=9,
        verticalalignment='center', fontfamily='monospace',
        bbox=dict(boxstyle='round', facecolor='lightyellow', alpha=0.9))

plt.tight_layout()
plt.savefig(output_path / 'sofa_comparison_plot.png', dpi=150, bbox_inches='tight')
plt.show()

print(f"\n   ✓ Plot saved to output/sofa_comparison_plot.png")


# =============================================================================
#                           FINAL SUMMARY
# =============================================================================
print("\n\n" + "=" * 80)
print("FINAL SUMMARY")
print("=" * 80)

print(f"""
┌──────────────────────────────────────────────────────────────────────────────┐
│                         COMPARISON RESULTS                                   │
├──────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│  AGREEMENT:                                                                  │
│    Total SOFA exact match: {exact_match_pct:>5.1f}%                                         │
│    Correlation:            {corr:>5.3f}                                          │
│                                                                              │
│  COMPONENT MATCH RATES:                                                      │""")

for comp in components:
    if comp in component_results:
        pct = component_results[comp]['match_pct']
        status = "✓" if pct == 100 else "⚠️"
        print(f"│    {comp.capitalize():<15}: {pct:>5.1f}%  {status}                                       │")

print(f"""│                                                                              │
│  TIMING:                                                                     │
│    Orchestrator: {method1_time:>6.2f}s                                                │
│    Polars:       {method2_time:>6.2f}s                                                │
│                                                                              │
└──────────────────────────────────────────────────────────────────────────────┘
""")

print("\nOutput files:")
print("   - output/sofa_orchestrator.csv")
print("   - output/sofa_polars.csv")
print("   - output/sofa_comparison.csv")
print("   - output/sofa_comparison_plot.png")

print("\n✓ Comparison complete!")
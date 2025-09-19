import marimo

__generated_with = "0.16.0"
app = marimo.App(width="medium")


@app.cell
def _():
    import marimo as mo
    import pandas as pd
    import polars as pl
    import numpy as np
    import matplotlib.pyplot as plt
    import seaborn as sns
    from pathlib import Path
    import sys
    from typing import Dict, List, Optional, Tuple
    import warnings
    warnings.filterwarnings('ignore')

    # Add repository root to sys.path so top-level clifpy is importable when running uninstalled
    project_root_path = Path(__file__).parent.parent
    if str(project_root_path) not in sys.path:
        sys.path.insert(0, str(project_root_path))

    mo.md("""
    # ICU Data Analysis with ECDF - Advanced Analytics

    This notebook demonstrates comprehensive ICU data analysis using **clifpy** with:

    🏥 **ICU-Focused Analysis**: Filter data to only ICU hospitalization periods
    📊 **ECDF Computation**: Create empirical cumulative distribution functions using Polars
    🧹 **Outlier Handling**: Clean physiologically implausible values
    📈 **Advanced Visualization**: Generate publication-ready plots
    💾 **Data Export**: Save analysis results and visualizations

    ## Workflow Overview
    1. **Load ADT** → Identify ICU hospitalizations
    2. **Filter Labs/Vitals** → Only load ICU periods
    3. **Clean Data** → Apply outlier handling
    4. **Compute ECDF** → Statistical analysis with Polars
    5. **Visualize & Export** → Save plots and results
    """)
    return Path, mo, np, pd, pl, plt


@app.cell
def _(mo):
    mo.md(
        """
    ## Configuration Parameters

    Edit these parameters for your specific dataset:
    """
    )
    return


@app.cell
def _():
    # Configuration - Edit these parameters for your data
    DATA_DIR_PATH = "/Users/sudo_sage/Documents/work/rush_clif/"  # Edit this path
    FILE_TYPE_FORMAT = "parquet"  # Edit this: "csv", "parquet", etc.
    TIMEZONE_SETTING = "US/Central"  # Edit this: "UTC", "US/Eastern", "US/Pacific", etc.
    SAMPLE_SIZE_LIMIT = None  # Edit this: None for all data, or integer for sample
    GRAPHS_OUTPUT_DIR = "graphs"  # Directory for saving plots

    # ECDF Configuration
    ECDF_PERCENTILES = [5, 10, 25, 50, 75, 90, 95]  # Percentiles to compute
    PLOT_DPI_SETTING = 300  # High resolution for publication quality
    FIGURE_SIZE_SETTING = (12, 8)  # Standard figure size
    return (
        DATA_DIR_PATH,
        ECDF_PERCENTILES,
        FIGURE_SIZE_SETTING,
        FILE_TYPE_FORMAT,
        GRAPHS_OUTPUT_DIR,
        PLOT_DPI_SETTING,
        TIMEZONE_SETTING,
    )


@app.cell
def _(GRAPHS_OUTPUT_DIR, Path, mo):
    # Create graphs directory if it doesn't exist
    graphs_directory_path = Path(GRAPHS_OUTPUT_DIR)
    graphs_directory_path.mkdir(exist_ok=True)

    mo.md(f"""
    ✅ **Graphs directory ready**: `{graphs_directory_path.absolute()}`

    All ECDF plots and visualizations will be saved here.
    """)
    return (graphs_directory_path,)


@app.cell
def _(mo):
    mo.md(
        """
    ## Step 1: Load ADT Table and Identify ICU Hospitalizations

    First, we load the ADT (Admission/Discharge/Transfer) table to identify which hospitalizations had ICU stays.
    This allows us to filter subsequent data loading to only relevant periods.
    """
    )
    return


@app.cell
def _(DATA_DIR_PATH, FILE_TYPE_FORMAT, TIMEZONE_SETTING):
    # Import ADT table class from clifpy
    from clifpy import Adt

    # Load ADT data using from_file method
    adt_table_obj = Adt.from_file(
        data_directory=DATA_DIR_PATH,
        filetype=FILE_TYPE_FORMAT,
        timezone=TIMEZONE_SETTING
    )

    print(f"ADT Data Loaded:")
    print(f"- {len(adt_table_obj.df)} ADT records")
    print(f"- {adt_table_obj.df['hospitalization_id'].nunique()} unique hospitalizations")
    print(f"- {adt_table_obj.df['location_category'].nunique()} different location categories")
    print(f"- Location categories: {sorted(adt_table_obj.df['location_category'].unique())}")
    return (adt_table_obj,)


@app.cell
def _(adt_table_obj, pl):
    # Filter for ICU records and extract hospitalization IDs
    icu_adt_records = adt_table_obj.df[adt_table_obj.df['location_category'] == 'icu'].copy()
    icu_hospitalization_ids_set = set(icu_adt_records['hospitalization_id'].unique())

    print(f"ICU Analysis:")
    print(f"- {len(icu_adt_records)} ICU ADT records")
    print(f"- {len(icu_hospitalization_ids_set)} unique hospitalizations with ICU stays")
    print(f"- ICU Coverage: {len(icu_hospitalization_ids_set) / adt_table_obj.df['hospitalization_id'].nunique() * 100:.1f}% of all hospitalizations")

    # Create ICU stay periods for later filtering
    icu_stay_periods_df = pl.from_pandas(icu_adt_records[['hospitalization_id', 'in_dttm', 'out_dttm']].copy())

    # Convert timezone-aware datetimes to timezone-naive for consistent joins
    icu_stay_periods_df = icu_stay_periods_df.with_columns([
        pl.col('in_dttm').dt.replace_time_zone(None),
        pl.col('out_dttm').dt.replace_time_zone(None)
    ])

    print(f"- ICU stay periods extracted: {icu_stay_periods_df.height} periods")
    return icu_hospitalization_ids_set, icu_stay_periods_df


@app.cell
def _(mo):
    mo.md(
        """
    ## Step 2: Load Labs and Vitals with ICU Filtering

    Now we load Labs and Vitals tables, filtering to only include data from hospitalizations that had ICU stays.
    This significantly reduces memory usage and focuses our analysis on relevant data.
    """
    )
    return


@app.cell
def _(
    DATA_DIR_PATH,
    FILE_TYPE_FORMAT,
    TIMEZONE_SETTING,
    icu_hospitalization_ids_set,
):
    from clifpy import Labs, Vitals

    # Load Vitals table filtered to ICU hospitalizations
    vitals_table_icu_filtered = Vitals.from_file(
        data_directory=DATA_DIR_PATH,
        filetype=FILE_TYPE_FORMAT,
        timezone=TIMEZONE_SETTING,
        filters={'hospitalization_id': list(icu_hospitalization_ids_set)},
        columns=['hospitalization_id', 'recorded_dttm', 'vital_category', 'vital_value']
    )

    vitals_load_stats = {
        'total_records': len(vitals_table_icu_filtered.df),
        'unique_hospitalizations': vitals_table_icu_filtered.df['hospitalization_id'].nunique(),
        'vital_categories': vitals_table_icu_filtered.df['vital_category'].nunique(),
        'categories_list': sorted(vitals_table_icu_filtered.df['vital_category'].unique())
    }

    print(f"Vitals Data (ICU Filtered):")
    print(f"- {vitals_load_stats['total_records']:,} vital sign measurements")
    print(f"- {vitals_load_stats['unique_hospitalizations']} unique ICU hospitalizations")
    print(f"- {vitals_load_stats['vital_categories']} different vital categories")
    print(f"- Categories: {vitals_load_stats['categories_list']}")
    return Labs, vitals_table_icu_filtered


@app.cell
def _(
    DATA_DIR_PATH,
    FILE_TYPE_FORMAT,
    Labs,
    TIMEZONE_SETTING,
    icu_hospitalization_ids_set,
):
    # Load Labs table filtered to ICU hospitalizations
    labs_table_icu_filtered = Labs.from_file(
        data_directory=DATA_DIR_PATH,
        filetype=FILE_TYPE_FORMAT,
        timezone=TIMEZONE_SETTING,
        filters={'hospitalization_id': list(icu_hospitalization_ids_set)},
        columns=['hospitalization_id', 'lab_result_dttm', 'lab_category', 'lab_value_numeric']
    )

    labs_load_stats = {
        'total_records': len(labs_table_icu_filtered.df),
        'unique_hospitalizations': labs_table_icu_filtered.df['hospitalization_id'].nunique(),
        'lab_categories': labs_table_icu_filtered.df['lab_category'].nunique(),
        'categories_list': sorted(labs_table_icu_filtered.df['lab_category'].unique())
    }

    print(f"Labs Data (ICU Filtered):")
    print(f"- {labs_load_stats['total_records']:,} lab measurements")
    print(f"- {labs_load_stats['unique_hospitalizations']} unique ICU hospitalizations")
    print(f"- {labs_load_stats['lab_categories']} different lab categories")
    print(f"- Categories: {labs_load_stats['categories_list']}")
    return (labs_table_icu_filtered,)


@app.cell
def _(mo):
    mo.md(
        """
    ## Step 3: Apply Outlier Handling

    Remove physiologically implausible values using clifpy's built-in outlier handling functionality.
    This ensures our ECDF analysis is based on clinically meaningful data.
    """
    )
    return


@app.cell
def _(vitals_table_icu_filtered):
    from clifpy.utils import apply_outlier_handling

    # Create a copy for comparison and apply outlier handling to vitals
    vitals_original_df = vitals_table_icu_filtered.df.copy()

    print("Applying outlier handling to Vitals data...")
    print("=" * 50)

    # Apply outlier handling (modifies vitals_table_icu_filtered.df in-place)
    apply_outlier_handling(vitals_table_icu_filtered)

    vitals_cleaned_df = vitals_table_icu_filtered.df.copy()

    # Calculate outlier statistics
    vitals_outlier_stats = {}
    for vital_cat_outlier in vitals_original_df['vital_category'].unique():
        vital_orig_cnt = len(vitals_original_df[vitals_original_df['vital_category'] == vital_cat_outlier])
        vital_clean_cnt = len(vitals_cleaned_df[
            (vitals_cleaned_df['vital_category'] == vital_cat_outlier) &
            vitals_cleaned_df['vital_value'].notna()
        ])
        vital_outliers_rm = vital_orig_cnt - vital_clean_cnt
        vitals_outlier_stats[vital_cat_outlier] = {
            'original': vital_orig_cnt,
            'cleaned': vital_clean_cnt,
            'outliers_removed': vital_outliers_rm,
            'outlier_percentage': (vital_outliers_rm / vital_orig_cnt * 100) if vital_orig_cnt > 0 else 0
        }

    print(f"\nVitals Outlier Handling Results:")
    for vital_cat_display, vital_stat in vitals_outlier_stats.items():
        print(f"- {vital_cat_display}: {vital_stat['outliers_removed']:,} outliers removed ({vital_stat['outlier_percentage']:.1f}%)")
    return apply_outlier_handling, vitals_cleaned_df, vitals_outlier_stats


@app.cell
def _(apply_outlier_handling, labs_table_icu_filtered):
    # Create a copy for comparison and apply outlier handling to labs
    labs_original_df = labs_table_icu_filtered.df.copy()

    print("Applying outlier handling to Labs data...")
    print("=" * 50)

    # Apply outlier handling (modifies labs_table_icu_filtered.df in-place)
    apply_outlier_handling(labs_table_icu_filtered)

    labs_cleaned_df = labs_table_icu_filtered.df.copy()

    # Calculate outlier statistics
    labs_outlier_stats = {}
    for lab_cat_outlier in labs_original_df['lab_category'].unique():
        lab_orig_cnt = len(labs_original_df[labs_original_df['lab_category'] == lab_cat_outlier])
        lab_clean_cnt = len(labs_cleaned_df[
            (labs_cleaned_df['lab_category'] == lab_cat_outlier) &
            labs_cleaned_df['lab_value_numeric'].notna()
        ])
        lab_outliers_rm = lab_orig_cnt - lab_clean_cnt
        labs_outlier_stats[lab_cat_outlier] = {
            'original': lab_orig_cnt,
            'cleaned': lab_clean_cnt,
            'outliers_removed': lab_outliers_rm,
            'outlier_percentage': (lab_outliers_rm / lab_orig_cnt * 100) if lab_orig_cnt > 0 else 0
        }

    print(f"\nLabs Outlier Handling Results:")
    for lab_cat_display, lab_stat in labs_outlier_stats.items():
        print(f"- {lab_cat_display}: {lab_stat['outliers_removed']:,} outliers removed ({lab_stat['outlier_percentage']:.1f}%)")
    return labs_cleaned_df, labs_outlier_stats


@app.cell
def _(mo):
    mo.md(
        """
    ## Step 4: Polars ECDF Computation for Vitals

    Convert data to Polars format and compute empirical cumulative distribution functions (ECDF)
    for each vital category during ICU stays only.
    """
    )
    return


@app.cell
def _(icu_stay_periods_df, pl, vitals_cleaned_df):
    # Convert vitals to Polars DataFrame
    vitals_polars_df = pl.from_pandas(vitals_cleaned_df)

    # Handle datetime conversion - check if already datetime or needs parsing
    try:
        # Try to convert timezone-aware datetime to timezone-naive for joins
        vitals_polars_df = vitals_polars_df.with_columns(
            pl.col('recorded_dttm').dt.replace_time_zone(None)
        )
    except:
        # If it's a string, parse it
        vitals_polars_df = vitals_polars_df.with_columns(
            pl.col('recorded_dttm').str.strptime(pl.Datetime, format='%Y-%m-%d %H:%M:%S', strict=False)
        )

    # Join with ICU stay periods to filter to ICU times only
    vitals_with_icu_periods = vitals_polars_df.join(
        icu_stay_periods_df,
        on='hospitalization_id',
        how='inner'
    )

    # Filter to measurements that occurred during ICU stays
    vitals_during_icu_df = vitals_with_icu_periods.filter(
        (pl.col('recorded_dttm') >= pl.col('in_dttm')) &
        (pl.col('recorded_dttm') <= pl.col('out_dttm')) &
        pl.col('vital_value').is_not_null()
    )

    print(f"Vitals ECDF Processing:")
    print(f"- Original vitals records: {vitals_polars_df.height:,}")
    print(f"- Vitals during ICU stays: {vitals_during_icu_df.height:,}")
    print(f"- Reduction: {(1 - vitals_during_icu_df.height / vitals_polars_df.height) * 100:.1f}%")
    return (vitals_during_icu_df,)


@app.cell
def _(ECDF_PERCENTILES, np, pl, vitals_during_icu_df):
    # Compute ECDF for each vital category
    print("Computing ECDF for each vital category...")

    vitals_ecdf_results = {}
    vital_categories_list = vitals_during_icu_df['vital_category'].unique().to_list()

    for vital_category in vital_categories_list:
        vital_cat_data = vitals_during_icu_df.filter(
            pl.col('vital_category') == vital_category
        ).select('vital_value').drop_nulls()

        if vital_cat_data.height > 0:
            vital_values = vital_cat_data['vital_value'].to_numpy()

            # Compute percentiles
            vital_percentiles_dict = {}
            for vital_p in ECDF_PERCENTILES:
                vital_percentiles_dict[f'p{vital_p}'] = np.percentile(vital_values, vital_p)

            # Create ECDF data points
            vital_sorted_values = np.sort(vital_values)
            vital_ecdf_values = np.arange(1, len(vital_sorted_values) + 1) / len(vital_sorted_values)

            vitals_ecdf_results[vital_category] = {
                'n_measurements': len(vital_values),
                'percentiles': vital_percentiles_dict,
                'min_value': float(np.min(vital_values)),
                'max_value': float(np.max(vital_values)),
                'mean_value': float(np.mean(vital_values)),
                'std_value': float(np.std(vital_values)),
                'ecdf_x': vital_sorted_values,
                'ecdf_y': vital_ecdf_values
            }

    print(f"ECDF computed for {len(vitals_ecdf_results)} vital categories:")
    for vital_cat_print, vital_res_print in vitals_ecdf_results.items():
        print(f"- {vital_cat_print}: {vital_res_print['n_measurements']:,} measurements")
    return (vitals_ecdf_results,)


@app.cell
def _(mo):
    mo.md(
        """
    ## Step 5: Polars ECDF Computation for Labs

    Similar ECDF computation for lab values during ICU stays, focusing on numeric lab results.
    """
    )
    return


@app.cell
def _(icu_stay_periods_df, labs_cleaned_df, pl):
    # Convert labs to Polars DataFrame
    labs_polars_df = pl.from_pandas(labs_cleaned_df)

    # Handle datetime conversion - check if already datetime or needs parsing
    try:
        # Try to convert timezone-aware datetime to timezone-naive for joins
        labs_polars_df = labs_polars_df.with_columns(
            pl.col('lab_result_dttm').dt.replace_time_zone(None)
        )
    except:
        # If it's a string, parse it
        labs_polars_df = labs_polars_df.with_columns(
            pl.col('lab_result_dttm').str.strptime(pl.Datetime, format='%Y-%m-%d %H:%M:%S', strict=False)
        )

    # Filter for numeric lab values only
    labs_numeric_only = labs_polars_df.filter(
        pl.col('lab_value_numeric').is_not_null() &
        pl.col('lab_value_numeric').is_finite()
    )

    # Join with ICU stay periods
    labs_with_icu_periods = labs_numeric_only.join(
        icu_stay_periods_df,
        on='hospitalization_id',
        how='inner'
    )

    # Filter to measurements that occurred during ICU stays
    labs_during_icu_df = labs_with_icu_periods.filter(
        (pl.col('lab_result_dttm') >= pl.col('in_dttm')) &
        (pl.col('lab_result_dttm') <= pl.col('out_dttm'))
    )

    print(f"Labs ECDF Processing:")
    print(f"- Original labs records: {labs_polars_df.height:,}")
    print(f"- Numeric labs: {labs_numeric_only.height:,}")
    print(f"- Labs during ICU stays: {labs_during_icu_df.height:,}")
    print(f"- Reduction: {(1 - labs_during_icu_df.height / labs_polars_df.height) * 100:.1f}%")
    return (labs_during_icu_df,)


@app.cell
def _(ECDF_PERCENTILES, labs_during_icu_df, np, pl):
    # Compute ECDF for each lab category
    print("Computing ECDF for each lab category...")

    labs_ecdf_results = {}
    lab_categories_list = labs_during_icu_df['lab_category'].unique().to_list()

    for lab_category in lab_categories_list:
        lab_cat_data = labs_during_icu_df.filter(
            pl.col('lab_category') == lab_category
        ).select('lab_value_numeric').drop_nulls()

        if lab_cat_data.height > 0:
            lab_values = lab_cat_data['lab_value_numeric'].to_numpy()

            # Compute percentiles
            lab_percentiles_dict = {}
            for lab_p in ECDF_PERCENTILES:
                lab_percentiles_dict[f'p{lab_p}'] = np.percentile(lab_values, lab_p)

            # Create ECDF data points
            lab_sorted_values = np.sort(lab_values)
            lab_ecdf_values = np.arange(1, len(lab_sorted_values) + 1) / len(lab_sorted_values)

            labs_ecdf_results[lab_category] = {
                'n_measurements': len(lab_values),
                'percentiles': lab_percentiles_dict,
                'min_value': float(np.min(lab_values)),
                'max_value': float(np.max(lab_values)),
                'mean_value': float(np.mean(lab_values)),
                'std_value': float(np.std(lab_values)),
                'ecdf_x': lab_sorted_values,
                'ecdf_y': lab_ecdf_values
            }

    print(f"ECDF computed for {len(labs_ecdf_results)} lab categories:")
    for lab_cat_print, lab_res_print in labs_ecdf_results.items():
        print(f"- {lab_cat_print}: {lab_res_print['n_measurements']:,} measurements")
    return (labs_ecdf_results,)


@app.cell
def _(mo):
    mo.md(
        """
    ## Step 6: Create and Save ECDF Visualizations

    Generate publication-quality plots for each category and save them to the graphs folder.
    """
    )
    return


@app.cell
def _(
    FIGURE_SIZE_SETTING,
    PLOT_DPI_SETTING,
    graphs_directory_path,
    plt,
    vitals_ecdf_results,
):
    # Create individual ECDF plots for each vital category
    plt.style.use('seaborn-v0_8')

    print("Creating Vitals ECDF plots...")

    vitals_plot_paths = {}
    for vital_cat_plot, vital_results_plot in vitals_ecdf_results.items():
        vital_fig, vital_ax = plt.subplots(figsize=FIGURE_SIZE_SETTING)

        # Plot ECDF
        vital_ax.plot(vital_results_plot['ecdf_x'], vital_results_plot['ecdf_y'], linewidth=2, alpha=0.8)

        # Add percentile lines
        for vital_percentile_line in [25, 50, 75]:
            vital_percentile_val = vital_results_plot['percentiles'][f'p{vital_percentile_line}']
            vital_ax.axvline(vital_percentile_val, color='red', linestyle='--', alpha=0.6,
                      label=f'{vital_percentile_line}th percentile: {vital_percentile_val:.1f}')

        vital_ax.set_xlabel(f'{vital_cat_plot.replace("_", " ").title()} Value')
        vital_ax.set_ylabel('Cumulative Probability')
        vital_ax.set_title(f'ECDF: {vital_cat_plot.replace("_", " ").title()} (ICU Only)\n'
                    f'N={vital_results_plot["n_measurements"]:,} measurements')
        vital_ax.grid(True, alpha=0.3)
        vital_ax.legend()

        # Save plot
        vital_plot_filename = f'vitals_ecdf_{vital_cat_plot.lower().replace(" ", "_")}.png'
        vital_plot_path = graphs_directory_path / vital_plot_filename
        plt.savefig(vital_plot_path, dpi=PLOT_DPI_SETTING, bbox_inches='tight')
        plt.close()

        vitals_plot_paths[vital_cat_plot] = vital_plot_path
        print(f"✅ Saved: {vital_plot_filename}")

    print(f"Completed {len(vitals_plot_paths)} vitals ECDF plots")
    return


@app.cell
def _(
    FIGURE_SIZE_SETTING,
    PLOT_DPI_SETTING,
    graphs_directory_path,
    labs_ecdf_results,
    plt,
):
    # Create individual ECDF plots for each lab category
    print("Creating Labs ECDF plots...")

    labs_plot_paths = {}
    for lab_cat_plot, lab_results_plot in labs_ecdf_results.items():
        lab_fig, lab_ax = plt.subplots(figsize=FIGURE_SIZE_SETTING)

        # Plot ECDF
        lab_ax.plot(lab_results_plot['ecdf_x'], lab_results_plot['ecdf_y'], linewidth=2, alpha=0.8, color='green')

        # Add percentile lines
        for lab_percentile_line in [25, 50, 75]:
            lab_percentile_val = lab_results_plot['percentiles'][f'p{lab_percentile_line}']
            lab_ax.axvline(lab_percentile_val, color='red', linestyle='--', alpha=0.6,
                      label=f'{lab_percentile_line}th percentile: {lab_percentile_val:.2f}')

        lab_ax.set_xlabel(f'{lab_cat_plot.replace("_", " ").title()} Value')
        lab_ax.set_ylabel('Cumulative Probability')
        lab_ax.set_title(f'ECDF: {lab_cat_plot.replace("_", " ").title()} (ICU Only)\n'
                    f'N={lab_results_plot["n_measurements"]:,} measurements')
        lab_ax.grid(True, alpha=0.3)
        lab_ax.legend()

        # Save plot
        lab_plot_filename = f'labs_ecdf_{lab_cat_plot.lower().replace(" ", "_")}.png'
        lab_plot_path = graphs_directory_path / lab_plot_filename
        plt.savefig(lab_plot_path, dpi=PLOT_DPI_SETTING, bbox_inches='tight')
        plt.close()

        labs_plot_paths[lab_cat_plot] = lab_plot_path
        print(f"✅ Saved: {lab_plot_filename}")

    print(f"Completed {len(labs_plot_paths)} labs ECDF plots")
    return


@app.cell
def _(
    PLOT_DPI_SETTING,
    graphs_directory_path,
    labs_ecdf_results,
    plt,
    vitals_ecdf_results,
):
    # Create comprehensive comparison plot
    comp_fig, (comp_ax1, comp_ax2) = plt.subplots(1, 2, figsize=(20, 8))

    # Vitals overview plot
    for vital_cat_comp, vital_res_comp in vitals_ecdf_results.items():
        comp_ax1.plot(vital_res_comp['ecdf_x'], vital_res_comp['ecdf_y'],
                label=f"{vital_cat_comp} (N={vital_res_comp['n_measurements']:,})",
                alpha=0.7, linewidth=2)

    comp_ax1.set_xlabel('Vital Sign Values')
    comp_ax1.set_ylabel('Cumulative Probability')
    comp_ax1.set_title('Vitals ECDF Comparison (ICU Only)')
    comp_ax1.grid(True, alpha=0.3)
    comp_ax1.legend(bbox_to_anchor=(1.05, 1), loc='upper left')

    # Labs overview plot
    for lab_cat_comp, lab_res_comp in labs_ecdf_results.items():
        comp_ax2.plot(lab_res_comp['ecdf_x'], lab_res_comp['ecdf_y'],
                label=f"{lab_cat_comp} (N={lab_res_comp['n_measurements']:,})",
                alpha=0.7, linewidth=2)

    comp_ax2.set_xlabel('Lab Values')
    comp_ax2.set_ylabel('Cumulative Probability')
    comp_ax2.set_title('Labs ECDF Comparison (ICU Only)')
    comp_ax2.grid(True, alpha=0.3)
    comp_ax2.legend(bbox_to_anchor=(1.05, 1), loc='upper left')

    plt.tight_layout()

    # Save comparison plot
    comparison_plot_path = graphs_directory_path / 'ecdf_comparison_overview.png'
    plt.savefig(comparison_plot_path, dpi=PLOT_DPI_SETTING, bbox_inches='tight')
    plt.close()

    print(f"✅ Saved comprehensive comparison: ecdf_comparison_overview.png")
    return


@app.cell
def _(mo):
    mo.md(
        """
    ## Step 7: Generate Summary Dashboard and Export Results

    Create comprehensive summary statistics and export ECDF data for further analysis.
    """
    )
    return


@app.cell
def _(labs_ecdf_results, pd, vitals_ecdf_results):
    # Create comprehensive summary statistics table
    summary_data_rows = []

    # Add vitals summary
    for vital_cat_summary, vital_results_summary in vitals_ecdf_results.items():
        summary_data_rows.append({
            'data_type': 'Vitals',
            'category': vital_cat_summary,
            'n_measurements': vital_results_summary['n_measurements'],
            'mean': vital_results_summary['mean_value'],
            'std': vital_results_summary['std_value'],
            'min': vital_results_summary['min_value'],
            'p25': vital_results_summary['percentiles']['p25'],
            'p50': vital_results_summary['percentiles']['p50'],
            'p75': vital_results_summary['percentiles']['p75'],
            'p95': vital_results_summary['percentiles']['p95'],
            'max': vital_results_summary['max_value']
        })

    # Add labs summary
    for lab_cat_summary, lab_results_summary in labs_ecdf_results.items():
        summary_data_rows.append({
            'data_type': 'Labs',
            'category': lab_cat_summary,
            'n_measurements': lab_results_summary['n_measurements'],
            'mean': lab_results_summary['mean_value'],
            'std': lab_results_summary['std_value'],
            'min': lab_results_summary['min_value'],
            'p25': lab_results_summary['percentiles']['p25'],
            'p50': lab_results_summary['percentiles']['p50'],
            'p75': lab_results_summary['percentiles']['p75'],
            'p95': lab_results_summary['percentiles']['p95'],
            'max': lab_results_summary['max_value']
        })

    ecdf_summary_stats = pd.DataFrame(summary_data_rows)

    print("📊 ECDF Summary Statistics:")
    print(f"- {len(vitals_ecdf_results)} vital categories analyzed")
    print(f"- {len(labs_ecdf_results)} lab categories analyzed")
    print(f"- Total measurements: {ecdf_summary_stats['n_measurements'].sum():,}")
    return (ecdf_summary_stats,)


@app.cell
def _(ecdf_summary_stats, graphs_directory_path):
    # Save summary statistics to CSV
    summary_csv_path = graphs_directory_path / 'ecdf_summary_statistics.csv'
    ecdf_summary_stats.to_csv(summary_csv_path, index=False)

    print(f"✅ Summary statistics saved: {summary_csv_path}")

    # Display top categories by measurement count
    print("\n🔝 Top Categories by Measurement Count:")
    top_categories = ecdf_summary_stats.nlargest(10, 'n_measurements')[['data_type', 'category', 'n_measurements']]
    for _, row in top_categories.iterrows():
        print(f"- {row['data_type']}: {row['category']} ({row['n_measurements']:,} measurements)")
    return


@app.cell
def _(graphs_directory_path, labs_ecdf_results, pl, vitals_ecdf_results):
    # Export ECDF data points for each category to parquet files
    print("💾 Exporting ECDF data points...")

    # Export vitals ECDF data
    for vital_cat_export, vital_results_export in vitals_ecdf_results.items():
        vital_ecdf_data_df = pl.DataFrame({
            'value': vital_results_export['ecdf_x'],
            'cumulative_probability': vital_results_export['ecdf_y'],
            'category': [vital_cat_export] * len(vital_results_export['ecdf_x']),
            'data_type': ['vitals'] * len(vital_results_export['ecdf_x'])
        })

        vitals_ecdf_parquet_path = graphs_directory_path / f'vitals_ecdf_data_{vital_cat_export.lower().replace(" ", "_")}.parquet'
        vital_ecdf_data_df.write_parquet(vitals_ecdf_parquet_path)

    # Export labs ECDF data
    for lab_cat_export, lab_results_export in labs_ecdf_results.items():
        lab_ecdf_data_df = pl.DataFrame({
            'value': lab_results_export['ecdf_x'],
            'cumulative_probability': lab_results_export['ecdf_y'],
            'category': [lab_cat_export] * len(lab_results_export['ecdf_x']),
            'data_type': ['labs'] * len(lab_results_export['ecdf_x'])
        })

        labs_ecdf_parquet_path = graphs_directory_path / f'labs_ecdf_data_{lab_cat_export.lower().replace(" ", "_")}.parquet'
        lab_ecdf_data_df.write_parquet(labs_ecdf_parquet_path)

    print(f"✅ ECDF data exported for {len(vitals_ecdf_results)} vitals and {len(labs_ecdf_results)} labs categories")
    return


@app.cell
def _(
    graphs_directory_path,
    labs_ecdf_results,
    labs_outlier_stats,
    mo,
    vitals_ecdf_results,
    vitals_outlier_stats,
):
    # Final summary and quality metrics report
    total_vitals_measurements = sum(results['n_measurements'] for results in vitals_ecdf_results.values())
    total_labs_measurements = sum(results['n_measurements'] for results in labs_ecdf_results.values())

    total_vitals_outliers = sum(stats['outliers_removed'] for stats in vitals_outlier_stats.values())
    total_labs_outliers = sum(stats['outliers_removed'] for stats in labs_outlier_stats.values())

    mo.md(f"""
    ## 🎯 Analysis Complete - Final Summary

    ### 📊 Data Processing Results

    **Vitals Analysis:**
    - **{len(vitals_ecdf_results)}** vital categories analyzed
    - **{total_vitals_measurements:,}** measurements in final ECDF analysis
    - **{total_vitals_outliers:,}** outliers removed during cleaning

    **Labs Analysis:**
    - **{len(labs_ecdf_results)}** lab categories analyzed
    - **{total_labs_measurements:,}** measurements in final ECDF analysis
    - **{total_labs_outliers:,}** outliers removed during cleaning

    ### 📁 Outputs Generated

    **Graphs Directory:** `{graphs_directory_path.absolute()}`

    **Individual ECDF Plots:**
    - {len(vitals_ecdf_results)} vitals ECDF plots (`vitals_ecdf_*.png`)
    - {len(labs_ecdf_results)} labs ECDF plots (`labs_ecdf_*.png`)
    - 1 comprehensive comparison plot (`ecdf_comparison_overview.png`)

    **Data Exports:**
    - Summary statistics (`ecdf_summary_statistics.csv`)
    - Individual ECDF data points (`.parquet` files for each category)

    ### 🚀 Next Steps

    1. **Review plots** in the graphs directory for insights
    2. **Analyze percentiles** for clinical decision-making
    3. **Use ECDF data** for comparative studies or modeling
    4. **Customize analysis** by modifying configuration parameters

    ---
    *Analysis completed using clifpy with Polars optimization for large-scale ICU data processing.*
    """)
    return


if __name__ == "__main__":
    app.run()

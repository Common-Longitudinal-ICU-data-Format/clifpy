import marimo

__generated_with = "0.15.3"
app = marimo.App()


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""
    # Unit Converter Dev

    Demonstration of `standardize_dose_to_limited_units()` using CLIF medication data.
    """
    )
    return


@app.cell
def _():
    import pandas as pd
    import sys
    from pathlib import Path
    from importlib import reload

    # Add parent directory to path
    sys.path.append(str(Path().absolute().parent))

    from clifpy.utils import unit_converter as uc
    reload(uc)
    return pd, uc


@app.cell(hide_code=True)
def _():
    import os
    os.getcwd()
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""## Convert to preferred units""")
    return


@app.cell
def _(pd):
    # Load medication data
    med_df = pd.read_parquet(
        'clifpy/data/clif_demo/clif_medication_admin_continuous.parquet'
    )

    # Add weight_kg column (using dummy weights for demo)
    med_df['weight_kg'] = 75.0  # Default weight for demo

    print(f"Loaded {len(med_df)} medication records")
    print(f"Unique dose units: {med_df['med_dose_unit'].nunique()}")
    med_df.value_counts(['med_category', 'med_dose_unit'], dropna=False)
    return (med_df,)


@app.cell
def _(med_df, uc):
    preferred_units = {
        'propofol': 'mcg/kg/min',
        'midazolam': 'mg/hr',
        'fentanyl': 'mcg/hr',
        'insulin': 'u/hr',
        'norepinephrine': 'ng/kg/min',
        'dextrose': 'g',
        'heparin': 'l/hr',
        'bivalirudin': 'ml/hr',
        'oxytocin': 'mu',
        # 'lactated_ringers_solution': 'ml',
        # 'liothyronine': 'u/hr',
        # 'zidovudine': 'iu/hr'
        }

    preferred_units_df, counts_preferred = uc.convert_dose_units_by_med_category(
        med_df,
        preferred_units = preferred_units,
        override = False
    )

    preferred_units_df
    return (counts_preferred,)


@app.cell
def _(counts_preferred):
    counts_preferred
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""### Test data""")
    return


@app.cell
def _(pd):
    pytest_df = pd.read_csv(
        "tests/fixtures/unit_converter/test_unit_converter - convert_dose_units_by_med_category.csv"
    )
    pytest_df
    return (pytest_df,)


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""## Standardize to limited units""")
    return


@app.cell
def _(pytest_df, uc):
    # Prepare required columns
    # input_df = med_df[['hospitalization_id', 'admin_dttm', 'med_dose', 'med_dose_unit', 'weight_kg']].copy()

    input_df = pytest_df[['rn','med_dose', 'med_dose_unit', 'weight_kg']].copy()

    # Run standardization
    limited_df, counts_df = uc.standardize_dose_to_limited_units(input_df)

    print("Conversion complete!")

    # Show sample conversions
    display_cols = ['med_dose', 'med_dose_unit', 'med_dose_unit_normalized', 
                    'med_dose_limited', 'med_dose_unit_limited', 'unit_class']

    limited_df#[display_cols]#.drop_duplicates('med_dose_unit').head(10)
    return counts_df, limited_df


@app.cell
def _(limited_df, mo):
    _df = mo.sql(
        f"""
        FROM limited_df
        SELECT *
        WHERE med_dose is not NULL and med_dose_unit is NULL
        """
    )
    return


@app.cell
def _(counts_df):
    counts_df #.sort_values('count', ascending=False)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""## Summary Statistics""")
    return


@app.cell
def _(converted_df):
    # Conversion statistics
    total = len(converted_df)
    converted = converted_df['med_dose_unit_converted'].notna().sum()

    print(f"Total records: {total}")
    print(f"Successfully converted: {converted} ({100*converted/total:.1f}%)")
    print(f"\nStandardized output units:")
    for unit in sorted(converted_df['med_dose_unit_converted'].dropna().unique()):
        count = (converted_df['med_dose_unit_converted'] == unit).sum()
        print(f"  {unit}: {count} records")
    return


@app.cell(hide_code=True)
def _():
    import marimo as mo
    return (mo,)


if __name__ == "__main__":
    app.run()

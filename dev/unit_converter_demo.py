import marimo

__generated_with = "0.15.5"
app = marimo.App(sql_output="pandas")


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""# Med Dosing Unit Converter Demo""")
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        rf"""
    ## Standardize dose units by medication

    In the most common use cases, we want to **standardize dose units by medication and pattern of administration** -- all propofol doses to be presented in mcg/kg/min in the continuous table and in mcg in the intermittent table, for example.

    To acheive this, simply call one of the two `convert_dose_units_*` functions (one for continuous and one for intermittent) from the CLIF orchestrator and provide **a dictionary mapping of medication categories to their preferred units**:
    """
    )
    return


@app.cell(hide_code=True)
def _(mo):
    from clifpy.clif_orchestrator import ClifOrchestrator

    co = ClifOrchestrator(config_path = "config/config.yaml")

    preferred_units_cont = {
        "propofol": "mcg/min",
        "fentanyl": "mcg/hr",
        "insulin": "u/hr",
        "midazolam": "mg/hr",
        "heparin": "u/min"
    }

    co.convert_dose_units_for_continuous_meds(preferred_units = preferred_units_cont) 

    mo.show_code()
    return co, preferred_units_cont


@app.cell
def _(mo):
    mo.md(
        r"""
    ### Returns

    Under the hood, this function automatically loads and uses the medication and vitals tables to generate two dataframes that are saved to the corresponding medication table instance by default:

    1. `co.medication_admin_continuous.df_converted` gives the updated medication table with the new columns appended:
        - `weight_kg`: the most recent weight relative to the `admin_dttm` pulled from the `vitals` table.
        - `_clean_unit`: cleaned source unit string where both 'U/h' and 'units / hour' would be standardized to 'u/hr', for example. 
        - `_unit_class`: distinguishes where the source unit is an amount (e.g. 'mcg'), a 'rate' (e.g. 'mcg/hr'), or 'unrecognized'.
        - `_convert_status`: documents whether the conversion is a "success" or, in the case of failure, the reason for failure, e.g. 'cannot convert amount to rate' for rows of propofol in 'mcg' that the users want to convert to 'mcg/kg/min'. 
        - `med_dose_converted`, `med_dose_unit_converted`: the converted results if the `_convert_status` is 'success', or fall back to the original `med_dose` and `_clean_unit` if failure.

    _Note: the following demo output omits some rows and columns for display purposes_
    """
    )
    return


@app.cell
def _(co):
    cont_converted_ = co.medication_admin_continuous.df_converted
    return (cont_converted_,)


@app.cell
def _(cont_converted_, mo):
    _df = mo.sql(
        f"""
        FROM cont_converted_
        SELECT * EXCLUDE ('med_order_id', 'med_group','med_route_name', 'med_route_category', 'mar_action_name', 'mar_action_category', 'med_name')
        WHERE med_category in ('heparin', 'propofol')
        ORDER BY hospitalization_id, admin_dttm
        """
    )
    return


@app.cell
def _(mo):
    mo.md(r"""2. `co.medication_admin_continuous.conversion_counts` shows an aggregated summary of which source units of which `med_category` are converted to which preferred units -- and their frequency counts. A useful quality check would be to filter for all the `_convert_status` that are not 'success.'""")
    return


@app.cell
def _(co):
    co.medication_admin_continuous.conversion_counts
    return


@app.cell
def _(mo):
    mo.md(
        r"""
    To access the results directly instead of from the table instance, turn off the `save_to_table` argument:

    ```python
    cont_converted, cont_counts = co.convert_dose_units_for_continuous_meds(
        preferred_units = preferred_units_cont,
        save_to_table = False
    ) 
    ```
    """
    )
    return


@app.cell
def _(mo):
    mo.md(
        r"""
    ### Override option

    The function automatically parses whether the provided `med_categories` and preferred units in the dictionary are acceptable and return errors or warnings when they are not. To override any code-breaking error such as an unidentified `med_category` or preferred unit string, turn on the arg `override = True`:

    ```python
    co.convert_dose_units_for_continuous_meds(
        preferred_units = preferred_units_cont,
        override = True
    ) 
    ```

    ### Acceptable unit formatting

    The unit strings in `preferred_units` dictionary need to be formatted a certain way for them to be accepted. (The original source unit strings in `med_dose_unit` do _not_ face such restrictions. Both 'mL' and 'milliliter' in `med_dose_unit` can be correctly parsed as 'ml', for example.)

    For a list of acceptable preferred units:

    - amount:
        - mass: `mcg`, `mg`, `ng`, `g`
        - volume: `ml`, `l`
        - unit: `mu`, `u`

    - weight: `/kg`

    - time: `/hr`, `/min`

    - rate: a combination of amount, weight, and time, e.g. 'mcg/kg/min', 'u/hr'.
        - the unit can be either weight-adjusted or not -- that is, both 'mcg/kg/min' and 'mcg/min' are acceptable. When no weight is available from the `vitals` table to enable conversion between weight-adjusted and weight-less units, an error will be returned.

    All strings should be in lower case with no whitespaces in between.
    """
    )
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""## Standardize to base units across medications""")
    return


@app.cell
def _(mo):
    mo.md(
        r"""
    In rarer cases, one might prefer all applicable units of the same class be collapsed onto the same scale across medications, e.g. both 'mcg/kg/min' and 'mg/hour' would be converted to the same 'mcg/min' -- referred to here as the "base unit" -- across all medications applicable.

    To enable this, turn on the `show_intermediate = True` argument:
    """
    )
    return


@app.cell
def _(co, mo, preferred_units_cont):
    cont_converted_detailed, _ = co.convert_dose_units_for_continuous_meds(
        preferred_units = preferred_units_cont,
        save_to_table=False,
        show_intermediate = True
    ) 

    mo.show_code()
    return (cont_converted_detailed,)


@app.cell
def _(mo):
    mo.md(r"""This would append a series of additional columns that were the intermediate results generated during the conversion, including the `_base_dose` and `_base_unit`.""")
    return


@app.cell
def _(cont_converted_detailed, mo):
    _df = mo.sql(
        f"""
        FROM cont_converted_detailed
        SELECT * EXCLUDE ('med_order_id', 'med_group','med_route_name', 'med_route_category', 'mar_action_name', 'mar_action_category', 'med_name')
        WHERE med_category in ('heparin', 'propofol')
        ORDER BY hospitalization_id, admin_dttm
        """
    )
    return


@app.cell
def _(mo):
    mo.md(
        r"""
    The set of base units are: 

    - amount: `mcg`, `ml`, `u`
    - time: `/min`
    - rate: a combination of amount and time, e.g. `mcg/min`, `u/min`.
        - Note that all base units would be weight-less.
    """
    )
    return


@app.cell(hide_code=True)
def _(mo):
    pytest_med_df_input = mo.sql(
        f"""
        -- FROM pytest_med_df
        -- SELECT
        --     med_category,
        --     med_dose,
        --     med_dose_unit,
        --     weight_kg
        -- WHERE _convert_status IS NOT NULL
        --     AND med_category in (
        --     'fentanyl', 'propofol', 'insulin')
        -- ORDER BY med_category, med_dose_unit
        """,
        output=False
    )
    return


@app.cell(hide_code=True)
def _import():
    import marimo as mo
    import pandas as pd
    import sys
    from pathlib import Path
    from importlib import reload
    import duckdb

    # Add parent directory to path
    sys.path.append(str(Path().absolute().parent))
    return (mo,)


if __name__ == "__main__":
    app.run()

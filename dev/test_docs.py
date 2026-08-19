import marimo

__generated_with = "0.16.2"
app = marimo.App(width="columns")


@app.cell
def _():
    from clifpy import ClifOrchestrator

    co = ClifOrchestrator(
        config_path="config/demo_data_config.yaml",
        # timezone='UTC',  # Override the timezone setting in the config file
    )

    # co.initialize(tables=['patient', 'labs', 'vitals'])
    return ClifOrchestrator, co


@app.cell
def _(co):
    co.initialize(
        tables=['patient', 'labs', 'vitals'],
        # sample_size=1000, 
        columns={
            'labs': ['hospitalization_id', 'lab_result_dttm', 'lab_value', 'lab_category'],
            'vitals': ['hospitalization_id', 'recorded_dttm', 'vital_value', 'vital_category']
        },
        filters={
            'labs': {'lab_category': ['hemoglobin', 'sodium', 'creatinine']},
            'vitals': {'vital_category': ['heart_rate', 'sbp', 'spo2']}
        }
    )

    co.validate_all()
    return


@app.cell
def _(co):
    co.labs.errors
    return


@app.cell
def _(co):
    import pandas as pd
    from clifpy import Labs
    isinstance(co.labs, Labs)
    return


@app.cell
def _(co):
    co.medication_admin_continuous.errors
    return


@app.cell
def _(ClifOrchestrator):
    co_demo = ClifOrchestrator(
        config_path='config/demo_data_config.yaml',
        timezone='US/Central'
    )
    co_demo.initialize()

    co_demo
    return


@app.cell
def _(co, vitals):
    co.initialize(tables=['vitals'])
    co.vitals.df

    vitals.filter
    return


if __name__ == "__main__":
    app.run()

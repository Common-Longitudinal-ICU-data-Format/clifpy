import marimo

__generated_with = "0.16.1"
app = marimo.App(width="columns")


@app.cell
def _():
    from clifpy import ClifOrchestrator

    co = ClifOrchestrator(config_path='config/config.yaml')

    co.initialize(tables=['medication_admin_continuous'])
    return (co,)


@app.cell
def _(co):
    co.medication_admin_continuous.errors
    return


@app.cell
def _():
    from clifpy.clif_orchestrator import ClifOrchestrator

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

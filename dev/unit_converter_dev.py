import marimo

__generated_with = "0.16.4"
app = marimo.App(width="columns")


@app.cell
def _():
    from clifpy.clif_orchestrator import ClifOrchestrator

    co = ClifOrchestrator(config_path = "config/config.yaml")

    preferred_units_cont = {
        "propofol": "mcg/min",
        "fentanyl": "mcg/hr",
        "insulin": "u/hr",
        "midazolam": "mg/hr",
        "heparin": "u/min"
    }

    co.convert_dose_units_for_continuous_meds(
        preferred_units = preferred_units_cont, override=True,
        hospitalization_ids=['21674796', '21676306']
        ) 
    return


if __name__ == "__main__":
    app.run()

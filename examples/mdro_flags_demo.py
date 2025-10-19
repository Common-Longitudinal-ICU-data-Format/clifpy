import marimo

__generated_with = "0.16.1"
app = marimo.App(width="medium")


@app.cell
def _():
    import marimo as mo
    import pandas as pd
    import sys
    from pathlib import Path

    # Add the clifpy package to the path
    sys.path.insert(0, str(Path(__file__).parent.parent))

    mo.md("""
    # MDRO Flag Calculation Demo

    **Multi-Drug Resistant Organism (MDRO) Detection**

    This notebook demonstrates how to use clifpy's MDRO flag calculation utility to identify:
    - **MDR** (Multi-Drug Resistant): Non-susceptible to ≥1 agent in ≥3 antimicrobial categories
    - **XDR** (Extensively Drug Resistant): Non-susceptible to ≥1 agent in all but ≤2 categories
    - **PDR** (Pandrug Resistant): Non-susceptible to all antimicrobial agents tested
    - **DLR** (Difficult to Treat Resistance): Non-susceptible to specific high-priority agents

    ## What is MDRO?

    MDRO classification helps identify organisms with concerning resistance patterns,
    guiding clinical decision-making and infection control practices.

    This demo focuses on **Pseudomonas aeruginosa**, a common healthcare-associated pathogen.
    """)
    return mo, pd


@app.cell
def _(mo):
    mo.md(
        """
    ## 1. Understanding the MDRO Configuration

    The MDRO detection system uses a YAML configuration file that defines:
    1. **Antimicrobial groups** - Which drugs belong to which categories
    2. **Resistance definitions** - Criteria for MDR/XDR/PDR/DLR classification

    Let's look at the configuration:
    """
    )
    return


@app.cell
def _():
    import yaml
    from pathlib import Path

    # Load the MDRO configuration
    config_path = Path(__file__).parent.parent / "clifpy" / "data" / "mdro.yaml"

    with open(config_path, 'r') as f:
        mdro_config = yaml.safe_load(f)

    # Extract P. aeruginosa configuration
    psar_config = mdro_config['organisms']['pseudomonas_aeruginosa']
    return (psar_config,)


@app.cell
def _(mo, pd, psar_config):
    # Show antimicrobial groups
    mo.md("""
    ### Antimicrobial Groups for *Pseudomonas aeruginosa*

    The following antimicrobial groups are used for MDRO classification:
    """)

    group_summary = []
    for group_name, agents in psar_config['antimicrobial_groups'].items():
        group_summary.append({
            'Group': group_name.replace('_', ' ').title(),
            'Agents': ', '.join(agents[:3]) + (f' (+{len(agents)-3} more)' if len(agents) > 3 else '')
        })

    pd.DataFrame(group_summary)
    return


@app.cell
def _(mo, psar_config):
    # Show resistance definitions
    mo.md("""
    ### Resistance Level Definitions
    """)

    for flag_name, flag_def in psar_config['resistance_definitions'].items():
        mo.md(f"""
        **{flag_def['name']} ({flag_name.upper()})**
        {flag_def['description']}
        """)
    return


@app.cell
def _(mo):
    mo.md(
        """
    ## 2. Loading Microbiology Data

    To calculate MDRO flags, we need two tables:
    1. **Culture table** - Contains organism identification and hospitalization linkage
    2. **Susceptibility table** - Contains antimicrobial susceptibility test results

    ### Sample Data Structure

    For this demo, we'll create sample data. In practice, you would load your own data:

    ```python
    from clifpy.tables import MicrobiologyCulture, MicrobiologySusceptibility

    culture = MicrobiologyCulture(
        data_directory="/path/to/data",
        filetype="parquet"
    )

    susceptibility = MicrobiologySusceptibility(
        data_directory="/path/to/data",
        filetype="parquet"
    )
    ```
    """
    )
    return


@app.cell
def _(pd):
    # Create sample culture data
    sample_culture_data = pd.DataFrame({
        'patient_id': ['P001', 'P001', 'P002', 'P003'],
        'hospitalization_id': ['H001', 'H001', 'H002', 'H003'],
        'organism_id': ['ORG001', 'ORG002', 'ORG003', 'ORG004'],
        'organism_category': ['pseudomonas_aeruginosa', 'escherichia_coli',
                             'pseudomonas_aeruginosa', 'pseudomonas_aeruginosa'],
        'organism_name': ['Pseudomonas aeruginosa', 'Escherichia coli',
                         'Pseudomonas aeruginosa', 'Pseudomonas aeruginosa'],
        'result_dttm': pd.to_datetime(['2024-01-15', '2024-01-16',
                                       '2024-02-10', '2024-03-05']),
        'fluid_category': ['blood_buffy', 'genito_urinary_tract',
                          'respiratory_tract_lower', 'blood_buffy'],
        'organism_group': ['pseudomonas_wo_cepacia_maltophilia', 'escherichia',
                          'pseudomonas_wo_cepacia_maltophilia',
                          'pseudomonas_wo_cepacia_maltophilia']
    })

    sample_culture_data
    return (sample_culture_data,)


@app.cell
def _(pd):
    # Create sample susceptibility data
    # This represents susceptibility testing results for the organisms
    sample_susceptibility_data = pd.DataFrame({
        'organism_id': [
            # ORG001 - MDR P. aeruginosa (resistant to 3+ groups)
            'ORG001', 'ORG001', 'ORG001', 'ORG001', 'ORG001', 'ORG001',
            # ORG003 - XDR P. aeruginosa (resistant to all but 1 group)
            'ORG003', 'ORG003', 'ORG003', 'ORG003', 'ORG003', 'ORG003', 'ORG003',
            # ORG004 - Susceptible P. aeruginosa
            'ORG004', 'ORG004', 'ORG004', 'ORG004'
        ],
        'antimicrobial_category': [
            # ORG001 - MDR (resistant to carbapenems, cephalosporins, fluoroquinolones)
            'meropenem', 'imipenem', 'ceftazidime', 'cefepime',
            'ciprofloxacin', 'piperacillin_tazobactam',
            # ORG003 - XDR (resistant to most agents, susceptible only to colistin)
            'meropenem', 'ceftazidime', 'cefepime', 'ciprofloxacin',
            'piperacillin_tazobactam', 'aztreonam', 'tobramycin',
            # ORG004 - Susceptible
            'meropenem', 'ceftazidime', 'ciprofloxacin', 'piperacillin_tazobactam'
        ],
        'susceptibility_category': [
            # ORG001
            'non_susceptible', 'non_susceptible', 'non_susceptible', 'intermediate',
            'non_susceptible', 'susceptible',
            # ORG003
            'non_susceptible', 'non_susceptible', 'non_susceptible', 'non_susceptible',
            'non_susceptible', 'intermediate', 'non_susceptible',
            # ORG004
            'susceptible', 'susceptible', 'susceptible', 'susceptible'
        ]
    })

    sample_susceptibility_data
    return (sample_susceptibility_data,)


@app.cell
def _(mo):
    mo.md(
        """
    ## 3. Creating Table Objects

    Now we'll create MicrobiologyCulture and MicrobiologySusceptibility objects
    from our sample data:
    """
    )
    return


@app.cell
def _(sample_culture_data, sample_susceptibility_data):
    from clifpy.tables import MicrobiologyCulture, MicrobiologySusceptibility

    # Create table objects from sample data
    culture = MicrobiologyCulture(data=sample_culture_data)
    susceptibility = MicrobiologySusceptibility(data=sample_susceptibility_data)
    return culture, susceptibility


@app.cell
def _(culture, mo, susceptibility):
    mo.md(
        f"""
    **Tables loaded successfully!**

    - Culture table: {len(culture.df)} organisms
    - Susceptibility table: {len(susceptibility.df)} test results
    - P. aeruginosa cultures: {(culture.df['organism_category'] == 'pseudomonas_aeruginosa').sum()}
    """
    )
    return


@app.cell
def _(mo):
    mo.md(
        """
    ## 4. Calculate MDRO Flags

    Now we'll use the `calculate_mdro_flags()` function to identify MDR/XDR/PDR/DLR organisms.

    The function will:
    1. Filter culture table to P. aeruginosa only
    2. LEFT JOIN with susceptibility data (preserves all cultures)
    3. Map antimicrobial agents to groups
    4. Calculate resistance flags based on criteria
    """
    )
    return


@app.cell
def _(culture, susceptibility):
    from clifpy.utils.mdro_flags import calculate_mdro_flags

    # Calculate MDRO flags for all P. aeruginosa
    mdro_results = calculate_mdro_flags(
        culture=culture,
        susceptibility=susceptibility,
        organism_name="pseudomonas_aeruginosa"
    )
    return calculate_mdro_flags, mdro_results


@app.cell
def _(mdro_results, mo):
    mo.md("""
    ### MDRO Flag Results

    Here are the calculated flags for each P. aeruginosa organism:
    """)

    mdro_results
    return


@app.cell
def _(mdro_results, mo):
    # Calculate summary statistics
    total_organisms = len(mdro_results)

    mdr_count = mdro_results['mdro_psar_mdr'].sum() if 'mdro_psar_mdr' in mdro_results.columns else 0
    xdr_count = mdro_results['mdro_psar_xdr'].sum() if 'mdro_psar_xdr' in mdro_results.columns else 0
    pdr_count = mdro_results['mdro_psar_pdr'].sum() if 'mdro_psar_pdr' in mdro_results.columns else 0
    dlr_count = mdro_results['mdro_psar_dlr'].sum() if 'mdro_psar_dlr' in mdro_results.columns else 0

    mo.md(f"""
    ### Summary Statistics

    **Total P. aeruginosa with susceptibility data:** {total_organisms}

    **Resistance Classifications:**
    - MDR (Multi-Drug Resistant): {mdr_count} ({mdr_count/total_organisms*100:.1f}%)
    - XDR (Extensively Drug Resistant): {xdr_count} ({xdr_count/total_organisms*100:.1f}%)
    - PDR (Pandrug Resistant): {pdr_count} ({pdr_count/total_organisms*100:.1f}%)
    - DLR (Difficult to Treat): {dlr_count} ({dlr_count/total_organisms*100:.1f}%)
    """)
    return


@app.cell
def _(mo):
    mo.md(
        """
    ## 5. Filter to Specific Hospitalizations

    You can also calculate MDRO flags for specific hospitalizations:
    """
    )
    return


@app.cell
def _(calculate_mdro_flags, culture, susceptibility):
    # Calculate flags for specific hospitalizations
    specific_hosps = ['H001', 'H003']

    mdro_filtered = calculate_mdro_flags(
        culture=culture,
        susceptibility=susceptibility,
        organism_name="pseudomonas_aeruginosa",
        hospitalization_ids=specific_hosps
    )
    return mdro_filtered, specific_hosps


@app.cell
def _(mdro_filtered, mo, specific_hosps):
    mo.md(f"""
    ### Results for Hospitalizations: {', '.join(specific_hosps)}
    """)

    mdro_filtered
    return


@app.cell
def _(mo):
    mo.md(
        """
    ## 6. Interpreting Results

    ### Understanding the Flags

    Each organism gets a binary flag (0 or 1) for each resistance level:

    - **mdro_psar_mdr = 1**: Organism is Multi-Drug Resistant
    - **mdro_psar_xdr = 1**: Organism is Extensively Drug Resistant
    - **mdro_psar_pdr = 1**: Organism is Pandrug Resistant
    - **mdro_psar_dlr = 1**: Organism shows Difficult to Treat Resistance

    ### Clinical Implications

    - **MDR organisms** may require consultation with infectious disease specialists
    - **XDR organisms** have very limited treatment options
    - **PDR organisms** are resistant to all tested agents - may require novel therapies
    - **DLR organisms** are resistant to all standard anti-pseudomonal agents

    ### Important Notes

    1. Flags are calculated based on **tested agents only**
    2. Both 'intermediate' and 'non_susceptible' results count as resistant
    3. Organisms without susceptibility testing are excluded from results
    4. Results should be interpreted in conjunction with clinical context
    """
    )
    return


@app.cell
def _(mo):
    mo.md(
        """
    ## 7. Advanced Usage

    ### With Cohort Date Filtering (Not shown in this demo)

    You can filter organisms by date range using a cohort DataFrame:

    ```python
    cohort_df = pd.DataFrame({
        'hospitalization_id': ['H001', 'H002'],
        'start_dttm': ['2024-01-01', '2024-02-01'],
        'end_dttm': ['2024-01-31', '2024-02-28']
    })

    mdro_flags = calculate_mdro_flags(
        culture=culture,
        susceptibility=susceptibility,
        organism_name="pseudomonas_aeruginosa",
        cohort=cohort_df
    )
    ```

    This filters to only include cultures with `result_dttm` within the specified date ranges.

    ### Adding Other Organisms

    The MDRO configuration supports multiple organisms. To add a new organism:

    1. Add organism configuration to `clifpy/data/mdro.yaml`
    2. Define antimicrobial groups
    3. Define resistance criteria
    4. Call `calculate_mdro_flags()` with the new organism name

    ## Next Steps

    - Load your own culture and susceptibility data
    - Calculate MDRO flags for your patient population
    - Track resistance trends over time
    - Integrate with infection control surveillance

    ## Resources

    - [CLIF Specification](https://clif-consortium.github.io/website/)
    - [CDC MDRO Guidelines](https://www.cdc.gov/hai/organisms/organisms.html)
    - Magiorakos AP, et al. Multidrug-resistant, extensively drug-resistant and pandrug-resistant bacteria. Clin Microbiol Infect. 2012.
    """
    )
    return


if __name__ == "__main__":
    app.run()

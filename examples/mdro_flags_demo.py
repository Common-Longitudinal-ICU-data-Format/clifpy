import marimo

__generated_with = "0.16.1"
app = marimo.App(width="full")


@app.cell
def _():
    import marimo as mo
    import pandas as pd
    import sys
    from pathlib import Path

    # Add the clifpy package to the path
    project_root = Path(__file__).parent.parent
    if str(project_root) not in sys.path:
        sys.path.insert(0, str(project_root))

    from clifpy.tables import MicrobiologyCulture, MicrobiologySusceptibility
    return MicrobiologyCulture, MicrobiologySusceptibility, Path, mo, pd


@app.cell
def _(Path):
    import yaml

    # Load the MDRO configuration
    config_path = Path(__file__).parent.parent / "clifpy" / "data" / "mdro.yaml"

    with open(config_path, 'r') as f:
        mdro_config = yaml.safe_load(f)

    # Extract P. aeruginosa configuration
    psar_config = mdro_config['organisms']['pseudomonas_aeruginosa']
    return (psar_config,)


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
def _(MicrobiologyCulture, MicrobiologySusceptibility):
    # Initialize tables from data files
    # TODO: Update the data_directory in clif_config.json to point to your CLIF data location

    culture = MicrobiologyCulture.from_file(
        config_path='clif_config.json'
    )

    susceptibility = MicrobiologySusceptibility.from_file(
        config_path='clif_config.json'
    )
    return culture, susceptibility


@app.cell
def _(culture):
    culture.df
    return


@app.cell
def _(susceptibility):
    susceptibility.df
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
    return (mdro_results,)


@app.cell
def _():
    # 17 must be tested
    # 4 are not tested

    # so 13 

    # for PDR all MUST MUST be tested 

    # for XDR 


    # For PDR must be non-susceptible to all the antimicrobial agents (17 - 4 = 13)
    # For XDR must be non susceptible to 1 or more agent in 6 of the categories / groups. If a pseudomonas is only tested for antimicrobials in 5 categories/groups, then it cant be an XDR. That is the "MDR, possible XDR" from FIgure 2. For us, we classify as MDR
    # For MDR, it must have been tested in 3 or more categories (which I suspect will always be the case)
    return


@app.cell
def _(mdro_results):
    mdro_results
    return


@app.cell
def _(mo):
    mo.md(
        """
    ## Validation: Check Flag Logic

    Let's verify that each MDRO flag is calculated correctly by checking the underlying resistance patterns.
    """
    )
    return


@app.cell
def _(mdro_results):
    # MDR Validation: Should have ≥3 resistant groups
    mdr_organisms = mdro_results[mdro_results['mdro_psar_mdr'] == 1]

    if len(mdr_organisms) > 0:
        # Get group columns
        mdr_group_cols = [col for col in mdro_results.columns if col.endswith('_group')]

        # Count resistant groups for each MDR organism
        mdr_resistant_group_counts = mdr_organisms[mdr_group_cols].sum(axis=1)

        # Check if all have ≥3 resistant groups
        mdr_all_valid = (mdr_resistant_group_counts >= 3).all()
        mdr_min_groups = mdr_resistant_group_counts.min()
        mdr_max_groups = mdr_resistant_group_counts.max()

        if mdr_all_valid:
            print(f"""
    ✓ MDR Validation PASSED
    - Total MDR organisms: {len(mdr_organisms)}
    - All have ≥3 resistant groups
    - Range: {mdr_min_groups}-{mdr_max_groups} resistant groups
    """)
        else:
            mdr_violations = mdr_organisms[mdr_resistant_group_counts < 3]
            print(f"""
    ✗ MDR Validation FAILED
    - {len(mdr_violations)} organisms flagged as MDR but have <3 resistant groups
    """)
    else:
        print("MDR Validation: No MDR organisms found")
    return


@app.cell
def _(mdro_results):
    # XDR Validation: Should have resistance in all but ≤2 groups
    # For P. aeruginosa with 8 groups: ≥6 resistant groups
    xdr_organisms = mdro_results[mdro_results['mdro_psar_xdr'] == 1]

    if len(xdr_organisms) > 0:
        xdr_group_cols = [col for col in mdro_results.columns if col.endswith('_group')]
        xdr_total_groups = len(xdr_group_cols)

        # Count resistant groups
        xdr_resistant_group_counts = xdr_organisms[xdr_group_cols].sum(axis=1)

        # For XDR: should have ≥ (total_groups - 2) resistant groups
        xdr_min_required = xdr_total_groups - 2
        xdr_all_valid = (xdr_resistant_group_counts >= xdr_min_required).all()
        xdr_min_groups = xdr_resistant_group_counts.min()
        xdr_max_groups = xdr_resistant_group_counts.max()

        if xdr_all_valid:
            print(f"""
    ✓ XDR Validation PASSED
    - Total XDR organisms: {len(xdr_organisms)}
    - All have ≥{xdr_min_required} resistant groups (out of {xdr_total_groups})
    - Range: {xdr_min_groups}-{xdr_max_groups} resistant groups
    """)
        else:
            xdr_violations = xdr_organisms[xdr_resistant_group_counts < xdr_min_required]
            print(f"""
    ✗ XDR Validation FAILED
    - {len(xdr_violations)} organisms flagged as XDR but have <{xdr_min_required} resistant groups
    """)
    else:
        print("XDR Validation: No XDR organisms found")
    return


@app.cell
def _(mdro_results):
    # PDR Validation: Should be resistant to ALL tested agents
    pdr_organisms = mdro_results[mdro_results['mdro_psar_pdr'] == 1]

    if len(pdr_organisms) > 0:
        pdr_agent_cols = [col for col in mdro_results.columns if col.endswith('_agent')]

        pdr_validation_results = []
        for pdr_idx, pdr_row in pdr_organisms.iterrows():
            # Get tested agents (non-null values)
            pdr_tested_agents = pdr_row[pdr_agent_cols].dropna()

            # Check if all are resistant (intermediate or non_susceptible)
            pdr_resistant_agents = pdr_tested_agents[pdr_tested_agents.isin(['intermediate', 'non_susceptible'])]

            pdr_all_resistant = len(pdr_resistant_agents) == len(pdr_tested_agents)
            pdr_validation_results.append(pdr_all_resistant)

        pdr_all_valid = all(pdr_validation_results)

        if pdr_all_valid:
            pdr_avg_tested = pdr_organisms[pdr_agent_cols].notna().sum(axis=1).mean()
            print(f"""
    ✓ PDR Validation PASSED
    - Total PDR organisms: {len(pdr_organisms)}
    - All are resistant to 100% of tested agents
    - Average agents tested: {pdr_avg_tested:.1f}
    """)
        else:
            pdr_violations = sum(not v for v in pdr_validation_results)
            print(f"""
    ✗ PDR Validation FAILED
    - {pdr_violations} organisms flagged as PDR but not resistant to all tested agents
    """)
    else:
        print("PDR Validation: No PDR organisms found")
    return


@app.cell
def _(mdro_results, pd):
    # DLR Validation: Should be resistant to all 8 specific agents
    dlr_organisms = mdro_results[mdro_results['mdro_psar_dtr'] == 1]

    if len(dlr_organisms) > 0:
        # The 8 required agents for DLR
        dlr_required_agents = [
            'piperacillin_tazobactam_agent',
            'ceftazidime_agent',
            'cefepime_agent',
            'aztreonam_agent',
            'meropenem_agent',
            'imipenem_agent',
            'ciprofloxacin_agent',
            'levofloxacin_agent'
        ]

        dlr_validation_results = []
        for dlr_idx, dlr_row in dlr_organisms.iterrows():
            # Check if all required agents are resistant
            dlr_tested_required = {agent: dlr_row.get(agent) for agent in dlr_required_agents if pd.notna(dlr_row.get(agent))}

            if len(dlr_tested_required) > 0:
                dlr_all_resistant = all(
                    val in ['intermediate', 'non_susceptible']
                    for val in dlr_tested_required.values()
                )
                dlr_validation_results.append((dlr_all_resistant, len(dlr_tested_required)))
            else:
                dlr_validation_results.append((False, 0))

        dlr_all_valid = all(v[0] for v in dlr_validation_results)
        dlr_avg_tested = sum(v[1] for v in dlr_validation_results) / len(dlr_validation_results) if dlr_validation_results else 0

        if dlr_all_valid:
            print(f"""
    ✓ DLR Validation PASSED
    - Total DLR organisms: {len(dlr_organisms)}
    - All are resistant to all tested required agents
    - Average required agents tested: {dlr_avg_tested:.1f} / 8
    """)
        else:
            dlr_violations = sum(not v[0] for v in dlr_validation_results)
            print(f"""
    ✗ DLR Validation FAILED
    - {dlr_violations} organisms flagged as DLR but not resistant to all required agents
    """)
    else:
        print("DLR Validation: No DLR organisms found")
    return


@app.cell
def _(mdro_results):
    # Summary
    total_organisms = len(mdro_results)
    mdr_count = mdro_results['mdro_psar_mdr'].sum()
    xdr_count = mdro_results['mdro_psar_xdr'].sum()
    pdr_count = mdro_results['mdro_psar_pdr'].sum()
    dlr_count = mdro_results['mdro_psar_dtr'].sum()

    print(f"""
    ## Summary

    Total P. aeruginosa organisms analyzed: {total_organisms}

    Classification                        Count    Percentage
    ---------------------------------------------------------
    MDR (Multi-Drug Resistant)            {mdr_count}      {mdr_count/total_organisms*100:.1f}%
    XDR (Extensively Drug Resistant)      {xdr_count}      {xdr_count/total_organisms*100:.1f}%
    PDR (Pandrug Resistant)               {pdr_count}      {pdr_count/total_organisms*100:.1f}%
    DLR (Difficult to Treat)              {dlr_count}      {dlr_count/total_organisms*100:.1f}%

    Note: An organism can have multiple classifications (e.g., an XDR organism is also MDR).
    """)
    return


if __name__ == "__main__":
    app.run()

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
    mdr_organisms = mdro_results[mdro_results['MDR'] == 1]

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
    xdr_organisms = mdro_results[mdro_results['XDR'] == 1]

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
def _(mdro_results, pd):
    # PDR Validation: ALL 12 defined agents must be tested AND all must be resistant
    pdr_organisms = mdro_results[mdro_results['PDR'] == 1]

    if len(pdr_organisms) > 0:
        pdr_agent_cols = [col for col in mdro_results.columns if col.endswith('_agent')]

        pdr_validation_results = []
        for pdr_idx, pdr_row in pdr_organisms.iterrows():
            # Get all agent values
            pdr_agent_values = pdr_row[pdr_agent_cols]

            # Check if ALL agents are tested (not "not_tested" and not NaN)
            pdr_all_tested = not any(
                pd.isna(val) or val == "not_tested"
                for val in pdr_agent_values
            )

            if pdr_all_tested:
                # All agents tested - check if all are resistant
                pdr_all_resistant = all(
                    val in ['intermediate', 'non_susceptible']
                    for val in pdr_agent_values
                )
                pdr_validation_results.append((True, pdr_all_resistant))
            else:
                # Incomplete testing - should not be PDR
                pdr_num_tested = sum(
                    1 for val in pdr_agent_values
                    if pd.notna(val) and val != "not_tested"
                )
                pdr_validation_results.append((False, pdr_num_tested))

        pdr_all_valid = all(v[0] and v[1] for v in pdr_validation_results)
        pdr_complete_testing = all(v[0] for v in pdr_validation_results)

        if pdr_all_valid:
            print(f"""
    ✓ PDR Validation PASSED
    - Total PDR organisms: {len(pdr_organisms)}
    - All have complete testing (12/12 agents) and all are resistant
    """)
        else:
            if not pdr_complete_testing:
                pdr_incomplete = sum(1 for v in pdr_validation_results if not v[0])
                print(f"""
    ✗ PDR Validation FAILED
    - {pdr_incomplete} organisms flagged as PDR but have incomplete testing
    - PDR requires ALL 12 defined agents to be tested
    """)
            else:
                pdr_violations = sum(1 for v in pdr_validation_results if v[0] and not v[1])
                print(f"""
    ✗ PDR Validation FAILED
    - {pdr_violations} organisms have complete testing but not all agents are resistant
    """)
    else:
        print("PDR Validation: No PDR organisms found")
    return


@app.cell
def _(mdro_results, pd):
    # DTR Validation: ALL 7 required agents must be tested AND all must be resistant
    dtr_organisms = mdro_results[mdro_results['DTR'] == 1]

    if len(dtr_organisms) > 0:
        # The 7 required agents for DTR (imipenem commented out in config)
        dtr_required_agents = [
            'piperacillin_tazobactam_agent',
            'ceftazidime_agent',
            'cefepime_agent',
            'aztreonam_agent',
            'meropenem_agent',
            # 'imipenem_agent',  # Commented out in YAML config
            'ciprofloxacin_agent',
            'levofloxacin_agent'
        ]

        dtr_validation_results = []
        for dtr_idx, dtr_row in dtr_organisms.iterrows():
            # Get values for all required agents
            dtr_agent_values = {
                agent: dtr_row.get(agent)
                for agent in dtr_required_agents
            }

            # Check if ALL 7 required agents are tested (not "not_tested" and not NaN)
            dtr_all_tested = all(
                pd.notna(val) and val != "not_tested"
                for val in dtr_agent_values.values()
            )

            if dtr_all_tested:
                # All 7 required agents tested - check if all are resistant
                dtr_all_resistant = all(
                    val in ['intermediate', 'non_susceptible']
                    for val in dtr_agent_values.values()
                )
                dtr_validation_results.append((True, dtr_all_resistant))
            else:
                # Incomplete testing - should not be DTR
                dtr_num_tested = sum(
                    1 for val in dtr_agent_values.values()
                    if pd.notna(val) and val != "not_tested"
                )
                dtr_validation_results.append((False, dtr_num_tested))

        dtr_all_valid = all(v[0] and v[1] for v in dtr_validation_results)
        dtr_complete_testing = all(v[0] for v in dtr_validation_results)

        if dtr_all_valid:
            print(f"""
    ✓ DTR Validation PASSED
    - Total DTR organisms: {len(dtr_organisms)}
    - All have complete testing (7/7 required agents) and all are resistant
    """)
        else:
            if not dtr_complete_testing:
                dtr_incomplete = sum(1 for v in dtr_validation_results if not v[0])
                dtr_avg_tested = sum(v[1] for v in dtr_validation_results if not v[0]) / dtr_incomplete if dtr_incomplete > 0 else 0
                print(f"""
    ✗ DTR Validation FAILED
    - {dtr_incomplete} organisms flagged as DTR but have incomplete testing
    - DTR requires ALL 7 required agents to be tested
    - Average tested for incomplete: {dtr_avg_tested:.1f} / 7
    """)
            else:
                dtr_violations = sum(1 for v in dtr_validation_results if v[0] and not v[1])
                print(f"""
    ✗ DTR Validation FAILED
    - {dtr_violations} organisms have complete testing but not all agents are resistant
    """)
    else:
        print("DTR Validation: No DTR organisms found")
    return


@app.cell
def _(mdro_results):
    # Filter to organisms with NO susceptible agents
    # Shows only highly resistant organisms where all tested agents are resistant

    # Get agent and flag columns
    agent_cols = [col for col in mdro_results.columns if col.endswith('_agent')]
    flag_cols = ['MDR', 'XDR', 'PDR', 'DTR']

    # Filter: keep rows where NO agent is "susceptible"
    no_susceptible = mdro_results.copy()
    for agent_col in agent_cols:
        no_susceptible = no_susceptible[no_susceptible[agent_col] != 'susceptible']

    # Select only flags and agents
    highly_resistant = no_susceptible[flag_cols + agent_cols]

    print(f"""
    ## Organisms with NO Susceptible Agents

    Filtered to organisms where ALL tested agents are either:
    - Resistant (intermediate or non_susceptible)
    - Not tested (not_tested)

    Total highly resistant organisms: {len(highly_resistant)} / {len(mdro_results)} ({len(highly_resistant)/len(mdro_results)*100:.1f}%)
    """)

    # Show flag combinations for these highly resistant organisms
    print("\nMDRO Flag Combinations (highly resistant organisms only):")
    print("-" * 60)
    flag_combinations = highly_resistant[flag_cols].value_counts().head(10)
    for idx, (flags, count) in enumerate(flag_combinations.items(), 1):
        mdr, xdr, pdr, dtr = flags
        flag_str = []
        if mdr: flag_str.append("MDR")
        if xdr: flag_str.append("XDR")
        if pdr: flag_str.append("PDR")
        if dtr: flag_str.append("DTR")
        print(f"{idx}. {' + '.join(flag_str) if flag_str else 'None'}: {count} organisms")
    return


@app.cell
def _(mdro_results):
    # Summary
    total_organisms = len(mdro_results)
    mdr_count = mdro_results['MDR'].sum()
    xdr_count = mdro_results['XDR'].sum()
    pdr_count = mdro_results['PDR'].sum()
    dtr_count = mdro_results['DTR'].sum()

    print(f"""
    ## Summary

    Total P. aeruginosa organisms analyzed: {total_organisms}

    Classification                        Count    Percentage
    ---------------------------------------------------------
    MDR (Multi-Drug Resistant)            {mdr_count}      {mdr_count/total_organisms*100:.1f}%
    XDR (Extensively Drug Resistant)      {xdr_count}      {xdr_count/total_organisms*100:.1f}%
    PDR (Pandrug Resistant)               {pdr_count}      {pdr_count/total_organisms*100:.1f}%
    DTR (Difficult to Treat)              {dtr_count}      {dtr_count/total_organisms*100:.1f}%

    Note: An organism can have multiple classifications (e.g., an XDR organism is also MDR).
    """)
    return


if __name__ == "__main__":
    app.run()

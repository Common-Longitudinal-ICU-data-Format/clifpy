from clifpy import ClifOrchestrator

co_pf = ClifOrchestrator(
    config_path = 'config/demo_data_config.yaml', 
    timezone='US/Eastern')

pf_cats_by_table = {
    'labs': ['po2_arterial'],
    'respiratory_support': ['fio2_set'] 
}

co_pf.create_wide_dataset(
    tables_to_load=list(pf_cats_by_table.keys()),
    category_filters=pf_cats_by_table
    # cohort_df=cohort_df
)

_df = co_pf.wide_df

print(_df.dtypes)
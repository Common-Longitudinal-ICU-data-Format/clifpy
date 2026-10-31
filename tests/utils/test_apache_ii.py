import pandas as pd

from clifpy.utils.apache_ii import calculate_apache_ii


def test_calculate_apache_ii_scores_first_24h_summary():
    df = pd.DataFrame(
        [{
            'hospitalization_id': 'H1',
            'temp_c': 38.7,
            'map_mm_hg': 80,
            'heart_rate': 110,
            'respiratory_rate': 25,
            'pao2_mm_hg': 60,
            'fio2': 0.5,
            'ph': 7.32,
            'sodium_meq_l': 146,
            'potassium_meq_l': 3.8,
            'creatinine_mg_dl': 1.8,
            'hematocrit_pct': 45,
            'wbc_x10_9_l': 15,
            'gcs_total': 14,
            'age_years': 68,
            'chronic_health': 'elective surgery',
        }]
    )

    result = calculate_apache_ii(df)

    assert result.loc[0, 'apache_ii_score'] == 18
    assert result.loc[0, 'apache_ii_age_points'] == 5
    assert result.loc[0, 'apache_ii_chronic_health_points'] == 2
    assert result.loc[0, 'apache_ii_oxygenation_points'] == 3


def test_calculate_apache_ii_handles_common_aliases_and_missing_values():
    df = pd.DataFrame(
        [{
            'hospitalization_id': 'H2',
            'temperature': 36.5,
            'mean_arterial_pressure': 75,
            'pulse': 70,
            'rr': 12,
            'po2_arterial': 90,
            'fi_o2': 0.21,
            'arterial_ph': 7.40,
            'na_meq_l': 140,
            'k_meq_l': 4.0,
            'serum_creatinine': 0.7,
            'hct_pct': 33,
            'white_blood_cell_count': 8,
            'glasgow_coma_scale_total': 15,
            'age': 54,
            'chronic_disease': 'none',
        }]
    )

    result = calculate_apache_ii(df)

    assert result.loc[0, 'apache_ii_score'] == 2
    assert result.loc[0, 'apache_ii_age_points'] == 2
    assert result.loc[0, 'apache_ii_chronic_health_points'] == 0

from typing import Optional, Dict
import pandas as pd
from .base_table import BaseTable


class HospitalDiagnosis(BaseTable):
    """
    Hospital diagnosis table wrapper inheriting from BaseTable.

    This class handles hospital diagnosis-specific data and validations while
    leveraging the common functionality provided by BaseTable. Hospital diagnosis
    codes are finalized billing diagnosis codes for hospital reimbursement,
    appropriate for calculation of comorbidity scores but should not be used
    as input features into a prediction model for an inpatient event.
    """

    def __init__(
        self,
        data_directory: str = None,
        filetype: str = None,
        timezone: str = "UTC",
        output_directory: Optional[str] = None,
        data: Optional[pd.DataFrame] = None
    ):
        """
        Initialize the hospital diagnosis table.

        Parameters
        ----------
        data_directory : str
            Path to the directory containing data files
        filetype : str
            Type of data file (csv, parquet, etc.)
        timezone : str
            Timezone for datetime columns
        output_directory : str, optional
            Directory for saving output files and logs
        data : pd.DataFrame, optional
            Pre-loaded data to use instead of loading from file
        """
        super().__init__(
            data_directory=data_directory,
            filetype=filetype,
            timezone=timezone,
            output_directory=output_directory,
            data=data
        )

        # Auto-load data if not provided
        if data is None and data_directory is not None and filetype is not None:
            self.load_table()

    def load_table(self):
        """Load hospital diagnosis table data from the configured data directory."""
        from ..utils.io import load_data

        if self.data_directory is None or self.filetype is None:
            raise ValueError("data_directory and filetype must be set to load data")

        self.df = load_data(
            self.table_name,
            self.data_directory,
            self.filetype,
            site_tz=self.timezone
        )

        if self.logger:
            self.logger.info(f"Loaded {len(self.df)} rows from {self.table_name} table")

    def get_diagnosis_summary(self) -> Dict:
        """Return comprehensive summary statistics for hospital diagnosis data."""
        if self.df is None:
            return {}

        stats = {
            'total_diagnoses': len(self.df),
            'unique_hospitalizations': self.df['hospitalization_id'].nunique() if 'hospitalization_id' in self.df.columns else 0,
            'unique_diagnosis_codes': self.df['diagnosis_code'].nunique() if 'diagnosis_code' in self.df.columns else 0
        }

        # Diagnosis code format distribution
        if 'diagnosis_code_format' in self.df.columns:
            stats['diagnosis_format_counts'] = self.df['diagnosis_code_format'].value_counts().to_dict()

        # Primary vs secondary diagnosis distribution
        if 'diagnosis_primary' in self.df.columns:
            primary_counts = self.df['diagnosis_primary'].value_counts().to_dict()
            stats['primary_diagnosis_counts'] = {
                'primary': primary_counts.get(1, 0),
                'secondary': primary_counts.get(0, 0)
            }

        # Present on admission distribution
        if 'poa_present' in self.df.columns:
            poa_counts = self.df['poa_present'].value_counts().to_dict()
            stats['poa_counts'] = {
                'present_on_admission': poa_counts.get(1, 0),
                'not_present_on_admission': poa_counts.get(0, 0)
            }

        return stats

    def get_primary_diagnosis_counts(self) -> pd.DataFrame:
        """Return DataFrame with counts of primary diagnoses by diagnosis code."""
        if self.df is None or 'diagnosis_primary' not in self.df.columns:
            return pd.DataFrame()

        primary_diagnoses = self.df[self.df['diagnosis_primary'] == 1]

        if primary_diagnoses.empty:
            return pd.DataFrame()

        diagnosis_counts = (primary_diagnoses.groupby(['diagnosis_code', 'diagnosis_code_format'])
                           .size()
                           .reset_index(name='count'))

        return diagnosis_counts.sort_values('count', ascending=False)

    def get_poa_statistics(self) -> Dict:
        """Calculate present on admission statistics by diagnosis type."""
        if self.df is None or 'poa_present' not in self.df.columns or 'diagnosis_primary' not in self.df.columns:
            return {}

        stats = {}

        # Overall POA statistics
        total_diagnoses = len(self.df)
        poa_present = len(self.df[self.df['poa_present'] == 1])
        poa_not_present = len(self.df[self.df['poa_present'] == 0])

        stats['overall'] = {
            'total_diagnoses': total_diagnoses,
            'poa_present_count': poa_present,
            'poa_not_present_count': poa_not_present,
            'poa_present_rate': (poa_present / total_diagnoses * 100) if total_diagnoses > 0 else 0
        }

        # POA statistics by primary/secondary diagnosis
        for diagnosis_type, diagnosis_value in [('primary', 1), ('secondary', 0)]:
            subset = self.df[self.df['diagnosis_primary'] == diagnosis_value]
            if not subset.empty:
                subset_total = len(subset)
                subset_poa_present = len(subset[subset['poa_present'] == 1])
                subset_poa_not_present = len(subset[subset['poa_present'] == 0])

                stats[diagnosis_type] = {
                    'total_diagnoses': subset_total,
                    'poa_present_count': subset_poa_present,
                    'poa_not_present_count': subset_poa_not_present,
                    'poa_present_rate': (subset_poa_present / subset_total * 100) if subset_total > 0 else 0
                }

        return stats

    def get_diagnosis_by_format(self) -> Dict:
        """Group diagnoses by format (ICD9/ICD10) and return summary statistics."""
        if self.df is None or 'diagnosis_code_format' not in self.df.columns:
            return {}

        format_stats = {}

        for format_type in self.df['diagnosis_code_format'].unique():
            subset = self.df[self.df['diagnosis_code_format'] == format_type]

            format_stats[format_type] = {
                'total_diagnoses': len(subset),
                'unique_diagnosis_codes': subset['diagnosis_code'].nunique() if 'diagnosis_code' in subset.columns else 0,
                'unique_hospitalizations': subset['hospitalization_id'].nunique() if 'hospitalization_id' in subset.columns else 0
            }

            # Primary vs secondary for this format
            if 'diagnosis_primary' in subset.columns:
                primary_counts = subset['diagnosis_primary'].value_counts().to_dict()
                format_stats[format_type]['primary_count'] = primary_counts.get(1, 0)
                format_stats[format_type]['secondary_count'] = primary_counts.get(0, 0)

            # POA statistics for this format
            if 'poa_present' in subset.columns:
                poa_counts = subset['poa_present'].value_counts().to_dict()
                format_stats[format_type]['poa_present_count'] = poa_counts.get(1, 0)
                format_stats[format_type]['poa_not_present_count'] = poa_counts.get(0, 0)

        return format_stats

    def get_hospitalization_diagnosis_counts(self) -> pd.DataFrame:
        """Return DataFrame with diagnosis counts per hospitalization."""
        if self.df is None or 'hospitalization_id' not in self.df.columns:
            return pd.DataFrame()

        hosp_counts = (self.df.groupby('hospitalization_id')
                      .agg({
                          'diagnosis_code': 'count',
                          'diagnosis_primary': lambda x: (x == 1).sum(),
                          'poa_present': lambda x: (x == 1).sum() if 'poa_present' in self.df.columns else 0
                      })
                      .reset_index())

        hosp_counts.columns = ['hospitalization_id', 'total_diagnoses', 'primary_diagnoses', 'poa_present_diagnoses']
        hosp_counts['secondary_diagnoses'] = hosp_counts['total_diagnoses'] - hosp_counts['primary_diagnoses']

        return hosp_counts.sort_values('total_diagnoses', ascending=False)
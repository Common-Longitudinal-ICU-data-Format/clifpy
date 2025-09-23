from typing import Optional, Dict
import pandas as pd
from .base_table import BaseTable


class PatientAssessments(BaseTable):
    """
    Patient assessments table wrapper inheriting from BaseTable.
    
    This class handles patient assessment data and validations while
    leveraging the common functionality provided by BaseTable.
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
        Initialize the patient_assessments table.
        
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
        # For backward compatibility, handle the old signature
        if data_directory is None and filetype is None and data is not None:
            # Old signature: patient_assessments(data)
            # Use dummy values for required parameters
            data_directory = "."
            filetype = "parquet"
        
        # Initialize assessment mappings
        self._assessment_category_to_group = None
        
        super().__init__(
            data_directory=data_directory,
            filetype=filetype,
            timezone=timezone,
            output_directory=output_directory,
            data=data
        )
        
        # Load assessment-specific schema data
        self._load_assessment_schema_data()

    def _load_assessment_schema_data(self):
        """Load assessment category to group mappings from the YAML schema."""
        if self.schema:
            self._assessment_category_to_group = self.schema.get('assessment_category_to_group_mapping', {})

    @property
    def assessment_category_to_group_mapping(self) -> Dict[str, str]:
        """Get the assessment category to group mapping from the schema."""
        return self._assessment_category_to_group.copy() if self._assessment_category_to_group else {}
    
    # Patient assessments-specific methods can be added here if needed
    # The base functionality (validate, isvalid, from_file) is inherited from BaseTable
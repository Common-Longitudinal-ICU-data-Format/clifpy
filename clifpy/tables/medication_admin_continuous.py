from typing import Optional, Dict, Tuple, Union, Set
import pandas as pd
from pyarrow import BooleanArray
from .base_table import BaseTable
import duckdb

class MedicationAdminContinuous(BaseTable):
    """
    Medication administration continuous table wrapper inheriting from BaseTable.
    
    This class handles medication administration continuous data and validations
    while leveraging the common functionality provided by BaseTable.
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
        Initialize the MedicationAdminContinuous table.
        
        This class handles continuous medication administration data, including validation,
        dose unit standardization, and unit conversion capabilities.
        
        Parameters
        ----------
        data_directory : str, optional
            Path to the directory containing data files. If None and data is provided,
            defaults to current directory.
        filetype : str, optional
            Type of data file (csv, parquet, etc.). If None and data is provided,
            defaults to 'parquet'.
        timezone : str, default="UTC"
            Timezone for datetime columns. Used for proper timestamp handling.
        output_directory : str, optional
            Directory for saving output files and logs. If not specified, outputs
            are saved to the current working directory.
        data : pd.DataFrame, optional
            Pre-loaded DataFrame to use instead of loading from file. Supports
            backward compatibility with direct DataFrame initialization.
        
        Notes
        -----
        The class supports two initialization patterns:
        1. Loading from file: provide data_directory and filetype
        2. Direct DataFrame: provide data parameter (legacy support)
        
        Upon initialization, the class loads medication schema data including
        category-to-group mappings from the YAML schema.
        """
        # For backward compatibility, handle the old signature
        if data_directory is None and filetype is None and data is not None:
            # Old signature: medication_admin_continuous(data)
            # Use dummy values for required parameters
            data_directory = "."
            filetype = "parquet"
        
        # Load medication mappings
        self._med_category_to_group = None
        
        super().__init__(
            data_directory=data_directory,
            filetype=filetype,
            timezone=timezone,
            output_directory=output_directory,
            data=data
        )
        
        # Load medication-specific schema data
        self._load_medication_schema_data()

    def _load_medication_schema_data(self):
        """
        Load medication-specific schema data from the YAML configuration.
        
        This method extracts medication category to group mappings from the loaded
        schema, which are used for medication classification and grouping operations.
        The mappings define relationships between medication categories (e.g., 'Antibiotics')
        and their broader therapeutic groups (e.g., 'Antimicrobials').
        
        The method is called automatically during initialization after the base
        schema is loaded.
        """
        if self.schema:
            self._med_category_to_group = self.schema.get('med_category_to_group_mapping', {})

    @property
    def med_category_to_group_mapping(self) -> Dict[str, str]:
        """
        Get the medication category to group mapping from the schema.
        
        Returns
        -------
        Dict[str, str]
            A dictionary mapping medication categories to their therapeutic groups.
            Returns a copy to prevent external modification of the internal mapping.
            Returns an empty dict if no mappings are loaded.
        
        Examples
        --------
        >>> mac = MedicationAdminContinuous(data)
        >>> mappings = mac.med_category_to_group_mapping
        >>> mappings['Antibiotics']
        'Antimicrobials'
        """
        return self._med_category_to_group.copy() if self._med_category_to_group else {}
    
    # Medication-specific methods can be added here if needed
    # The base functionality (validate, isvalid, from_file) is inherited from BaseTable
    
    @property
    def _acceptable_dose_unit_patterns(self) -> Set[str]:
        pass
    
    def resolve_mar_action_duplicates(self) -> pd.DataFrame:
        pass
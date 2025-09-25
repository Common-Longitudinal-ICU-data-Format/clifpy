from typing import Optional
import pandas as pd
from .base_table import BaseTable


class CrrtTherapy(BaseTable):
    """
    CRRT (Continuous Renal Replacement Therapy) table wrapper inheriting from BaseTable.
    
    This class handles CRRT therapy data including dialysis modes, flow rates,
    and ultrafiltration parameters while leveraging the common functionality 
    provided by BaseTable.
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
        Initialize the CRRT therapy table.
        
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
from typing import Optional, Union
import pandas as pd
from .base_table import BaseTable
from ..utils.waterfall import process_resp_support_waterfall


class RespiratorySupport(BaseTable):
    """
    Respiratory support table wrapper inheriting from BaseTable.
    
    This class handles respiratory support data and validations while
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
        Initialize the respiratory_support table.
        
        Parameters:
            data_directory (str): Path to the directory containing data files
            filetype (str): Type of data file (csv, parquet, etc.)
            timezone (str): Timezone for datetime columns
            output_directory (str, optional): Directory for saving output files and logs
            data (pd.DataFrame, optional): Pre-loaded data to use instead of loading from file
        """
        
        super().__init__(
            data_directory=data_directory,
            filetype=filetype,
            timezone=timezone,
            output_directory=output_directory,
            data=data
        )
    
    def waterfall(
        self,
        *,
        id_col: str = "hospitalization_id",
        bfill: bool = False,
        verbose: bool = True,
        return_dataframe: bool = False
    ) -> Union['RespiratorySupport', pd.DataFrame]:
        """
        Clean + waterfall-fill the respiratory_support table.
        
        Parameters:
            id_col (str): Encounter-level identifier column (default: hospitalization_id)
            bfill (bool): If True, numeric setters are back-filled after forward-fill
            verbose (bool): Print progress messages
            return_dataframe (bool): If True, returns DataFrame instead of RespiratorySupport instance
            
        Returns:
            RespiratorySupport: New instance with processed data (or DataFrame if return_dataframe=True)
        
        Note:
            The waterfall function expects data in UTC timezone. If your data is in a 
            different timezone, it will be converted to UTC for processing.
            The original object is not modified; a new instance is returned.
        
        Example:
            >>> processed = resp_support.waterfall()
            >>> processed.validate()  # Can run validation on processed data
            >>> df = processed.df     # Access the DataFrame
        """
        if self.df is None or self.df.empty:
            raise ValueError("No data available to process. Load data first.")
        
        # Create a copy to avoid modifying the original data
        df_copy = self.df.copy()
        
        # Convert to UTC if the recorded_dttm column has timezone info
        if 'recorded_dttm' in df_copy.columns and df_copy['recorded_dttm'].dt.tz is not None:
            original_tz = df_copy['recorded_dttm'].dt.tz
            df_copy['recorded_dttm'] = df_copy['recorded_dttm'].dt.tz_convert('UTC')
            if verbose:
                print(f"Converting timezone from {original_tz} to UTC for waterfall processing")
            
        # Use the existing waterfall function
        processed_df = process_resp_support_waterfall(
            df_copy,
            id_col=id_col,
            bfill=bfill,
            verbose=verbose
        )
        
        # Return DataFrame if requested
        if return_dataframe:
            return processed_df
            
        # Otherwise, create a new RespiratorySupport instance with processed data
        return RespiratorySupport(
            data_directory=self.data_directory,
            filetype=self.filetype,
            timezone=self.timezone,
            output_directory=self.output_directory,
            data=processed_df
        )
    
    # Respiratory support-specific methods can be added here if needed
    # The base functionality (validate, isvalid, from_file) is inherited from BaseTable

    

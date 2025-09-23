from typing import Optional
import pandas as pd
from .base_table import BaseTable


class Position(BaseTable):
    """
    Position table wrapper inheriting from BaseTable.
    
    This class handles patient position data and validations while
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
        Initialize the position table.
        
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
            # Old signature: position(data)
            # Use dummy values for required parameters
            data_directory = "."
            filetype = "parquet"
        
        super().__init__(
            data_directory=data_directory,
            filetype=filetype,
            timezone=timezone,
            output_directory=output_directory,
            data=data
        )
    
    def get_position_category_stats(self) -> pd.DataFrame:
        """
        Return summary statistics for each position category, including missingness and unique patient counts.
        Expects columns: 'position_category', 'position_name', and optionally 'hospitalization_id'.
        """
        if self.df is None or 'position_category' not in self.df.columns or 'hospitalization_id' not in self.df.columns:
            return {"status": "Missing columns"}

        agg_dict = {
            'count': ('position_category', 'count'),
            'unique': ('hospitalization_id', 'nunique'),
        }

        stats = (
            self.df
            .groupby('position_category')
            .agg(**agg_dict)
            .round(2)
        )

        return stats
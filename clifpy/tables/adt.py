from datetime import datetime
from typing import Optional, List, Dict
import pandas as pd
import os
from .base_table import BaseTable


class Adt(BaseTable):
    """
    ADT (Admission/Discharge/Transfer) table wrapper inheriting from BaseTable.
    
    This class handles ADT-specific data and validations while
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
        Initialize the ADT table.
        
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
            # Old signature: adt(data)
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

    # ------------------------------------------------------------------
    # ADT Specific Methods
    # ------------------------------------------------------------------
    def check_overlapping_admissions(self, save_overlaps: bool = False, overlaps_output_directory: Optional[str] = None) -> int:
        """
        Check for overlapping admissions within the same hospitalization.

        Identifies cases where a patient has overlapping stays in different locations
        within the same hospitalization (i.e., the out_dttm of one location is after
        the in_dttm of the next location).

        Parameters:
            save_overlaps (bool): If True, save detailed overlap information to CSV. Default is False.
            overlaps_output_directory (str, optional): Directory for saving the overlaps CSV file. 
                If None, uses the output_directory provided at initialization.

        Returns:
            int: Count of unique hospitalizations that have overlapping admissions

        Raises:
            RuntimeError: If an error occurs during processing
        """
        try:
            if self.df is None:
                return 0

            if 'hospitalization_id' not in self.df.columns:
                error = "hospitalization_id is missing."
                raise ValueError(error)

            # Sort by hospitalization_id and in_dttm to make comparisons easier
            data = self.df.sort_values(by=['hospitalization_id', 'in_dttm'])

            overlaps = []
            overlapping_hospitalizations = set()

            # Group by hospitalization_id to compare bookings for each hospitalization
            for hospitalization_id, group in data.groupby('hospitalization_id'):
                for i in range(len(group) - 1):
                    # Current and next bookings
                    current = group.iloc[i]
                    next = group.iloc[i + 1]

                    # Check if the locations are different and times overlap
                    if (
                        current['location_name'] != next['location_name'] and
                        current['out_dttm'] > next['in_dttm']
                    ):
                        overlapping_hospitalizations.add(hospitalization_id)

                        if save_overlaps:
                            overlaps.append({
                                'hospitalization_id': hospitalization_id,
                                'Initial Location': current['location_name'],
                                'Initial Location Category': current['location_category'],
                                'Overlapping Location': next['location_name'],
                                'Overlapping Location Category': next['location_category'],
                                'Admission Start': current['in_dttm'],
                                'Admission End': current['out_dttm'],
                                'Next Admission Start': next['in_dttm']
                            })

            # Save overlaps to CSV if requested
            if save_overlaps and overlaps:
                overlaps_df = pd.DataFrame(overlaps)
                # Determine the directory to save the overlaps file
                save_dir = overlaps_output_directory if overlaps_output_directory is not None else self.output_directory
                if save_dir is not None:
                    os.makedirs(save_dir, exist_ok=True)
                    file_path = os.path.join(save_dir, 'overlapping_admissions.csv')
                    overlaps_df.to_csv(file_path, index=False)
                else:
                    # Fallback to original method if no directory is specified
                    self.save_dataframe(overlaps_df, 'overlapping_admissions')

            return len(overlapping_hospitalizations)

        except Exception as e:
            # Handle errors gracefully
            raise RuntimeError(f"Error checking time overlap: {str(e)}")

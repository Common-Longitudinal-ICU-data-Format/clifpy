from typing import Optional
import pandas as pd
from .base_table import BaseTable


class RenalReplacementTherapy(BaseTable):
    """
    Renal replacement therapy (RRT) settings and measurements (CLIF 3.0).

    Covers both CRRT sub-modes (scuf, cvvh, cvvhd, cvvhdf, avvh) and intermittent
    modes (ihd, iuf) in one long-form table, one setting/measurement per row.

    The table holds machine settings and rates only. Net ultrafiltration is
    recorded in ``output`` under ``output_group = ultrafiltration``, and
    replacement/dialysate fluid delivered during RRT in ``input`` under
    ``input_group = renal_replacement_fluids``.

    Thin wrapper inheriting from BaseTable. Schema is selected by ``clif_version``;
    this table is defined in the CLIF 3.0 schema set.
    """

    def __init__(
        self,
        data_directory: str = None,
        filetype: str = None,
        timezone: str = "UTC",
        output_directory: Optional[str] = None,
        data: Optional[pd.DataFrame] = None,
        clif_version: Optional[str] = None
    ):
        """
        Initialize the renal_replacement_therapy table.

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
        clif_version : str, optional
            CLIF schema version to validate against (defaults to the package default)
        """
        super().__init__(
            data_directory=data_directory,
            filetype=filetype,
            timezone=timezone,
            output_directory=output_directory,
            data=data,
            clif_version=clif_version
        )

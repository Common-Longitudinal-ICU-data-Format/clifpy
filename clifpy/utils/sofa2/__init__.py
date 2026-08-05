"""SOFA-2 scoring module for clifpy.

This module provides functions to calculate SOFA-2 (Sequential Organ Failure Assessment)
scores based on the updated SOFA-2 specification.

Public API:
    calculate_sofa2: Calculate SOFA-2 scores for a cohort with time windows
    calculate_sofa2_daily: Calculate daily SOFA-2 scores with carry-forward logic
    SOFA2Config: Configuration dataclass for customizing calculation parameters
"""

from ._utils import SOFA2Config
from ._core import calculate_sofa2, calculate_sofa2_daily

__all__ = [
    'calculate_sofa2',
    'calculate_sofa2_daily',
    'SOFA2Config',
]

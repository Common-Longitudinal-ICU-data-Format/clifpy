"""Rolling (event-driven) SOFA-2 scoring module.

Instead of aggregating worst observations per time window (windowed approach),
the rolling approach processes observations chronologically and emits a row
whenever a subscore changes or a new worst-ever observation is recorded.

Public API:
    calculate_rolling_hemo: Calculate rolling hemostasis subscore (POC)
    RollingSOFA2Config: Configuration for rolling scoring parameters
"""

from ._config import RollingSOFA2Config
from ._hemo import _calculate_rolling_hemo as calculate_rolling_hemo

__all__ = [
    'calculate_rolling_hemo',
    'RollingSOFA2Config',
]

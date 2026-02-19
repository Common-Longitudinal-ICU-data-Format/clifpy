"""Re-export from canonical location for convenience.

The canonical implementation lives in clifpy.utils.sofa2._perf.
This file exists so benchmark scripts can do:
    from step_timer import StepTimer, NoOpTimer
"""
from clifpy.utils.sofa2._perf import StepTimer, NoOpTimer  # noqa: F401

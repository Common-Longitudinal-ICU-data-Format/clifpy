"""Configuration for rolling (event-driven) SOFA-2 scoring."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass
class RollingSOFA2Config:
    """Configuration for rolling (event-driven) SOFA-2 scoring.

    Unlike the windowed SOFA2Config (which uses pre-window lookback hours),
    the rolling config uses expiry durations — how long a subscore persists
    after the last observation before reverting to NULL.

    Attributes
    ----------
    hemo_expiry_hours : float | None
        Hours after last platelet observation before hemo score reverts to NULL.
        Set to None to disable expiry (score persists indefinitely).
        Default 48.0.
    """

    hemo_expiry_hours: float | None = 48.0

    # Future subscores:
    # brain_expiry_hours: float | None = ...
    # resp_expiry_hours: float | None = ...
    # cv_expiry_hours: float | None = ...
    # liver_expiry_hours: float | None = ...
    # kidney_expiry_hours: float | None = ...

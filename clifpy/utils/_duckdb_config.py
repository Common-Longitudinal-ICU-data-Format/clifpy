"""DuckDB resource configuration for compute-heavy pipelines.

Provides DuckDBResourceConfig — a shared config class that bundles
memory, disk, and batching settings. Reusable across calculate_sofa2(),
create_wide_dataset(), convert_wide_to_hourly(), and future utils.
"""
from __future__ import annotations

import shutil
from dataclasses import dataclass
from pathlib import Path


@dataclass
class DuckDBResourceConfig:
    """DuckDB resource limits for compute-heavy pipelines.

    When all fields are None (the default), DuckDB uses system defaults
    (~80% of RAM, unlimited temp, all cores). Zero-config users
    experience zero change — no artificial constraints imposed.

    Attributes
    ----------
    memory_limit : str, optional
        DuckDB memory limit (e.g., '8GB'). Spills to disk when exceeded.
        Default: None (DuckDB uses ~80% of system RAM).
    temp_directory : str, optional
        Directory for DuckDB spill files.
        Default: None (DuckDB uses '.tmp' in current working directory).
    max_temp_directory_size : str, optional
        Max disk space for spill files (e.g., '10GB'). When exceeded,
        DuckDB raises an error instead of filling the disk.
        Default: None (unlimited).
    batch_size : int, optional
        Process cohort in chunks of this size to reduce peak memory.
        Each batch reloads CLIF tables (I/O cost) but limits intermediate
        result size. Default: None (process all at once).
    """

    memory_limit: str | None = None
    temp_directory: str | None = None
    max_temp_directory_size: str | None = None
    batch_size: int | None = None

    @classmethod
    def from_system(cls, memory_fraction: float = 0.7) -> DuckDBResourceConfig:
        """Create a config based on detected system resources.

        Uses psutil to detect available RAM and disk, then sets
        conservative limits. Useful for users who want guardrails
        without manually checking system specs.

        Parameters
        ----------
        memory_fraction : float, default 0.7
            Fraction of available RAM to allocate to DuckDB.

        Returns
        -------
        DuckDBResourceConfig

        Examples
        --------
        >>> config = DuckDBResourceConfig.from_system()
        >>> print(config.summary())
        memory_limit:             12GB
        temp_directory:            system default (.tmp in CWD)
        max_temp_directory_size:   50GB
        batch_size:               disabled (all at once)
        """
        import psutil

        mem = psutil.virtual_memory()
        available_gb = mem.available / (1024**3)
        limit_gb = int(available_gb * memory_fraction)

        # Detect disk space for temp directory
        disk = shutil.disk_usage(Path.home())
        disk_free_gb = int(disk.free / (1024**3) * 0.5)  # use 50% of free disk

        return cls(
            memory_limit=f'{max(1, limit_gb)}GB',
            max_temp_directory_size=f'{max(1, disk_free_gb)}GB',
        )

    def summary(self) -> str:
        """Return a human-readable summary of this config."""
        lines = [
            f"memory_limit:             {self.memory_limit or 'system default (~80% RAM)'}",
            f"temp_directory:            {self.temp_directory or 'system default (.tmp in CWD)'}",
            f"max_temp_directory_size:   {self.max_temp_directory_size or 'unlimited'}",
            f"batch_size:               {self.batch_size or 'disabled (all at once)'}",
        ]
        return '\n'.join(lines)

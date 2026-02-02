"""
Table One Generator with Privacy Protection for CLIF.

This module generates standardized Table 1 (baseline characteristics) summaries
for CLIF cohorts with built-in privacy protection for federated research.

Privacy features:
- Cell suppression for counts below threshold (default: 10)
- Optional count rounding to nearest 5 or 10
- Percentage suppression when denominator is suppressed

Example usage:
    from clifpy.utils.table_one import TableOneGenerator, PrivacyConfig

    # Configure privacy settings
    privacy = PrivacyConfig(min_cell_size=10, round_counts_to=5)

    # Generate Table 1
    generator = TableOneGenerator(cohort_df, privacy_config=privacy)
    table_one = generator.generate()

    # Export
    generator.to_csv("output/table_one_SITE.csv")
    generator.to_json("output/table_one_SITE.json")
"""

from dataclasses import dataclass, field
from typing import Optional, List, Dict, Any, Union
import pandas as pd
import numpy as np
from datetime import datetime
import json


@dataclass
class PrivacyConfig:
    """Configuration for privacy protection in Table 1 output.

    Attributes:
        min_cell_size: Minimum count for cell to be reported (default: 10).
                       Cells below this threshold will be suppressed.
        round_counts_to: Round all counts to nearest value (e.g., 5 or 10).
                         Set to None to disable rounding.
        suppress_percentages: If True, suppress percentages when count is suppressed.
        suppression_label: Label to use for suppressed cells (default: "<10").
    """

    min_cell_size: int = 10
    round_counts_to: Optional[int] = None
    suppress_percentages: bool = True
    suppression_label: str = "<10"

    def __post_init__(self):
        if self.min_cell_size < 1:
            raise ValueError("min_cell_size must be at least 1")
        if self.round_counts_to is not None and self.round_counts_to < 1:
            raise ValueError("round_counts_to must be at least 1 or None")


@dataclass
class VariableConfig:
    """Configuration for a single variable in Table 1.

    Attributes:
        name: Column name in the DataFrame.
        label: Display label for the variable.
        var_type: Type of variable ('continuous', 'categorical', 'binary').
        categories: For categorical variables, list of categories to include.
        summary_stats: For continuous variables, which stats to compute.
    """

    name: str
    label: str
    var_type: str = "continuous"  # 'continuous', 'categorical', 'binary'
    categories: Optional[List[str]] = None
    summary_stats: List[str] = field(
        default_factory=lambda: ["median", "q1", "q3", "missing"]
    )


# Default CLIF Table 1 variables
DEFAULT_CLIF_VARIABLES = [
    VariableConfig("age_at_admission", "Age (years)", "continuous"),
    VariableConfig("sex_category", "Sex", "categorical"),
    VariableConfig("race_category", "Race", "categorical"),
    VariableConfig("ethnicity_category", "Ethnicity", "categorical"),
    VariableConfig("icu_los_hours", "ICU Length of Stay (hours)", "continuous"),
    VariableConfig("hospital_los_days", "Hospital Length of Stay (days)", "continuous"),
    VariableConfig(
        "hospital_mortality", "In-Hospital Mortality", "binary", categories=["Yes", "No"]
    ),
    VariableConfig(
        "received_imv", "Received Invasive Mechanical Ventilation", "binary"
    ),
    VariableConfig("received_vasopressors", "Received Vasopressors", "binary"),
    VariableConfig("received_crrt", "Received CRRT", "binary"),
]


class TableOneGenerator:
    """Generate standardized Table 1 summaries with privacy protection.

    This class produces Table 1 (baseline characteristics) tables that are
    safe for sharing in federated research contexts.
    """

    def __init__(
        self,
        data: pd.DataFrame,
        variables: Optional[List[VariableConfig]] = None,
        privacy_config: Optional[PrivacyConfig] = None,
        site_name: Optional[str] = None,
        cohort_description: Optional[str] = None,
    ):
        """Initialize the Table One generator.

        Args:
            data: DataFrame containing the cohort data.
            variables: List of VariableConfig objects defining variables to summarize.
                       If None, uses DEFAULT_CLIF_VARIABLES.
            privacy_config: Privacy settings. If None, uses default PrivacyConfig.
            site_name: Name of the site (for output labeling).
            cohort_description: Description of the cohort for metadata.
        """
        self.data = data
        self.variables = variables or DEFAULT_CLIF_VARIABLES
        self.privacy = privacy_config or PrivacyConfig()
        self.site_name = site_name or "SITE"
        self.cohort_description = cohort_description
        self._results: Optional[pd.DataFrame] = None
        self._metadata: Dict[str, Any] = {}

    def _apply_privacy(self, count: int) -> Union[int, str]:
        """Apply privacy protection to a count.

        Returns the count (possibly rounded) or suppression label if below threshold.
        """
        if count < self.privacy.min_cell_size:
            return self.privacy.suppression_label

        if self.privacy.round_counts_to:
            return (
                round(count / self.privacy.round_counts_to)
                * self.privacy.round_counts_to
            )

        return count

    def _calculate_percentage(
        self, count: Union[int, str], total: int
    ) -> Union[float, str]:
        """Calculate percentage with privacy protection."""
        if isinstance(count, str):  # Suppressed
            return self.privacy.suppression_label if self.privacy.suppress_percentages else ""
        if total == 0:
            return 0.0
        return round(100 * count / total, 1)

    def _summarize_continuous(
        self, series: pd.Series, config: VariableConfig
    ) -> Dict[str, Any]:
        """Summarize a continuous variable."""
        result = {"variable": config.label, "type": "continuous"}

        # Calculate stats on non-missing values
        valid = series.dropna()
        n_total = len(series)
        n_valid = len(valid)
        n_missing = n_total - n_valid

        # Apply privacy to counts
        result["n"] = self._apply_privacy(n_valid)
        result["missing_n"] = self._apply_privacy(n_missing)
        result["missing_pct"] = self._calculate_percentage(n_missing, n_total)

        # Only report statistics if we have enough non-suppressed data
        if isinstance(result["n"], int) and n_valid >= self.privacy.min_cell_size:
            if "median" in config.summary_stats:
                result["median"] = round(valid.median(), 1)
            if "mean" in config.summary_stats:
                result["mean"] = round(valid.mean(), 1)
            if "std" in config.summary_stats:
                result["std"] = round(valid.std(), 1)
            if "q1" in config.summary_stats:
                result["q1"] = round(valid.quantile(0.25), 1)
            if "q3" in config.summary_stats:
                result["q3"] = round(valid.quantile(0.75), 1)
            if "min" in config.summary_stats:
                result["min"] = round(valid.min(), 1)
            if "max" in config.summary_stats:
                result["max"] = round(valid.max(), 1)
        else:
            # Suppress statistics if count is suppressed
            for stat in ["median", "mean", "std", "q1", "q3", "min", "max"]:
                if stat in config.summary_stats:
                    result[stat] = self.privacy.suppression_label

        return result

    def _summarize_categorical(
        self, series: pd.Series, config: VariableConfig
    ) -> List[Dict[str, Any]]:
        """Summarize a categorical variable."""
        results = []
        n_total = len(series)

        # Get value counts
        counts = series.value_counts(dropna=False)

        # Determine categories to report
        if config.categories:
            categories = config.categories
        else:
            categories = [c for c in counts.index if pd.notna(c)]

        for cat in categories:
            count = counts.get(cat, 0)
            protected_count = self._apply_privacy(count)

            results.append(
                {
                    "variable": config.label,
                    "category": str(cat),
                    "type": "categorical",
                    "n": protected_count,
                    "pct": self._calculate_percentage(protected_count, n_total),
                }
            )

        # Add missing count
        missing_count = counts.get(np.nan, 0) + (
            n_total - counts.sum() if pd.isna(counts.index).any() else 0
        )
        if missing_count > 0 or True:  # Always include missing row
            missing_count = series.isna().sum()
            results.append(
                {
                    "variable": config.label,
                    "category": "Missing",
                    "type": "categorical",
                    "n": self._apply_privacy(missing_count),
                    "pct": self._calculate_percentage(
                        self._apply_privacy(missing_count), n_total
                    ),
                }
            )

        return results

    def _summarize_binary(
        self, series: pd.Series, config: VariableConfig
    ) -> List[Dict[str, Any]]:
        """Summarize a binary variable."""
        n_total = len(series)

        # Handle different binary representations
        if series.dtype == bool:
            n_yes = series.sum()
        elif series.dtype in ["int64", "float64"]:
            n_yes = (series == 1).sum()
        else:
            # Try to detect yes/true values
            yes_values = ["yes", "Yes", "YES", "true", "True", "TRUE", "1", 1, True]
            n_yes = series.isin(yes_values).sum()

        n_no = n_total - n_yes - series.isna().sum()
        n_missing = series.isna().sum()

        results = [
            {
                "variable": config.label,
                "category": "Yes",
                "type": "binary",
                "n": self._apply_privacy(n_yes),
                "pct": self._calculate_percentage(self._apply_privacy(n_yes), n_total),
            },
            {
                "variable": config.label,
                "category": "No",
                "type": "binary",
                "n": self._apply_privacy(n_no),
                "pct": self._calculate_percentage(self._apply_privacy(n_no), n_total),
            },
        ]

        if n_missing > 0:
            results.append(
                {
                    "variable": config.label,
                    "category": "Missing",
                    "type": "binary",
                    "n": self._apply_privacy(n_missing),
                    "pct": self._calculate_percentage(
                        self._apply_privacy(n_missing), n_total
                    ),
                }
            )

        return results

    def generate(self) -> pd.DataFrame:
        """Generate the Table 1 summary.

        Returns:
            DataFrame containing the Table 1 summary with privacy protection applied.
        """
        rows = []

        # Add total N row
        total_n = len(self.data)
        rows.append(
            {
                "variable": "Total N",
                "category": "",
                "type": "count",
                "n": self._apply_privacy(total_n),
                "pct": 100.0,
            }
        )

        # Process each variable
        for var_config in self.variables:
            if var_config.name not in self.data.columns:
                continue  # Skip variables not in the data

            series = self.data[var_config.name]

            if var_config.var_type == "continuous":
                row = self._summarize_continuous(series, var_config)
                rows.append(row)
            elif var_config.var_type == "categorical":
                rows.extend(self._summarize_categorical(series, var_config))
            elif var_config.var_type == "binary":
                rows.extend(self._summarize_binary(series, var_config))

        self._results = pd.DataFrame(rows)

        # Store metadata
        self._metadata = {
            "site_name": self.site_name,
            "cohort_description": self.cohort_description,
            "generated_at": datetime.now().isoformat(),
            "privacy_config": {
                "min_cell_size": self.privacy.min_cell_size,
                "round_counts_to": self.privacy.round_counts_to,
                "suppress_percentages": self.privacy.suppress_percentages,
            },
            "total_n_raw": total_n,  # Store raw N for internal use
            "variables_included": [v.name for v in self.variables if v.name in self.data.columns],
            "variables_missing": [v.name for v in self.variables if v.name not in self.data.columns],
        }

        return self._results

    def get_metadata(self) -> Dict[str, Any]:
        """Get metadata about the generated table."""
        if not self._metadata:
            raise ValueError("Must call generate() before getting metadata")
        return self._metadata

    def to_csv(self, path: str, include_metadata: bool = True) -> None:
        """Export Table 1 to CSV.

        Args:
            path: Output file path.
            include_metadata: If True, includes metadata rows at the top.
        """
        if self._results is None:
            self.generate()

        if include_metadata:
            # Write metadata as comments
            with open(path, "w") as f:
                f.write(f"# Site: {self._metadata['site_name']}\n")
                f.write(f"# Generated: {self._metadata['generated_at']}\n")
                f.write(f"# Min Cell Size: {self._metadata['privacy_config']['min_cell_size']}\n")
                if self._metadata["cohort_description"]:
                    f.write(f"# Cohort: {self._metadata['cohort_description']}\n")
                f.write("#\n")
                self._results.to_csv(f, index=False)
        else:
            self._results.to_csv(path, index=False)

    def to_json(self, path: str) -> None:
        """Export Table 1 to JSON with full metadata.

        Args:
            path: Output file path.
        """
        if self._results is None:
            self.generate()

        output = {
            "metadata": self._metadata,
            "table_one": self._results.to_dict(orient="records"),
        }

        with open(path, "w") as f:
            json.dump(output, f, indent=2, default=str)

    def to_dict(self) -> Dict[str, Any]:
        """Return Table 1 as a dictionary.

        Returns:
            Dictionary with 'metadata' and 'table_one' keys.
        """
        if self._results is None:
            self.generate()

        return {
            "metadata": self._metadata,
            "table_one": self._results.to_dict(orient="records"),
        }


def generate_table_one(
    data: pd.DataFrame,
    variables: Optional[List[VariableConfig]] = None,
    min_cell_size: int = 10,
    round_counts_to: Optional[int] = None,
    site_name: Optional[str] = None,
) -> pd.DataFrame:
    """Convenience function to generate Table 1 with default settings.

    Args:
        data: DataFrame containing the cohort data.
        variables: List of VariableConfig objects. If None, uses CLIF defaults.
        min_cell_size: Minimum cell size for privacy (default: 10).
        round_counts_to: Round counts to nearest value (optional).
        site_name: Name of the site.

    Returns:
        DataFrame containing the Table 1 summary.

    Example:
        >>> table1 = generate_table_one(cohort_df, site_name="UCMC")
        >>> table1.to_csv("table_one_UCMC.csv")
    """
    privacy = PrivacyConfig(min_cell_size=min_cell_size, round_counts_to=round_counts_to)
    generator = TableOneGenerator(data, variables=variables, privacy_config=privacy, site_name=site_name)
    return generator.generate()

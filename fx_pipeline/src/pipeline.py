"""Orchestrates extract → transform → quality → gold-style output."""

from src.extract import fetch_fx_rates
from src.transform import json_to_dataframe
from src.quality import validate_not_empty, validate_required_columns
from src.utils import load_config, ensure_directory


def run_pipeline(config_path: str = "config.yaml") -> None:
    """
    Run the full FX pipeline: load config, extract, transform, validate, write layers.

    Args:
        config_path: Location of the YAML configuration file.
    """
    pass

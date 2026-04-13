"""Small helpers shared across the pipeline."""

import yaml


def load_config(path: str) -> dict:
    """
    Load pipeline settings from a YAML file.

    Args:
        path: Path to config.yaml.

    Returns:
        Configuration as a dictionary.
    """
    pass


def ensure_directory(path: str) -> None:
    """
    Create a directory if it does not already exist.

    Args:
        path: Folder path (e.g. under data/bronze).
    """
    pass

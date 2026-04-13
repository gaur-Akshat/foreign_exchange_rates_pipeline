"""Silver layer: turn raw JSON into structured tabular data."""

import pandas as pd


def json_to_dataframe(raw_data: dict) -> pd.DataFrame:
    """
    Convert raw JSON from the extract step into a pandas DataFrame.

    Args:
        raw_data: Dictionary returned by the API / extract step.

    Returns:
        DataFrame with columns suitable for downstream quality checks and gold output.
    """
    pass

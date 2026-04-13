import pandas as pd


def validate_not_empty(df: pd.DataFrame) -> bool:
    """
    Args:
        df: Data to validate.

    Returns:
        True if the frame is non-empty, False otherwise.
    """
    pass


def validate_required_columns(df: pd.DataFrame, columns: list) -> bool:
    """
    Args:
        df: Data to validate.
        columns: List of required column names.

    Returns:
        True if all columns exist, False otherwise.
    """
    pass

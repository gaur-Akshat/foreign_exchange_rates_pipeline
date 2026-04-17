import pandas as pd
import json
from pathlib import Path


def validate_bronze_file(file_path):
    with open(file_path, "r") as f:
        data = json.load(f)

    if not data:
        raise ValueError(f"{file_path} is empty")

    if "rates" not in str(data):
        raise ValueError(f"{file_path} missing rates data")

def get_latest_bronze_date(bronze_path):
    files = sorted(Path(bronze_path).glob("*.json"))
    if not files:
        return None
    latest_date = None

    for file in files:
        with open(file, "r") as f:
            data = json.load(f)
        if "date" in data:
            current_date = pd.to_datetime(data["date"]).date()

            if latest_date is None or current_date > latest_date:
                latest_date = current_date

    return latest_date

def validate_silver_data(df):
    if df.empty:
        raise ValueError("Silver data is empty")

    required = [
        "rate_date",
        "base_currency",
        "target_currency",
        "exchange_rate",
    ]

    for col in required:
        if col not in df.columns:
            raise ValueError(f"Missing column: {col}")

    if df[required].isnull().any().any():
        raise ValueError("Null values found in silver data")

    if (df["exchange_rate"] <= 0).any():
        raise ValueError("Invalid exchange rates found")

    if df.duplicated(
        subset=["rate_date", "base_currency", "target_currency"]
    ).any():
        raise ValueError("Duplicate records found")
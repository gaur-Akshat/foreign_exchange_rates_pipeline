import json
import pandas as p
from pathlib import Path

def validate_bronze_file(file_path):
    data = json.loads(Path(file_path).read_text())
    if not data:
        raise ValueError(f"{file_path} is empty")
    if "rates" not in str(data):
        raise ValueError(f"{file_path} missing rates data")


def get_latest_bronze_date(bronze_path):
    files = sorted(Path(bronze_path).glob("*.json"))
    latest = None
    for file in files:
        data = json.loads(file.read_text())
        if "date" in data:
            d = p.to_datetime(data["date"]).date()
            if latest is None or d > latest:
                latest = d
    return latest


def validate_silver_data(df):
    required = ["rate_date", "base_currency", "target_currency", "exchange_rate"]
    if df.empty:
        raise ValueError("Silver data is empty")
    for col in required:
        if col not in df.columns:
            raise ValueError(f"Missing column: {col}")
    if df[required].isnull().any().any():
        raise ValueError("Null values found in silver data")
    if (df["exchange_rate"] <= 0).any():
        raise ValueError("Invalid exchange rates found")
    if df.duplicated(subset=["rate_date", "base_currency", "target_currency"]).any():
        raise ValueError("Duplicate records found")
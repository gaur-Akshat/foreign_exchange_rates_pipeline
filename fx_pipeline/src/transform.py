import json
import logging
from pathlib import Path
import pandas as pd

from src.quality import validate_bronze_file, validate_silver_data

logger = logging.getLogger(__name__)

def read_bronze_files(config):
    bronze_path = (Path(config.get("_project_root", ".")) / config["paths"]["bronze"]).resolve()
    return sorted(bronze_path.glob("*.json"))

def transform_record(bronze_json):
    rows = []

    metadata = bronze_json.get("metadata", {})
    payload = bronze_json["data"]

    base_currency = payload["base"]
    rate_date = payload["date"]
    rates = payload["rates"]

    for currency, rate in rates.items():
        rows.append({
            "rate_date": rate_date,
            "base_currency": base_currency,
            "target_currency": currency,
            "exchange_rate": rate,
            "ingestion_time": metadata.get("ingestion_time"),
            "load_batch_id": metadata.get("load_batch_id") or metadata.get("batch_id"),
            "endpoint_name": metadata.get("endpoint"),
            "response_status": metadata.get("status"),
        })

    return rows

def transform_data(config):
    """Transform all Bronze files into Silver tables."""

    logger.info("Reading Bronze files")
    files = read_bronze_files(config)
    logger.info("Found %s Bronze files", len(files))

    if not files:
        raise ValueError("No Bronze files found. Run extraction before transformation.")

    all_rows = []
    for file_path in files:
        if config.get("pipeline", {}).get("enable_bronze_validation", True):
            validate_bronze_file(str(file_path))

        with Path(file_path).open("r", encoding="utf-8") as handle:
            bronze_json = json.load(handle)

        all_rows.extend(transform_record(bronze_json))

    df = pd.DataFrame(all_rows)
    df["rate_date"] = pd.to_datetime(df["rate_date"]).dt.normalize()
    df["ingestion_time"] = pd.to_datetime(df["ingestion_time"])
    df["exchange_rate"] = pd.to_numeric(df["exchange_rate"])
    df = df.sort_values(
        by=["rate_date", "base_currency", "target_currency", "ingestion_time"]
    )

    df = df.drop_duplicates(
        subset=["rate_date", "base_currency", "target_currency"], keep="last"
    ).reset_index(drop=True)

    if config.get("pipeline", {}).get("enable_silver_validation", True):
        validate_silver_data(df)

    return df

def save_silver(df, config):
    logger.info("Saving %s records to Silver layer", len(df))
    silver_path = (Path(config.get("_project_root", ".")) / config["paths"]["silver"]).resolve()
    silver_path.mkdir(parents=True, exist_ok=True)
    output_file = silver_path / "exchange_rates.parquet"
    df.to_parquet(output_file, index=False)
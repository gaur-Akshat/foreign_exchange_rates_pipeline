import json
import logging
from pathlib import Path
import pandas as p

from src.quality import validate_bronze_file, validate_silver_data
logger = logging.getLogger(__name__)

def _root(config):
    return Path(config.get("_project_root", "."))

def read_bronze_files(config):
    return sorted((_root(config) / config["paths"]["bronze"]).resolve().glob("*.json"))

def transform_record(bronze_json):
    metadata, payload = bronze_json["metadata"], bronze_json["data"]
    return [
        {
            "rate_date": payload["date"],
            "base_currency": payload["base"],
            "target_currency": currency,
            "exchange_rate": rate,
            "ingestion_time": metadata["ingestion_time"],
            "batch_id": metadata.get("load_batch_id") or metadata.get("batch_id"),
            "endpoint_name": metadata["endpoint"],
            "response_status": metadata["status"],
        }
        for currency, rate in payload["rates"].items()
    ]

def transform_data(config):
    """Transform all Bronze files into Silver tables."""
    files = read_bronze_files(config)
    logger.info("Found %s Bronze files", len(files))
    if not files:
        raise ValueError("No Bronze files found. Run extraction before transformation.")
    validate = config.get("pipeline", {}).get("enable_bronze_validation", True)
    all_rows = []
    for fp in files:
        if validate:
            validate_bronze_file(str(fp))
        all_rows.extend(transform_record(json.loads(fp.read_text(encoding="utf-8"))))

    df = p.DataFrame(all_rows)
    save_bronze_sql(df, config)

    df["rate_date"] = p.to_datetime(df["rate_date"]).dt.normalize()
    df["ingestion_time"] = p.to_datetime(df["ingestion_time"])
    df["exchange_rate"] = p.to_numeric(df["exchange_rate"])
    df = (
        df.sort_values(["rate_date", "base_currency", "target_currency", "ingestion_time"])
        .drop_duplicates(subset=["rate_date", "base_currency", "target_currency"], keep="last")
        .reset_index(drop=True)
    )
    if config.get("pipeline", {}).get("enable_silver_validation", True): validate_silver_data(df)
    return df

def save_silver(df, config):
    logger.info("Saving %s records to Silver layer", len(df))
    silver_path = (_root(config) / config["paths"]["silver"]).resolve()
    silver_path.mkdir(parents=True, exist_ok=True)
    df.to_parquet(silver_path / "exchange_rates.parquet", index=False)

def save_bronze_sql(df, config):
    logger.info("Saving Bronze SQL layer")
    bronze_sql_path = (_root(config) / config["paths"].get("bronze_sql", "data/bronze/bronze_sql")).resolve()
    bronze_sql_path.mkdir(parents=True, exist_ok=True)
    df.to_parquet(bronze_sql_path / "bronze_exchange_rates.parquet", index=False)
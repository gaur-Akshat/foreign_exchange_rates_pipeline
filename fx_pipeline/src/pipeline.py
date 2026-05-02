import logging
from datetime import datetime, timedelta
import pandas as p

from src.extract import extract_data
from src.transform import transform_data, save_silver
from src.gold import run_gold_layer
from src.logger import setup_logging

logger = logging.getLogger(__name__)

def apply_retention(df, days):
    if not days:
        return df
    cutoff = p.Timestamp.today().normalize() - p.Timedelta(days=days - 1)
    return df[df["rate_date"] >= cutoff].reset_index(drop=True)


def run_pipeline(config):
    setup_logging(config)
    logger.info("Pipeline started")
    try:
        today = datetime.today().date()
        backfill_days = config["pipeline"]["backfill_days"]
        retention_days = config["pipeline"]["retention_days"]
        run_mode = config.get("pipeline", {}).get("run_mode", "auto")

        if run_mode == "reprocess":
            logger.info("Reprocess mode enabled. Skipping API extraction.")
        else:
            start_date = today - timedelta(days=backfill_days - 1)
            logger.info("Extracting data from %s to %s", start_date, today)
            extract_data(config, start_date, today)

        df = transform_data(config)
        df = apply_retention(df, retention_days)
        save_silver(df, config)

        logger.info("Processing Gold layer")
        gold_data = run_gold_layer(df, config)

        if config.get("database", {}).get("enabled", False):
            from src.db import create_database_if_not_exists
            from src.load import load_bronze_to_sql, load_silver_to_sql, load_gold_to_sql
            try:
                logger.info("Ensuring database exists")
                create_database_if_not_exists()
                logger.info("Loading Bronze to SQL Server")
                load_bronze_to_sql(config)
                logger.info("Loading Silver to SQL Server")
                load_silver_to_sql(df, config)
                logger.info("Loading Gold to SQL Server")
                load_gold_to_sql(gold_data, config)
            except Exception as exc:
                logger.warning("Could not connect to SQL server. Skipping database load. Reason: %s", exc)

        logger.info("Pipeline completed successfully")
    except Exception as e:
        logger.error("Pipeline failed: %s", e, exc_info=True)
        raise
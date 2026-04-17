import logging
from datetime import datetime, timedelta

from src.extract import extract_data
from src.transform import transform_data, save_silver
from src.gold import run_gold_layer
from src.logger import setup_logging

logger = logging.getLogger(__name__)

def apply_retention(df, days):
    import pandas as pd

    if not days:
        return df

    cutoff = pd.Timestamp.today().normalize() - pd.Timedelta(days=days - 1)
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
            # extract
            start_date = today - timedelta(days=backfill_days - 1)
            logger.info("Extracting data from %s to %s", start_date, today)
            extract_data(config, start_date, today)

        # transform
        logger.info("Transforming data")
        df = transform_data(config)

        # retention
        logger.info("Applying retention policy")
        df = apply_retention(df, retention_days)

        # silver
        save_silver(df, config)

        # gold
        logger.info("Processing Gold layer")
        gold_data = run_gold_layer(df, config)

        # sql load
        if config.get("database", {}).get("enabled", False):
            from src.db import create_database_if_not_exists
            from src.load import load_to_sql, load_gold_to_sql
            
            try:
                logger.info("Ensuring database exists")
                create_database_if_not_exists()
                logger.info("Loading data to SQL Server")
                load_to_sql(df, config)
                load_gold_to_sql(gold_data, config)
            except Exception as exc:
                logger.warning(f"Could not connect to SQL server. Skipping database load. Reason: {exc}")
        logger.info("Pipeline completed successfully")

    except Exception as e:
        logger.error(f"Pipeline failed: {e}", exc_info=True)
        raise
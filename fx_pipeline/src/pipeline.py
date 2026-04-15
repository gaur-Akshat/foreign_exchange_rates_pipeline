import logging
import pandas as pd
from datetime import datetime, timedelta
import os

from src.extract import extract_data
from src.transform import transform_data, save_silver

from src.logger import setup_logging
logger = logging.getLogger(__name__)

def apply_retention_policy(config, days=30):
    s_path = config['data']['silver_path']
    file_path = os.path.join(s_path, "exchange_rates.parquet")

    if not os.path.exists(file_path):
        return

    df = pd.read_parquet(file_path)

    df['rate_date'] = pd.to_datetime(df['rate_date'])
    cutoff = datetime.today() - timedelta(days=days)
    df_new = df[df['rate_date'] >= cutoff]
    df_new.to_parquet(file_path, index=False)

def get_latest_bronze_date(bronze_path):
    files = [f for f in os.listdir(bronze_path) if f.endswith(".json")]
    if not files:
        return None
    dates = [
        f.replace("exchange_rates_", "").replace(".json", "")
        for f in files
    ]
    return max(dates)

def run_pipeline(config):
    setup_logging()
    logger.info("Pipeline started")
    try:
        bronze_path = config['data']['bronze_path']
        latest_date = get_latest_bronze_date(bronze_path)
        today = datetime.today().date()

        if latest_date is None:
            logger.info("First run → backfill last 30 days")
            start_date = today - timedelta(days=30)
            extract_data(config, start_date, today)
        else:
            logger.info("Incremental run")
            start_date = datetime.strptime(latest_date, "%Y-%m-%d").date() + timedelta(days=1)

            if start_date <= today:
                logger.info(f"Fetching data from {start_date} to {today}")
                extract_data(config, start_date, today)
            else:
                logger.info("No new data to fetch")
        logger.info("Starting transformation")
        df = transform_data(config)
        save_silver(df, config)
        logger.info("Transformation completed")
        logger.info("Applying retention policy")
        apply_retention_policy(config)

        logger.info("Pipeline completed successfully")
        
    except Exception as e:
        logger.error(f"Pipeline failed: {str(e)}", exc_info=True)
        raise
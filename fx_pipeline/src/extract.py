import requests
import json

from pathlib import Path
from datetime import datetime

import logging
import time
logger = logging.getLogger(__name__)

def generate_batch_id():
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    return f"fx_{timestamp}"

def fetch_latest(config):
    logger.info("Fetching latest exchange rates")
    url = config['api']['url']
    base = config['base_currency']
    target = ",".join(config['target_currencies'])

    response = requests.get(f"{url}?from={base}&symbols={target}", timeout=config['api']['timeout'])
    response.raise_for_status()
    return response

def fetch_historical(config, start_date, end_date):
    logger.info("Fetching historical data from %s to %s", start_date, end_date)
    base_url = config['api']['url']
    base = config['base_currency']
    target = ",".join(config['target_currencies'])
    url = f"{base_url}/{start_date}..{end_date}?from={base}&symbols={target}"
    retries = 3
    for attempt in range(retries):
        try:
            response = requests.get(url, timeout=config['api']['timeout'])
            response.raise_for_status()
            return response
        except Exception as e:
            logger.warning(f"Attempt {attempt+1} failed: {e}")
            time.sleep(2)

    raise Exception("Failed to fetch historical data after retries")

def save_bronze(data, metadata, config, date_str):
    bronze_path = (Path(config.get("_project_root", ".")) / config["paths"]["bronze"]).resolve()
    bronze_path.mkdir(parents=True, exist_ok=True)

    with open(bronze_path / f"exchange_rates_{date_str}.json", "w") as f:
        json.dump({"metadata": metadata, "data": data}, f, indent=2)


def extract_data(config, start_date=None, end_date=None):
    logger.info("Starting extraction process")
    batch_id = generate_batch_id()
    ingestion_time = datetime.now().isoformat()

    if start_date and end_date:
        response = fetch_historical(config, start_date, end_date)
        data = response.json()
        for date, rates in data["rates"].items():
            daily_data = {
                "base": data["base"],
                "date": date,
                "rates": rates
            }
            metadata = {
                "batch_id": batch_id,
                "ingestion_time": ingestion_time,
                "endpoint": "historical",
                "status": response.status_code
            }
            save_bronze(daily_data, metadata, config, date)

    else:
        response = fetch_latest(config)
        data = response.json()
        date = data["date"]
        daily_data = {
            "base": data["base"],
            "date": date,
            "rates": data["rates"]
        }
        metadata = {
            "batch_id": batch_id,
            "ingestion_time": ingestion_time,
            "endpoint": "latest",
            "status": response.status_code
        }
        save_bronze(daily_data, metadata, config, date)
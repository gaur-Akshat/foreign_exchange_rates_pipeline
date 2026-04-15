import requests
import json
import os
from datetime import datetime
import logging

logger = logging.getLogger(__name__)


def generate_batch_id():
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    return f"fx_{timestamp}"


def fetch_latest(config):
    logger.info("Fetching latest exchange rates")
    url = config['api']['url']
    base = config['base_currency']
    target = ",".join(config['target_currencies'])

    timeout = config['api'].get('timeout', 10)
    response = requests.get(f"{url}?from={base}&symbols={target}", timeout=timeout)
    return response


def fetch_historical(config, start_date, end_date):
    logger.info(f"Fetching historical data from {start_date} to {end_date}")
    base = config['base_currency']
    target = ",".join(config['target_currencies'])

    url = f"https://api.frankfurter.app/{start_date}..{end_date}?from={base}&symbols={target}"

    timeout = config['api'].get('timeout', 10)
    response = requests.get(url, timeout=timeout)
    return response


def save_bronze(data, metadata, config, date_str):
    logger.info(f"Saving Bronze data for {date_str}")
    bronze_path = config["data"]["bronze_path"]

    os.makedirs(bronze_path, exist_ok=True)

    filename = f"{bronze_path}/exchange_rates_{date_str}.json"

    bronze_record = {
        "metadata": metadata,
        "data": data,
    }

    with open(filename, "w") as f:
        json.dump(bronze_record, f, indent=2)


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
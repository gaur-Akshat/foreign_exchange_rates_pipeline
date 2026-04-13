import requests
import json
from datetime import datetime

def generate_batch_id():
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S") # timestamp format YYYYMMDD_HHMMSS
    return f"fx_{timestamp}"

def fetch_latest(config):
    url = config['api']['url']
    base = config['base_currency']
    target = ",".join(config['target_currencies'])
    response = requests.get(f"{url}?from={base}&symbols={target}")

    return response

def fetch_historical(config, start_date, end_date):
    base = config['base_currency']
    target = ",".join(config['target_currencies'])
    url = config['api']['url']
    url = f"{url}?from={base}&symbols={target}&start={start_date}&end={end_date}"
    response = requests.get(url)
    return response

def save_bronze(data, metadata, config, date_str):
    bronze_path = config["data"]["bronze_path"]
    filename = f"{bronze_path}/exchange_rates_{date_str}.json"
    bronze_record = {
        "metadata": metadata,
        "data": data,
    }

    with open(filename, "w") as f:
        json.dump(bronze_record, f, indent=2)

def extract_data(config, start_date=None, end_date=None):
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
        metadata = {
            "batch_id": batch_id,
            "ingestion_time": ingestion_time,
            "endpoint": "latest",
            "status": response.status_code
        }

        save_bronze(data, metadata, config, date)
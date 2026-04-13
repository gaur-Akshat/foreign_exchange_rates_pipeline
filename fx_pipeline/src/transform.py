import os
import json
import pandas as pd

def read_bronze_files(config):
    bronze_path = config['data']['bronze_path']
    files = [
        os.path.join(bronze_path, f)
        for f in os.listdir(bronze_path) if f.endswith('.json')
    ]

    return files

def transform_record(bronze_json):
    '''convert json into table'''

    rows = []
    metadata = bronze_json['metadata']
    data = bronze_json['data']
    base = data['base']
    date = data['date']
    rates = data['rates']
    ingestion_time = metadata['ingestion_time']
    batch_id = metadata['batch_id']


    for currency, rate in rates.items():
        rows.append({
            "rate_date": date,
            "base_currency": base,
            "target_currency": currency,
            "exchange_rate": rate,
            "ingestion_time": ingestion_time,
            "batch_id": batch_id
        })

    return rows


def transform_data(config):
    '''transform all bronze files into silver tables'''
    files = read_bronze_files(config)
    all_rows = []
    for file in files:
        with open(file, 'r') as f:
            bronze_json = json.load(f)
        
        rows = transform_record(bronze_json)
        all_rows.extend(rows)
    df = pd.DataFrame(all_rows)
    df["rate_date"] = pd.to_datetime(df["rate_date"])
    df["ingestion_time"] = pd.to_datetime(df["ingestion_time"])
    return df

def save_silver(df, config):
    silver_path = config["data"]["silver_path"]
    output_file = os.path.join(silver_path, "exchange_rates.parquet")
    df.to_parquet(output_file, index=False)

    
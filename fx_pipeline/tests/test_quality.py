import pandas as pd
from src.quality import normalize_bronze_snapshot_records, validate_silver_dataframe

def test_normalize_bronze_snapshot_records_supports_range_payload():
    bronze_json = {
        "metadata": {
            "load_batch_id": "fx_20260416_000001",
            "ingestion_time": "2026-04-16T10:00:00",
            "endpoint_name": "historical_range",
            "response_status": 200,
        },
        "raw_response": {
            "base": "USD",
            "start_date": "2026-04-01",
            "end_date": "2026-04-02",
            "rates": {
                "2026-04-01": {"EUR": 0.92, "INR": 84.1},
                "2026-04-02": {"EUR": 0.93, "INR": 84.4},
            },
        },
    }

    records = normalize_bronze_snapshot_records(bronze_json)

    assert len(records) == 2
    assert records[0]["base_currency"] == "USD"
    assert records[0]["metadata"]["load_batch_id"] == "fx_20260416_000001"

def test_validate_silver_dataframe_accepts_unique_positive_rows():
    df = pd.DataFrame(
        [
            {
                "rate_date": pd.Timestamp("2026-04-01"),
                "base_currency": "USD",
                "target_currency": "EUR",
                "exchange_rate": 0.92,
                "ingestion_time": pd.Timestamp("2026-04-16T10:00:00"),
                "load_batch_id": "fx_20260416_000001",
            },
            {
                "rate_date": pd.Timestamp("2026-04-02"),
                "base_currency": "USD",
                "target_currency": "EUR",
                "exchange_rate": 0.93,
                "ingestion_time": pd.Timestamp("2026-04-16T10:00:00"),
                "load_batch_id": "fx_20260416_000001",
            },
        ]
    )
    validate_silver_dataframe(df, expected_latest_rate_date="2026-04-02")
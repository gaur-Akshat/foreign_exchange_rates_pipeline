# Foreign Exchange (FX) Data Pipeline

This is a local Data Engineering project that implements a robust, automated data pipeline for extracting, transforming, and loading Foreign Exchange (FX) rates. The project adopts the **Medallion Architecture** (Bronze → Silver → Gold) to ensure data quality, auditability, and analytical readiness.


- **Automated Incremental Loading**: Intelligently interrogates local storage to detect the latest ingestion date. Triggers a 30-day historical backfill on first run, and delta incremental loads on subsequent runs.
- **Medallion Architecture**: Strict separation of data tiers ensuring raw data is never lost and downstream transformations are reproducible.
- **Metadata Tagging**: All raw API payloads are enriched with ingestion timestamps, HTTP status codes, and generated batch IDs for end-to-end lineage tracking.
- **Data Normalization (JSON to Tabular)**: Utilizes `pandas` to unnest dynamic JSON dictionaries into strict, typed relational structures.
- **Optimized Storage**: Saves Silver layer data as partitioned columnar **Parquet** files, drastically improving disk space and query speeds.
- **Data Lifecycle Management**: Features an automated retention policy that gracefully prunes Silver layer records older than the designated 30-day window.
- **Resiliency & Logging**: Comprehensive logging via the `src.logger` module ensures all extraction, transformation, and retention events are captured for easy debugging.


1. **Bronze Layer (`data/bronze/`)**: Acts as an immutable land-zone. Raw JSON from the [Frankfurter API](https://www.frankfurter.app/) is stored strictly as it is received, appended with metadata.
2. **Silver Layer (`data/silver/`)**: The raw JSON is processed and flattened. Currencies are unpivoted into columns: `rate_date`, `base_currency`, `target_currency`, and `exchange_rate`. Data types are enforced, and the result is saved as a Parquet dataset.
3. **Gold Layer (`data/gold/`)**: *(Planned Phase)* Will host curated views, moving-averages, and BI-ready aggregates based off the Silver tables.


```text
fx_pipeline/
│
├── config.yaml          # Configuration settings (API URL, base/target currencies)
├── main.py              # Main entry point to initialize the config and trigger pipeline
├── requirements.txt     # Python dependencies (requests, pandas, pyarrow, pyyaml)
├── README.md            # Project documentation
│
├── data/                # Data storage directories
│   ├── bronze/          # Immutable raw JSON files
│   ├── silver/          # Cleaned, typed Parquet files
│   └── gold/            # Final analytics-ready outputs (future)
│
├── logs/                # Local runtime logs
│
└── src/                 # Core pipeline modules
    ├── logger.py        # Logging configuration
    ├── extract.py       # API connection, pagination, & backfill logic
    ├── transform.py     # JSON unnesting, pandas manipulation, Parquet writing
    └── pipeline.py      # Orchestrator combining extract, transform, & retention
```


- Python 3.8+
- Virtual environment (recommended)


1. Clone or download the repository.
2. Navigate to the project root directory (`fx_pipeline`).
3. Set up a virtual environment and install the required dependencies:

```bash
python -m venv .venv
source .venv/bin/activate  # On Windows use: .venv\Scripts\activate
pip install -r requirements.txt
```

Ensure your `config.yaml` is set up correctly. This file dictates your pipeline's target, for example:
```yaml
api:
  url: "https://api.frankfurter.app/latest"
  timeout: 10
base_currency: "USD"
target_currencies: 
  - "EUR"
  - "GBP"
  - "JPY"
data:
  bronze_path: "data/bronze"
  silver_path: "data/silver"
```

Run the pipeline using `main.py`:

```bash
python main.py
```

The pipeline will determine if it requires a backfill or an incremental fetch, process the data, perform retention cleanup, and log the entire process to your terminal and the `logs/` directory.

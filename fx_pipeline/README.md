# Foreign Exchange Rate Analytics Pipeline

A fully automated, local **Bronze → Silver → Gold** data engineering pipeline that collects, validates, transforms and publishes foreign exchange rate analytics using the [Frankfurter API](https://www.frankfurter.app). Built as a first-year Data Engineering project.

---

## Problem Statement

A finance analytics team needs a lightweight local system to track exchange rate movements without manually checking websites. The pipeline must:
- Preserve raw API responses as an immutable audit trail
- Validate data quality at every stage
- Produce clean, analytics-ready outputs for reporting and trend analysis
- Load results into a relational database for querying

---

## Architecture

```
Frankfurter API
      │
      ▼
  [ EXTRACT ]  →  Bronze Layer  (raw JSON files per day)
      │
      ▼
  [ TRANSFORM ] →  Silver Layer  (validated, flat Parquet)
      │
      ▼
  [ GOLD ]      →  Gold Layer    (5 analytics Parquet marts)
      │
      ▼
  [ LOAD ]      →  MySQL Database (Silver + Gold tables)
      │
      ▼
  [ LOGS ]      →  data/logs/pipeline_YYYY-MM-DD.log
```

**Bronze** — Raw API payloads stored as JSON with full ingestion metadata in `data/bronze/exchange_rates/`.  
**Silver** — Validated, deduplicated, flattened currency records stored as Parquet in `data/silver/exchange_rates/`.  
**Gold** — Analytics-ready marts stored as Parquet in `data/gold/marts/`.

---

## Project Structure

```text
fx_pipeline/
├── config.yaml               ← pipeline configuration
├── main.py                   ← entry point
├── scheduler.bat             ← Windows Task Scheduler automation
├── explanation.md            ← full code walkthrough
├── requirements.txt
├── data/
│   ├── bronze/
│   │   └── exchange_rates/   ← raw JSON files (one per API call)
│   ├── silver/
│   │   └── exchange_rates/   ← exchange_rates.parquet
│   ├── gold/
│   │   └── marts/            ← 5 analytics Parquet files
│   └── logs/                 ← pipeline_YYYY-MM-DD.log
├── notebooks/
│   └── gold_reporting.ipynb  ← interactive reporting dashboard
├── src/
│   ├── extract.py            ← API ingestion → Bronze
│   ├── transform.py          ← Bronze → Silver
│   ├── quality.py            ← Bronze + Silver validation
│   ├── gold.py               ← Silver → Gold marts
│   ├── load.py               ← Gold + Silver → MySQL
│   ├── db.py                 ← MySQL engine + DB initialisation
│   ├── pipeline.py           ← orchestrator
│   └── logger.py             ← logging setup
└── tests/
    ├── test_gold.py
    └── test_quality.py
```

---

## Gold Outputs

| File | Description |
|---|---|
| `gold_daily_currency_rates`       | All exchange rates sorted by date and currency |
| `gold_currency_movement`          | Day-over-day absolute and percentage rate changes |
| `gold_weekly_currency_summary`    | Weekly avg, high, low, and % change per currency |
| `gold_strength_rankings`          | Currencies ranked by weekly performance |
| `gold_conversion_reference`       | Conversion lookup table for 1, 10, 100, 1000 base units |

---

## Data Quality Checks

Validation runs at two stages automatically:

**Bronze** (before transformation):
- File is not empty
- The word `"rates"` is present in the file content

**Silver** (after transformation):
- DataFrame is not empty
- Required columns exist: `rate_date`, `base_currency`, `target_currency`, `exchange_rate`
- No null values in required columns
- All exchange rates are positive (> 0)
- No duplicate `rate_date + base_currency + target_currency` combinations

---

## Setup

### 1. Create and activate virtual environment

```bash
python -m venv .venv
.venv\Scripts\activate
```

### 2. Install dependencies

```bash
pip install -r requirements.txt
```

### 3. Configure the pipeline

Edit `config.yaml` to set your base currency, target currencies, storage paths, backfill period, and database settings:

```yaml
base_currency: USD

target_currencies:
  - EUR
  - GBP
  - JPY
  - INR
  - CAD
  - AUD

api:
  url: "https://api.frankfurter.app"
  timeout: 10

paths:
  bronze: "data/bronze/exchange_rates"
  silver: "data/silver/exchange_rates"
  gold: "data/gold/marts"
  logs: "data/logs"

pipeline:
  backfill_days: 30
  retention_days: 30

database:
  enabled: false          # set to true to load into MySQL
  silver_table: "silver_exchange_rates"

logging:
  level: "INFO"
```

### 4. Configure database credentials (optional)

Create a `.env` file in the project root (next to `main.py`). This file is gitignored and keeps credentials out of version control:

```
DB_USERNAME=root
DB_PASSWORD=yourpassword
DB_SERVER=localhost
DB_NAME=fx_rates
```

---

## Running the Pipeline

### Standard run (auto mode)

Fetches the last 30 days from the API, transforms, builds Gold marts, and optionally loads to MySQL:

```bash
python main.py
```

### Reprocess mode

Re-runs transformation and Gold from existing Bronze files — no API call made. Useful for debugging or re-running after a code change:

1. Add `run_mode: "reprocess"` under `pipeline:` in `config.yaml`
2. Run `python main.py`

---

## MySQL Database Loading

SQL loading is **disabled by default**. To enable:

1. Set `database.enabled: true` in `config.yaml`
2. Fill in your credentials in `.env`
3. Ensure MySQL Server is running on your machine
4. Run `python main.py`

The pipeline will automatically:
- Create the database (`fx_rates`) if it doesn't exist
- Append new rows to `silver_exchange_rates`
- Fully replace each Gold table on every run

> If MySQL is unavailable, the pipeline logs a warning and completes successfully without crashing.

---

## Scheduling (Windows)

A `scheduler.bat` script is included to automate daily runs via **Windows Task Scheduler**:

1. Open **Windows Task Scheduler**
2. Create a **Basic Task** → set trigger to **Daily**
3. Action: **Start a program** → browse and select `scheduler.bat`
4. Set **"Start in"** to the absolute path of the `fx_pipeline/` directory

The script activates the virtual environment, runs the pipeline, and appends all output to `data/logs/scheduler.log`.

---

## Reporting

Open `notebooks/gold_reporting.ipynb` in Jupyter to:
- Browse all Gold mart DataFrames interactively
- View weekly performance rankings
- Plot exchange rate movements over time as a line chart

---

## API Source

Exchange rate data is sourced from the **Frankfurter API** — a free, open-source API backed by the European Central Bank:

- Base URL: `https://www.frankfurter.app`
- Historical endpoint: `/{start_date}..{end_date}?from=USD&symbols=EUR,...`
- No API key required

---

## Key Design Decisions

| Decision | Reason |
|---|---|
| Credentials in `.env`, not `config.yaml` | Security — secrets never enter version control |
| Bronze stored as raw JSON per day | Full audit trail; re-transformable without API calls |
| Parquet format for Silver and Gold | Columnar, fast reads, small file size vs CSV |
| Retry logic on API calls (3 attempts) | Networks are unreliable; prevents false failures |
| SQL load wrapped in `try/except` | Missing DB server never kills the pipeline |
| `keep="last"` deduplication in Silver | Most recently ingested record wins on overlap |
| `CREATE DATABASE IF NOT EXISTS` | No manual DB setup needed on first run |

---

> For a full line-by-line code walkthrough, see [`explanation.md`](./explanation.md).
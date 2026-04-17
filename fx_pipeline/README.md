# Foreign Exchange Rate Analytics Pipeline

This project implements a local Bronze -> Silver -> Gold data pipeline for exchange-rate analytics using the Frankfurter API. It preserves raw API responses in Bronze, validates and standardizes row-level currency data in Silver, and publishes Gold marts for daily reporting, movement analysis, weekly summaries, strength rankings, and conversion references.

## Problem Statement

A finance analytics team needs a lightweight local system to monitor exchange rates without manually checking websites. The pipeline must preserve raw API responses, create a trusted historical dataset, and generate reusable analytics outputs for reporting and trend analysis.

## Architecture

- Bronze: raw API payloads are stored with ingestion metadata in `data/bronze/exchange_rates/`.
- Silver: validated, flattened currency records are stored as Parquet in `data/silver/exchange_rates/`.
- Gold: analytics-ready marts are stored as Parquet in `data/gold/marts/`.

## Project Structure

```text
fx_pipeline/
|-- config/
|   |-- config.yaml
|-- data/
|   |-- bronze/
|   |   `-- exchange_rates/
|   |-- silver/
|   |   `-- exchange_rates/
|   |-- gold/
|   |   `-- marts/
|   `-- logs/
|-- notebooks/
|   `-- gold_reporting.ipynb
|-- src/
|   |-- db.py
|   |-- extract.py
|   |-- gold.py
|   |-- load.py
|   |-- logger.py
|   |-- pipeline.py
|   |-- quality.py
|   `-- transform.py
|-- tests/
|   |-- test_gold.py
|   `-- test_quality.py
|-- main.py
|-- scheduler.bat
`-- requirements.txt
```

## Data Model

Silver and Gold datasets are built around these fields:

- `rate_date`
- `base_currency`
- `target_currency`
- `exchange_rate`
- `ingestion_time`
- `load_batch_id`

## Gold Outputs

The pipeline writes the following Gold marts:

- `gold_daily_currency_rates.parquet`
- `gold_currency_movement.parquet`
- `gold_weekly_currency_summary.parquet`
- `gold_strength_rankings.parquet`
- `gold_conversion_reference.parquet`

## Quality Checks

The pipeline validates:

- Bronze schema presence for metadata, base, dates, and rates
- Silver null checks on required business columns
- Silver positive-rate checks
- Silver uniqueness on `rate_date + base_currency + target_currency`
- Silver freshness against the latest extracted Bronze snapshot

## Setup

```bash
python -m venv .venv
.venv\Scripts\activate
pip install -r requirements.txt
```

Update `config/config.yaml` to choose your base currency, target currencies, storage paths, and pipeline settings.

## Running The Pipeline

Default run:

```bash
python main.py
```

Reprocess Bronze to Silver to Gold without calling the API:

1. Set `pipeline.run_mode` to `reprocess` in `config/config.yaml`.
2. Run `python main.py`.

## Optional SQL Loading

SQL loading is disabled by default. If you want to publish Silver and Gold tables to SQL Server, set `database.enabled: true` and provide the required environment variables:

- `DB_USERNAME`
- `DB_PASSWORD`
- `DB_SERVER`
- `DB_NAME`

## Scheduling

A minimal `scheduler.bat` script is included in the project root to facilitate daily automation via **Windows Task Scheduler**.
It automatically activates the virtual environment, executes the pipeline, and redirects output to `data/logs/scheduler.log`.

To schedule:
1. Open Windows Task Scheduler.
2. Create a Basic Task, set trigger to "Daily".
3. Action: "Start a program".
4. Browse and select `scheduler.bat`.
5. Set "Start in" to the absolute path of the `fx_pipeline` directory.

## Reporting

Use [notebooks/gold_reporting.ipynb](/E:/Data_Engineering_First_Year_Project/foreign_exchange_rates_pipeline/fx_pipeline/notebooks/gold_reporting.ipynb) to explore Gold outputs and create charts or summary tables locally.

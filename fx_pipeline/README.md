# fx_pipeline

A small local data engineering project that models a **foreign exchange (FX) rate** pipeline using a **Bronze → Silver → Gold** layout. Raw API responses land in bronze, cleaned tabular data in silver, and final analytics-ready outputs in gold.

## Folder structure

| Path | Role |
|------|------|
| `data/bronze/` | Raw extracts (e.g. JSON from the API) |
| `data/silver/` | Cleaned, typed tables (e.g. Parquet/CSV) |
| `data/gold/` | Curated metrics or aggregates for analysis |
| `src/` | Python modules: extract, transform, quality checks, utils, orchestration |
| `config.yaml` | Example settings (base currency, targets, API URL) |
| `run_pipeline.py` | Script that starts the pipeline |

## How to run

1. Create a virtual environment (recommended) and install dependencies:

   ```bash
   pip install -r requirements.txt
   ```

2. From the `fx_pipeline` directory (project root), run:

   ```bash
   python run_pipeline.py
   ```

The pipeline code is scaffolded only; you will need to implement the functions in `src/` and point `config.yaml` at a real FX API before it does useful work.

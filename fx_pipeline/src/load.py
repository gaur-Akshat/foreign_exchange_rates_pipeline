import pandas as p
from pathlib import Path
from src.db import get_engine

def _root(config):
    return Path(config.get("_project_root", "."))

def load_bronze_to_sql(config):
    bronze_sql_path = (_root(config) / config.get("paths", {}).get("bronze_sql", "data/bronze/bronze_sql")).resolve()
    df = p.read_parquet(bronze_sql_path / "bronze_exchange_rates.parquet")
    df.to_sql(
        name=config.get("database", {}).get("bronze_table", "bronze_exchange_rates"),
        con=get_engine(),
        if_exists="replace",
        index=False,
    )

def load_silver_to_sql(df, config):
    df_sql = df.drop(columns=["endpoint_name", "response_status"], errors="ignore")
    
    df_sql.to_sql(
        name=config.get("database", {}).get("silver_table", "silver_exchange_rates"),
        con=get_engine(),
        if_exists="replace",
        index=False,
    )

def load_gold_to_sql(gold_marts, config):
    engine = get_engine()
    for name, df in gold_marts.items():
        df.to_sql(name = name, con = engine, if_exists = "replace", index = False, method = "multi", chunksize = 1000)
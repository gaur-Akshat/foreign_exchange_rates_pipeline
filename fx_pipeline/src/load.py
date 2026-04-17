from src.db import get_engine, create_database_if_not_exists

def load_to_sql(df, config):
    create_database_if_not_exists()
    engine = get_engine()
    table_name = config.get("database", {}).get("silver_table", "silver_exchange_rates")
    
    df.to_sql(
        name=table_name,
        con=engine,
        if_exists="append",
        index=False
    )

def load_gold_to_sql(gold_marts, config):
    engine = get_engine()
    
    for mart_name, mart_df in gold_marts.items():
        mart_df.to_sql(
            name=mart_name,
            con=engine,
            if_exists="replace",
            index=False,
            method="multi",
            chunksize=1000,
        )
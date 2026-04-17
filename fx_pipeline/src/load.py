from src.db import get_engine

def load_to_sql(df, config):
    table_name = config["database"]["silver_table"]
    engine = get_engine()

    df = df.drop_duplicates(subset=["rate_date", "base_currency", "target_currency"])

    df.to_sql(
        name=table_name,
        con=engine,
        if_exists="append",
        index=False,
        method="multi",
        chunksize=1000,
    )

def load_gold_to_sql(gold_marts, config):
    table_mapping = config["database"]["gold_tables"]
    engine = get_engine()
    for mart_name, mart_df in gold_marts.items():
        table_name = table_mapping[mart_name]
        mart_df.to_sql(
            name=table_name,
            con=engine,
            if_exists="replace",
            index=False,
            method="multi",
            chunksize=1000,
        )
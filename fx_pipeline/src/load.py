from src.db import get_engine

import yaml

def load_to_sql(df):
    with open("config.yaml", "r") as file:
        config = yaml.safe_load(file)
    table_name = config["database"]["table_silver"]
    engine = get_engine()

    df = df.drop_duplicates(
        subset=["rate_date", "base_currency", "target_currency"]
    )

    df.to_sql(
        name=table_name,
        con=engine,
        if_exists="append",
        index=False,
        method="multi",
        chunksize=1000
    )


def load_gold_to_sql(summary_df, trends_df):
    with open("config.yaml", "r") as file:
        config = yaml.safe_load(file)
    table_summary = config["database"]["table_gold_summary"]
    table_trends = config["database"]["table_gold_trends"]
    engine = get_engine()

    summary_df.to_sql(
        name=table_summary,
        con=engine,
        if_exists="replace",
        index=False,
        method="multi",
        chunksize=1000
    )

    trends_df.to_sql(
        name=table_trends,
        con=engine,
        if_exists="replace",
        index=False,
        method="multi",
        chunksize=1000
    )
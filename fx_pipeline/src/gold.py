import pandas as p
import os
import logging

logger = logging.getLogger(__name__)

def daily_rates(df):
    return df.sort_values(["rate_date", "target_currency"]).reset_index(drop=True)

def currency_movement(df):
    df = df.sort_values(["target_currency", "rate_date"])
    df["previous_rate"] = df.groupby("target_currency")["exchange_rate"].shift(1)
    df["absolute_movement"] = df["exchange_rate"] - df["previous_rate"]
    df["percentage_movement"] = (
        df["absolute_movement"] / df["previous_rate"]
    ) * 100

    df.fillna(
        {
            "previous_rate": df["exchange_rate"],
            "absolute_movement": 0.0,
            "percentage_movement": 0.0,
        },
        inplace=True,
    )

    return df

def weekly_summary(df):
    df = df.sort_values(["target_currency", "rate_date"])
    latest_date = df["rate_date"].max()
    last_7_days = df[df["rate_date"] >= latest_date - p.Timedelta(days=6)]

    summary = last_7_days.groupby("target_currency").agg(
        weekly_avg=("exchange_rate", "mean"),
        weekly_high=("exchange_rate", "max"),
        weekly_low=("exchange_rate", "min"),
        start_rate=("exchange_rate", "first"),
        end_rate=("exchange_rate", "last"),
    ).reset_index()

    summary["weekly_change"] = summary["end_rate"] - summary["start_rate"]
    summary["weekly_pct_change"] = (summary["weekly_change"] / summary["start_rate"]) * 100
    summary["weekly_pct_change"] = summary["weekly_pct_change"].fillna(0.0)

    return summary

def strength_ranking(df):
    df = df.sort_values("weekly_pct_change", ascending=False)
    df["rank"] = df["weekly_pct_change"].rank(ascending=False).astype(int)

    return df

def conversion_table(df):
    latest = df.sort_values("rate_date").groupby("target_currency").tail(1)
    units = [1, 10, 100, 1000]
    rows = []
    for _, row in latest.iterrows():
        for u in units:
            rows.append({
                "target_currency": row["target_currency"],
                "base_amount": u,
                "converted_amount": u * row["exchange_rate"],
            })

    return p.DataFrame(rows)

def run_gold_layer(df, config):
    logger.info("Running Gold Layer")
    gold_path = config["paths"]["gold"]
    os.makedirs(gold_path, exist_ok=True)

    daily = daily_rates(df)
    movement = currency_movement(daily.copy())
    weekly = weekly_summary(daily.copy())
    ranking = strength_ranking(weekly.copy())
    conversion = conversion_table(daily.copy())
    datasets = {
        "gold_daily_currency_rates": daily, 
        "gold_currency_movement": movement,
        "gold_weekly_currency_summary": weekly,
        "gold_strength_rankings": ranking,
        "gold_conversion_reference": conversion,
    }
    excel_folder = os.path.join(gold_path, "excel")
    os.makedirs(excel_folder, exist_ok=True)

    for name, data in datasets.items():
        parquet_path = os.path.join(gold_path, f"{name}.parquet")
        data.to_parquet(parquet_path, index=False)
        excel_path = os.path.join(excel_folder, f"{name}.xlsx")
        data.to_excel(excel_path, index=False)
    logger.info(f"Saved {name} (Parquet and Excel) to {gold_path}")

    return datasets
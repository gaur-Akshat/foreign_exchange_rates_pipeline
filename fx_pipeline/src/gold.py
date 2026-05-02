import pandas as p
import logging
from pathlib import Path

logger = logging.getLogger(__name__)

def daily_rates(df):
    return (
        df.drop(columns=["endpoint_name", "response_status"]).sort_values(["rate_date", "target_currency"]).reset_index(drop=True)
    )

def currency_movement(df):
    df = df.sort_values(["target_currency", "rate_date"])
    df["previous_rate"] = df.groupby("target_currency")["exchange_rate"].shift(1)
    df["absolute_movement"] = df["exchange_rate"] - df["previous_rate"]
    df["percentage_movement"] = (df["absolute_movement"] / df["previous_rate"]) * 100
    df.fillna({"previous_rate": df["exchange_rate"], "absolute_movement": 0.0, "percentage_movement": 0.0}, inplace=True)
    return df

def weekly_summary(df):
    df = df.sort_values(["target_currency", "rate_date"])
    last_7 = df[df["rate_date"] >= df["rate_date"].max() - p.Timedelta(days=6)]
    summary = last_7.groupby("target_currency").agg(
        weekly_avg=("exchange_rate", "mean"),
        weekly_high=("exchange_rate", "max"),
        weekly_low=("exchange_rate", "min"),
        start_rate=("exchange_rate", "first"),
        end_rate=("exchange_rate", "last"),
    ).reset_index()
    summary["weekly_change"] = summary["end_rate"] - summary["start_rate"]
    summary["weekly_pct_change"] = (summary["weekly_change"] / summary["start_rate"] * 100).fillna(0.0)
    return summary

def strength_ranking(df):
    df = df.sort_values("weekly_pct_change", ascending=False).reset_index(drop=True)
    df["rank"] = df["weekly_pct_change"].rank(ascending=False).astype(int)
    return df[["rank", "target_currency", "weekly_pct_change"]]

def conversion_table(df):
    latest = df.sort_values("rate_date").groupby("target_currency").tail(1)
    rows = [
        {
            "base_currency": row["base_currency"],
            "target_currency": row["target_currency"],
            "1": round(row["exchange_rate"], 6),
            "10": round(row["exchange_rate"] * 10, 6),
            "100": round(row["exchange_rate"] * 100, 6),
            "1000": round(row["exchange_rate"] * 1000, 6),
        }
        for _, row in latest.iterrows()
    ]
    return p.DataFrame(rows)

def run_gold_layer(df, config):
    logger.info("Running Gold Layer")
    gold_path = (Path(config.get("_project_root", ".")) / config["paths"]["gold"]).resolve()
    excel_folder = gold_path / "excel"
    excel_folder.mkdir(parents=True, exist_ok=True)

    daily = daily_rates(df)
    weekly = weekly_summary(daily.copy())
    datasets = {
        "gold_daily_currency_rates": daily,
        "gold_currency_movement": currency_movement(daily.copy()),
        "gold_weekly_currency_summary": weekly,
        "gold_strength_rankings": strength_ranking(weekly.copy()),
        "gold_conversion_reference": conversion_table(daily.copy()),
    }

    for name, data in datasets.items():
        data.to_parquet(gold_path / f"{name}.parquet", index=False)
        data.to_excel(excel_folder / f"{name}.xlsx", index=False)
    logger.info("Saved %s (Parquet and Excel) to %s", name, gold_path)

    return datasets
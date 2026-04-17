import pandas as pd

from src.gold import (
    build_conversion_reference,
    build_currency_movement,
    build_strength_rankings,
    build_weekly_currency_summary,
)

def sample_daily_rates():
    return pd.DataFrame(
        [
            {
                "rate_date": pd.Timestamp("2026-04-09"),
                "base_currency": "USD",
                "target_currency": "EUR",
                "exchange_rate": 0.90,
            },
            {
                "rate_date": pd.Timestamp("2026-04-10"),
                "base_currency": "USD",
                "target_currency": "EUR",
                "exchange_rate": 0.92,
            },
            {
                "rate_date": pd.Timestamp("2026-04-11"),
                "base_currency": "USD",
                "target_currency": "EUR",
                "exchange_rate": 0.94,
            },
            {
                "rate_date": pd.Timestamp("2026-04-09"),
                "base_currency": "USD",
                "target_currency": "JPY",
                "exchange_rate": 151.0,
            },
            {
                "rate_date": pd.Timestamp("2026-04-10"),
                "base_currency": "USD",
                "target_currency": "JPY",
                "exchange_rate": 150.5,
            },
            {
                "rate_date": pd.Timestamp("2026-04-11"),
                "base_currency": "USD",
                "target_currency": "JPY",
                "exchange_rate": 149.5,
            },
        ]
    )

def test_build_currency_movement_creates_previous_and_percentage_fields():
    movement_df = build_currency_movement(sample_daily_rates())
    assert {"previous_rate", "absolute_movement", "percentage_movement"} <= set(
        movement_df.columns
    )
    assert (movement_df["previous_rate"] > 0).all()

def test_build_weekly_summary_and_rankings():
    weekly_summary_df = build_weekly_currency_summary(sample_daily_rates())
    rankings_df = build_strength_rankings(weekly_summary_df)
    assert "weekly_average_rate" in weekly_summary_df.columns
    assert "appreciation_rank" in rankings_df.columns
    assert len(rankings_df) == 2

def test_build_conversion_reference_uses_configured_amounts():
    config = {"gold": {"conversion_units": [1, 10, 100]}}
    conversion_df = build_conversion_reference(sample_daily_rates(), config)
    assert set(conversion_df["base_amount"]) == {1, 10, 100}
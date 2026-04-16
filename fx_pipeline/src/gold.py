import pandas as pd
import logging
import os

logger = logging.getLogger(__name__)

def create_monthly_summary(df):
    df['rate_date'] = pd.to_datetime(df['rate_date'])
    df['year_month'] = df['rate_date'].dt.to_period('M').dt.to_timestamp()
    
    summary_df = df.groupby(['year_month', 'base_currency', 'target_currency']).agg(
        avg_rate=('exchange_rate', 'mean'),
        min_rate=('exchange_rate', 'min'),
        max_rate=('exchange_rate', 'max'),
        volatility=('exchange_rate', 'std')
    ).reset_index()
    
    summary_df['volatility'] = summary_df['volatility'].fillna(0)
    
    return summary_df

def create_daily_trends(df):
    df['rate_date'] = pd.to_datetime(df['rate_date'])
    df = df.sort_values(by=['base_currency', 'target_currency', 'rate_date'])
    
    grouped = df.groupby(['base_currency', 'target_currency'])    
    df['rate_7d_avg'] = grouped['exchange_rate'].transform(lambda x: x.rolling(window=7, min_periods=1).mean())
    df['rate_30d_avg'] = grouped['exchange_rate'].transform(lambda x: x.rolling(window=30, min_periods=1).mean())
    df['daily_pct_change'] = grouped['exchange_rate'].transform(lambda x: x.pct_change() * 100)
    
    df['daily_pct_change'] = df['daily_pct_change'].fillna(0)
    
    return df

def process_gold_layer(df, config):
    logger.info("Processing Gold layer")
    
    gold_path = config['data']['gold_path']
    os.makedirs(gold_path, exist_ok=True)
    
    logger.info("Generating Monthly Summary")
    summary_df = create_monthly_summary(df.copy())
    summary_df.to_parquet(os.path.join(gold_path, "gold_rate_monthly_summary.parquet"), index=False)
    
    logger.info("Generating Daily Trends")
    trends_df = create_daily_trends(df.copy())
    trends_df.to_parquet(os.path.join(gold_path, "gold_rate_daily_trends.parquet"), index=False)
    
    logger.info("Gold layer processing complete")
    return summary_df, trends_df
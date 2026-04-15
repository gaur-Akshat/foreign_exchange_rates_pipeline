from src.extract import extract_data
from src.transform import transform_data, save_silver
from src.pipeline import apply_retention_policy
import yaml

with open("E:/Data_Engineering_First_Year_Project/foreign_exchange_rates_pipeline/fx_pipeline/config.yaml") as f:
    config = yaml.safe_load(f)

extract_data(config)
df = transform_data(config)
save_silver(df, config)

def run_pipeline():
    extract_data(config)

    df = transform_data(config)
    save_silver(df, config)

    apply_retention_policy(config, days=30)
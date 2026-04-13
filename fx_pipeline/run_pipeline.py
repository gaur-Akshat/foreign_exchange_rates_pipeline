from src.extract import extract_data
from src.transform import transform_data, save_silver
import yaml

with open("config.yaml") as f:
    config = yaml.safe_load(f)

extract_data(config)
df = transform_data(config)
save_silver(df, config)
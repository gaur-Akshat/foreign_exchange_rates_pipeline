from src.extract import extract_data
import yaml

with open(".config.yaml") as f:
    config = yaml.safe_load(f)

extract_data(config)
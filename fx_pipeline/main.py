import yaml
from pathlib import Path
from src.pipeline import run_pipeline

def load_config(config_path="config.yaml"):
    path = Path(config_path)
    if not path.exists():
        raise FileNotFoundError(f"{config_path} not found")
    config = yaml.safe_load(path.read_text(encoding="utf-8"))
    config["_project_root"] = str(path.parent.resolve())
    return config

if __name__ == "__main__":
    run_pipeline(load_config())
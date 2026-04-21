import yaml
from pathlib import Path
from src.pipeline import run_pipeline

def load_config(config_path="config.yaml"):
    path = Path(config_path)

    if not path.exists():
        raise FileNotFoundError(f"{config_path} not found")

    with path.open("r", encoding="utf-8") as f:
        config = yaml.safe_load(f)

    project_root = path.parent
    config["_project_root"] = str(project_root.resolve())
    return config

if __name__ == "__main__":
    config = load_config()
    run_pipeline(config)
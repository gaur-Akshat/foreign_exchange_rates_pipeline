import yaml
from pathlib import Path
from src.pipeline import run_pipeline

def load_config(config_path=None):
    candidates = (Path(config_path),) if config_path else (Path("config/config.yaml"), Path("config.yaml"))
    path = next((c for c in candidates if c.exists()), candidates[0]).resolve()
    
    with path.open("r", encoding="utf-8") as f:
        config = yaml.safe_load(f)
        
    project_root = path.parent.parent if path.parent.name == "config" else path.parent
    config["_project_root"] = str(project_root.resolve())
    return config

if __name__ == "__main__":
    config = load_config()
    run_pipeline(config)
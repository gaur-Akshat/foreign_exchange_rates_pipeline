import logging
from datetime import datetime
from pathlib import Path

def setup_logging(config):
    root = logging.getLogger()
    root.handlers.clear()
    logs_dir = (Path(config.get("_project_root", ".")) / config["paths"]["logs"]).resolve()
    logs_dir.mkdir(parents=True, exist_ok=True)
    logging.basicConfig(
        level=getattr(logging, config.get("logging", {}).get("level", "INFO")),
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        handlers=[
            logging.FileHandler(logs_dir / f"pipeline_{datetime.today().date()}.log"),
            logging.StreamHandler(),
        ],
    )
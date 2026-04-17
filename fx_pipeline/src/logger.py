import logging
from datetime import datetime
from pathlib import Path

def setup_logging(config):
    if logging.getLogger().hasHandlers():
        logging.getLogger().handlers.clear()

    logs_dir = (Path(config.get("_project_root", ".")) / config["data"]["logs_path"]).resolve()
    logs_dir.mkdir(parents=True, exist_ok=True)
    log_file = logs_dir / f"pipeline_{datetime.today().date()}.log"

    logging.basicConfig(
        level=getattr(logging, config.get("logging", {}).get("level", "INFO")),
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        handlers=[
            logging.FileHandler(log_file),
            logging.StreamHandler(),
        ],
    )
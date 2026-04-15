import logging
import os
from datetime import datetime

def setup_logging():
    if logging.getLogger().hasHandlers():
        logging.getLogger().handlers.clear()

    script_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.dirname(script_dir)

    logs_dir = os.path.join(project_root, "logs")
    os.makedirs(logs_dir, exist_ok=True)

    log_file = os.path.join(
        logs_dir,
        f"pipeline_{datetime.today().date()}.log"
    )

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        handlers=[
            logging.FileHandler(log_file),
            logging.StreamHandler()
        ]
    )
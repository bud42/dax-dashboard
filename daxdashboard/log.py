import logging
import os
from pathlib import Path


def setup_logging(app_name='daxdashboard'):
    log_dir = Path.home() / f".{app_name.lower()}" / "logs"
    log_dir.mkdir(parents=True, exist_ok=True)

    log_file = log_dir / "app.log"

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        handlers=[
            logging.FileHandler(log_file),
            logging.StreamHandler()
        ]
    )

    logging.info('DAXdashboard app started')

    return logging.getLogger('daxdashboard')


# Run setup and get the logger to use elsewhere
logger = setup_logging('daxdashboard')

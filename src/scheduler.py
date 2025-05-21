#!/usr/bin/env python3

import os
import time
import schedule
import logging
import subprocess
from datetime import datetime

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("logs/scheduler.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("scheduler")

# Get the scrape interval from environment variable, default to 1 hour (3600 seconds)
SCRAPE_INTERVAL = int(os.environ.get('SCRAPE_INTERVAL', 3600))


def run_wildweb_scraper():
    """Run the WildWeb scraper"""
    logger.info(f"Running WildWeb scraper at {datetime.now()}")
    
    try:
        # Run the scraper script
        subprocess.run(
            ["python", "-m", "src.scrapers.wildweb", "--db", "./data/db/graviris.db"],
            check=True
        )
        logger.info("WildWeb scraper completed successfully")
    except subprocess.CalledProcessError as e:
        logger.error(f"Error running WildWeb scraper: {str(e)}")
    except Exception as e:
        logger.error(f"Unexpected error: {str(e)}")


if __name__ == "__main__":
    logger.info(f"Starting scheduler with {SCRAPE_INTERVAL} seconds interval")
    
    # Schedule the WildWeb scraper to run at the specified interval
    schedule.every(SCRAPE_INTERVAL).seconds.do(run_wildweb_scraper)
    
    # Run the scraper immediately on startup
    run_wildweb_scraper()
    
    # Keep running the scheduler
    while True:
        schedule.run_pending()
        time.sleep(1)
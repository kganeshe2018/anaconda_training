# src/main.py

from src.db.db_setup import AppDB
from src.helpers.logger import LoggerFactory

logger = LoggerFactory.get_logger("Main")

def main():
    logger.info("===== Starting Fund DB Setup Workflow =====")
    AppDB.create_database()
    AppDB.setup_tables()
    logger.info("===== Fund DB Setup Completed Successfully =====")

if __name__ == "__main__":
    main()
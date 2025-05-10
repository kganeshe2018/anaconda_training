import sqlite3
import polars as pl

from src.app.config import Config
from src.base.base_model import BaseModel
from src.helpers.logger import LoggerFactory
from src.helpers.db_utils import execute_query

logger = LoggerFactory.get_logger("AppDB")

class SetupDB(BaseModel):

    def  __init__(self, app_config: Config):
        super().__init__(app_config)
        self.db_path = Config.DB_PATH
        self.funds_folder = Config.FUNDS_FOLDER
        self.sql_base_table_script_path = Config.BASE_TABLES_SQL_SCRIPT_PATH
        self.sql_master_reference_script_path = Config.MASTER_REFERENCE_SQL_SCRIPT_PATH

    def _compute(self) -> None:
        self.create_database()
        self.setup_tables(self.sql_master_reference_script_path)
        self.setup_tables(self.sql_base_table_script_path)

    def create_database(self):
        if self.db_path.exists():
            logger.info(f"Database already exists at {self.db_path}, skipping creation.")
            return

        logger.info(f"Creating new database at {self.db_path}...")
        conn = sqlite3.connect(self.db_path)
        conn.close()
        logger.info(f"New database created at {self.db_path}...")

    def setup_tables(self,file_path:str):
        logger.info(f"Setting up tables using script: {file_path}")
        try:
            conn = sqlite3.connect(self.db_path)
            with open(file_path, 'r') as f:
                script = f.read()
                conn.executescript(script)
                logger.info("Executed SQL script successfully.")
            conn.commit()
            conn.close()
            logger.info(f"Database setup completed for: {self.db_path}")
        except Exception as e:
            logger.error(f"Failed to set up tables: {e}")
            raise
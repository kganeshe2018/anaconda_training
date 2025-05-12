import sqlite3
from src.base.base_model import BaseModel
from src.helpers.logger import LoggerFactory
from config.settings import AppConfig

logger = LoggerFactory.get_logger("AppDB")

class SetupDB(BaseModel):
    """
    Database setup class that handles creation and initialization of the application's SQLite database.
    
    This class is responsible for creating the database file if it doesn't exist and
    setting up all necessary tables using SQL scripts.
    """

    def __init__(self, app_config: AppConfig):
        """
        Initialize the SetupDB class.
        
        Args:
            app_config (Config): Application configuration object
        """
        super().__init__(app_config)
        self.config = app_config
        self.db_path = self.config.db_path
        self.funds_folder = self.config.funds_folder
        self.sql_base_table_script_path = self.config.sql_query_base_tables
        self.sql_master_reference_script_path = self.config.sql_query_master_reference

    def _compute(self) -> None:
        """
        Execute the database setup process.
        
        This method creates the database if it doesn't exist and sets up tables
        by executing the master reference and base table SQL scripts.
        """
        self.create_database()
        self.setup_tables(self.sql_master_reference_script_path)
        self.setup_tables(self.sql_base_table_script_path)

    def create_database(self):
        """
        Create a new SQLite database if it doesn't exist.
        
        If the database already exists, this method will log this information and return
        without making any changes.
        """
        if self.db_path.exists():
            logger.info(f"Database already exists at {self.db_path}, skipping creation.")
            return

        logger.info(f"Creating new database at {self.db_path}...")
        conn = sqlite3.connect(self.db_path)
        conn.close()
        logger.info(f"New database created at {self.db_path}...")

    def setup_tables(self, file_path: str):
        """
        Set up database tables by executing SQL scripts.
        
        Args:
            file_path (str): Path to the SQL script file to execute
            
        Raises:
            Exception: If there's an error during table setup, the exception is logged and re-raised
        """
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
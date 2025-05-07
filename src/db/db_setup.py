import sqlite3
import polars as pl
from src.app.config import DB_PATH, MASTER_REFERENCE_SQL_SCRIPT_PATH, FUNDS_FOLDER
from src.helpers.logger import LoggerFactory

logger = LoggerFactory.get_logger("AppDB")

class AppDB:
    db_path = DB_PATH
    sql_script_path = MASTER_REFERENCE_SQL_SCRIPT_PATH
    funds_folder = FUNDS_FOLDER

    @classmethod
    def create_database(cls):
        if cls.db_path.exists():
            logger.info(f"Database already exists at {cls.db_path}, skipping creation.")
            return

        logger.info(f"Creating new database at {cls.db_path}...")
        conn = sqlite3.connect(cls.db_path)
        with open(cls.sql_script_path, 'r') as f:
            conn.executescript(f.read())
        conn.commit()
        conn.close()
        logger.info("Reference tables created successfully.")

    @classmethod
    def setup_tables(cls):
        conn = sqlite3.connect(cls.db_path)
        with open(cls.sql_script_path, 'r') as f:
            conn.executescript(f.read())
        conn.commit()
        conn.close()


    @classmethod
    def load_fund_csvs_to_db(cls):
        conn = sqlite3.connect(cls.db_path)
        fund_dataframes = []

        for csv_file in cls.funds_folder.glob("*.csv"):
            fund_name = csv_file.stem
            df = pl.read_csv(csv_file)
            df = df.with_columns(pl.lit(fund_name).alias("fund_name"))
            fund_dataframes.append(df)
            logger.info(f"Loaded file: {csv_file.name} with {len(df)} records.")

        combined_df = pl.concat(fund_dataframes)
        combined_df.to_pandas().to_sql("fund_positions", conn, if_exists="replace", index=False)

        logger.info(f"Ingested {len(combined_df)} total rows into 'fund_positions'.")
        conn.close()

    @staticmethod
    def execute_query(query: str, params: tuple = ()) -> pl.DataFrame:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        cursor.execute(query, params)
        columns = [desc[0] for desc in cursor.description]
        data = cursor.fetchall()
        conn.close()
        return pl.DataFrame(data, schema=columns)

    @staticmethod
    def execute_non_query(query: str, params: tuple = ()):
        conn = sqlite3.connect(DB_PATH)
        cur = conn.cursor()
        cur.execute(query, params)
        conn.commit()
        conn.close()
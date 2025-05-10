import sqlite3
from typing import Literal
import polars as pl

from src.app.config import Config
from src.helpers.logger import LoggerFactory

__all__ = ["execute_query", "execute_non_query", "copy_data_from_src_to_tgt"]


logger = LoggerFactory.get_logger(__name__)

def execute_query(app_config: Config, query: str, params: tuple = ()) -> pl.DataFrame:
    conn = sqlite3.connect(app_config.DB_PATH)
    cursor = conn.cursor()
    cursor.execute(query, params)
    columns = [desc[0] for desc in cursor.description]
    data = cursor.fetchall()
    conn.close()
    return pl.DataFrame(data, schema=columns,orient="row")

def execute_non_query(app_config: Config,query: str, params: tuple = ()) -> None:
    conn = sqlite3.connect(app_config.DB_PATH)
    cur = conn.cursor()
    cur.execute(query, params)
    conn.commit()
    conn.close()

def write_df_to_sqlite(app_config: Config,df: pl.DataFrame, table_name: str, if_exists: str = "replace"):
    if df.is_empty():
        raise ValueError("DataFrame is empty. Nothing to write.")

    if not table_name:
        raise ValueError("Table name must be provided.")

    conn = sqlite3.connect(app_config.DB_PATH)
    try:
        # Truncate table contents
        conn.execute(f"DELETE FROM {table_name};")
        conn.commit()

        # Load new data
        df.to_pandas().to_sql(table_name, conn, if_exists="append", index=False)
    finally:
        conn.close()

def copy_data_from_src_to_tgt(
    app_config: Config,
    staging_table: str,
    publish_table: str,
    mode: Literal["replace", "append"] = "replace"
):
    """
    Copies all data from a staging table to a publish table in the same SQLite database.

    Args:
        staging_table (str): Name of the staging table to read from.
        publish_table (str): Name of the publish table to write to.
        mode (str): 'replace' to truncate and load, 'append' to add to existing data.
    """
    conn = sqlite3.connect(app_config.DB_PATH)
    try:
        # Step 1: Read from staging
        df = pl.read_database(f"SELECT * FROM {staging_table}", conn, infer_schema_length=10000000)

        if df.is_empty():
            logger.warning(f"No data in staging table: {staging_table}")
            return

        # Step 2: Truncate publish table if mode is 'replace'
        if mode == "replace":
            conn.execute(f"DELETE FROM {publish_table}")
            conn.commit()

        # Step 3: Write to publish table
        df.to_pandas().to_sql(publish_table, conn, if_exists="append", index=False)
        logger.info(f"Copied {df.shape[0]} rows from {staging_table} to {publish_table}")

    except Exception as e:
        logger.error(f"Failed to copy data from {staging_table} to {publish_table}: {e}")
        raise IOError("Failed to copy data from staging table to publish table.")
    finally:
        conn.close()
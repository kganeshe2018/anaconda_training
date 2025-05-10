import re
import sqlite3
import polars as pl

from config.settings import AppConfig
from src.base.base_model import BaseModel
from src.helpers.db_utils import execute_query
from src.helpers.logger import LoggerFactory
from src.helpers.utils import extract_report_date, get_files_in_directory, read_file_as_string

logger = LoggerFactory.get_logger(__name__)

class IngestFundsData(BaseModel):

    def  __init__(self, app_config: AppConfig):
        super().__init__(app_config)
        self.config = app_config
        self.db_path = self.config.db_path
        self.funds_folder = self.config.funds_folder


    def _compute(self) -> None:
        self.ingest()


    def ingest(self):
        logger.info("Starting ingestion of fund CSVs...")
        col_fund_name = "FUND NAME"
        col_datetime = "DATETIME"
        query_file_name = self.config.sql_query_get_active_funds_cfg
        try:
            sql = read_file_as_string(query_file_name)
            df = execute_query(self.config, sql.format(dte=self.config.data_date))
            if df.is_empty():
                return None

            fund_names = df[col_fund_name].to_list()
            pattern = re.compile(r'(' + '|'.join(map(re.escape, fund_names)) + r')', re.IGNORECASE)
            matched_dfs = []
            for file_path in get_files_in_directory(self.funds_folder, "*.csv"):
                filename = file_path.name
                match = pattern.search(filename)
                if not match:
                    continue
                fund_name = match.group(0)
                df = pl.read_csv(file_path)
                df = df.with_columns(pl.lit(extract_report_date(filename)).alias(col_datetime))
                cols = [col_datetime] + [col for col in df.columns if col != col_datetime]
                df = df.select(cols)
                df = df.with_columns(pl.lit(fund_name).alias(col_fund_name))
                matched_dfs.append(df)

            if not matched_dfs:
                logger.info("No matching files found.")
                return None

            all_data = pl.concat(matched_dfs, how="diagonal")
            conn = sqlite3.connect(self.config.db_path)
            all_data.to_pandas().to_sql("tbl_raw_fund_position_details", conn, if_exists="append", index=False)
            conn.close()

            logger.info(f"Inserted {all_data.shape[0]} rows into tbl_raw_fund_position_details.")
            return None

        except Exception as e:
            logger.error(f"Error in extract_fund_name_from_filename: {e}")
            return None


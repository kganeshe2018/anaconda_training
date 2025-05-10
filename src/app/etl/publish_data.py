import polars as pl
from datetime import datetime
from dateutil.relativedelta import relativedelta
from src.app.config import Config
from src.base.base_model import BaseModel
from src.helpers.db_utils import copy_data_from_src_to_tgt
from src.helpers.logger import LoggerFactory
from src.helpers.utils import extract_report_date, get_files_in_directory, read_file_as_string

logger = LoggerFactory.get_logger(__name__)

class PublishData(BaseModel):

    def  __init__(self, app_config: Config):
        super().__init__(app_config)
        self.config = app_config
        self.db_path = Config.DB_PATH
        self.funds_folder = Config.FUNDS_FOLDER
        self.tbl_raw = "tbl_raw_fund_position_details"
        self.reference_data_query_file_name = self.config.sql_query_get_reference_data

    def _compute(self) -> None:
        self.publish_reference_data()
        self.publish_fund_data()
        pass

    def publish_reference_data(self) -> None:

        #Publish Equity Prices
        logger.info("Starting publish of Equity Prices...")
        copy_data_from_src_to_tgt(self.config, "tbl_stg_equity_prices", "tbl_pub_equity_prices")
        logger.info("Completed publish of Equity Prices...")

        #Publish Equity Reference
        logger.info("Starting publish of Equity Reference...")
        copy_data_from_src_to_tgt(self.config, "tbl_stg_equity_reference", "tbl_stg_equity_reference")
        logger.info("Completed publish of Equity Reference...")
        pass

    def publish_fund_data(self) -> None:
        #Publish Funds Data
        copy_data_from_src_to_tgt(self.config, "tbl_stg_fund_position_details", "tbl_pub_fund_position_details")
        pass

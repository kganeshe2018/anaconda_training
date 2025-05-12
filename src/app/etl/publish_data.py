from src.base.base_model import BaseModel
from src.helpers.db_utils import copy_data_from_src_to_tgt
from src.helpers.logger import LoggerFactory

logger = LoggerFactory.get_logger(__name__)

class PublishData(BaseModel):
    """
    Class for publishing data from staging tables to production tables.

    This class handles the final step in the ETL process, copying data from staging
    tables to published tables that are used for reporting and analysis.
    """

    def __init__(self, app_config):
        """
        Initialize the PublishData class.

        Args:
            app_config (AppConfig): Application configuration object containing paths and settings
        """
        super().__init__(app_config)
        self.config = app_config
        self.db_path = self.config.db_path
        self.funds_folder = self.config.funds_folder
        self.tbl_raw = "tbl_raw_fund_position_details"
        self.reference_data_query_file_name = self.config.sql_query_get_raw_reference_data

    def _compute(self) -> None:
        """
        Execute the data publishing process.

        This method is called by the BaseModel's run method and serves as the main
        entry point for the data publishing process. It publishes both reference data
        and fund data to their respective production tables.

        Returns:
            None
        """
        self.publish_reference_data()
        self.publish_fund_data()

    def publish_reference_data(self) -> None:
        """
        Publish reference data from staging to production tables.

        This method copies equity prices and equity reference data from staging tables
        to published tables, making them available for reporting and analysis.

        Returns:
            None
        """
        #Publish Equity Prices
        logger.info("Starting publish of Equity Prices...")
        copy_data_from_src_to_tgt(self.config, "tbl_stg_equity_prices", "tbl_pub_equity_prices")
        logger.info("Completed publish of Equity Prices...")

        #Publish Equity Reference
        logger.info("Starting publish of Equity Reference...")
        copy_data_from_src_to_tgt(self.config, "tbl_stg_equity_reference", "tbl_pub_equity_reference")
        logger.info("Completed publish of Equity Reference...")

    def publish_fund_data(self) -> None:
        """
        Publish fund position data from staging to production tables.

        This method copies fund position details from staging tables to published tables,
        making them available for reporting and analysis.

        Returns:
            None
        """
        #Publish Funds Data
        logger.info("Starting publish of Fund Position Details...")
        copy_data_from_src_to_tgt(self.config, "tbl_stg_fund_position_details", "tbl_pub_fund_position_details")
        logger.info("Completed publish of Fund Position Details...")

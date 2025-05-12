from pathlib import Path
import polars as pl
import pandas as pd

from config.settings import AppConfig
from src.base.base_model import BaseModel
from src.helpers.db_utils import execute_query
from src.helpers.logger import LoggerFactory
from src.helpers.utils import read_file_as_string

logger = LoggerFactory.get_logger(__name__)

class GenerateReconReport(BaseModel):
    """
    Class for generating reconciliation reports between fund data and reference data.

    This class compares fund position data with reference data to identify discrepancies
    in prices. It generates an Excel report with separate sheets for each fund, showing
    the joined data and highlighting price differences.
    """

    def __init__(self, app_config: AppConfig):
        """
        Initialize the GenerateReconReport class.

        Args:
            app_config (AppConfig): Application configuration object containing paths and settings
        """
        super().__init__(app_config)
        self.config = app_config
        self.db_path = app_config.db_path
        self.funds_folder = app_config.funds_folder
        self.sql_path_pub_reference_data = app_config.sql_query_get_pub_reference_data
        self.sql_path_pub_funds_equities_data = app_config.sql_query_get_pub_funds_equities_data
        self.report_recon_path = app_config.report_recon_path
        self.sql_path_active_funds_cfg = app_config.sql_query_get_active_funds_cfg
        self.col_fund_name = "FUND NAME"

    def _compute(self) -> None:
        """
        Execute the reconciliation report generation process.

        This method is called by the BaseModel's run method and serves as the main
        entry point for the report generation process.

        Returns:
            None
        """
        logger.info("Starting reconciliation report generation...")
        self.export_filtered_joins_to_excel_polars()
        logger.info("Reconciliation report generation completed.")

    def get_reference_data(self) -> pl.DataFrame:
        """
        Retrieve reference data from the database.

        Executes a SQL query to fetch reference price data from the published tables,
        using the configured SQL query file.

        Returns:
            pl.DataFrame: DataFrame containing reference price data
        """
        logger.info("Loading reference data...")
        sql = read_file_as_string(file_path=self.sql_path_pub_reference_data)
        logger.debug(f"Reference SQL:\n{sql}")
        df = execute_query(self.config, sql)
        logger.info(f"Reference data loaded: {df.shape[0]} rows.")
        return df

    def get_funds_data(self) -> pl.DataFrame:
        """
        Retrieve fund data from the database.

        Executes a SQL query to fetch fund equity data from the published tables,
        using the configured SQL query file and data date.

        Returns:
            pl.DataFrame: DataFrame containing fund equity data
        """
        logger.info("Loading fund equities data...")
        sql = read_file_as_string(file_path=self.sql_path_pub_funds_equities_data).format(dte=self.config.data_date)
        logger.debug(f"Funds SQL:\n{sql}")
        df = execute_query(self.config, sql)
        logger.info(f"Fund data loaded: {df.shape[0]} rows.")
        return df

    def get_active_funds_cfg(self) -> pl.DataFrame:
        """
        Retrieve active funds configuration from the database.

        Executes a SQL query to fetch the list of active funds that should be included
        in the reconciliation report, using the configured SQL query file and data date.

        Returns:
            pl.DataFrame: DataFrame containing active funds configuration
        """
        logger.info("Loading active funds configuration...")
        sql = read_file_as_string(file_path=self.sql_path_active_funds_cfg).format(dte=self.config.data_date)
        logger.debug(f"Active funds SQL:\n{sql}")
        df = execute_query(self.config, sql)
        logger.info(f"Active funds loaded: {df.shape[0]} rows.")
        return df

    def export_filtered_joins_to_excel_polars(self):
        """
        Generate and export the reconciliation report to Excel.

        This method retrieves fund data and reference data, joins them on date and symbol,
        calculates price differences, and exports the results to an Excel file with
        separate sheets for each active fund.

        Returns:
            None
        """
        logger.info("Starting export of fund-level joined data to Excel...")

        main_df = self.get_funds_data()
        join_df = self.get_reference_data()
        output_file = Path(self.report_recon_path)
        output_file.parent.mkdir(parents=True, exist_ok=True)

        filter_list = self.get_active_funds_cfg()[self.col_fund_name].to_list()
        logger.info(f"Found {len(filter_list)} active funds to process.")
        filter_column = self.col_fund_name
        join_on = ['DATETIME', 'SYMBOL']
        how = "inner"

        # Drop conflicting columns
        drop_cols = [col for col in join_df.columns if col in main_df.columns and col not in join_on and col != filter_column]
        if drop_cols:
            logger.debug(f"Dropping overlapping columns from join_df: {drop_cols}")
        cleaned_join_df = join_df.drop(drop_cols)

        # Join and compute diff
        full_joined_df = main_df.join(cleaned_join_df, on=join_on, how=how)
        full_joined_df = full_joined_df.with_columns(
            (pl.col("PRICE_FUND") - pl.col("PRICE_REF")).alias("PRICE_DIFF")
        )
        logger.info(f"Joined dataset contains {full_joined_df.shape[0]} rows.")

        sheet_count = 0
        with pd.ExcelWriter(output_file, engine="openpyxl", mode="w") as writer:
            for fund in filter_list:
                group_df = full_joined_df.filter(pl.col(filter_column) == fund)
                if group_df.is_empty():
                    logger.warning(f"No data found for fund: {fund}. Skipping sheet.")
                    continue
                group_df.to_pandas().to_excel(writer, index=False, sheet_name=str(fund)[:31])
                logger.debug(f"Sheet written for fund: {fund} ({group_df.shape[0]} rows).")
                sheet_count += 1

        logger.info(f"Exported {sheet_count} fund sheets to Excel at: {output_file}")

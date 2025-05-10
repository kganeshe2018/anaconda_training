from pathlib import Path
import polars as pl
import pandas as pd
from src.app.config import Config
from src.base.base_model import BaseModel
from src.helpers.db_utils import execute_query
from src.helpers.logger import LoggerFactory
from src.helpers.utils import read_file_as_string

logger = LoggerFactory.get_logger(__name__)

class GenerateReconReport(BaseModel):

    def __init__(self, app_config: Config):
        super().__init__(app_config)
        self.config = app_config
        self.db_path = Config.DB_PATH
        self.funds_folder = Config.FUNDS_FOLDER
        self.sql_path_pub_reference_data = Config.SQL_QUERY_GET_PUB_REFERENCE_DATA
        self.sql_path_pub_funds_equities_data = Config.SQL_QUERY_GET_PUB_FUNDS_EQUITIES_DATA
        self.report_recon_path = Config.REPORT_RECON_PATH
        self.sql_path_active_funds_cfg = Config.SQL_QUERY_GET_ACTIVE_FUNDS_CFG
        self.col_fund_name = "FUND NAME"

    def _compute(self) -> None:
        logger.info("Starting reconciliation report generation...")
        self.export_filtered_joins_to_excel_polars()
        logger.info("Reconciliation report generation completed.")

    def get_reference_data(self) -> pl.DataFrame:
        logger.info("Loading reference data...")
        sql = read_file_as_string(file_path=self.sql_path_pub_reference_data)
        logger.debug(f"Reference SQL:\n{sql}")
        df = execute_query(self.config, sql)
        logger.info(f"Reference data loaded: {df.shape[0]} rows.")
        return df

    def get_funds_data(self) -> pl.DataFrame:
        logger.info("Loading fund equities data...")
        sql = read_file_as_string(file_path=self.sql_path_pub_funds_equities_data).format(dte=self.config.data_date)
        logger.debug(f"Funds SQL:\n{sql}")
        df = execute_query(self.config, sql)
        logger.info(f"Fund data loaded: {df.shape[0]} rows.")
        return df

    def get_active_funds_cfg(self) -> pl.DataFrame:
        logger.info("Loading active funds configuration...")
        sql = read_file_as_string(file_path=self.sql_path_active_funds_cfg).format(dte=self.config.data_date)
        logger.debug(f"Active funds SQL:\n{sql}")
        df = execute_query(self.config, sql)
        logger.info(f"Active funds loaded: {df.shape[0]} rows.")
        return df

    def export_filtered_joins_to_excel_polars(self):
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

import polars as pl
from typing import Optional
from src.base.base_model import BaseModel
from src.helpers.db_utils import execute_query
from src.helpers.logger import LoggerFactory

from src.helpers.utils import read_file_as_string

logger = LoggerFactory.get_logger(__name__)

class GeneratePerfReport(BaseModel):
    """
    Class for generating performance reports for funds.

    This class analyzes fund data to identify the best performing funds for each month
    based on rate of return calculations. It generates an Excel report with the results.
    """

    def __init__(self, app_config):
        """
        Initialize the GeneratePerfReport class.

        Args:
            app_config (AppConfig): Application configuration object containing paths and settings
        """
        super().__init__(app_config)
        self.config = app_config
        self.db_path = self.config.db_path
        self.funds_folder = self.config.funds_folder
        self.sql_path_pub_reference_data = self.config.sql_query_get_pub_reference_data
        self.sql_path_pub_funds_equities_data = self.config.sql_query_get_pub_funds_equities_data
        self.report_perf_path = self.config.report_perf_path
        self.sql_path_active_funds_cfg = self.config.sql_query_get_active_funds_cfg
        self.col_fund_name = "FUND NAME"

    def _compute(self) -> None:
        """
        Execute the performance report generation process.

        This method is called by the BaseModel's run method and serves as the main
        entry point for the report generation process.

        Returns:
            None
        """
        self.generate_best_performing_fund_report()

    def get_funds_data(self) -> pl.DataFrame:
        """
        Retrieve fund data from the database.

        Executes a SQL query to fetch fund equity data from the published tables,
        using the configured SQL query file and data date.

        Returns:
            pl.DataFrame: DataFrame containing fund equity data
        """
        sql = read_file_as_string(file_path=self.sql_path_pub_funds_equities_data).format(dte=self.config.data_date)
        logger.debug(sql)
        df = execute_query(self.config, sql)
        return df

    def generate_best_performing_fund_report(
            self,
            date_col: str = "DATETIME",
            fund_col: str = "FUND NAME",
            mv_col: str = "MARKET VALUE",
            pl_col: str = "REALISED P/L",
            return_df: bool = False
    ) -> Optional[pl.DataFrame]:
        """
        Generate a report of the best performing funds for each month.

        This method calculates the rate of return for each fund in each month by comparing
        the start and end market values and adding realized profit/loss. It then identifies
        the best performing fund for each month and exports the results to an Excel file.

        Args:
            date_col (str, optional): Name of the date column. Defaults to "DATETIME".
            fund_col (str, optional): Name of the fund name column. Defaults to "FUND NAME".
            mv_col (str, optional): Name of the market value column. Defaults to "MARKET VALUE".
            pl_col (str, optional): Name of the profit/loss column. Defaults to "REALISED P/L".
            return_df (bool, optional): Whether to return the results DataFrame. Defaults to False.

        Returns:
            Optional[pl.DataFrame]: DataFrame of best performing funds if return_df is True, None otherwise
        """
        df = self.get_funds_data()
        output_file= self.report_perf_path

        df = df.with_columns(
            pl.col(date_col).cast(pl.Date).alias(date_col)
        )

        df = df.with_columns(
            pl.col(date_col).dt.strftime("%Y-%m").alias("MONTH")
        )

        df_sorted = df.sort([fund_col, "MONTH", date_col])

        fund_month_grouped = df_sorted.group_by([fund_col, "MONTH"], maintain_order=True).agg([
            pl.col(date_col).first().alias("START DATE"),
            pl.col(date_col).last().alias("END DATE"),
            pl.col(mv_col).first().alias("START MV"),
            pl.col(mv_col).last().alias("END MV"),
            pl.col(pl_col).sum().alias("REALIZED PL")
        ])

        fund_month_grouped = fund_month_grouped.with_columns(
            ((pl.col("END MV") - pl.col("START MV") + pl.col("REALIZED PL")) / pl.col("START MV")).alias("RATE OF RETURN")
        )

        best_funds = fund_month_grouped.sort("RATE OF RETURN", descending=True).group_by("MONTH").agg([
            pl.col(fund_col).first().alias("BEST FUND"),
            pl.col("RATE OF RETURN").first(),
            pl.col("START MV").first(),
            pl.col("END MV").first(),
            pl.col("REALIZED PL").first()
        ])

        best_funds.sort("MONTH").to_pandas().to_excel(output_file, index=False, sheet_name="Best Funds")
        logger.info(f"Performance fund report saved to {output_file}")

        if return_df:
            return best_funds
        return None

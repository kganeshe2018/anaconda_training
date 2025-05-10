from pathlib import Path

import polars as pl
import pandas as pd
from typing import List, Union, Optional
from pathlib import Path
from src.app.config import Config
from src.base.base_model import BaseModel
from src.helpers.db_utils import execute_query
from src.helpers.logger import LoggerFactory
from typing import List

from src.helpers.utils import read_file_as_string

logger = LoggerFactory.get_logger(__name__)

class GeneratePerfReport(BaseModel):

    def  __init__(self, app_config: Config):
        super().__init__(app_config)
        self.config = app_config
        self.db_path = Config.DB_PATH
        self.funds_folder = Config.FUNDS_FOLDER
        self.sql_path_pub_reference_data = Config.SQL_QUERY_GET_PUB_REFERENCE_DATA
        self.sql_path_pub_funds_equities_data = Config.SQL_QUERY_GET_PUB_FUNDS_EQUITIES_DATA
        self.report_perf_path = Config.REPORT_PERF_PATH
        self.sql_path_active_funds_cfg = Config.SQL_QUERY_GET_ACTIVE_FUNDS_CFG
        self.col_fund_name = "FUND NAME"

        self.generate_best_performing_fund_report()


    def get_funds_data(self) -> pl.DataFrame:
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
            return_df: bool = False  #
    ) -> Optional[pl.DataFrame]:
        df = self.get_funds_data()
        output_file= self.report_perf_path

        # Step 1: Ensure DATETIME is a proper Date type
        df = df.with_columns(
            pl.col(date_col).cast(pl.Date).alias(date_col)
        )

        # Step 2: Create MONTH column from DATETIME
        df = df.with_columns(
            pl.col(date_col).dt.strftime("%Y-%m").alias("MONTH")
        )

        # Step 3: Sort and group to calculate start/end market values and sum P/L
        df_sorted = df.sort([fund_col, "MONTH", date_col])

        fund_month_grouped = df_sorted.group_by([fund_col, "MONTH"], maintain_order=True).agg([
            pl.col(date_col).first().alias("START DATE"),
            pl.col(date_col).last().alias("END DATE"),
            pl.col(mv_col).first().alias("START MV"),
            pl.col(mv_col).last().alias("END MV"),
            pl.col(pl_col).sum().alias("REALIZED PL")
        ])

        # Step 4: Calculate Rate of Return
        fund_month_grouped = fund_month_grouped.with_columns(
            ((pl.col("END MV") - pl.col("START MV") + pl.col("REALIZED PL")) / pl.col("START MV")).alias("RATE OF RETURN")
        )

        # Step 5: Extract best performing fund per month
        best_funds = fund_month_grouped.sort("RATE OF RETURN", descending=True).group_by("MONTH").agg([
            pl.col(fund_col).first().alias("BEST FUND"),
            pl.col("RATE OF RETURN").first(),
            pl.col("START MV").first(),
            pl.col("END MV").first(),
            pl.col("REALIZED PL").first()
        ])

        # Step 6: Write to Excel
        best_funds.sort("MONTH").to_pandas().to_excel(output_file, index=False, sheet_name="Best Funds")
        logger.info(f"Performance fund report saved to {output_file}")

        if return_df:
            return best_funds
        return None

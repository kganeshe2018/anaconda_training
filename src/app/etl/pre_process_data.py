import polars as pl
from datetime import datetime
from dateutil.relativedelta import relativedelta
from src.base.base_model import BaseModel
from src.helpers.db_utils import execute_query, write_df_to_sqlite, copy_data_from_src_to_tgt
from src.helpers.logger import LoggerFactory
from src.helpers.utils import extract_report_date, get_files_in_directory, read_file_as_string

logger = LoggerFactory.get_logger(__name__)

class PreprocessData(BaseModel):

    def  __init__(self, app_config):
        super().__init__(app_config)
        self.config = app_config
        self.db_path = self.config.db_path
        self.funds_folder = self.config.funds_folder
        self.tbl_raw = "tbl_raw_fund_position_details"
        self.reference_data_query_file_name = self.config.sql_query_get_raw_reference_data

    def _compute(self) -> None:
        self.preprocess_reference_price_data_fix_date()
        self.preprocess_reference_attr()
        self.preprocess_funds()
        pass

    @staticmethod
    def get_month_ends(start: datetime, end: datetime) -> list:
        start = datetime.strptime(str(start), "%Y-%m-%d").date()
        end = datetime.strptime(str(end), "%Y-%m-%d").date()
        current = start.replace(day=1) + relativedelta(months=1) - relativedelta(days=1)
        dates = []
        while current <= end:
            dates.append(current)
            current = (current + relativedelta(days=1)).replace(day=1) + relativedelta(months=1) - relativedelta(days=1)
        return dates

    def upsample_month_end(
            self,
            df: pl.DataFrame,
            datetime_col: str,
            group_cols: list[str]
    ) -> pl.DataFrame:
        df = df.with_columns(pl.col(datetime_col))
        start = df[datetime_col].min()
        end = df[datetime_col].max()

        all_month_ends = self.get_month_ends(start, end)

        generated_rows = []

        for keys, group in df.group_by(group_cols, maintain_order=True):
            key_dict = dict(zip(group_cols, keys))

            # Month-ends already in this group's original data
            existing_dates = group[datetime_col].to_list()
            missing_month_ends = [d for d in all_month_ends if d not in existing_dates]

            if not missing_month_ends:
                continue

            scaffold = pl.DataFrame({
                datetime_col: missing_month_ends
            }).with_columns([
                *[pl.lit(key_dict[col]).alias(col) for col in group_cols]
            ])

            # Append to group and forward fill
            temp = pl.concat([group, scaffold], how="diagonal").sort(datetime_col)
            filled = temp.fill_null(strategy="forward")

            # Extract only the newly added month-end rows
            filtered = filled.filter(pl.col(datetime_col).is_in(scaffold[datetime_col].implode()))
            generated_rows.append(filtered)

        # Combine all results: original + generated
        final = pl.concat([df] + generated_rows, how="diagonal").sort(group_cols + [datetime_col])
        return final

    def preprocess_reference_price_data_fix_date(self) -> None:
        sql = read_file_as_string(self.reference_data_query_file_name)
        df = execute_query(self.config,sql)
        if "DATETIME" not in df.columns:
            raise KeyError("Column 'DATETIME' not found in equity_price table")

        # Step 2: Convert DATETIME to YYYY-MM-DD
        df = df.with_columns(
            pl.col("DATETIME")
            .str.strptime(pl.Date, "%m/%d/%Y", strict=False)
            .cast(pl.Date)
            .alias("DATETIME")
        )

        df_final = self.upsample_month_end(df, datetime_col="DATETIME", group_cols=["SYMBOL"])

        df_final = df_final.with_columns(
            pl.col("DATETIME").dt.strftime("%Y-%m-%d").alias("DATETIME")
        )

        # Step 3: Write cleaned data to staging table
        write_df_to_sqlite(self.config, df_final, "tbl_stg_equity_prices")

        logger.info(f"Inserted {df_final.shape[0]} rows into tbl_stg_equity_prices.")
        return None

    def preprocess_reference_attr(self) -> None:
        copy_data_from_src_to_tgt(self.config, "equity_reference", "tbl_stg_equity_reference")

    def preprocess_funds(self) -> None:
        copy_data_from_src_to_tgt(self.config, "tbl_raw_fund_position_details", "tbl_stg_fund_position_details")
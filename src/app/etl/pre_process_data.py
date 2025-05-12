import polars as pl
from datetime import datetime
from dateutil.relativedelta import relativedelta
from src.base.base_model import BaseModel
from src.helpers.db_utils import execute_query, write_df_to_sqlite, copy_data_from_src_to_tgt
from src.helpers.logger import LoggerFactory
from src.helpers.utils import extract_report_date, get_files_in_directory, read_file_as_string

logger = LoggerFactory.get_logger(__name__)

class PreprocessData(BaseModel):
    """
    Class for preprocessing data before it's used for reporting and analysis.

    This class handles data cleaning, transformation, and preparation tasks such as
    fixing date formats, upsampling data to include month-end dates, and moving data
    from raw tables to staging tables.
    """

    def __init__(self, app_config):
        """
        Initialize the PreprocessData class.

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
        Execute the data preprocessing workflow.

        This method is called by the BaseModel's run method and serves as the main
        entry point for the data preprocessing process. It runs all the preprocessing
        steps in the correct sequence.

        Returns:
            None
        """
        self.preprocess_reference_price_data_fix_date()
        self.preprocess_reference_attr()
        self.preprocess_funds()

    @staticmethod
    def get_month_ends(start: datetime, end: datetime) -> list:
        """
        Generate a list of month-end dates between start and end dates.

        Args:
            start (datetime): Start date
            end (datetime): End date

        Returns:
            list: List of month-end dates between start and end dates
        """
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
        """
        Add missing month-end dates to a DataFrame by forward-filling values.

        This method identifies missing month-end dates in the data and adds new rows
        with forward-filled values from the most recent available date. This ensures
        that every month has an end-of-month value for reporting and analysis.

        Args:
            df (pl.DataFrame): Input DataFrame
            datetime_col (str): Name of the datetime column
            group_cols (list[str]): List of column names to group by

        Returns:
            pl.DataFrame: DataFrame with added month-end dates
        """
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
        """
        Preprocess reference price data by fixing date formats and upsampling month-end dates.

        This method reads reference price data from the database, converts dates to a
        consistent format, adds missing month-end dates using the upsample_month_end method,
        and writes the processed data to the staging table.

        Returns:
            None

        Raises:
            KeyError: If the DATETIME column is not found in the equity_price table
        """
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
        """
        Copy equity reference data from raw to staging table.

        This method copies equity reference data from the raw equity_reference table
        to the staging tbl_stg_equity_reference table without any transformations.

        Returns:
            None
        """
        logger.info("Copying equity reference data to staging table...")
        copy_data_from_src_to_tgt(self.config, "equity_reference", "tbl_stg_equity_reference")
        logger.info("Completed copying equity reference data to staging table.")

    def preprocess_funds(self) -> None:
        """
        Copy fund position data from raw to staging table.

        This method copies fund position details from the raw table to the staging table
        without any transformations.

        Returns:
            None
        """
        logger.info("Copying fund position data to staging table...")
        copy_data_from_src_to_tgt(self.config, "tbl_raw_fund_position_details", "tbl_stg_fund_position_details")
        logger.info("Completed copying fund position data to staging table.")

from pathlib import Path


class Config:
    BASE_DIR = Path(__file__).resolve().parent.parent.parent
    DATA_DATE = '2025-07-01'

    DB_PATH = BASE_DIR / "data/db/app_db.sqlite"
    SQLALCHEMY_DATABASE_URI = f"sqlite:///{DB_PATH}"
    SQLALCHEMY_TRACK_MODIFICATIONS = False

    FUNDS_FOLDER = BASE_DIR / "data/ext-funds"
    REPORT_OUTPUT_FOLDER = BASE_DIR / "outputs"
    REPORT_RECON_PATH = REPORT_OUTPUT_FOLDER / "equities_recon_report.xlsx"
    REPORT_PERF_PATH = REPORT_OUTPUT_FOLDER / "funds_perf_report.xlsx"

    TBL_RAW_FUNDS_DETAILS = "tbl_raw_fund_position_details"

    SQL_QUERY_MASTER_REFERENCE = BASE_DIR / "data/reference-data/master-reference-sql.sql"
    SQL_QUERY_BASE_TABLES = BASE_DIR / "data/reference-data/create-table.sql"
    SQL_QUERY_GET_ACTIVE_FUNDS_CFG = BASE_DIR / "data/sql/query_get_active_funds_cfg.sql"
    SQL_QUERY_GET_RAW_REFERENCE_DATA = BASE_DIR / "data/sql/query_get_raw_reference_data.sql"
    SQL_QUERY_GET_PUB_REFERENCE_DATA = BASE_DIR / "data/sql/query_get_pub_reference_data.sql"
    SQL_QUERY_GET_PUB_FUNDS_EQUITIES_DATA = BASE_DIR / "data/sql/query_get_pub_funds_equities_data.sql"

    def __init__(self):
        self.base_dir = Config.BASE_DIR
        self.db_path = Config.DB_PATH
        self.master_reference_sql_script_path = Config.SQL_QUERY_MASTER_REFERENCE
        self.base_tables_sql_script_path = Config.SQL_QUERY_BASE_TABLES
        self.funds_folder = Config.FUNDS_FOLDER

        self.sql_alchemy_database_uri = Config.SQLALCHEMY_DATABASE_URI
        self.sql_alchemy_track_modifications = Config.SQLALCHEMY_TRACK_MODIFICATIONS
        self.sql_query_get_active_funds = Config.SQL_QUERY_GET_ACTIVE_FUNDS_CFG
        self.sql_query_get_raw_reference_data = Config.SQL_QUERY_GET_RAW_REFERENCE_DATA
        self.sql_query_get_pub_reference_data = Config.SQL_QUERY_GET_PUB_REFERENCE_DATA
        self.sql_query_get_funds_equities_data = Config.SQL_QUERY_GET_PUB_FUNDS_EQUITIES_DATA

        self.report_output_folder = Config.REPORT_OUTPUT_FOLDER
        self.report_recon_path = Config.REPORT_RECON_PATH
        self.report_perf_path = Config.REPORT_PERF_PATH
        self.tbl_raw_funds_details = Config.TBL_RAW_FUNDS_DETAILS
        self.data_date = Config.DATA_DATE

    # --- Getter Methods with Type Hints ---

    def get_base_dir(self) -> Path:
        return self.base_dir

    def get_db_path(self) -> Path:
        return self.db_path

    def get_master_reference_sql_script_path(self) -> Path:
        return self.master_reference_sql_script_path

    def get_base_tables_sql_script_path(self) -> Path:
        return self.base_tables_sql_script_path

    def get_funds_folder(self) -> Path:
        return self.funds_folder

    def get_sqlalchemy_uri(self) -> str:
        return self.sql_alchemy_database_uri

    def get_sqlalchemy_track_modifications(self) -> bool:
        return self.sql_alchemy_track_modifications

    def get_query_get_active_funds_cfg(self) -> Path:
        return self.sql_query_get_active_funds

    def get_query_get_raw_reference_data(self) -> Path:
        return self.sql_query_get_raw_reference_data

    def get_query_get_pub_reference_data(self) -> Path:
        return self.sql_query_get_pub_reference_data

    def get_query_get_pub_funds_equities_data(self) -> Path:
        return self.sql_query_get_funds_equities_data

    def get_report_output_folder(self) -> Path:
        return self.report_output_folder

    def get_report_recon_path(self) -> Path:
        return self.report_recon_path

    def get_report_perf_path(self) -> Path:
        return self.report_perf_path

    def get_tbl_raw_funds_details(self) -> str:
        return self.tbl_raw_funds_details

    def get_data_date(self) -> str:
        return self.data_date

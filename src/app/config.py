from pathlib import Path

class Config:
    BASE_DIR = Path(__file__).resolve().parent.parent.parent
    DB_PATH = BASE_DIR / "data/db/app_db.sqlite"
    MASTER_REFERENCE_SQL_SCRIPT_PATH = BASE_DIR / "data/reference-data/master-reference-sql.sql"
    BASE_TABLES_SQL_SCRIPT_PATH = BASE_DIR / "data/reference-data/create-table.sql"
    FUNDS_FOLDER = BASE_DIR / "data/ext-funds"
    SQLALCHEMY_DATABASE_URI = f"sqlite:///{DB_PATH}"
    SQLALCHEMY_TRACK_MODIFICATIONS = False
    SQL_QUERY_GET_ACTIVE_FUNDS_CFG = BASE_DIR / "data/sql/query_get_active_funds_cfg.sql"
    SQL_QUERY_GET_RAW_REFERENCE_DATA = BASE_DIR / "data/sql/query_get_raw_reference_data.sql"
    SQL_QUERY_GET_PUB_REFERENCE_DATA = BASE_DIR / "data/sql/query_get_pub_reference_data.sql"
    SQL_QUERY_GET_PUB_FUNDS_EQUITIES_DATA = BASE_DIR / "data/sql/query_get_pub_funds_equities_data.sql"
    REPORT_OUTPUT_FOLDER = BASE_DIR / "outputs"
    REPORT_RECON_PATH = REPORT_OUTPUT_FOLDER / "equities_recon_report.xlsx"
    REPORT_PERF_PATH = REPORT_OUTPUT_FOLDER / "funds_perf_report.xlsx"


    def __init__(self):
        self.base_dir = Config.BASE_DIR
        self.db_path = Config.DB_PATH
        self.master_reference_sql_script_path = Config.MASTER_REFERENCE_SQL_SCRIPT_PATH
        self.base_tables_sql_script_path = Config.BASE_TABLES_SQL_SCRIPT_PATH
        self.funds_folder = Config.FUNDS_FOLDER
        self.sql_alchemy_database_uri = Config.SQLALCHEMY_DATABASE_URI
        self.sql_alchemy_track_modifications = Config.SQLALCHEMY_TRACK_MODIFICATIONS
        self.sql_query_get_active_funds = Config.SQL_QUERY_GET_ACTIVE_FUNDS_CFG
        self.sql_query_get_reference_data = Config.SQL_QUERY_GET_RAW_REFERENCE_DATA
        self.data_date = '2025-07-01'
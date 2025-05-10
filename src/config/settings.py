# src/config/settings.py
from pathlib import Path
from pydantic import  Field
from pydantic_settings import BaseSettings
BASE_DIR = Path(__file__).resolve().parent.parent.parent

class AppConfig(BaseSettings):
    data_date: str = Field(..., validation_alias="DATA_DATE")
    db_path: Path = Field(..., validation_alias="DB_PATH")
    funds_folder: Path = Field(..., validation_alias="FUNDS_FOLDER")
    report_output_folder: Path = Field(..., validation_alias="REPORT_OUTPUT_FOLDER")
    report_recon_path: Path = Field(..., validation_alias="REPORT_RECON_PATH")
    report_perf_path: Path = Field(..., validation_alias="REPORT_PERF_PATH")
    tbl_raw_funds_details: str = Field(..., validation_alias="TBL_RAW_FUNDS_DETAILS")

    sql_query_master_reference: Path = Field(..., validation_alias="SQL_QUERY_MASTER_REFERENCE")
    sql_query_base_tables: Path = Field(..., validation_alias="SQL_QUERY_BASE_TABLES")
    sql_query_get_active_funds_cfg: Path = Field(..., validation_alias="SQL_QUERY_GET_ACTIVE_FUNDS_CFG")
    sql_query_get_raw_reference_data: Path = Field(..., validation_alias="SQL_QUERY_GET_RAW_REFERENCE_DATA")
    sql_query_get_pub_reference_data: Path = Field(..., validation_alias="SQL_QUERY_GET_PUB_REFERENCE_DATA")
    sql_query_get_pub_funds_equities_data: Path = Field(..., validation_alias="SQL_QUERY_GET_PUB_FUNDS_EQUITIES_DATA")

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.db_path = BASE_DIR / self.db_path
        self.funds_folder = BASE_DIR / self.funds_folder
        self.report_output_folder = BASE_DIR / self.report_output_folder
        self.report_recon_path = BASE_DIR / self.report_recon_path
        self.report_perf_path = BASE_DIR / self.report_perf_path
        self.sql_query_master_reference = BASE_DIR / self.sql_query_master_reference
        self.sql_query_base_tables = BASE_DIR / self.sql_query_base_tables
        self.sql_query_get_active_funds_cfg = BASE_DIR / self.sql_query_get_active_funds_cfg
        self.sql_query_get_raw_reference_data = BASE_DIR / self.sql_query_get_raw_reference_data
        self.sql_query_get_pub_reference_data = BASE_DIR / self.sql_query_get_pub_reference_data
        self.sql_query_get_pub_funds_equities_data = BASE_DIR / self.sql_query_get_pub_funds_equities_data

    model_config = {
        "env_file": ".env"
    }

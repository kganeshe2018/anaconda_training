from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent.parent

DB_PATH = BASE_DIR / "data/db/app_db.sqlite"
MASTER_REFERENCE_SQL_SCRIPT_PATH = BASE_DIR / "data/reference-data/master-reference-sql.sql"
BASE_TABLES_SQL_SCRIPT_PATH = BASE_DIR / "data/reference-data/create-table.sql"
FUNDS_FOLDER = BASE_DIR / "data/ext-funds"

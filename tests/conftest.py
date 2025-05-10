# tests/conftest.py
from pathlib import Path
from unittest.mock import MagicMock

import pytest
import polars as pl

@pytest.fixture
def mock_config():
    config = MagicMock()
    config.data_date = "2024-01-31"
    config.BASE_DIR = Path(__file__).resolve().parent.parent
    config.DB_PATH = config.BASE_DIR / "data/db/app_db.sqlite"

    config.FUNDS_FOLDER = "test-data/"
    config.SQL_QUERY_GET_PUB_REFERENCE_DATA = "ref.sql"
    config.SQL_QUERY_GET_PUB_FUNDS_EQUITIES_DATA = "fund.sql"
    config.SQL_QUERY_GET_ACTIVE_FUNDS_CFG = "active.sql"
    config.REPORT_PERF_PATH = "tests/output/perf_report.xlsx"
    return config

@pytest.fixture
def sample_fund_data():
    return pl.DataFrame({
        "DATETIME": ["2024-01-01", "2024-01-31", "2024-01-15", "2024-01-31"],
        "FUND NAME": ["Alpha", "Alpha", "Beta", "Beta"],
        "MARKET VALUE": [1000, 1200, 1000, 1100],
        "REALISED P/L": [100, 0, 50, 0]
    })

@pytest.fixture
def sample_reference_df():
    return pl.DataFrame({
        "DATETIME": ["01/31/2024", "02/29/2024"],
        "SYMBOL": ["AAPL", "AAPL"],
        "PRICE": [150.0, 155.0]
    })


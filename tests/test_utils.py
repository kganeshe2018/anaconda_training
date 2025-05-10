import pytest
from src.app.config import Config
from src.helpers.utils import (_extract_mm_dd_yyyy, _extract_yyyy_mm_dd, _extract_yyyymmdd, _extract_dd_mm_yyyy, extract_report_date,)
from src.app.etl.pre_process_data import PreprocessData
import polars as pl

def test_extract_dd_mm_yyyy():
    assert _extract_dd_mm_yyyy("mend-report Wallington.30_11_2022.csv") == "2022-11-30"
    assert _extract_dd_mm_yyyy("lala.30/11/20221.txt") == "2022-11-30"
    assert _extract_dd_mm_yyyy("incorrectFileType.txt") is None


def test_extract_mm_dd_yyyy():
    assert _extract_mm_dd_yyyy("Report-of-Gohen.11-30-2022 breakdown.csv") == "2022-11-30"
    assert _extract_mm_dd_yyyy("lala.12-01-2021.txt") == "2021-12-01"
    assert _extract_mm_dd_yyyy("incorrectFileType.txt") is None

def test_extract_yyyy_mm_dd():
    assert _extract_yyyy_mm_dd("Report-of-Gohen.2022-11-30 .csv") == "2022-11-30"
    assert _extract_yyyy_mm_dd("lala .2022-11-30.csv") == "2022-11-30"
    assert _extract_yyyy_mm_dd("lala .2022_11_30.csv") == "2022-11-30"
    assert _extract_yyyy_mm_dd("incorrectFileType.gg") is None

def test_extract_yyyymmdd():
    assert _extract_yyyymmdd("TT_monthly_Trustmind.20220831.csv") == "2022-08-31"
    assert _extract_yyyymmdd("TT_monthly_Trustmind.20220831 - lala.csv") == "2022-08-31"
    assert _extract_yyyymmdd("backup_20230501.zip") == "2023-05-01"
    assert _extract_yyyymmdd("incorrectFileType.docx") is None

def test_extract_report_date():
    assert extract_report_date("Report-of-Gohen.11-30-2022.csv") == "2022-11-30"
    assert extract_report_date("rpt-Catalysm.2022-09-30.csv") == "2022-09-30"
    assert extract_report_date("TT_monthly_Trustmind.20220831.csv") == "2022-08-31"
    assert extract_report_date("incorrectFileType.txt") is None

def test_upsample_month_end():
    preprocessing_model = PreprocessData(Config())

    # Input: Original data
    df = pl.DataFrame({
        "DATETIME": ["2023-02-15", "2023-02-28", "2023-03-10", "2023-04-30"],
        "FUND_NAME": ["Applebead"] * 4,
        "SYMBOL": ["TJX"] * 4,
        "PRICE": [95, 100, 102, 110]
    }).with_columns(pl.col("DATETIME").cast(pl.Date))


    # Apply function
    result = preprocessing_model.upsample_month_end(
        df,
        datetime_col="DATETIME",
        group_cols=["FUND_NAME", "SYMBOL"]
    )

    # Convert to dict for easy assertions
    result_dict = {
        row["DATETIME"].strftime("%Y-%m-%d") if hasattr(row["DATETIME"], "strftime") else row["DATETIME"]: row["PRICE"]
        for row in result.to_dicts()
    }
    # 1. Original dates must be present
    for date_str in ["2023-02-15", "2023-02-28", "2023-03-10", "2023-04-30"]:
        assert date_str in result_dict

    # 2. Expected generated month-end: 2023-03-31 (forward-fill from 2023-03-10)
    assert "2023-03-31" in result_dict
    assert result_dict["2023-03-31"] == 102

    # 3. No overwriting of original values
    assert result_dict["2023-03-10"] == 102
    assert result_dict["2023-02-15"] == 95

    # 4. Final row count = original rows + missing month-ends (1 added row)
    assert result.shape[0] == 5
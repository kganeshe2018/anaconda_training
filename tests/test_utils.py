from unittest.mock import patch, MagicMock
import pytest

from app.etl.generate_perf_report import GeneratePerfReport
from helpers.utils import (_extract_mm_dd_yyyy, _extract_yyyy_mm_dd, _extract_yyyymmdd, _extract_dd_mm_yyyy, extract_report_date,)
from app.etl.pre_process_data import PreprocessData
from app.etl.publish_data import PublishData

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

def test_upsample_month_end(mock_config):
    preprocessing_model = PreprocessData(app_config=mock_config)

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

@patch("app.etl.pre_process_data.read_file_as_string")
@patch("app.etl.pre_process_data.execute_query")
@patch("app.etl.pre_process_data.write_df_to_sqlite")
def test_preprocess_reference_price_data_fix_date(
    mock_write,
    mock_execute,
    mock_read_file,
    mock_config,
        sample_reference_df
):
    # Setup mock returns
    mock_read_file.return_value = "SELECT * FROM equity_price"
    mock_execute.return_value = sample_reference_df

    # Create instance
    preprocessor = PreprocessData(app_config=mock_config)
    preprocessor.reference_data_query_file_name = "dummy.sql"

    # Mock upsample method
    preprocessor.upsample_month_end = MagicMock(return_value=sample_reference_df.with_columns(
        pl.col("DATETIME").str.strptime(pl.Date, "%m/%d/%Y").cast(pl.Date)
    ))

    # Run method
    preprocessor.preprocess_reference_price_data_fix_date()

    # Assertions
    mock_read_file.assert_called_once_with("dummy.sql")
    mock_execute.assert_called_once()
    mock_write.assert_called_once()

    # Check if DATETIME was converted to YYYY-MM-DD format
    written_df = mock_write.call_args[0][1]  # second arg is the DataFrame
    assert "DATETIME" in written_df.columns
    assert written_df["DATETIME"].dtype == pl.Utf8
    assert all(len(str(date)) == 10 for date in written_df["DATETIME"])  # YYYY-MM-DD

def test_generate_best_performing_fund_logic_exact(mock_config, sample_fund_data):
    mock_config.REPORT_PERF_PATH = "test_outputs/fund_report.xlsx"
    report = GeneratePerfReport(mock_config)

    report.get_funds_data = lambda: sample_fund_data

    result_df = report.generate_best_performing_fund_report(return_df=True)

    assert isinstance(result_df, pl.DataFrame)
    assert result_df.shape[0] == 1

    best_fund = result_df[0, "BEST FUND"]
    ror = result_df[0, "RATE OF RETURN"]

    assert best_fund == "Alpha"
    assert abs(ror - 0.3) < 1e-6

@patch("src.app.etl.generate_perf_report.GeneratePerfReport.generate_best_performing_fund_report")
def test_compute_calls_generate_report(mock_generate, mock_config):
    with patch.object(GeneratePerfReport, "generate_best_performing_fund_report") as mock_generate:
        report = GeneratePerfReport(mock_config)
        report._compute()
        mock_generate.assert_called_once()
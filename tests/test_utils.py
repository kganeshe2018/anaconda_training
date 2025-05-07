import pytest
from src.helpers.utils import (extract_mm_dd_yyyy,extract_yyyy_mm_dd,extract_yyyymmdd,extract_report_date)

def test_extract_mm_dd_yyyy():
    assert extract_mm_dd_yyyy("Report-of-Gohen.11-30-2022.csv") == "2022-11-30"
    assert extract_mm_dd_yyyy("lala.12-01-2021.txt") == "2021-12-01"
    assert extract_mm_dd_yyyy("incorrectFileType.txt") is None

def test_extract_yyyy_mm_dd():
    assert extract_yyyy_mm_dd("Report-of-Gohen.2022-11-30.csv") == "2022-11-30"
    assert extract_yyyy_mm_dd("lala.2022-11-30.csv") == "2022-11-30"
    assert extract_yyyy_mm_dd("incorrectFileType.gg") is None

def test_extract_yyyymmdd():
    assert extract_yyyymmdd("TT_monthly_Trustmind.20220831.csv") == "2022-08-31"
    assert extract_yyyymmdd("backup_20230501.zip") == "2023-05-01"
    assert extract_yyyymmdd("incorrectFileType.docx") is None

def test_extract_report_date():
    assert extract_report_date("Report-of-Gohen.11-30-2022.csv") == "2022-11-30"
    assert extract_report_date("rpt-Catalysm.2022-09-30.csv") == "2022-09-30"
    assert extract_report_date("TT_monthly_Trustmind.20220831.csv") == "2022-08-31"
    assert extract_report_date("incorrectFileType.txt") is None
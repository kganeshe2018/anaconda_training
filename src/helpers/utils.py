# src/utils.py
import re
from datetime import datetime
from typing import Optional

def extract_mm_dd_yyyy(filename: str) -> Optional[str]:
    match = re.search(r'(\d{2})-(\d{2})-(\d{4})', filename)
    if match:
        try:
            return datetime.strptime(match.group(0), "%m-%d-%Y").strftime("%Y-%m-%d")
        except ValueError:
            return None
    return None

def extract_yyyy_mm_dd(filename: str) -> Optional[str]:
    match = re.search(r'(\d{4})-(\d{2})-(\d{2})', filename)
    if match:
        try:
            return datetime.strptime(match.group(0), "%Y-%m-%d").strftime("%Y-%m-%d")
        except ValueError:
            return None
    return None

def extract_yyyymmdd(filename: str) -> Optional[str]:
    match = re.search(r'(\d{8})', filename)
    if match:
        try:
            return datetime.strptime(match.group(0), "%Y%m%d").strftime("%Y-%m-%d")
        except ValueError:
            return None
    return None

def extract_report_date(filename: str) -> Optional[str]:
    for extractor in [extract_mm_dd_yyyy, extract_yyyy_mm_dd, extract_yyyymmdd]:
        date = extractor(filename)
        if date:
            return date
    return None

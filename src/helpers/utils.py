import re
from datetime import datetime
from typing import Optional
from pathlib import Path

from src.helpers.db_utils import execute_query
from src.app.config import Config
from src.helpers.logger import LoggerFactory

logger = LoggerFactory.get_logger(__name__)

__all__ = ["extract_report_date","get_files_in_directory","read_file_as_string"]

def _extract_dd_mm_yyyy(filename: str) -> Optional[str]:
    match = re.search(r'(\d{2})[-_/\\|](\d{2})[-_/\\|](\d{4})', filename)
    if match:
        day, month, year = match.groups()
        try:
            return datetime.strptime(f"{day}-{month}-{year}", "%d-%m-%Y").strftime("%Y-%m-%d")
        except ValueError:
            return None
    return None


def _extract_mm_dd_yyyy(filename: str) -> Optional[str]:
    match = re.search(r'(\d{2})[-_/\\|](\d{2})[-_/\\|](\d{4})', filename)
    if match:
        month, day, year = match.groups()
        try:
            return datetime.strptime(f"{day}-{month}-{year}", "%d-%m-%Y").strftime("%Y-%m-%d")
        except ValueError:
            return None
    return None

def _extract_yyyy_mm_dd(filename: str) -> Optional[str]:
    match = re.search(r'(\d{4})[-_/\\|](\d{2})[-_/\\|](\d{2})', filename)
    if match:
        year, month, day = match.groups()
        try:
            return datetime.strptime(f"{day}-{month}-{year}", "%d-%m-%Y").strftime("%Y-%m-%d")
        except ValueError:
            return None
    return None

def _extract_yyyymmdd(filename: str) -> Optional[str]:
    match = re.search(r'(\d{8})', filename)
    if match:
        try:
            return datetime.strptime(match.group(0), "%Y%m%d").strftime("%Y-%m-%d")
        except ValueError:
            return None
    return None

def extract_report_date(filename: str) -> Optional[str]:
    for extractor in [_extract_mm_dd_yyyy, _extract_yyyy_mm_dd, _extract_yyyymmdd,_extract_dd_mm_yyyy]:
        date = extractor(filename)
        if date:
            return date
    return None

def read_file_as_string(file_path: str | Path) -> str:
    file_path = Path(file_path)
    if not file_path.exists():
        raise FileNotFoundError(f"File not found: {file_path}")

    with file_path.open("r", encoding="utf-8") as f:
        return f.read()


def get_files_in_directory(directory: str | Path, extension: str = "*") -> list[Path]:
    directory = Path(directory)
    if not directory.is_dir():
        raise NotADirectoryError(f"{directory} is not a valid directory.")

    return list(directory.glob(extension))
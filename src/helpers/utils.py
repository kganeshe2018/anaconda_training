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
    """
    Extract a date from a filename in dd-mm-yyyy format and convert to yyyy-mm-dd.
    
    Args:
        filename (str): Filename containing a date in dd-mm-yyyy format
        
    Returns:
        Optional[str]: Formatted date as yyyy-mm-dd if found and valid, None otherwise
    """
    match = re.search(r'(\d{2})[-_/\\|](\d{2})[-_/\\|](\d{4})', filename)
    if match:
        day, month, year = match.groups()
        try:
            return datetime.strptime(f"{day}-{month}-{year}", "%d-%m-%Y").strftime("%Y-%m-%d")
        except ValueError:
            return None
    return None


def _extract_mm_dd_yyyy(filename: str) -> Optional[str]:
    """
    Extract a date from a filename in mm-dd-yyyy format and convert to yyyy-mm-dd.
    
    Args:
        filename (str): Filename containing a date in mm-dd-yyyy format
        
    Returns:
        Optional[str]: Formatted date as yyyy-mm-dd if found and valid, None otherwise
    """
    match = re.search(r'(\d{2})[-_/\\|](\d{2})[-_/\\|](\d{4})', filename)
    if match:
        month, day, year = match.groups()
        try:
            return datetime.strptime(f"{day}-{month}-{year}", "%d-%m-%Y").strftime("%Y-%m-%d")
        except ValueError:
            return None
    return None

def _extract_yyyy_mm_dd(filename: str) -> Optional[str]:
    """
    Extract a date from a filename in yyyy-mm-dd format and convert to yyyy-mm-dd.
    
    Args:
        filename (str): Filename containing a date in yyyy-mm-dd format
        
    Returns:
        Optional[str]: Formatted date as yyyy-mm-dd if found and valid, None otherwise
    """
    match = re.search(r'(\d{4})[-_/\\|](\d{2})[-_/\\|](\d{2})', filename)
    if match:
        year, month, day = match.groups()
        try:
            return datetime.strptime(f"{day}-{month}-{year}", "%d-%m-%Y").strftime("%Y-%m-%d")
        except ValueError:
            return None
    return None

def _extract_yyyymmdd(filename: str) -> Optional[str]:
    """
    Extract a date from a filename in yyyymmdd format and convert to yyyy-mm-dd.
    
    Args:
        filename (str): Filename containing a date in yyyymmdd format
        
    Returns:
        Optional[str]: Formatted date as yyyy-mm-dd if found and valid, None otherwise
    """
    match = re.search(r'(\d{8})', filename)
    if match:
        try:
            return datetime.strptime(match.group(0), "%Y%m%d").strftime("%Y-%m-%d")
        except ValueError:
            return None
    return None

def extract_report_date(filename: str) -> Optional[str]:
    """
    Extract a date from a filename by trying multiple date formats.
    
    Attempts to extract dates in the following formats:
    - mm-dd-yyyy
    - yyyy-mm-dd
    - yyyymmdd
    - dd-mm-yyyy
    
    Args:
        filename (str): Filename that may contain a date
        
    Returns:
        Optional[str]: Formatted date as yyyy-mm-dd if found and valid, None otherwise
    """
    for extractor in [_extract_mm_dd_yyyy, _extract_yyyy_mm_dd, _extract_yyyymmdd,_extract_dd_mm_yyyy]:
        date = extractor(filename)
        if date:
            return date
    return None

def read_file_as_string(file_path: str | Path) -> str:
    """
    Read the contents of a file and return it as a string.
    
    Args:
        file_path (str | Path): Path to the file to read
        
    Returns:
        str: Contents of the file as a string
        
    Raises:
        FileNotFoundError: If the file does not exist
    """
    file_path = Path(file_path)
    if not file_path.exists():
        raise FileNotFoundError(f"File not found: {file_path}")

    with file_path.open("r", encoding="utf-8") as f:
        return f.read()


def get_files_in_directory(directory: str | Path, extension: str = "*") -> list[Path]:
    """
    Get a list of files in a directory, optionally filtered by extension.
    
    Args:
        directory (str | Path): Directory to search for files
        extension (str, optional): File extension pattern to filter by. Defaults to "*".
        
    Returns:
        list[Path]: List of Path objects for files in the directory
        
    Raises:
        NotADirectoryError: If the directory does not exist or is not a directory
    """
    directory = Path(directory)
    if not directory.is_dir():
        raise NotADirectoryError(f"{directory} is not a valid directory.")

    return list(directory.glob(extension))
import logging
from pathlib import Path

class LoggerFactory:
    @staticmethod
    def get_logger(name: str, log_file: str = None, level=logging.INFO) -> logging.Logger:
        logger = logging.getLogger(name)
        logger.setLevel(level)

        # Prevent adding multiple handlers during repeated calls
        if not logger.handlers:
            formatter = logging.Formatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s")

            # Console handler
            console_handler = logging.StreamHandler()
            console_handler.setFormatter(formatter)
            logger.addHandler(console_handler)

            # Optional file handler
            if log_file:
                log_path = Path(log_file)
                log_path.parent.mkdir(parents=True, exist_ok=True)
                file_handler = logging.FileHandler(log_file)
                file_handler.setFormatter(formatter)
                logger.addHandler(file_handler)

        return logger

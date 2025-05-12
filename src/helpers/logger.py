import logging
from pathlib import Path

class LoggerFactory:
    """
    Factory class for creating and configuring loggers.
    
    This class provides a centralized way to create logger instances with consistent
    formatting and output handling.
    """
    
    @staticmethod
    def get_logger(name: str, log_file: str = None, level=logging.INFO) -> logging.Logger:
        """
        Create or retrieve a configured logger instance.
        
        Creates a logger with the specified name and configuration. If the logger already exists,
        it returns the existing instance. The logger is configured with a console handler and
        optionally a file handler if log_file is specified.
        
        Args:
            name (str): Name of the logger
            log_file (str, optional): Path to the log file. If provided, logs will be written to this file.
            level: Logging level (e.g., logging.INFO, logging.DEBUG). Defaults to logging.INFO.
            
        Returns:
            logging.Logger: Configured logger instance
        """
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

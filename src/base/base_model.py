from src.helpers.logger import LoggerFactory
from src.app.config import Config
logger = LoggerFactory.get_logger(__name__)

class BaseModel:
    """
    Base class for all data processing models in the application.
    
    This class provides a common structure and interface for all models,
    ensuring consistency in how they're initialized and executed.
    """
    
    def __init__(self, app_config: Config):
        """
        Initialize the base model with configuration.
        
        Args:
            app_config (Config): Application configuration object containing settings
                                 and parameters for the model
        """
        logger.info("Initializing BaseModel...")
        self.app_config = app_config

    def _compute(self) -> None:
        """
        Abstract method for implementing the model's computation logic.
        
        This method should be overridden by subclasses to implement their specific
        data processing logic.
        
        Returns:
            None
        """
        pass

    def run(self):
        """
        Execute the model's computation logic.
        
        This method serves as the public interface for running the model's
        processing logic by calling the _compute method.
        
        Returns:
            Any value returned by the _compute method
        """
        return self._compute()
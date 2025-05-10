from src.helpers.logger import LoggerFactory
from src.app.config import Config
logger = LoggerFactory.get_logger(__name__)

class BaseModel:
    def __init__(self, app_config: Config ):
        logger.info("Initializing BaseModel...")
        self.app_config = app_config

    def _compute(self) -> None:
        pass

    def run(self):
        self._compute()
from src.app.etl.generate_perf_report import GeneratePerfReport
from src.app.etl.generate_recon_report import GenerateReconReport
from src.app.etl.pre_process_data import PreprocessData
from src.app.etl.publish_data import PublishData
from src.db.db_setup import SetupDB
from src.app.etl.ingest_external_data import IngestFundsData
from src.helpers.logger import LoggerFactory
from src.config.settings import AppConfig

logger = LoggerFactory.get_logger("Main")

def init():
    settings = AppConfig()
    models = [
        SetupDB(settings),
        IngestFundsData(settings),
        PreprocessData(settings),
        PublishData(settings),
        GenerateReconReport(settings),
        GeneratePerfReport(settings)
    ]
    [model.run() for model in models]

if __name__ == "__main__":
    init()

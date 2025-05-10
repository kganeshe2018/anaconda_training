from src.app.etl.generate_perf_report import GeneratePerfReport
from src.app.etl.generate_recon_report import GenerateReconReport
from src.app.etl.pre_process_data import PreprocessData
from src.app.etl.publish_data import PublishData
from src.db.db_setup import SetupDB
from src.app.etl.ingest_external_data import IngestFundsData
from src.helpers.logger import LoggerFactory
from src.app.config import Config

logger = LoggerFactory.get_logger("Main")

def init():
    app_config = Config()
    models = [
        SetupDB(app_config),
        IngestFundsData(app_config),
        PreprocessData(app_config),
        PublishData(app_config),
        GenerateReconReport(app_config),
        GeneratePerfReport(app_config)
    ]
    [model.run() for model in models]

if __name__ == "__main__":
    init()
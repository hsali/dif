import logging
from pyspark.sql import SparkSession

# logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Define the ETL class


class BaseETL:
    def __init__(self, pipeline_name, app_name="DIF ETL Job"):
        self.spark = None
        self.app_name = app_name

    def initialize_spark(self):
        self.spark = SparkSession.builder.appName(self.app_name).getOrCreate()

    def initialize_logger(self):
        logging.basicConfig(level=logging.INFO)
        logger = logging.getLogger(__name__)

    def extract(self, source_path):
        logger.info("Extracting data from source: {}".format(source_path))
        # Add your code to extract data from the source

    def transform(self):
        logger.info("Transforming data")
        # Add your code to transform the extracted data

    def load(self, target_path):
        logger.info("Loading data to target: {}".format(target_path))
        # Add your code to load the transformed data to the target

    def run(self, source_path, target_path):
        self.initialize_spark()
        self.initialize_logger()

        self.extract(source_path)
        self.transform()
        self.load(target_path)

        self.spark.stop()


if __name__ == "__main__":
    etl = ETL()
    etl.run("/path/to/source", "/path/to/target")

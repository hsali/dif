import logging
import copy

# from pyspark.sql import SparkSession

# logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Define the ETL class
# job -. read source -> transform(s) -> write to target
# Jobs - job1, job2, job3


class CustomLoggerAdapter(logging.LoggerAdapter):
    def process(self, msg, kwargs):
        return "[%s] %s" % (self.extra["prefix"], msg), kwargs


class BaseETLJob:
    def __init__(self, config, app_name="DIF ETL Job"):
        self.spark = None
        self.app_name = app_name
        self.config = config
        self.df = None
        self.df_name = "df"

    def initialize_spark(self):
        self.spark = SparkSession.builder.appName(self.app_name).getOrCreate()

    def initialize_logger(self):
        logging.basicConfig(level=logging.INFO)
        logger = logging.getLogger(__name__)

    def extract(self, source):
        logger.info("Extracting data from source: {}".format(source))
        # Add your code to extract data from the source
        self.df = (
            self.spark.read.format(source["type"])
            .options(**source.get("options"))
            .load()
        )
        self.spark.registerDataFrameAsTable(self.df, self.df_name)

    def transform(self, transformations):
        if len(transformations) == 0:
            logger.info("No transformations to perform")
            return
        logger.info(f"Transforming data {transformations}")

        # Add your code to transform the extracted data
        transformations_meta = copy.deepcopy(transformations)
        for transformation in transformations_meta:
            # logger prefix append based on transformation["name"]
            adapter = CustomLoggerAdapter(logger, {"prefix": transformation["name"]})
            adapter.info(f"Transforming data using SQL: {transformation['sql']}")
            self.df = self.spark.sql(transformation["sql"])
            transformation["count"] = self.df.count()
            transformation["columns"] = self.df.columns
            transformation["schema"] = self.df.schema
            # print df schema and count
            adapter.info(f"Schema: {self.df.schema}")
            adapter.info(f"Count: {transformation['count']}")
            adapter.info(f"Columns: {transformation['columns']}")
            adapter.verbose("Data Preview:")
            adapter.verbose(f"{self.df.show()}")
            self.spark.registerDataFrameAsTable(self.df, transformation["name"])
        # logger prefix removed based on transformation["name"]

    def load(self, sink):
        logger.info("Loading data to target: {}".format(sink))
        # print options in beautify format
        logger.info(f"Options: {sink.get('options')}")
        # Add your code to load the transformed data to the target
        self.df.write.format(sink["type"]).options(**sink.get("options")).save()

    def run(self, job_name):
        # self.initialize_spark()
        self.load_configuration()
        self.extract(self.config.etl_config[job_name]["source"])
        self.transform(self.config.etl_config[job_name].get("transformations"))
        self.load(self.config.etl_config[job_name]["sink"])
        self.spark.stop()


def load_configuration(pipeline_name="pipelines/customer_table/customer_table.py"):
    # Load the configuration
    import importlib.util

    spec = importlib.util.spec_from_file_location("etl_config", pipeline_name)
    config = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(config)
    return config


def configuration_validator(config):
    # Validate the configuration
    return config


if __name__ == "__main__":

    etl_config = load_configuration()
    etl_config = configuration_validator(etl_config)
    etl = BaseETLJob(etl_config)
    etl.run("job1")
    etl.run("job2")

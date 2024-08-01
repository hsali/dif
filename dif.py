import logging
import copy
import configs

from pyspark.sql import SparkSession

# logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Define the ETL class
# job -. read source -> transform(s) -> write to target
# Jobs - job1, job2, job3


class CustomLoggerAdapter(logging.LoggerAdapter):
    def process(self, msg, kwargs):
        return "[%s] %s" % (self.extra["prefix"], msg), kwargs


class SingletonMeta(type):
    """
    The Singleton class can be implemented in different ways in Python. Some
    possible methods include: base class, decorator, metaclass. We will use the
    metaclass because it is best suited for this purpose.
    """

    _instances = {}

    def __call__(cls, *args, **kwargs):
        """
        Possible changes to the value of the `__init__` argument do not affect
        the returned instance.
        """
        if cls not in cls._instances:
            instance = super().__call__(*args, **kwargs)
            cls._instances[cls] = instance
        return cls._instances[cls]


class SparkSingletonInstance(metaclass=SingletonMeta):
    def some_business_logic(self):
        """
        Finally, any singleton should define some business logic, which can be
        executed on its instance.
        """
        # ...
        pass

    def __init__(self, app_name="DIF ETL Job") -> None:
        logger.info("Initializing Spark Singleton Instance")
        self.app_name = app_name
        self.spark = SparkSession.builder.appName(self.app_name).getOrCreate()
        self.sc = self.spark.sparkContext
        self.sqlContext = self.spark.sqlContext


class BaseETLJob:
    def __init__(self, config, app_name="DIF ETL Job"):

        self.sparkInstance = SparkSingletonInstance()
        self.spark = self.sparkInstance.spark
        self.app_name = app_name
        self.config = config
        self.df = None
        self.df_name = "df"
        self.shared_context = {}

    def initialize_spark(self):
        self.spark = SparkSession.builder.appName(self.app_name).getOrCreate()

    def initialize_logger(self):
        logging.basicConfig(level=logging.INFO)
        logger = logging.getLogger(__name__)

    def extract(self, source, extract_callback=None):
        logger.info("Extracting data from source: {}".format(source))

        if extract_callback:
            logger.info("Extracting data using custom callback function")
            self.df = extract_callback(self, source=source)
            logger.info("Extracted data using custom callback function")
        else:
            self.df = (
                self.spark.read.format(source["type"])
                .options(**source.get("options"))
                .load()
            )
        self.spark.registerDataFrameAsTable(self.df, self.df_name)

    def transform(self, transformations, transform_callback=None):
        if transform_callback:
            logger.info("Transforming data using custom callback function")
            self.df = transform_callback(self, transformations=transformations)
            logger.info("Transformed data using custom callback function")
            return
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

    def load(self, sink, load_callback=None):
        logger.info("Loading data to target: {}".format(sink))
        # print options in beautify format
        logger.info(f"Options: {sink.get('options')}")
        # Add your code to load the transformed data to the target'
        if load_callback:
            logger.info("Loading data using custom callback function")
            load_callback(self, sink=sink)
            logger.info("Loaded data using custom callback function")
            return
        if sink is dict:
            logger.info("Loading data using default callback function")
            # handle partitions and bucketing
            self.df.write.format(sink["type"]).options(**sink.get("options")).save()
            logger.info("Loaded data using default callback function")
        elif sink is list:
            for sink_config in sink:
                adapter = CustomLoggerAdapter(logger, {"prefix": sink_config["name"]})
                adapter.info(f"sink_options: {sink_config}")
                adapter.info(f"Loading data using default callback function")
                self.df.write.format(sink_config["type"]).options(
                    **sink_config.get("options")
                ).save()
                adapter.info(f"Loaded data using default callback function")

    def run(self, job_name):
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
    logger.info("Validating the configuration")
    # print configuration dict in beautify format
    logger.info(f"Configuration: {config}")
    # Check if the configuration has the required attributes
    if not hasattr(config, "etl_config"):
        raise ValueError("Configuration must contain 'etl_config' attribute")
    if not isinstance(config.etl_config, dict):
        raise ValueError("'etl_config' must be a dictionary")
    for job_name, job_config in config.etl_config.items():
        if not isinstance(job_config, dict):
            raise ValueError(f"Job configuration for '{job_name}' must be a dictionary")
        if "source" not in job_config:
            raise ValueError(
                f"Job configuration for '{job_name}' must contain 'source'"
            )
        if "sink" not in job_config:
            raise ValueError(f"Job configuration for '{job_name}' must contain 'sink'")
    logger.info("Configuration is valid")
    return config


def configuration_transformation(config):
    # Parse the configuration
    logger.info("Parsing the configuration")
    for job_name, job_config in config.etl_config.items():
        if "transformations" in job_config:
            transformations = job_config["transformations"]
            for transformation in transformations:
                transformation["sql"] = transformation.get("sql", "")
                transformation["name"] = transformation.get("name", "")

    logger.info("Configuration is parsed")
    return config


if __name__ == "__main__":

    etl_config = load_configuration()
    etl_config = configuration_validator(etl_config)
    etl = BaseETLJob(etl_config)
    etl.run("job1")
    etl.run("job2")

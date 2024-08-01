import logging
import copy
import os

# from pyspark.sql import SparkSession

# # logging
# logging.basicConfig(level=logging.INFO)
# root_logger_prefix = os.path.splitext(os.path.basename(__file__))[0]
# logger = logging.getLogger(root_logger_prefix)


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

    def __init__(self) -> None:
        logger.info("Initializing Spark Singleton Instance")


# if __name__ == "__main__":
#     # The client code.

#     s1 = SparkSingletonInstance()
#     s2 = SparkSingletonInstance()

#     if id(s1) == id(s2):
#         logger.info("Singleton works, both variables contain the same instance.")
#     else:
#         logger.info("Singleton failed, variables contain different instances.")


# update logger prefix to include class name
# logger = logging.LoggerAdapter(logger, {"prefix": "SparkSingletonInstance"})
# logger.info("Singleton works, both variables contain the same instance.")
# logger = logging.LoggerAdapter(logger, {"prefix": "SparkSingletonInstance1"})
# logger.info("Singleton failed, variables contain different instances.")

# logger.info('l1')
# # update logger prefix using logger context
# # update logger prefix using logger context
# with logging.LoggerAdapter(logger, {"prefix": "NewPrefix"}).contextualize():
#     logger.info("Singleton works, both variables contain the same instance.")

# logger.info('l2')


import logging


class CustomLoggerAdapter(logging.LoggerAdapter):
    def process(self, msg, kwargs):
        return "[%s] %s" % (self.extra["prefix"], msg), kwargs


# Set up the base logger
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
handler = logging.StreamHandler()
formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
handler.setFormatter(formatter)
logger.addHandler(handler)

# Use the custom logger adapter


logger.info("l1")
# Log messages using the adapter

for logger_name in ["l1", "l2"]:
    adapter = CustomLoggerAdapter(logger, {"prefix": logger_name})
    adapter.info("This is an info message")

logger.info("l2")

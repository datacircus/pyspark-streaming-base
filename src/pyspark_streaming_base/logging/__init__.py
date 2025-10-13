from typing import Optional, Any
from pyspark.sql import SparkSession


class SparkLoggingProvider:
    """
    The SparkLogger class wraps the SparkSession._jvm logger. This lets us
    control the log.level using spark.sparkContext.setLogLevel("INFO")
    for TRACE, INFO, WARN, ERROR
    """

    def get_logger(self, spark: SparkSession, prefix: Optional[str] = None) -> Any:
        """
        warning: call this method once in your constructor, as each call to
        log4j_logger.LogManager.getLogger(log_prefix) will return a new java object.
        * note: Any is used as the annotation for return type given the jvm gateway access call
        """

        log_prefix = prefix if prefix is not None else self.__class__.__name__
        log4j_logger = spark.sparkContext._gateway.jvm.org.apache.log4j
        return log4j_logger.LogManager.getLogger(log_prefix)
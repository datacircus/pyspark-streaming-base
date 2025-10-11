from typing import Optional, Dict, Self
from pyspark_streaming_base.logging import SparkLoggingProvider
from pyspark.sql import SparkSession


class App(SparkLoggingProvider):

    spark: SparkSession

    spark_conf: dict = {}

    app_name: str = "pyspark_streaming_base:default:app"
    app_version: str = "0.0.1"
    app_logging_prefix: str = "App:core"

    # todo: set after generating the class (could use a builder here)
    _initialized: bool = False

    # placeholder for the SparkLoggingProvider py4j.gateway instance
    logger = None

    @staticmethod
    def generate_spark_session() -> SparkSession:
        """
        Note: This generated SparkSession will run in local[*] mode.
        For non-local testing, use the SparkSession.builder.getOrCreate() method.\
        Supports:
            1. Reusing the Databricks Notebook Session (for iterative construction of applications)
            2. Separating the building of the Runtime SparkSession for proper dependency injection
            3. Supports Running the class locally, or directly from the unittest environment

        # note: we need to bring local Kafka jars and local Delta Lake jars to the default session
        # since this is being used for testing mainly!
        """
        spark_session = (
            SparkSession.builder.master("local[*]")
            .appName("pyspark_streaming_base")
            .config("spark.jars.packages",
                    "io.delta:delta-spark_2.13:4.0.0,org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.1")
            .config("spark.driver.extraJavaOptions", "-Divy.cache.dir=/tmp -Divy.home=/tmp -Dio.netty.tryReflectionSetAccessible=true")
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config("spark.sql.catalog.spark_catalog","org.apache.spark.sql.delta.catalog.DeltaCatalog")
            .config("spark.cores.max", "2")
            .config("spark.sql.session.timeZone", "UTC")
            .config("spark.sql.parquet.mergeSchema", "false")
            .config("spark.sql.parquet.filterPushdown", "true")
            .config("spark.hadoop.parquet.summary.metadata.level", "NONE")
            .getOrCreate()
        )
        return spark_session

    # todo: it would be nice to use dotenv to augment the configuration
    # this can still work as long as we do it as a decorator to the "SparkSession" builder

    def __init__(self,
                 session: Optional[SparkSession] = None,
                 app_config: Dict[str, str] = None) -> None:
        """
        Initializes the class with a Spark session and sets up logging for the application.

        If no Spark session is provided, a new session is created using the static method
        `App.generate_spark_session`. The log level is set to "INFO" through the Spark
        configuration. Additionally, a logging prefix can be configured via Spark
        configuration using the key `spark.app.logger.prefix`. If overridden and defined,
        this prefix is respected. Logging is managed using the instance-specific logger.

        :param session: An optional `SparkSession` instance to initialize the class with.
                        If not provided, a new session is generated.
        :type session: Optional[SparkSession]
        """

        ## can set log level using configuration `spark.conf.set("spark.log.level", "INFO")`
        # self.spark.sparkContext.setLogLevel("INFO")

        if session is None:
            self.spark = App.generate_spark_session()
        else:
            self.spark = session

        # todo: decision: if the app_config is passed into the constructor, then we can initialize the app here
        if app_config is not None:
            self.with_config(app_config)
            self.initialize()

    def with_config(self, app_config: Dict[str, str]) -> Self:
        """
        Note: This method should only be used while building the base application prior to calling the initialize method.
        Updates the current instance with the provided application configuration. This method processes the
        given dictionary and sets appropriate configurations for keys that start with "spark.*".

        :param app_config: A dictionary containing application configuration settings. Keys that begin
                           with "spark." will be applied to the spark configuration.
        :return: The current instance with updated configurations.
        """
        if self._initialized:
            raise RuntimeError("Cannot update application config via with_config after initialization.")
        if app_config is not None:
            for key, value in app_config.items():
                if key.startswith("spark."):
                    self.spark.conf.set(key, value)
        return self

    def initialize(self) -> Self:

        # check for the spark.app.name
        self.app_name = self.spark.conf.get(key="spark.app.name", default=self.app_name)

        # check for spark.app.version
        self.app_version = self.spark.conf.get(key="spark.app.version", default=self.app_version)

        # check the logging prefix if it is overridden, then respect the new value
        self.app_logging_prefix = self.spark.conf.get(
            key="spark.app.logging.prefix", default=self.app_logging_prefix
        )
        self.logger = self.get_logger(spark=self.spark, prefix=self.app_logging_prefix)

        # for now this is just to do some basic validation
        self._initialized = True



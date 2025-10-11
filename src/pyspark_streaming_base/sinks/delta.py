from typing import Optional, Dict, Self
from pyspark.sql import DataFrame
from pyspark.sql import SparkSession
from pyspark.sql.streaming import DataStreamWriter, StreamingQuery

from pyspark_streaming_base.sinks.streaming_sink import StreamingSink


class DeltaStreamingSink(StreamingSink):

    config_prefix_for_table: str

    sink_options: Dict[str, str | None] = {
        'checkpointLocation': None,
        'outputMode': 'append',
        # for managing idempotent writes
        # txnAppId should match the application:checkpoint_version to be unique
        'txnAppId': None,
        # txnVersion (monotonically increasing number) - should be provided by the batchId
        'txnVersion': None,
        # default to turning mergeSchema off. This is to prevent bad changes from automatically
        # propagating to downstream data consumers
        'mergeSchema': 'false',
        # sets some constraints on the records per file
        # consider changing this to match the avg row size per file * number of records per file
        # to clamp each file at a specific size
        'maxRecordsPerFile': '100000',
        # allows you to provide custom metadata that shows up in commit info or via DESCRIBE HISTORY
        # can also use `spark.databricks.delta.commitInfo.userMetadata` instead of the direct option
        'userMetadata': None,
        # path is used for non-managed tables
        'path': None,
        'queryName': 'delta:sink:default',
        'spark.databricks.delta.autoCompact.enabled': 'true',
        'spark.databricks.delta.autoCompact.minNumFiles': '10',
        'spark.databricks.delta.optimizeWrite.enabled': 'true',
    }

    def __init__(self, config_prefix: Optional[str] = None,
                 config: Optional[Dict[str, str]] = None) -> None:
        super().__init__(
            sink_format='delta',
            config_prefix=config_prefix,
            config=config
        )

        self.config_prefix_for_table = f"{self.config_prefix}.table"

    # todo: create common delta utils for things like tableName and is_managed
    def table_name(self, session: Optional[SparkSession] = None) -> str:
        spark = session or SparkSession.active()
        catalog = spark.conf.get(f"{self.config_prefix_for_table}.catalog", None)
        database_or_schema = spark.conf.get(f"{self.config_prefix_for_table}.databaseOrSchema", None)
        table_name = spark.conf.get(f"{self.config_prefix_for_table}.tableName", None)
        if catalog is None and database_or_schema is None and table_name is None:
            raise ValueError(f"Missing configuration for {self.config_prefix_for_table}")

        return ".".join(filter(None, [catalog, database_or_schema, table_name]))

    @staticmethod
    def is_managed(table_name: str):
        return table_name.find(".") > -1

    def with_config(self, config: Dict[str, str], session: Optional[SparkSession] = None) -> Self:
        """
        Applies configurations to the current instance using the provided SparkSession and
        a set of configurations defined in a dictionary. This method first utilizes
        the parent class's with_config functionality to apply the basic configuration
        to the SparkSession object. Next, it applies additional updates using the
        with_config_from_spark method (specific to this instance). Finally, this method
        marks the instance as initialized for future use.

        :param config: A dictionary storing key-value pairs of Spark configuration settings.
        :param session: An optional SparkSession instance to which the configuration
            and updates will be applied. If not provided, the configuration will only
            be processed based on the supplied dictionary.
        :return: The updated instance of the class with the applied configurations.
        """
        # apply the spark.* configuration into the SparkSession
        super().with_config(config, session)

        # then applies the with_config_from_spark updates
        super().with_config_from_spark(session)

        # lastly, setting the initialized flag
        # which is here mainly for future use
        self._initialized = True

        return self

    def generate(self, df: DataFrame) -> DataStreamWriter:
        # will decorate the DataStreamWriter with queryName, outputMode, clusterBy or partitionBy, trigger

        # todo: need to fix this to be smart about how we specify the streaming options vs the instance config
        stream_options = self.options()
        final_options = {
            'checkpointLocation': stream_options['checkpointLocation'],
            'mergeSchema': stream_options['mergeSchema'],
            'path': stream_options['path']
        }
        return self.generate_write_stream(df, {k:v for k,v in final_options.items() if v is not None})

    def fromDF(self, df: DataFrame) -> DataStreamWriter:
        # alias method: since DeltaTable.forName(spark, 'catalog.schema.tableName').toDF() is familiar
        # app.sink().fromDF(df).queryName('sink:query:id').outputMode('append').clusterBy(col('x'), col('y')).trigger(availableNow=True).
        # app.sink().fromDF(df).queryName('sink:query:id').outputMode('append').partitionBy(col('x'), col('y')).trigger(availableNow=True).
        return self.generate(df)

    def execute(self, dsw: DataStreamWriter) -> StreamingQuery:
        pass
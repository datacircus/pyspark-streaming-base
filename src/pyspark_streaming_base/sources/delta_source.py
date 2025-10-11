from typing import Optional, Dict, Self
from pyspark.sql import SparkSession
from pyspark.sql.streaming import DataStreamReader
from pyspark_streaming_base.sources.streaming_source import StreamingSource

class DeltaStreamingSource(StreamingSource):

    source_options: Dict[str, str | None] = {
        "startingVersion": None,
        'maxFilesPerTrigger': '1',
        'maxBytesPerTrigger': '1g',
        'withEventTimeOrder': 'true',
        'ignoreChanges': 'true',
        'ignoreDeletes': 'true',
        # provide an accessible location to load a table at a path
        # if you provide `.table.tableName, .table.databaseOrSchema` and `.table.catalog`
        # then the table will be loaded using the DataStreamReader.table method, and the path
        # will be ignored
        'path': None,
    }

    config_prefix_for_table: str

    def __init__(self, config_prefix: Optional[str] = None,
                 config: Optional[Dict[str, str]] = None):
        # note: config_prefix in the superclass will always be `spark.app.source`
        # you can use `spark.app.source2`, or anything really to support many 'sources' in your config
        super().__init__(
            source_format='delta',
            config_prefix=config_prefix,
            config=config
        )

        self.config_prefix_for_table = f"{self.config_prefix}.table"

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
        self.with_config_from_spark(session)

        # lastly, setting the initialized flag
        # which is here mainly for future use
        self._initialized = True

        return self

    def tableName(self, session: Optional[SparkSession] = None) -> str:
        """
        Generates a fully qualified table name by concatenating the catalog, database or schema,
        and table name as defined in Spark configuration. If no session is provided, it utilizes
        the active Spark session. If any of the required configuration parameters are missing,
        an exception is raised.

        :param session: Spark session to use when retrieving configuration settings.
            If not provided, the active Spark session is used. Defaults to None.
        :return: Fully qualified table name as a string derived from the catalog,
            database or schema, and table name configured in the Spark session.
        :rtype: str
        :raises ValueError: If any of the required configuration parameters
            for catalog, database or schema, or table name are missing.
        """
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


    def get_or_default(self, session: SparkSession, source_option: str) -> str:
        """
        Retrieves a configuration option from the Spark session. If the configuration
        option is not set or contains an empty string, the method returns the default
        value from `self.source_options`.

        :param session: Instance of SparkSession used to retrieve the configuration.
        :param source_option: Name of the configuration option to be retrieved.
        :return: The configuration value from the session if set, otherwise the
            default value defined in `self.source_options`.
        :rtype: str
        """
        value = session.conf.get(f"{self.config_options_prefix}.{source_option}", '')
        if len(value) > 0:
            return value
        else:
            return self.source_options[source_option]

    def with_config_from_spark(self, session: Optional[SparkSession] = None) -> Self:
        """
        Updates the source options of the current object based on the provided or active
        SparkSession configuration. Retrieves the existing values from the SparkSession
        configuration and applies them to update the object's options. If no session is provided,
        it defaults to the currently active SparkSession.

        :param session: The SparkSession to retrieve configuration values from.
                        If None is provided, uses the active SparkSession.
        :type session: Optional[SparkSession]
        :return: The current object with updated source options.
        :rtype: Self
        """
        # todo: this gets called twice, and we can ignore the first in most cases...
        # SparkSession.active().conf.get('spark.app.source.delta.options.startingVersion')
        spark = session or SparkSession.active()
        modified = {k: self.get_or_default(spark, k) for k, v in self.source_options.items()}
        self.source_options.update(modified)
        return self

    def generate(self, session: Optional[SparkSession] = None) -> DataStreamReader:
        """
        Generates a DataStreamReader configured for reading data from the specified source.

        The method creates a DataStreamReader using the app's data stream generation function
        based on the provided source configuration. The DataStreamReader is formatted for
        reading data from Kafka.

        :return: A DataStreamReader configured for consuming data from Kafka.
        :rtype: DataStreamReader
        """
        spark = session or SparkSession.active()
        if not self._initialized:
            self.with_config({}, spark)

        # note: if we have 'path' set, and we have a fully qualified managed table,
        # then we need to clear the 'path' - but Spark will also fail in that mode for us.

        return self.generate_read_stream(spark, self.options())

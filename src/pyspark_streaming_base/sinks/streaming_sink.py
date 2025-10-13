from typing import Optional, Dict, Self
from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
from pyspark.sql.streaming import DataStreamWriter

class StreamingSink:

    format: str
    config_prefix: str = 'spark.app.sink'
    config_options_prefix: str

    # sink_options can be set using the `.options(**)` method on the DataStreamWriter
    # these are the default values for the sink options
    # these can be overridden by the user in the config file or spark.conf
    sink_options: Dict[str, str | None] = {
        'outputMode': 'append',
        'checkpointLocation': None,
        'queryName': 'streaming:sink:default',
        'partitionBy': None,
        'clusterBy': None,
        'path': None,
        'mode': 'errorIfExists'
    }

    _initialized: bool = False

    def __init__(self,
                 sink_format: str,
                 config_prefix: Optional[str] = None,
                 config: Optional[Dict[str, str]] = None) -> None:
        self.format = sink_format

        if config_prefix is not None:
            self.config_prefix = f"{config_prefix}.{self.format}"

        self.config_options_prefix = f"{self.config_prefix}.options"

        if config is not None:
            # this method call will first apply your spark.conf dictionary,
            # then call with_config_from_spark to update the source options
            self.with_config(config)
        else:
            # set any values directly from the spark configuration
            # this would be any default values from spark-submit --conf ... or spark.properties...
            self.with_config_from_spark()

    def with_config(self, config: Dict[str, str], session: Optional[SparkSession] = None) -> Self:
        """
        Applies given configurations to the active SparkSession. This method iterates
        through the provided configuration dictionary and sets any key-value pairs
        that start with "spark." into the Spark application's configuration.

        :param session: Bring your own SparkSession. Default is None.
        :param config: A dictionary containing configuration key-value pairs where
            keys that start with "spark." are applied to the SparkSession.
        :type config: Dict[str, str]
        :return: The current instance with updated Spark configurations.
        :rtype: Self
        """
        # with_config applies any spark.* config directly to the active SparkSession
        spark = session or SparkSession.active()
        for key, value in config.items():
            if key.startswith("spark."):
                spark.conf.set(key, value)

        return self

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
        modified = {k: self.get_or_default(spark, k) for k, v in self.sink_options.items()}
        self.sink_options.update(modified)
        return self

    def get_or_default(self, session: SparkSession, sink_option: str) -> str:
        """
        Retrieves a configuration option from the Spark session. If the configuration
        option is not set or contains an empty string, the method returns the default
        value from `self.source_options`.

        :param session: Instance of SparkSession used to retrieve the configuration.
        :param sink_option: Name of the configuration option to be retrieved.
        :return: The configuration value from the session if set, otherwise the
            default value defined in `self.source_options`.
        :rtype: str
        """
        value = session.conf.get(f"{self.config_options_prefix}.{sink_option}", '')
        if len(value) > 0:
            return value
        else:
            return self.sink_options[sink_option]

    def options(self) -> Dict[str, str]:
        """
        Produces a dictionary of options after processing keys by removing the
        prefix if applicable. The method filters out options where the value
        is None. Each key in the source options is adjusted based on whether
        it starts with the `config_options_prefix`.

        :example:

        :returns: A dictionary where keys are processed by removing the prefix
            specified by the `config_options_prefix` and values are directly
            copied from the source options if they are not None.
        :rtype: Dict[str, str]
        """
        return {
            k if not str(k).startswith(self.config_options_prefix)
            else k.replace(f".{self.config_options_prefix}", ''): v
            for k, v in self.sink_options.items() if v is not None
        }

    def generate_write_stream(self, df: DataFrame, s_options: Dict[str, str]) -> DataStreamWriter:
        """
        Generates a DataStreamWriter configured with the specified options and format.

        This method takes a DataFrame and a dictionary of options, applies these
        options to the DataFrame's writeStream property, and sets the format to the
        predefined format of the instance. It then returns the configured
        DataStreamWriter ready for further processing, such as starting the write
        operation.

        :param df: The input DataFrame to be written as a streaming output.
        :type df: DataFrame
        :param s_options: A dictionary of options to configure the streaming write
            operation. These can include properties like checkpoint location, output
            mode, etc.
        :type s_options: Dict[str, str]
        :return: A configured DataStreamWriter for managing the streaming write
            operation.
        :rtype: DataStreamWriter
        """
        return df.writeStream.options(**s_options).format(self.format)

    def generate(self, df: DataFrame) -> DataStreamWriter:
        """
        This method must be implemented by subclasses to generate a DataStreamWriter
        object from the provided DataFrame. The method is responsible for converting
        or processing the given DataFrame into a DataStreamWriter instance, which can
        be used for writing streaming data to various output sinks. Subclasses should
        provide the specific implementation details based on their requirements.

        :param df: The input DataFrame to be converted or processed into a
            DataStreamWriter. Represents the streaming data.
        :type df: DataFrame
        :return: A DataStreamWriter object that defines the sink and behavior
            for streaming data output.
        :rtype: DataStreamWriter
        :raises NotImplementedError: If the subclass does not implement this method.
        """
        return self.generate_write_stream(df, self.options())

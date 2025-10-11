from typing import Optional, Dict, Self

from pyspark.sql import SparkSession
from pyspark.sql.streaming import DataStreamReader

class StreamingSource:
    """
    Represents a source of streaming data for a Spark application.

    This class serves as a base class for defining streaming data sources in a Spark application.
    It provides a structure for configuration and initialization of the streaming source. The
    class itself is abstract and must be subclassed to provide specific implementations for
    configuration and data stream generation.

    :ivar format: The format of the streaming source. Default is "unbound".
    :type format: str
    :ivar config_prefix: Prefix for the configuration related to this source.
    :type config_prefix: str
    :ivar config_options_prefix: Prefix for the configuration options specific to the source.
    :type config_options_prefix: str
    :ivar _initialized: Internal state to track if the source is initialized.
    :type _initialized: bool
    """
    format: str
    config_prefix: str
    config_options_prefix: str

    source_options: Dict[str, str | None] = {}

    _initialized: bool = False


    def generate_read_stream(self, spark: SparkSession, s_options: Dict[str, str]) -> DataStreamReader:
        """
        Generates a configured DataStreamReader for use with the provided SparkSession
        and streaming options.

        This static method configures a DataStreamReader based on the provided
        streaming options. It allows dynamic setup of the reader, facilitating
        read operations for streaming data sources.

        :param spark: The SparkSession object used to access Spark functionalities.
        :type spark: SparkSession
        :param s_options: A dictionary of string key-value pairs specifying the
            configuration options for the DataStreamReader.
        :type s_options: Dict[str, str]
        :return: A DataStreamReader initialized with the specified configuration.
        :rtype: DataStreamReader
        """

        return spark.readStream.options(**s_options).format(self.format)

    def __init__(self, source_format: str,
                 config_prefix: Optional[str] = None,
                 config: Optional[Dict[str, str]] = None) -> None:
        """
        Initializes the instance for handling configuration settings specific to a
        data source format. This configuration setup supports optional prefixes for
        configurations and allows overrides from a predefined dictionary.

        The constructor ensures that configuration options are appropriately set,
        either via a provided dictionary or sourced from the current Spark session's
        configuration.

        :param source_format:
            The format of the data source (e.g., "kafka", "delta"). Determines how the
            configuration options are structured and applied.
        :param config_prefix:
            An optional string used as a prefix for configuration keys. This helps
            to distinguish configurations specific to this instance from other
            configurations with similar keys.
        :param config:
            An optional dictionary of configuration keys and values. These values are
            applied to override or define configuration options directly.
        """
        self.format = source_format
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
        Raises a NotImplementedError to indicate this method must be implemented by
        subclasses. This method is intended to provide additional configuration derived
        from a Spark environment. Pull expected values from spark.conf.get('key', default='')
        to simplify how source configurations are applied.

        :return: The updated instance of the class, configured with Spark-specific
            settings.
        :rtype: Self
        """
        raise NotImplementedError("This method must be implemented by subclasses")

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
            for k, v in self.source_options.items() if v is not None
        }

    def generate(self) -> DataStreamReader:
        """
        Generates a DataStreamReader.

        This method is intended to be implemented by subclasses to return an instance of
        DataStreamReader. If not implemented, calling this method will raise a
        NotImplementedError.

        Implement by using self.app.generate_read_stream(self.options())

        :raises NotImplementedError: If the method is not implemented by the subclass.
        :return: A DataStreamReader instance.
        :rtype: DataStreamReader
        """
        raise NotImplementedError("This method must be implemented by subclasses")
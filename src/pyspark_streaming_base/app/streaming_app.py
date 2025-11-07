from typing import Optional, Dict, Self
from pathlib import Path
from pyspark.sql import SparkSession

from pyspark_streaming_base.app import App
from pyspark_streaming_base.sinks import DeltaStreamingSink
from pyspark_streaming_base.sources import KafkaStreamingSource, DeltaStreamingSource


class StreamingApp(App):
    # use to set the basedir for the application checkpoints (/Volumes/, etc)
    app_checkpoints_path: Optional[str] = None
    # use to cleanly separate the checkpoints for different versions of the application
    app_checkpoint_version: Optional[str] = None

    _source: KafkaStreamingSource | DeltaStreamingSource | None = None

    _sink: DeltaStreamingSink | None = None

    def __init__(self,
                 session: Optional[SparkSession] = None,
                 app_config: Optional[Dict[str, str]] = None) -> None:

        super().__init__(session, app_config)


    def initialize(self) -> Self:
        super().initialize()
        self.app_checkpoints_path = self.spark.conf.get(
            key='spark.app.checkpoints.path',
            default=self.app_checkpoints_path)

        self.app_checkpoint_version = self.spark.conf.get(
            key='spark.app.checkpoint.version',
            default=self.app_checkpoint_version)

        self._initialized = True
        return self

    def checkpoint_location(self) -> Path:
        """
        Retrieve the file system path for storing application checkpoints. This method combines the application's
        checkpoint root path, name, and version to construct a complete path to the checkpoint's directory.
        If no valid checkpoint path or version is specified, an exception will be raised indicating the
        requirements for setting these values.

        :raises RuntimeError: If the checkpoint path or version is not properly configured.

        :return: The complete path for the application's checkpoints.
        :rtype: Path
        """
        if self.app_checkpoints_path is not None:
            base_path = Path(self.app_checkpoints_path)
            return base_path.joinpath(
                self.app_name,
                self.app_checkpoint_version or "stable", "_checkpoints"
            )
        else:
            raise RuntimeError(
                "StreamingApp checkpoints require spark.app.checkpoints.path and "
                "spark.app.checkpoints.version"
            )

    # all about sources
    def with_source(self, source: KafkaStreamingSource | DeltaStreamingSource) -> Self:
        """
        Sets the Kafka streaming source for the current object and returns the instance
        itself. This method enables chaining and allows the configuration of the object
        by specifying the desired KafkaStreamingSource.

        :param source: The KafkaStreamingSource instance to be set for the current object.
        :type source: KafkaStreamingSource
        :return: The current object with the updated Kafka streaming source.
        :rtype: Self
        """
        self._source = source
        return self

    def with_kafka_source(self, config_prefix: Optional[str] = None,
                 config: Optional[Dict[str, str]] = None) -> Self:
        """
        Configures Kafka as the source for the streaming application. Sets up a Kafka
        Streaming Source with the provided configuration prefix and additional
        configuration options.

        :param config_prefix: A string that represents the prefix for Kafka
            configuration properties, or None if no such prefix is used.
        :param config: A dictionary of Kafka configuration properties to
            provide additional settings for the Kafka source, or None if no
            additional settings are specified.
        :return: Returns the instance of the current object for method
            chaining.
        """
        self._source = KafkaStreamingSource(config_prefix=config_prefix, config=config)
        return self

    def with_delta_source(self,
                          config_prefix: Optional[str] = None,
                          config: Optional[Dict[str, str]] = None) -> Self:
        """
        Sets a Delta streaming source for the current instance.

        This method configures a Delta streaming source using the provided
        `config_prefix` and `config` parameters. The instance's
        `_source` attribute is set to a new instance of `DeltaStreamingSource`
        created with these parameters, and the instance itself is returned.

        :param config_prefix: The prefix to use for the streaming source's
            configuration, useful for scoping configuration entries
            in grouped settings. If `None`, no prefix is applied.
        :type config_prefix: str, optional
        :param config: A dictionary containing key-value pairs used as
            the configuration settings for the Delta source.
            This may include any settings necessary to establish or
            configure the Delta source.
        :type config: dict, optional
        :return: The instance of the class, allowing method chaining
            after setting the Delta source.
        :rtype: Self
        """
        self._source = DeltaStreamingSource(config_prefix=config_prefix, config=config)
        return self

    def kafka_source(self) -> KafkaStreamingSource:
        """
        Returns the Kafka streaming source if the current source is an instance of
        KafkaStreamingSource. Raises a RuntimeError if the current source is not of
        the expected type.

        :return: The current source as a KafkaStreamingSource.

        :rtype: KafkaStreamingSource

        :raises RuntimeError: If the current source is not a KafkaStreamingSource.
        """
        if isinstance(self._source, KafkaStreamingSource):
            return self._source
        raise RuntimeError("source is not a KafkaStreamingSource")

    def delta_source(self) -> DeltaStreamingSource:
        """
        Returns the streaming source if it is an instance of DeltaStreamingSource. If the source is not of
        type DeltaStreamingSource, raises a RuntimeError specifying the mismatch.

        :raises RuntimeError: If the source is not an instance of DeltaStreamingSource.
        :return: The streaming source of type DeltaStreamingSource.
        :rtype: DeltaStreamingSource
        """
        if isinstance(self._source, DeltaStreamingSource):
            return self._source
        raise RuntimeError("source is not a DeltaStreamingSource")

    def source(self) -> KafkaStreamingSource | DeltaStreamingSource | None:
        """
        Retrieves the source associated with the object.

        This method returns the source of the object, which could be an instance
        of `KafkaStreamingSource`, `DeltaStreamingSource`, or `None` if no source
        is associated.

        :return: The current streaming source, or None if no source is defined.
        :rtype: KafkaStreamingSource | DeltaStreamingSource | None
        """
        if isinstance(self._source, KafkaStreamingSource):
            return self.kafka_source()
        elif isinstance(self._source, DeltaStreamingSource):
            return self.delta_source()
        return None

    # all about sinks
    def with_sink(self, sink: DeltaStreamingSink) -> Self:
        """
        Sets the sink for the current object and returns the updated object.

        This method allows the user to set the sink used for the functionality
        of the object. It is intended to update the sink instance and maintain
        the updated state within the object.

        :param sink: The sink to set for the current object.
        :type sink: DeltaStreamingSink
        :return: The updated instance of the current object after setting the sink.
        :rtype: Self
        """
        self._sink = sink
        return self

    def with_delta_sink(self, config_prefix: Optional[str] = None, config: Optional[Dict[str, str]] = None) -> Self:
        """
        Configures the Delta sink for the current app instance.

        This method sets up a DeltaStreamingSink using the provided configuration prefix
        and configuration dictionary. It assigns the newly created sink object to the `_sink`
        attribute of the current instance and then returns the updated instance. This is
        commonly used for setting up a Delta sink within a streaming pipeline.

        :param config_prefix: An optional prefix string used for configuring the Delta sink.
        :param config: An optional dictionary containing key-value pairs
            for the configuration settings of the Delta sink.
        :return: The updated instance with the Delta sink configured.
        """
        self._sink = DeltaStreamingSink(config_prefix=config_prefix, config=config)
        return self

    def sink(self) -> DeltaStreamingSink | None:
        """
        Returns the delta streaming sink, if defined.

        The method provides access to the internal `_sink` attribute which
        represents the delta streaming sink. It will return the sink if
        it exists, otherwise it will return `None`.

        :return: The delta streaming sink instance or `None` if not set.
        :rtype: DeltaStreamingSink | None
        """
        return self._sink

    def delta_sink(self) -> DeltaStreamingSink:
        """
        Returns the underlying sink if it is an instance of DeltaStreamingSink.

        This method checks if the `_sink` attribute is an instance of
        `DeltaStreamingSink` and, if so, returns it. Otherwise, it raises a
        `RuntimeError`.

        :raises RuntimeError: If `_sink` is not an instance of
            `DeltaStreamingSink`.
        :return: The `_sink` attribute if it is an instance of
            `DeltaStreamingSink`.
        :rtype: DeltaStreamingSink
        """
        if isinstance(self._sink, DeltaStreamingSink):
            return self._sink
        raise RuntimeError("sink is not a DeltaStreamingSink")
from typing import Optional, Dict, Self

from pyspark.sql.streaming import DataStreamReader

from pyspark_streaming_base.app import StreamingApp

# the streaming app provides access to the singleton SparkSession
# the alternative is to use SparkSession.active() to get the active session
# it all really depends on if it makes sense to carry a reference around

class KafkaStreamingSource:
    """
    Provides functionality to define and configure a Kafka streaming source
    within a streaming application. This class is responsible for managing
    the integration of a Kafka source into a streaming application's pipeline.

    :ivar app: The streaming application instance to which this Kafka source
        is attached.
    :type app: StreamingApp
    :ivar config_prefix: The configuration key prefix used to specify Kafka
        options in the application settings.
    :type config_prefix: str
    """
    app: StreamingApp
    config_prefix: str = "spark.app.source.kafka"
    config_options_prefix: str = f"{config_prefix}.options"

    _initialized: bool = False

    # @link: https://spark.apache.org/docs/latest/streaming/structured-streaming-kafka-integration.html
    options: Dict[str, str|None] = {
        # failOnDataLoss (bool) is used to ignore offsets that have already been deleted between app runs
        # A 'data-loss' situation occurs if we can't 'rehydrate' the lost data
        # we can make a decision based on the situation (can we replay or not?)
        "failOnDataLoss": "true",
        # sets a unique group to ensure that the offsets fetched and split by partition are not split between concurrent
        # spark applications. Each groupIdPrefix should be unique ({app_name}:{checkpointId})
        "groupIdPrefix": None,
        # this will provide an array of headers as a array[struct<key:binary,value:binary>]
        "includeHeaders": "false",
        # kafka connector settings
        "subscribe": None,
        "kafka.bootstrap.servers": None,
        # set the parse mode : PERMISSIVE or FAIL_FAST
        "mode": "FAIL_FAST",
        # Handling Kafka Topic Partition Offset Reading
        # startingOffsets takes the 'earliest' or 'latest' convienence options, but can also take
        # a JSON blob of the partition and offsets
        "startingOffsets": "earliest",
        # startingTimestamp takes a numeric long value (epoch millis) as a string
        "startingTimestamp": None,
        # if the startingTimestamp doesn't exist in the Kafka log (it has been deleted),
        # then we will start off with the earliest
        # available offsets per partition: option ('error' | 'earliest')
        "startingOffsetsByTimestampStrategy": None,
        # how often to check for new data on the topic (set to 5 seconds, default is 10 milliseconds)
        "fetchOffset.retryIntervalMs": "10",
        # endingOffsets and endingTimestamp is ONLY USED IN BATCH MODE:
        # with that said: it is common to provide the startingOffsets along with the endingOffsets, or
        # mix startingTimestamp with specific endingOffsets to create a specific set of records for processing
        # (the options startingTimestamp and startingOffsets are mutually exclusive)
        #
        # consider a replay job from {A as topicA:[0:100,1:100,2:100]-> B as topicA:[0:1000,1:1000,2:1000]}
        # where topicA has partions 0,1,2 we set the startingOffsets and the endingOffsets, and the job will
        # happily reprocess that exact batch or streaming micro-batch
        # tip: use: trigger(availableNow=True) - in order to use throttling to recover
        # note: if the application has already run at least once, then the checkpoints won't honor changes
        # to the [starting|ending]Offsets
        "endingOffsets": None,
        # endingTimestamp takes a numeric long value (epoch millis) as a string, and will search the
        # Kafka topic via the admin client to resolve the offset boundary set by the endingTimestamp
        "endingTimestamp": None,
        "minPartitions": "36",
        # an alternative to setting the `minPartitions`, we can consider N records across any partitions to be
        # enough to trigger a new micro-batch
        # tip: if you expect between 1k-10k records per second, you might want to try splitting the
        # range 1/2 or 1/4 (2500 or 5000) to account for normal daily change in rates
        "minOffsetsPerTrigger": None,
        # this enables us to throttle how many records we can consume (as an upward bound) per micro-batch
        # adjusting this value enables us to control the memory-pressure in our application
        # given we can control how many or how few records to process every time the application runs
        "maxOffsetsPerTrigger": "5000",
        # Limit the maximum number of records present in a partition. By default, Spark has a 1-1 mapping of
        # topicPartitions to Spark partitions consuming from Kafka. If you set this option, Spark will divvy up
        # Kafka partitions to smaller pieces so that each partition has up to maxRecordsPerPartition records
        "maxRecordsPerPartition": "100"
    }

    def __init__(self, app: StreamingApp,
                 config_prefix: Optional[str] = None,
                 config: Optional[Dict[str, str]] = None) -> None:
        """
        Initializes a component with a reference to a StreamingApp instance, an optional configuration
        prefix, and an optional configuration dictionary. The configuration prefix, if provided, is
        used as a namespace or identifier for grouping configuration values. When a configuration
        dictionary is passed, it is applied to the instance during initialization.

        :param app: An instance of the StreamingApp to which the component belongs.
        :param config_prefix: An optional string that serves as a prefix or namespace for the component's
            configuration. Default is None.
        :param config: An optional dictionary of string key-value pairs used to initialize the
            configuration for the component. Default is None.
        """
        self.app = app

        # use the runtime configuration to update the defaults
        # you can call this method a second time to pull in any new overrides
        # before calling generate

        if config_prefix is not None:
            self.config_prefix = config_prefix

        if config is not None:
            # this method call will first apply your spark.conf dictionary,
            # then call with_config_from_spark to update the source options
            self.with_config(config)
        else:
            # set any values directly from the spark configuration
            # this would be any default values from spark-submit --conf ... or spark.properties...
            self.with_config_from_spark()


    def with_config(self, config: Dict[str, str]) -> Self:
        # with_config applies any spark.* config directly to the active SparkSession
        for key, value in config.items():
            if key.startswith("spark."):
                self.app.spark.conf.set(key, value)

        # then applies the with_config_from_spark updates
        self.with_config_from_spark()

        # lastly, setting the initialized flag
        # which is here mainly for future use
        self._initialized = True

        return self

    def with_config_from_spark(self) -> Self:
        """
        A method that configures the Spark options for the application based on
        specified configurations and default values. The configurations are
        retrieved dynamically from Spark's runtime configurations, allowing for
        flexible specification and updating of options related to streaming and
        Kafka integrations.

        This method modifies the `options` attribute of the caller, setting various
        parameters based on Spark runtime settings. It retrieves settings for failure
        handling, Kafka bootstrapping, topic subscriptions, offsets, and partitioning,
        among others. Default values are used where Spark configuration keys are not
        explicitly set.

        :raises KeyError: Raised when specific Spark configuration keys required for the
            setup are missing or unresolvable.

        :return: Returns the updated instance of the class.
        :rtype: Self
        """

        # simplify passing spark by reference
        spark = self.app.spark

        self.options["failOnDataLoss"] = spark.conf.get(
            key=f"{self.config_options_prefix}.failOnDataLoss",
            default=self.options["failOnDataLoss"],
        )

        # sets the unique groupId prefix
        # note: the default here will use the app_name:app_checkpoint_version
        # this is done to ensure that we don't generate the same groupId prefix for
        # multiple running applications
        self.options["groupIdPrefix"] = spark.conf.get(
            key=f"{self.config_options_prefix}.groupIdPrefix",
            default=f"{self.app.app_name}:{self.app.app_checkpoint_version}",
        )

        # do we want to include the Kafka headers Array?
        self.options["includeHeaders"] = spark.conf.get(
            key=f"{self.config_options_prefix}.includeHeaders", default="false"
        )

        # if subscribe is None, then the application can also take assignments
        # (which is not a usual use case) - it is more common from the driver -> executors per batch
        self.options["subscribe"] = spark.conf.get(
            key=f"{self.config_prefix}.topic", default=self.options["subscribe"]
        )
        # example: hostname:9092,hostname2:9092
        self.options["kafka.bootstrap.servers"] = spark.conf.get(
            key=f"{self.config_options_prefix}.kafka.bootstrap.servers", default=None
        )
        self.options["mode"] = spark.conf.get(
            key=f"{self.config_options_prefix}.mode", default=self.options["mode"]
        )
        self.options["startingOffsets"] = spark.conf.get(
            key=f"{self.config_options_prefix}.startingOffsets",
            default=self.options["startingOffsets"],
        )
        self.options["startingTimestamp"] = spark.conf.get(
            key=f"{self.config_options_prefix}.startingTimestamp", default=None
        )
        self.options["startingOffsetsByTimestampStrategy"] = spark.conf.get(
            key=f"{self.config_options_prefix}.startingOffsetsByTimestampStrategy",
            default=None,
        )
        self.options["fetchOffset.retryIntervalMs"] = spark.conf.get(
            key=f"{self.config_options_prefix}.fetchOffset.retryIntervalMs",
            default=self.options["fetchOffset.retryIntervalMs"],
        )
        self.options["endingOffsets"] = spark.conf.get(
            key=f"{self.config_options_prefix}.endingOffsets", default=None
        )
        self.options["endingTimestamp"] = spark.conf.get(
            key=f"{self.config_options_prefix}.endingTimestamp", default=None
        )
        self.options["minPartitions"] = spark.conf.get(
            key=f"{self.config_options_prefix}.minPartitions",
            default=self.options["minPartitions"],
        )
        self.options["minOffsetsPerTrigger"] = spark.conf.get(
            key=f"{self.config_options_prefix}.minOffsetsPerTrigger",
            default=self.options["minOffsetsPerTrigger"],
        )
        self.options["maxOffsetsPerTrigger"] = spark.conf.get(
            key="spark.app.source.kafka.options.maxOffsetsPerTrigger", default="5000"
        )
        return self

    def source_config(self) -> Dict[str, str]:
        """
        Generates a dictionary by filtering the `options` instance attribute for
        key-value pairs where the value is not `None`.

        This method iterates over the `options` attribute, which is expected to be
        a dictionary, and returns a filtered dictionary containing only the key-value
        pairs where the value is not `None`. Useful for creating a configuration
        dictionary based on the user's provided options.

        :return: A dictionary of key-value pairs from `options` where the value is not
            `None`
        :rtype: Dict[str, str]
        """
        return {
            k if not str(k).startswith(self.config_options_prefix)
            else k.replace(f".{self.config_options_prefix}", ''): v
            for k, v in self.options.items() if v is not None
        }


    def generate(self) -> DataStreamReader:
        """
        Generates a DataStreamReader configured for reading data from the specified source.

        The method creates a DataStreamReader using the app's data stream generation function
        based on the provided source configuration. The DataStreamReader is formatted for
        reading data from Kafka.

        :return: A DataStreamReader configured for consuming data from Kafka.
        :rtype: DataStreamReader
        """

        if not self._initialized:
            self.with_config({})
        return (
            self
            .app
            .generate_read_stream(self.source_config())
            .format("kafka")
        )


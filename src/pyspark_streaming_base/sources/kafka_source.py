from typing import Optional, Dict, Self

from pyspark.sql import SparkSession
from pyspark.sql.streaming import DataStreamReader
from pyspark_streaming_base.sources import StreamingSource

# the streaming app provides access to the singleton SparkSession
# the alternative is to use SparkSession.active() to get the active session
# it all really depends on if it makes sense to carry a reference around

class KafkaStreamingSource(StreamingSource):

    # @link: https://spark.apache.org/docs/latest/streaming/structured-streaming-kafka-integration.html
    source_options = {
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

    def __init__(self, config_prefix: Optional[str] = None,
                 config: Optional[Dict[str, str]] = None) -> None:
        """
        Initializes a component with a reference to a StreamingApp instance, an optional configuration
        prefix, and an optional configuration dictionary. The configuration prefix, if provided, is
        used as a namespace or identifier for grouping configuration values. When a configuration
        dictionary is passed, it is applied to the instance during initialization.

        :param config_prefix: An optional string that serves as a prefix or namespace for the component's
            configuration. Default is None.
        :param config: An optional dictionary of string key-value pairs used to initialize the
            configuration for the component. Default is None.
        """
        super().__init__(
            source_format='kafka',
            config_prefix=config_prefix,
            config=config
        )


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

    def with_config_from_spark(self, session: Optional[SparkSession] = None) -> Self:
        """
        Updates the configuration of a Kafka-based data source by retrieving relevant
        settings from the given Spark session or the active Spark session if no session
        is provided. The function simplifies the management of Kafka options, such as
        topic subscription, starting and ending offsets, and bootstrap server
        configuration, ensuring these options are dynamically obtained from Spark's
        runtime configuration.

        The updates to the `source_options` ensure that the options can dynamically
        adhere to the requirements of a distributed processing environment. Additionally,
        it supports flexibility in handling Kafka headers, starting positions for reading
        data, partition configurations, and batching configurations, among others.

        :param session: The SparkSession instance from which the Kafka options are
            retrieved. If None, the function uses the active Spark session. Defaults
            to None.
        :return: The updated instance with Kafka-related configuration options
            populated from the Spark session.
        :rtype: Self
        """

        # simplify passing spark by reference
        spark = session or SparkSession.active()

        self.source_options["failOnDataLoss"] = spark.conf.get(
            key=f"{self.config_options_prefix}.failOnDataLoss",
            default=self.source_options["failOnDataLoss"],
        )

        # sets the unique groupId prefix
        # note: the default here will use the `app_name:app_checkpoint_version`
        # this is done to ensure that we don't generate the same groupId prefix for
        # multiple running applications
        self.source_options["groupIdPrefix"] = spark.conf.get(
            key=f"{self.config_options_prefix}.groupIdPrefix",
            default=f"{spark.conf.get('spark.app.name')}:{spark.conf.get('spark.app.checkpoints.version')}",
        )

        # do we want to include the Kafka headers Array?
        self.source_options["includeHeaders"] = spark.conf.get(
            key=f"{self.config_options_prefix}.includeHeaders", default="false"
        )

        # if subscribe is None, then the application can also take assignments
        # (which is not a usual use case) - it is more common from the driver -> executors per batch
        self.source_options["subscribe"] = spark.conf.get(
            key=f"{self.config_prefix}.topic", default=self.source_options["subscribe"]
        )
        # example: hostname:9092,hostname2:9092
        self.source_options["kafka.bootstrap.servers"] = spark.conf.get(
            key=f"{self.config_options_prefix}.kafka.bootstrap.servers", default=None
        )
        self.source_options["mode"] = spark.conf.get(
            key=f"{self.config_options_prefix}.mode", default=self.source_options["mode"]
        )
        self.source_options["startingOffsets"] = spark.conf.get(
            key=f"{self.config_options_prefix}.startingOffsets",
            default=self.source_options["startingOffsets"],
        )
        self.source_options["startingTimestamp"] = spark.conf.get(
            key=f"{self.config_options_prefix}.startingTimestamp", default=None
        )
        self.source_options["startingOffsetsByTimestampStrategy"] = spark.conf.get(
            key=f"{self.config_options_prefix}.startingOffsetsByTimestampStrategy",
            default=None,
        )
        self.source_options["fetchOffset.retryIntervalMs"] = spark.conf.get(
            key=f"{self.config_options_prefix}.fetchOffset.retryIntervalMs",
            default=self.source_options["fetchOffset.retryIntervalMs"],
        )
        self.source_options["endingOffsets"] = spark.conf.get(
            key=f"{self.config_options_prefix}.endingOffsets", default=None
        )
        self.source_options["endingTimestamp"] = spark.conf.get(
            key=f"{self.config_options_prefix}.endingTimestamp", default=None
        )
        self.source_options["minPartitions"] = spark.conf.get(
            key=f"{self.config_options_prefix}.minPartitions",
            default=self.source_options["minPartitions"],
        )
        self.source_options["minOffsetsPerTrigger"] = spark.conf.get(
            key=f"{self.config_options_prefix}.minOffsetsPerTrigger",
            default=self.source_options["minOffsetsPerTrigger"],
        )
        self.source_options["maxOffsetsPerTrigger"] = spark.conf.get(
            key="spark.app.source.kafka.options.maxOffsetsPerTrigger", default="5000"
        )
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
        return self.generate_read_stream(spark, self.options())

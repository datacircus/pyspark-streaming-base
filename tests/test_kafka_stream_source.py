import pytest

from typing import Dict

from pyspark.sql import DataFrame
from pyspark.sql.streaming import DataStreamReader

from pyspark_streaming_base.app import StreamingApp
from pyspark_streaming_base.sources.kafka import KafkaStreamingSource

def test_kafka_streaming_source():
    # create the base application
    app: StreamingApp = (
        StreamingApp()
        # this allows you to modify the application,
        # for example, using `.with_kafka_source({...})`
        # or with, `.with_delta_sink({...})`
        # or to hook into the app for simple testing
        .with_config({
            'spark.app.checkpoints.path': '/src/test/resources/',
            'spark.app.checkpoints.version': '1.0.0'
        })
        .initialize()
    )

    # create the kafka source
    kafka_source: KafkaStreamingSource = (
        KafkaStreamingSource(app, config_prefix='spark.app.source.kafka')
        .with_config({
            'spark.app.source.kafka.topic': 'test_topic',
            'spark.app.source.kafka.options.kafka.bootstrap.servers': 'localhost:9092'
        })
        .with_config({
            'spark.app.source.kafka.options.startingOffsets': 'earliest'
        })
    )

    # now assert something cool
    generated_options: Dict[str, str] = kafka_source.source_config()

    assert generated_options['subscribe'] == 'test_topic'
    assert generated_options['kafka.bootstrap.servers'] == 'localhost:9092'
    assert generated_options['startingOffsets'] == 'earliest'

    reader: DataStreamReader = kafka_source.generate()
    df: DataFrame = reader.load()

    assert df.isStreaming is True

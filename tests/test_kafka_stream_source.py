from typing import Dict

from pyspark.sql import DataFrame
from pyspark.sql.streaming import DataStreamReader

from pyspark_streaming_base.app import StreamingApp

def test_kafka_streaming_source():
    # create the base application
    app: StreamingApp = (
        StreamingApp()
        .with_config({
            'spark.app.checkpoints.path': '/src/test/resources/',
            'spark.app.checkpoints.version': '1.0.0'
        })
        .with_kafka_source(config_prefix='spark.app.source', config={
            'spark.app.source.kafka.topic': 'test_topic',
            'spark.app.source.kafka.options.kafka.bootstrap.servers': 'localhost:9092',
            'spark.app.source.kafka.options.startingOffsets': 'earliest',
            'spark.app.source.kafka.options.mode': 'PERMISSIVE',
        })
        .initialize()
    )

    # now assert something cool
    generated_options: Dict[str, str] = app.source().options()

    assert generated_options['subscribe'] == 'test_topic'
    assert generated_options['kafka.bootstrap.servers'] == 'localhost:9092'
    assert generated_options['startingOffsets'] == 'earliest'
    assert generated_options['mode'] == 'PERMISSIVE'

    reader: DataStreamReader = app.source().generate()
    df: DataFrame = reader.load()

    assert df.isStreaming is True

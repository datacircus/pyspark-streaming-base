import pytest
from pathlib import Path
from pyspark.sql import DataFrame
from pyspark_streaming_base.app import StreamingApp
from pyspark_streaming_base.sources.delta_source import DeltaStreamingSource


def test_delta_streaming_source():
    table_name_for_test = 'test_table'
    app: StreamingApp = (
        StreamingApp()
        .with_config({
            'spark.app.checkpoints.path': '/src/test/resources/',
            'spark.app.checkpoints.version': '1.0.0'
        })
        .initialize()
    )

    # separate generation of the delta source
    delta_source: DeltaStreamingSource = (
        DeltaStreamingSource(config_prefix='spark.app.source')
        .with_config({
            'spark.app.source.delta.options.startingVersion': '2',
            'spark.app.source.delta.options.maxFilesPerTrigger': '4',
            'spark.app.source.delta.options.ignoreChanges': 'true',
            'spark.app.source.delta.options.withEventTimeOrder': 'true',
            # if the table has a path, then we expect it is unmanaged
            'spark.app.source.delta.options.path': Path(__file__).parent
            .joinpath('resources/delta_streaming_source', table_name_for_test).absolute().as_posix(),
        })
        # separating the config for readability
        .with_config({
            'spark.app.source.delta.table.tableName': table_name_for_test,
            #'spark.app.source.delta.table.databaseOrSchema': 'default',
            #'spark.app.source.delta.table.catalog': 'development'
        })
    )

    # apply the source to the app
    app.with_source(delta_source)

    with pytest.raises(RuntimeError):
        app.kafka_source()

    # testing the option generation and fetching of the delta_source via the app
    generated_options = app.delta_source().options()

    assert generated_options['startingVersion'] == '2'
    assert generated_options['maxFilesPerTrigger'] == '4'
    assert generated_options['maxBytesPerTrigger'] == '1g'

    assert table_name_for_test == app.delta_source().tableName()

    df: DataFrame = app.delta_source().generate().load()
    assert df.isStreaming is True

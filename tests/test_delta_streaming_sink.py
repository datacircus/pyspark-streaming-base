from pyspark.sql import DataFrame
from pyspark.sql.streaming import StreamingQuery
from pyspark_streaming_base.app import StreamingApp
from pyspark_streaming_base.sinks import DeltaStreamingSink
from pyspark_streaming_base.sources.delta_source import DeltaStreamingSource
from pathlib import Path

def test_delta_streaming_sink():

    delta_source_table_name = 'test_table'
    test_app_name = 'test:streaming:delta_source_to_delta_sink'
    checkpoints_path = Path('./resources/checkpoints/').absolute().as_posix()
    delta_sink_table_name = 'covid19'
    delta_sink_table_dir = Path('./resources/delta_streaming_sink/').joinpath(delta_sink_table_name).absolute()

    app: StreamingApp = (
        StreamingApp()
        .with_config({
            'spark.app.name': test_app_name,
            'spark.app.version': '0.0.1',
            'spark.app.checkpoints.path': checkpoints_path,
            'spark.app.checkpoints.version': '1.0.0'
        })
        .initialize()
    )

    # checkpoint_location = app.checkpoint_location()
    #if checkpoint_location.is_dir():
        # todo: remove all files recursively
        # checkpoint_location.rmdir()

    # separate generation of the delta source
    delta_source: DeltaStreamingSource = (
        DeltaStreamingSource(config_prefix='spark.app.source')
        .with_config({
            # we can't time travel to versions that don't exist, so start at 0
            # since this is the same behavior as a fresh table read
            'spark.app.source.delta.options.startingVersion': '0',
            'spark.app.source.delta.options.maxFilesPerTrigger': '4',
            'spark.app.source.delta.options.ignoreChanges': 'true',
            'spark.app.source.delta.options.withEventTimeOrder': 'true',
            # if the table has a path, then we expect it is unmanaged
            'spark.app.source.delta.options.path': Path(__file__).parent
            .joinpath('resources/delta_streaming_source', delta_source_table_name).absolute().as_posix(),
        })
        # separating the config for readability
        .with_config({
            'spark.app.source.delta.table.tableName': delta_source_table_name,
            # 'spark.app.source.delta.table.databaseOrSchema': 'default',
            # 'spark.app.source.delta.table.catalog': 'development'
        })
    )

    # apply the source to the app
    app.with_source(delta_source)

    # create the sink options
    delta_sink: DeltaStreamingSink = (
        DeltaStreamingSink(config_prefix='spark.app.sink')
        .with_config({
            'spark.app.sink.delta.options.checkpointLocation': app.checkpoint_location().as_posix(),
            'spark.app.sink.delta.options.maxRecordsPerFile': '100000',
            'spark.app.sink.delta.options.mergeSchema': 'true',
            'spark.app.sink.delta.options.queryName': 'delta:sink:covid19',
            'spark.app.sink.delta.options.outputMode': 'append',
            'spark.app.sink.delta.options.path': delta_sink_table_dir.as_posix(),
            'spark.app.sink.delta.table.name': delta_sink_table_name,
            'spark.app.sink.delta.trigger.enabled': 'true',
            'spark.app.sink.delta.trigger.availableNow': 'true',
        })
    )

    app.with_sink(delta_sink)

    sink_options = app.delta_sink().options()
    assert sink_options['checkpointLocation'] == app.checkpoint_location().as_posix()
    assert sink_options['maxRecordsPerFile'] == '100000'

    df: DataFrame = app.delta_source().generate().load()
    assert df.isStreaming is True

    query: StreamingQuery = (
        delta_sink.fromDF(df)
        .queryName(sink_options['queryName'])
        .trigger(availableNow=True)
        .outputMode(sink_options['outputMode'])
        .start()
    )

    # try writing all data from the 'test_table' source -> 'covid19' sink
    query.processAllAvailable()
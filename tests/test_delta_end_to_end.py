from pyspark.sql import DataFrame
from pyspark.sql.streaming import StreamingQuery
from pyspark_streaming_base.app import StreamingApp
from pyspark_streaming_base.sinks import DeltaStreamingSink
from pyspark_streaming_base.sources.delta_source import DeltaStreamingSource
from pathlib import Path

def test_delta_end_to_end():

    delta_source_table_name = 'test_table'
    test_app_name = 'test:streaming:delta_end_to_end'
    checkpoints_path = Path(__file__).parent.joinpath('./resources/checkpoints/').absolute().as_posix()
    delta_sink_table_name = 'covid19_e2e'
    delta_sink_table_dir = (Path(__file__).parent.joinpath('./resources/delta_streaming_sink/')
                            .joinpath(delta_sink_table_name).absolute())

    spark_config: dict[str, str] = {
        'spark.app.name': test_app_name,
        'spark.app.version': '0.0.1',
        'spark.app.checkpoints.path': checkpoints_path,
        'spark.app.checkpoint.version': '1.0.0',
        # we can't time travel to versions that don't exist, so start at 0
        # since this is the same behavior as a fresh table read
        'spark.app.source.delta.options.startingVersion': '0',
        'spark.app.source.delta.options.maxFilesPerTrigger': '4',
        'spark.app.source.delta.options.ignoreChanges': 'true',
        'spark.app.source.delta.options.withEventTimeOrder': 'true',
        # if the table has a path, then we expect it is unmanaged
        'spark.app.source.delta.options.path': Path(__file__).parent
        .joinpath('resources/delta_streaming_source', delta_source_table_name).absolute().as_posix(),
        'spark.app.source.delta.table.tableName': delta_source_table_name,
        # 'spark.app.source.delta.table.databaseOrSchema': 'default',
        # 'spark.app.source.delta.table.catalog': 'development'
        'spark.app.sink.delta.options.checkpointLocation': Path(__file__).parent.joinpath('./resources/checkpoints/').joinpath(test_app_name).joinpath('1.0.0').joinpath('_checkpoints').absolute().as_posix(),
        'spark.app.sink.delta.options.maxRecordsPerFile': '100000',
        'spark.app.sink.delta.options.mergeSchema': 'true',
        'spark.app.sink.delta.options.queryName': 'delta:sink:covid19',
        'spark.app.sink.delta.options.outputMode': 'append',
        'spark.app.sink.delta.options.path': delta_sink_table_dir.as_posix(),
        'spark.app.sink.delta.table.name': delta_sink_table_name,
        'spark.app.sink.delta.trigger.enabled': 'true',
        'spark.app.sink.delta.trigger.availableNow': 'true',
    }

    app: StreamingApp = StreamingApp()

    # Apply spark config to the app's SparkSession
    for key, value in spark_config.items():
        app.spark.conf.set(key, value)

    app.initialize()

    # separate generation of the delta source
    delta_source: DeltaStreamingSource = (
        DeltaStreamingSource(config_prefix='spark.app.source')
    )

    # apply the source to the app
    app.with_source(delta_source)

    # create the sink options
    delta_sink: DeltaStreamingSink = (
        DeltaStreamingSink(config_prefix='spark.app.sink')
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

    # Read the results from the delta sink table
    result_df: DataFrame = (
        app.spark.read.format('delta')
        .load(delta_sink_table_dir.as_posix())
    )

    # Novel test: verify data was written by checking that the result set
    # contains actual data and compute a simple data fingerprint
    row_count = result_df.count()
    assert row_count > 0, "Expected data to be written to sink table"

    # Compute a deterministic fingerprint: count of non-null values across all columns
    # This ensures we not only have rows, but they contain actual data
    from pyspark.sql.functions import col, sum as spark_sum, when

    non_null_counts = result_df.select([
        spark_sum(when(col(c).isNotNull(), 1).otherwise(0)).alias(f"{c}_non_null")
        for c in result_df.columns
    ]).collect()[0]

    total_non_null_values = sum(non_null_counts.asDict().values())
    assert total_non_null_values > row_count, "Expected meaningful data, not just empty rows"

    # Verify the table has the expected structure by checking column count
    assert len(result_df.columns) > 0, "Expected table to have columns"


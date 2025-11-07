import pytest

from pyspark_streaming_base.app import StreamingApp

expected_checkpoint_dir = '/src/test/resources/pyspark_streaming_base:default:app/stable/_checkpoints'

def test_app_init():
    app: StreamingApp = (
        StreamingApp()
        # this allows you to modify the application,
        # for example, using `.with_kafka_source({...})`
        # or with, `.with_delta_sink({...})`
        # or to hook into the app for simple testing
        .with_config({
            'spark.app.name': 'pyspark_streaming_base:default:app',
            'spark.app.checkpoints.path': '/src/test/resources/',
            'spark.app.checkpoints.version': '1.0.0'
        })
        .initialize()
    )
    assert app.checkpoint_location().as_posix() == expected_checkpoint_dir

    with pytest.raises(RuntimeError):
        app.with_config({
            'spark.app.name': 'this.should.fail.cause.we.are.initialized'
        })

def test_app_simple_init():
    app: StreamingApp = StreamingApp(
        app_config={
            'spark.app.name': 'pyspark_streaming_base:default:app',
            'spark.app.checkpoints.path': '/src/test/resources/',
            'spark.app.checkpoints.version': '1.0.0'
        }
    )
    # note: there is no need to initialize here since
    # by applying the config with the constructor, we can automate the initialization step

    assert app.checkpoint_location().as_posix() == expected_checkpoint_dir

    with pytest.raises(RuntimeError):
        app.with_config({
            'spark.app.name': 'this.should.fail.cause.we.are.initialized'
        })

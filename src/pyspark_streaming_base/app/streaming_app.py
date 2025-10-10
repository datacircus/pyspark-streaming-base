from typing import Optional, Dict, Self
from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql.streaming import DataStreamReader, DataStreamWriter, StreamingQuery

from pyspark_streaming_base.app import App

class StreamingApp(App):
    # use to set the basedir for the application checkpoints (/Volumes/, etc)
    app_checkpoints_path: Optional[str] = None
    # use to cleanly separate the checkpoints for different versions of the application
    app_checkpoint_version: Optional[str] = None

    def __init__(self,
                 session: Optional[SparkSession] = None,
                 app_config: Dict[str, str] = None) -> None:
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

    def generate_read_stream(self, s_options: Dict[str, str]) -> DataStreamReader:
        """
        Generate the DataStreamReader. Note: This method can be further composed before calling "load"
        :param s_options: The dictionary of key/value pairs
        :return: The DataStreamReader object for composition
        """

        return self.spark.readStream.options(**s_options)

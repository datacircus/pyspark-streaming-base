# pyspark-streaming-base
This project provides a set of base classes that simplify the art of crafting bullet-proof Spark Structured Streaming applications.

<img width="348" height="367" alt="AutoSloth" src="https://github.com/user-attachments/assets/446a8a26-4d27-4d20-b45a-4394d823f5ec" />

## Getting Started

### Installation

```bash
pip install pyspark-streaming-base
```

Or with uv:
```bash
uv add pyspark-streaming-base
```

### Quick Example: Delta-to-Delta Streaming

Here's a complete example of a streaming application that reads from one Delta table and writes to another:

```python
from pathlib import Path
from pyspark_streaming_base.app import StreamingApp
from pyspark_streaming_base.sources import DeltaStreamingSource
from pyspark_streaming_base.sinks import DeltaStreamingSink

# Define paths
source_table_name = 'source_table'
sink_table_name = 'sink_table'
checkpoints_path = '/data/checkpoints'
source_table_path = f'/data/delta_tables/{source_table_name}'
sink_table_path = f'/data/delta_tables/{sink_table_name}'

# Configuration dictionary
spark_config = {
    # Application configuration
    'spark.app.name': 'delta-to-delta-streaming',
    'spark.app.version': '1.0.0',
    'spark.app.checkpoints.path': checkpoints_path,
    'spark.app.checkpoint.version': '1.0.0',

    # Source: Delta table configuration
    'spark.app.source.delta.options.path': source_table_path,
    'spark.app.source.delta.options.startingVersion': '0',
    'spark.app.source.delta.options.maxFilesPerTrigger': '10',
    'spark.app.source.delta.options.ignoreChanges': 'true',
    'spark.app.source.delta.options.withEventTimeOrder': 'true',
    'spark.app.source.delta.table.tableName': source_table_name,

    # Sink: Delta table configuration
    'spark.app.sink.delta.options.path': sink_table_path,
    'spark.app.sink.delta.options.outputMode': 'append',
    'spark.app.sink.delta.options.mergeSchema': 'true',
    'spark.app.sink.delta.options.maxRecordsPerFile': '100000',
    'spark.app.sink.delta.options.queryName': 'delta-to-delta-query',
    'spark.app.sink.delta.table.name': sink_table_name,
}

# Create and configure the application
app = StreamingApp()

# Apply all configurations to Spark RuntimeConf
for key, value in spark_config.items():
    app.spark.conf.set(key, value)

# Initialize the application
app.initialize()

# Set up source and sink (they read from RuntimeConf automatically)
delta_source = DeltaStreamingSource(config_prefix='spark.app.source')
delta_sink = DeltaStreamingSink(config_prefix='spark.app.sink')
app.with_source(delta_source).with_sink(delta_sink)

# Run the streaming query
df = app.delta_source().generate().load()
sink_options = app.delta_sink().options()

query = (
    delta_sink.fromDF(df)
    .queryName(sink_options['queryName'])
    .trigger(availableNow=True)  # Process all available data
    .outputMode(sink_options['outputMode'])
    .start()
)

query.awaitTermination()
```

### Key Features

- **Flexible Configuration**: Use RuntimeConf or builder pattern approaches
- **Three-Tier Config System**: Defaults → SparkSession config → Direct config
- **Built-in Sources**: Kafka and Delta Lake streaming sources
- **Built-in Sinks**: Delta Lake streaming sink
- **Checkpoint Management**: Automatic checkpoint location handling with version isolation
- **Testing Friendly**: Easy to configure for local testing with embedded Spark

### Documentation

For comprehensive documentation including configuration reference, testing patterns, and best practices, see:
- [Complete Getting Started Guide](https://github.com/datacircus/pyspark-streaming-base/blob/main/docs/overview.md)
- [Developer Guide (CLAUDE.md)](https://github.com/datacircus/pyspark-streaming-base/blob/main/CLAUDE.md)

## Local Developer Environment

### Prerequisites

- **Python 3.13+** - Managed via [uv](https://docs.astral.sh/uv/guides/install-python/)
- **Java 17 or 21** - Required for PySpark 4.0.1 (Spark uses Scala 2.13)
- **PySpark 4.0.1** - Specified in [pyproject.toml](https://github.com/datacircus/pyspark-streaming-base/blob/main/pyproject.toml)

### Java Setup (Mac)

```bash
# Install Java 21
brew install openjdk@21

# Set JAVA_HOME (add to ~/.zshrc or ~/.bashrc)
export JAVA_HOME=$(/usr/libexec/java_home -v 21)

# Verify installation
/usr/libexec/java_home -V
```

### Setup and Development

```bash
# Install Python 3.13
uv python install 3.13

# Sync dependencies
uv sync

# Build the package
uv build

# Run tests with coverage
uv run pytest --cov=pyspark_streaming_base --cov-report term

# Or use the Makefile
make build  # Runs sync, ruff check, pytest, and build
make test   # Runs pytest only
```

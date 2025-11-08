# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

pyspark-streaming-base provides base classes for building bullet-proof Spark Structured Streaming applications. The library abstracts the complexity of configuring Kafka and Delta Lake sources/sinks with a fluent builder pattern.

## Development Environment

### Prerequisites
- Python 3.13+ (managed via `uv`)
- Java 17 or 21 (Spark 4.0.1 compatible)
- PySpark 4.0.1
- Delta Lake 4.0.0
- Scala 2.13

### Java Setup
Set `JAVA_HOME` to JDK 21:
```bash
export JAVA_HOME=$(/usr/libexec/java_home -v 21)
```

### Package Management
This project uses [uv](https://docs.astral.sh/uv/) for dependency management:
```bash
uv sync                                                  # Install dependencies
uv build                                                 # Build package
uv run pytest                                            # Run all tests
uv run pytest --cov=pyspark_streaming_base --cov-report term  # Run with coverage
uv run pytest tests/test_streaming_app.py               # Run single test file
uv run pytest tests/test_streaming_app.py::test_app_init     # Run single test
```

### Makefile Commands
```bash
make dev        # Install Python 3.13
make version    # Show uv version
make build      # Sync deps, run ruff check, run tests, build package
make test       # Run pytest
```

## Architecture

### Core Components

**App (base class)**: Foundation for all streaming applications
- Manages SparkSession lifecycle
- Provides logging via `SparkLoggingProvider`
- Supports configuration via `with_config()` method or constructor `app_config` parameter
- Configuration keys prefixed with `spark.*` are applied to SparkSession
- Must call `initialize()` after `with_config()` (unless passing config to constructor)

**StreamingApp**: Extends `App` with streaming-specific functionality
- Manages checkpoint locations via `checkpoint_location()` method
- Checkpoint path structure: `{app_checkpoints_path}/{app_name}/{app_checkpoint_version}/_checkpoints`
- Requires `spark.app.checkpoints.path` and `spark.app.checkpoint.version` configuration
- Supports fluent builder pattern for configuring sources and sinks

**Sources** (inherit from `StreamingSource`):
- `KafkaStreamingSource`: Kafka topics as streaming source
- `DeltaStreamingSource`: Delta tables as streaming source
- Configuration via `config_prefix` (defaults to `spark.app.source`) + format name (e.g., `spark.app.source.kafka`)
- Options configured via `spark.app.source.{format}.options.*` keys
- Call `generate()` to get configured `DataStreamReader`

**Sinks** (inherit from `StreamingSink`):
- `DeltaStreamingSink`: Write to Delta tables
- Configuration via `config_prefix` (defaults to `spark.app.sink`) + format name
- Options configured via `spark.app.sink.{format}.options.*` keys
- Call `generate(df)` or `fromDF(df)` to get configured `DataStreamWriter`

### Configuration Pattern

All sources and sinks follow a three-tier configuration approach:

1. **Default values**: Hardcoded in `source_options` or `sink_options` dictionaries
2. **SparkSession config**: Retrieved via `with_config_from_spark()` using prefixed keys
3. **Direct config**: Passed via `config` parameter to constructor

Configuration prefixes enable multiple instances (e.g., `spark.app.source2.delta` for a second Delta source).

### Builder Pattern Example

```python
app = (
    StreamingApp()
    .with_config({
        'spark.app.name': 'my-streaming-app',
        'spark.app.checkpoints.path': '/path/to/checkpoints',
        'spark.app.checkpoint.version': '1.0.0'
    })
    .initialize()
    .with_kafka_source(config={'spark.app.source.topic': 'input-topic'})
    .with_delta_sink(config={'spark.app.sink.delta.options.path': '/path/to/output'})
)

# Access configured source/sink
reader = app.kafka_source().generate()
df = reader.load()
writer = app.delta_sink().generate(df)
query = writer.start()
```

## Testing

Tests are in `tests/` directory with corresponding resource directories in `tests/resources/`.

- Use `App.generate_spark_session()` static method for local testing with embedded Kafka/Delta jars
- Tests demonstrate both builder pattern (`with_config().initialize()`) and constructor pattern (`app_config={...}`)
- After initialization, calling `with_config()` raises `RuntimeError` to prevent accidental reconfiguration

## Key Configuration Keys

### Application-level
- `spark.app.name`: Application name
- `spark.app.version`: Application version
- `spark.app.logging.prefix`: Logging prefix (default: "App:core")
- `spark.app.checkpoints.path`: Base path for checkpoints
- `spark.app.checkpoint.version`: Checkpoint version for isolation

### Kafka Source
- `spark.app.source.topic`: Kafka topic to subscribe
- `spark.app.source.kafka.options.kafka.bootstrap.servers`: Kafka brokers
- `spark.app.source.kafka.options.startingOffsets`: "earliest" or "latest"
- `spark.app.source.kafka.options.maxOffsetsPerTrigger`: Throttle records per batch
- `spark.app.source.kafka.options.groupIdPrefix`: Unique group ID (defaults to `{app_name}:{checkpoint_version}`)

### Delta Source
- `spark.app.source.delta.options.path`: Path to Delta table (for unmanaged tables)
- `spark.app.source.table.catalog`: Catalog name (for managed tables)
- `spark.app.source.table.databaseOrSchema`: Database/schema name
- `spark.app.source.table.tableName`: Table name
- `spark.app.source.delta.options.maxFilesPerTrigger`: Throttle files per batch

### Delta Sink
- `spark.app.sink.delta.options.path`: Output path (for unmanaged tables)
- `spark.app.sink.table.catalog`: Catalog name (for managed tables)
- `spark.app.sink.table.databaseOrSchema`: Database/schema name
- `spark.app.sink.table.tableName`: Table name
- `spark.app.sink.delta.options.checkpointLocation`: Checkpoint location
- `spark.app.sink.delta.options.outputMode`: "append", "complete", or "update"

## Code Patterns

### Managed vs Unmanaged Tables
- Unmanaged: Specify `path` option
- Managed: Specify `table.catalog`, `table.databaseOrSchema`, and `table.tableName` configs
- `is_managed()` static method checks if table name contains "." separator

### SparkSession Access
- Sources/sinks support optional `session` parameter (defaults to `SparkSession.active()`)
- `App.generate_spark_session()` creates local session with Delta and Kafka jars for testing

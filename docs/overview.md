# Getting Started with pyspark-streaming-base

This guide provides an overview of how to build bullet-proof Spark Structured Streaming applications using the `pyspark-streaming-base` framework.

## Table of Contents

- [Core Concepts](#core-concepts)
- [Configuration Approaches](#configuration-approaches)
- [RuntimeConf Approach (Recommended for Testing)](#runtimeconf-approach-recommended-for-testing)
- [Builder Pattern Approach](#builder-pattern-approach)
- [Complete Example: Delta-to-Delta Streaming](#complete-example-delta-to-delta-streaming)
- [Key Configuration Keys](#key-configuration-keys)
- [Testing Your Application](#testing-your-application)

## Core Concepts

The `pyspark-streaming-base` framework provides three main components:

1. **StreamingApp**: The foundation that manages your SparkSession, checkpoint locations, and application lifecycle
2. **StreamingSource**: Abstractions for reading from Kafka and Delta Lake
3. **StreamingSink**: Abstractions for writing to Delta Lake

All configuration follows a **three-tier approach**:
1. Default values (hardcoded in the library)
2. SparkSession config (retrieved via `spark.conf`)
3. Direct config (passed via constructors or `with_config()`)

## Configuration Approaches

There are two primary ways to configure your streaming application:

### 1. RuntimeConf Approach (Recommended for Testing)

Directly set configuration on the SparkSession's `RuntimeConf` before initializing the application. This approach provides maximum flexibility and is ideal for testing scenarios.

### 2. Builder Pattern Approach (Recommended for Production)

Use the fluent `.with_config()` method to configure components. This approach is more declarative and works well with configuration files.

## RuntimeConf Approach (Recommended for Testing)

The RuntimeConf approach gives you fine-grained control over the SparkSession configuration **before** the application is initialized.

### Pattern

```python
from pyspark_streaming_base.app import StreamingApp

# Step 1: Create the application (without initialization)
app = StreamingApp()

# Step 2: Define all configurations in a single dictionary
spark_config = {
    # Application settings
    'spark.app.name': 'my-streaming-app',
    'spark.app.version': '1.0.0',
    'spark.app.checkpoints.path': '/path/to/checkpoints',
    'spark.app.checkpoint.version': '1.0.0',

    # Source configurations
    'spark.app.source.delta.options.path': '/path/to/source/table',
    'spark.app.source.delta.options.startingVersion': '0',

    # Sink configurations
    'spark.app.sink.delta.options.path': '/path/to/sink/table',
    'spark.app.sink.delta.options.outputMode': 'append',
}

# Step 3: Apply configurations to SparkSession RuntimeConf
for key, value in spark_config.items():
    app.spark.conf.set(key, value)

# Step 4: Initialize the application
app.initialize()

# Step 5: Configure sources and sinks (they read from RuntimeConf automatically)
from pyspark_streaming_base.sources import DeltaStreamingSource
from pyspark_streaming_base.sinks import DeltaStreamingSink

delta_source = DeltaStreamingSource(config_prefix='spark.app.source')
delta_sink = DeltaStreamingSink(config_prefix='spark.app.sink')

app.with_source(delta_source)
app.with_sink(delta_sink)

# Step 6: Run your streaming query
df = app.delta_source().generate().load()
query = delta_sink.fromDF(df).start()
query.awaitTermination()
```

### Why Use RuntimeConf?

- **Centralized configuration**: All configs in one place for easy review
- **Testing flexibility**: Easy to modify configs in test scenarios
- **Environment-specific settings**: Load configs from environment variables or files
- **Debugging**: Clear visibility into what configurations are being applied

### Important Notes

⚠️ **Configuration Key Precision**: Pay attention to singular vs. plural in config keys:
- `spark.app.checkpoint.version` (singular) - NOT `checkpoints.version`
- `spark.app.checkpoints.path` (plural)

⚠️ **Initialization Order**: You must call `app.initialize()` **after** setting RuntimeConf values, or they won't be picked up.

## Builder Pattern Approach

The builder pattern uses method chaining with `.with_config()` for a more declarative style.

### Pattern

```python
from pyspark_streaming_base.app import StreamingApp
from pyspark_streaming_base.sources import DeltaStreamingSource
from pyspark_streaming_base.sinks import DeltaStreamingSink

app = (
    StreamingApp()
    .with_config({
        'spark.app.name': 'my-streaming-app',
        'spark.app.version': '1.0.0',
        'spark.app.checkpoints.path': '/path/to/checkpoints',
        'spark.app.checkpoint.version': '1.0.0',
    })
    .initialize()
)

delta_source = (
    DeltaStreamingSource(config_prefix='spark.app.source')
    .with_config({
        'spark.app.source.delta.options.path': '/path/to/source/table',
        'spark.app.source.delta.options.startingVersion': '0',
    })
)

delta_sink = (
    DeltaStreamingSink(config_prefix='spark.app.sink')
    .with_config({
        'spark.app.sink.delta.options.path': '/path/to/sink/table',
        'spark.app.sink.delta.options.outputMode': 'append',
    })
)

app.with_source(delta_source).with_sink(delta_sink)

df = app.delta_source().generate().load()
query = delta_sink.fromDF(df).start()
query.awaitTermination()
```

### Alternative: Constructor Pattern

You can also pass configuration directly to the constructor:

```python
app = StreamingApp(
    app_config={
        'spark.app.name': 'my-streaming-app',
        'spark.app.checkpoints.path': '/path/to/checkpoints',
        'spark.app.checkpoint.version': '1.0.0',
    }
)
# No need to call initialize() - it's automatic when using constructor config
```

## Complete Example: Delta-to-Delta Streaming

This example demonstrates a complete end-to-end streaming application that reads from one Delta table and writes to another.

```python
from pathlib import Path
from pyspark.sql import DataFrame
from pyspark.sql.streaming import StreamingQuery
from pyspark_streaming_base.app import StreamingApp
from pyspark_streaming_base.sources import DeltaStreamingSource
from pyspark_streaming_base.sinks import DeltaStreamingSink

# Define paths and names
source_table_path = Path('/data/source/table')
sink_table_path = Path('/data/sink/table')
checkpoints_path = Path('/data/checkpoints')

# Create configuration dictionary
spark_config = {
    # Application settings
    'spark.app.name': 'delta-to-delta-streaming',
    'spark.app.version': '1.0.0',
    'spark.app.checkpoints.path': checkpoints_path.as_posix(),
    'spark.app.checkpoint.version': '1.0.0',

    # Source: Delta table configuration
    'spark.app.source.delta.options.path': source_table_path.as_posix(),
    'spark.app.source.delta.options.startingVersion': '0',
    'spark.app.source.delta.options.maxFilesPerTrigger': '10',
    'spark.app.source.delta.options.ignoreChanges': 'true',

    # Sink: Delta table configuration
    'spark.app.sink.delta.options.path': sink_table_path.as_posix(),
    'spark.app.sink.delta.options.outputMode': 'append',
    'spark.app.sink.delta.options.mergeSchema': 'true',
    'spark.app.sink.delta.options.maxRecordsPerFile': '100000',
}

# Create and configure the application
app = StreamingApp()

# Apply all configurations to Spark RuntimeConf
for key, value in spark_config.items():
    app.spark.conf.set(key, value)

# Initialize the application
app.initialize()

# Create source and sink (they automatically read from RuntimeConf)
delta_source = DeltaStreamingSource(config_prefix='spark.app.source')
delta_sink = DeltaStreamingSink(config_prefix='spark.app.sink')

# Attach to the application
app.with_source(delta_source)
app.with_sink(delta_sink)

# Generate the streaming DataFrame
df: DataFrame = app.delta_source().generate().load()

# Start the streaming query
sink_options = app.delta_sink().options()
query: StreamingQuery = (
    delta_sink.fromDF(df)
    .trigger(availableNow=True)  # Process all available data
    .outputMode(sink_options['outputMode'])
    .start()
)

# Wait for completion or run indefinitely
query.awaitTermination()
```

## Key Configuration Keys

### Application-Level

| Key | Description | Example |
|-----|-------------|---------|
| `spark.app.name` | Application name (required) | `'my-streaming-app'` |
| `spark.app.version` | Application version | `'1.0.0'` |
| `spark.app.checkpoints.path` | Base path for checkpoints (required) | `'/data/checkpoints'` |
| `spark.app.checkpoint.version` | Checkpoint version for isolation (required) | `'1.0.0'` |

### Delta Source

| Key | Description | Example |
|-----|-------------|---------|
| `spark.app.source.delta.options.path` | Path to Delta table | `'/data/source/table'` |
| `spark.app.source.delta.options.startingVersion` | Delta version to start from | `'0'` or `'latest'` |
| `spark.app.source.delta.options.maxFilesPerTrigger` | Throttle files per batch | `'10'` |
| `spark.app.source.delta.options.ignoreChanges` | Ignore schema changes | `'true'` or `'false'` |
| `spark.app.source.delta.options.withEventTimeOrder` | Order by event time | `'true'` or `'false'` |

### Delta Sink

| Key | Description | Example |
|-----|-------------|---------|
| `spark.app.sink.delta.options.path` | Output path | `'/data/sink/table'` |
| `spark.app.sink.delta.options.outputMode` | Output mode | `'append'`, `'complete'`, `'update'` |
| `spark.app.sink.delta.options.mergeSchema` | Allow schema evolution | `'true'` or `'false'` |
| `spark.app.sink.delta.options.maxRecordsPerFile` | Records per file limit | `'100000'` |
| `spark.app.sink.delta.options.checkpointLocation` | Custom checkpoint path | Auto-set from app |

### Kafka Source

| Key | Description | Example |
|-----|-------------|---------|
| `spark.app.source.topic` | Kafka topic to consume | `'my-topic'` |
| `spark.app.source.kafka.options.kafka.bootstrap.servers` | Kafka brokers | `'localhost:9092'` |
| `spark.app.source.kafka.options.startingOffsets` | Starting offset | `'earliest'` or `'latest'` |
| `spark.app.source.kafka.options.maxOffsetsPerTrigger` | Throttle records | `'1000'` |

## Testing Your Application

### Basic Test Pattern

```python
import pytest
from pathlib import Path
from pyspark.sql import DataFrame
from pyspark_streaming_base.app import StreamingApp

def test_my_streaming_app():
    # Setup
    test_source_path = Path(__file__).parent / 'resources' / 'source_table'
    test_sink_path = Path(__file__).parent / 'resources' / 'sink_table'

    spark_config = {
        'spark.app.name': 'test-streaming-app',
        'spark.app.checkpoints.path': '/tmp/checkpoints',
        'spark.app.checkpoint.version': '1.0.0',
        'spark.app.source.delta.options.path': test_source_path.as_posix(),
        'spark.app.sink.delta.options.path': test_sink_path.as_posix(),
    }

    app = StreamingApp()
    for key, value in spark_config.items():
        app.spark.conf.set(key, value)
    app.initialize()

    # ... configure sources and sinks ...

    # Run streaming query
    query.processAllAvailable()

    # Verify results
    result_df = app.spark.read.format('delta').load(test_sink_path.as_posix())
    assert result_df.count() > 0
```

### Data Quality Testing

Go beyond simple row counts by validating data quality:

```python
from pyspark.sql.functions import col, sum as spark_sum, when

def test_data_quality():
    # ... setup and run streaming query ...

    result_df = app.spark.read.format('delta').load(sink_path)

    # Verify data was written
    row_count = result_df.count()
    assert row_count > 0, "Expected data to be written"

    # Compute non-null fingerprint across all columns
    non_null_counts = result_df.select([
        spark_sum(when(col(c).isNotNull(), 1).otherwise(0)).alias(f"{c}_non_null")
        for c in result_df.columns
    ]).collect()[0]

    total_non_null_values = sum(non_null_counts.asDict().values())
    assert total_non_null_values > row_count, "Expected meaningful data, not empty rows"
```

## Tips and Best Practices

1. **Choose the Right Approach**: Use RuntimeConf for testing and flexibility, use builder pattern for production clarity
2. **Configuration Prefixes**: Use different prefixes (e.g., `spark.app.source2`) for multiple sources/sinks
3. **Checkpoint Isolation**: Always set `spark.app.checkpoint.version` to isolate different versions of your app
4. **Schema Evolution**: Enable `mergeSchema` on sinks if your schema might change over time
5. **Throttling**: Use `maxFilesPerTrigger` (Delta) or `maxOffsetsPerTrigger` (Kafka) to control throughput
6. **Testing**: Use `trigger(availableNow=True)` in tests to process all data without waiting

## Next Steps

- Check out the test examples in `tests/test_delta_end_to_end.py`
- Review the `CLAUDE.md` file for architecture details
- Explore the source code in `src/pyspark_streaming_base/`

For more information, visit the [GitHub repository](https://github.com/datacircus/pyspark-streaming-base).

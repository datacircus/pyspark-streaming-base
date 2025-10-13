# pyspark-streaming-base
This project provides a set of base classes that simplify the art of crafting bullet-proof Spark Structured Streaming applications.

## The Environment
* [uv](https://docs.astral.sh/uv/guides/install-python/)
* [Spark 4.0.1](https://spark.apache.org/downloads.html). This should match the pyspark version in the [pyproject.toml](./pyproject.toml)

## Java Home
> Setup on Mac
```bash
brew install openjdk@17
brew install openjdk@21
```

Here is an example from my local install. I needed to manually symlink since java 23 was taking over the mac
```bash
ln -s /opt/homebrew/Cellar/openjdk@21/21.0.8/libexec/openjdk.jdk /Users/scotthaines/Library/Java/JavaVirtualMachines/openjdk@21.0.8
```

Check that your version exists
```bash
/usr/libexec/java_home -V
```

The following I added to my `~/.zshrc`. Spark 4.0.1 supports Java 17 or 21, and Scala 2.13. Use jdk 21.
```text
export JAVA_HOME=$(/usr/libexec/java_home -v 21)
```

> Using `uv` : https://docs.astral.sh/uv/getting-started/features/#projects

```bash
uv python install 3.13
```

## Sync to match the `uv.lock`
```bash
uv sync
```

```bash
uv build
```

```bash
uv run pytest --cov=pyspark_streaming_base --cov-report term
```

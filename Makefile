dev:
	@uv install python 3.13

version:
	@uv version --short

build:
	@uv sync
	@uv run ruff check
	@uv run pytest
	@uv build

test:
	@uv run pytest

test-install:
	@uv run --with pyspark_streaming_base --no-project -- python -c "from pyspark_streaming_base.app import App"


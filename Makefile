dev:
	@uv install python 3.12.3

lock:
	@uv lock

version:
	@uv version --short

bump:
	@uv version --bump patch

build:
	@uv sync
	@uv run ruff check
	@uv run pytest
	@uv build

test:
	@uv run pytest

test-install:
	@uv run --with pyspark_streaming_base --no-project -- python -c "from pyspark_streaming_base.app import App"


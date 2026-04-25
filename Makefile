.PHONY: test cov lint clean

test:
	uv run pytest

cov:
	uv run pytest --cov=flowrhythm tests/

lint:
	ruff flowrhythm tests

clean:
	rm -rf htmlcov .coverage flowrhythm.egg-info __pycache__ .pytest_cache

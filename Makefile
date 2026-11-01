.PHONY: install lint test demo wheel deploy-dev deploy-prod setup-dev setup-prod

install:
	python -m pip install -e ".[dev]"

lint:
	ruff check src tests
	ruff format --check src tests

test:
	pytest

demo:
	python -m pulsecity.demo

wheel:
	python -m pip wheel --no-deps --wheel-dir dist .

deploy-dev:
	databricks bundle deploy -t dev

deploy-prod:
	databricks bundle deploy -t prod

setup-dev:
	databricks bundle run -t dev pulsecity_setup

setup-prod:
	databricks bundle run -t prod pulsecity_setup

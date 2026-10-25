.PHONY: help install test run-spark run-streaming run-local generate-data clean

help:
	@echo "RetailCo POS Data Platform CLI"
	@echo "------------------------------------------------------"
	@echo "make install       Install dependencies"
	@echo "make test          Run pytest automated test suite"
	@echo "make run-local     Execute end-to-end local batch pipeline"
	@echo "make run-spark     Run PySpark batch transformation job"
	@echo "make run-streaming Run Apache Beam streaming pipeline"
	@echo "make generate-data Generate synthetic 5,000 POS records"
	@echo "make clean         Clean temp files and caches"

install:
	pip install -r requirements.txt

test:
	pytest -v

run-local:
	python3 scripts/run_local_pipeline.py

run-spark:
	python3 spark/retailco_spark_job.py --input data/raw/clean_batch.csv --output data/output/spark_run --enable-salting --salts 4

run-streaming:
	./streaming/run_streaming.sh local

generate-data:
	python3 scripts/generate_synthetic_pos_data.py --count 5000 --output data/raw/synthetic_batch.csv

clean:
	find . -type d -name "__pycache__" -exec rm -rf {} +
	find . -type d -name ".pytest_cache" -exec rm -rf {} +
	rm -rf data/output/*

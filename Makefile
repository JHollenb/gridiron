.PHONY: setup clean ingest-dry ingest

# Python Env Variables
UV = uv run
SCHEMA = ./configs/schema.yaml
INPUT ?= downloads/test_data
OUTPUT = ./data/raw_pool

POOL = $(OUTPUT)
DUMP_FILE = 'test.csv'
NUM_PLAYS = 50

setup:
	@echo "Installing dependencies with uv..."
	uv sync

clean:
	@echo "Cleaning pycache..."
	find . -type d -name "__pycache__" -exec rm -rf {} +

# Usage: make ingest-dry INPUT=./downloads/2022
ingest-dry:
	$(UV) python src/ingest.py --input $(INPUT) --schema $(SCHEMA) --output $(OUTPUT) --dry-run

# Usage: make ingest INPUT=./downloads/2022
ingest:
	$(UV) python src/ingest.py --input $(INPUT) --schema $(SCHEMA) --output $(OUTPUT)

dump:
	$(UV) python src/export.py --pool $(POOL) --num-plays $(NUM_PLAYS) --output $(DUMP_FILE)


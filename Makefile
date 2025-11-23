.PHONY: setup clean ingest-dry ingest

# Python Env Variables
UV = uv run
SCHEMA = ./configs/schema.yaml
INPUT ?= downloads/test_data
OUTPUT = ./data/raw_pool

POOL = $(OUTPUT)
DUMP_FILE = 'test.csv'
NUM_PLAYS = 20

TEST_IN_DIR = data/nfl-bdb/2018/raw
TEST_IN = week_data-full.csv
TEST_OUT = test_data/n$(NUM_PLAYS)_$(TEST_IN)

setup:
	@echo "Installing dependencies with uv..."
	uv sync

clean:
	@echo "Cleaning pycache..."
	find . -type d -name "__pycache__" -exec rm -rf {} +

test:
	@echo "Testing"
	$(UV) python scripts/random_plays_sampler.py $(TEST_IN_DIR)/$(TEST_IN) $(TEST_OUT) $(NUM_PLAYS) --seed 42


server:
	$(UV) streamlit run app/main.py

dummy-server: dummy-data
	$(MAKE) ingest INPUT=./data/raw_downloads/dummy_season
	$(MAKE) server

# Usage: make ingest-dry INPUT=./downloads/2022
ingest-dry:
	$(UV) python src/ingest.py --input $(INPUT) --schema $(SCHEMA) --output $(OUTPUT) --dry-run

# Usage: make ingest INPUT=./downloads/2022
ingest:
	$(UV) python src/ingest.py --input $(INPUT) --schema $(SCHEMA) --output $(OUTPUT)

dump:
	$(UV) python src/export.py --pool $(POOL) --num-plays $(NUM_PLAYS) --output $(DUMP_FILE)

dummy-data:
	$(UV) python scripts/generate_dummy_data.py

diagnose:
	mkdir tmp
	$(MAKE) ingest | tee -a tmp/diagnose.log 
	$(UV) python scripts/diagnose_pool.py | tee -a tmp/diagnose.log

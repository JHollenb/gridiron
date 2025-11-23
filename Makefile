.PHONY: setup clean ingest-dry ingest

# Python Env Variables
UV = uv run
SCHEMA = ./configs/schema.yaml
INPUT ?= downloads/test_data
OUTPUT = ./data/raw_pool

POOL = $(OUTPUT)
DUMP_FILE = 'test.csv'
NUM_PLAYS = 20

setup:
	@echo "Installing dependencies with uv..."
	uv sync

clean:
	@echo "Cleaning pycache..."
	find . -type d -name "__pycache__" -exec rm -rf {} +

clean-data-pool:
	rm -rf $(OUTPUT)

generate-test-data: generate-test-data-2018 generate-test-data-2023

generate-test-data-2018:
	$(UV) python scripts/random_plays_sampler.py \
		./data/nfl-bdb/2018/raw/week_data-full.csv \
		./test_data/2018/2018-$(NUM_PLAYS)plays.csv \
		--n $(NUM_PLAYS) --year 2018 --seed 42
	@head ./test_data/2018/2018-$(NUM_PLAYS)plays.csv 

generate-test-data-2023:
	$(UV) python scripts/random_plays_sampler.py \
		./data/nfl-bdb/raw/2026/train/input_2023_w01.csv \
		./test_data/2023/2023-$(NUM_PLAYS)plays.csv \
		--n $(NUM_PLAYS) --year 2023 --seed 42
	@head ./test_data/2023/2023-$(NUM_PLAYS)plays.csv 

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
	cp $(SCHEMA) $(OUTPUT)-schema.yaml

dump:
	$(UV) python src/export.py --pool $(POOL) --num-plays $(NUM_PLAYS) --output $(DUMP_FILE)

dummy-data:
	$(UV) python scripts/generate_dummy_data.py

diagnose:
	mkdir tmp
	$(MAKE) ingest | tee -a tmp/diagnose.log 
	$(UV) python scripts/diagnose_pool.py | tee -a tmp/diagnose.log

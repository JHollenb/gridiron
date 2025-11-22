# Gridiron: NFL NGS Data Lake & Tensor Pipeline

A high-performance ingestion and query system for NFL Next Gen Stats (NGS) tracking data. This project normalizes disparate CSV sources (Big Data Bowl years, etc.) into a unified, schema-enforced Data Lake backed by Partitioned Parquet.

## 🏗 Architecture

### The "Side-Car" Data Lake Strategy
Instead of one massive file, we use a partitioned directory structure. This allows for infinite appending, fast querying, and safer updates.

1.  **Raw Pool (Immutable):** The normalized tracking data (x, y, s, a, etc.).
2.  **Feature Store (Side-Car):** Calculated metrics (e.g., "Distance to nearest defender") stored in parallel directories. Queries join these at runtime.

```text
data/
├── raw_pool/                     # The Core Dataset
│   ├── season=2021/
│   │   ├── gameId=2021090900/
│   │   │   └── tracking.parquet
│   │   └── ...
│   └── season=2022/ ...
└── derived_features/             # Side-Car Features
    ├── feature=pff_grades/
    └── feature=velocity_vectors/
````

## 🚀 Tech Stack
- **Manager:** `uv` (Lightning fast Python package manager) 
- **Processing:** `Polars` (Rust-backed DataFrames, lazy evaluation)
- **Storage:** Parquet (Snappy compression)
- **CLI:** `argparse`
    

## 🛠 Setup & Usage
### 1. Prerequisities
Ensure you have [uv](https://github.com/astral-sh/uv) installed.

### 2. Installation
```bash
make setup
```

### 3. Ingesting Data
Process a directory of raw CSVs into the Data Lake.

```bash
# Dry Run (See summary statistics only, no writing)
make ingest-dry INPUT=./raw_downloads/2022_season

# Live Run (Write to data pool)
make ingest INPUT=./raw_downloads/2022_season
```

### 4. Querying (Python Example)
```python
from src.query import GridironQuery

# Connect to the pool
db = GridironQuery("./data/raw_pool")

# Get a Tensor-ready dataset of 50 random pass plays
df = db.sample_plays(
    n=50, 
    filters=[
        (pl.col("event") == "pass_forward"),
        (pl.col("offenseFormation") == "SHOTGUN")
    ]
)
```

---

### 2. Directory Structure

```text
.
├── Makefile
├── pyproject.toml
├── configs
│   └── schema.yaml        # The source-of-truth for column definitions
├── data
│   └── raw_pool           # Where parquet files will live
├── src
│   ├── __init__.py
│   ├── ingest.py          # The Writer/Normalizer
│   └── query.py           # The Reader/Sampler
└── README.md
````

# Frontend
See [this](https://www.kaggle.com/code/huntingdata11/plotly-animated-and-interactive-nfl-plays)

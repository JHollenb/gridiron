# Gridiron Data Lake: Usage Guide

This document outlines how to interact with the Gridiron NGS Data Lake. The system uses **Polars** and **Partitioned Parquet** to handle large-scale NFL tracking data efficiently.

## 1\. The Architecture

Unlike a standard CSV workflow, we do not load the entire dataset into memory. The data lives on disk in a partitioned structure:

```text
data/raw_pool/
├── season=2021/
│   ├── gameId=2021090900/
│   │   └── tracking.parquet
│   └── ...
└── season=2022/
    └── ...
```

**Key Concept: Lazy Evaluation**
When you query the pool, Polars scans the *metadata* first. It only reads the heavy tracking data (x, y, s, a) when you explicitly call `.collect()`. This allows you to query 500GB of data on a laptop instantly.

-----

## 2\. Basic Setup

Always access the data via the `GridironQuery` wrapper. It handles path resolution and standardizes access.

```python
from src.query import GridironQuery
import polars as pl

# Initialize the connection (Instant - no data loaded yet)
db = GridironQuery("./data/raw_pool")
```

-----

## 3\. ML Workflow: creating Training Batches

For machine learning, you rarely want the whole dataset. You usually want a **random batch of $N$ plays** that meet specific criteria.

### A. Sampling Random Plays

Use `sample_plays()` to get a clean DataFrame containing exactly $N$ plays.

```python
# Get 32 random plays (Typical mini-batch size)
batch_df = db.sample_plays(n=32, seed=42)

print(batch_df.shape) 
# Returns all frames for those 32 plays
```

### B. Filtering (Conditional Batches)

If you are training a model specifically on **Pass Coverage**, you don't want Run plays or Special Teams.

```python
# Define filters using Polars expressions
filters = [
    (pl.col("event") == "pass_forward"),      # Only pass plays
    (pl.col("offenseFormation") == "SHOTGUN") # Specific formation
]

# The sampler applies these filters FIRST, then picks 50 random plays from the results
shotgun_pass_batch = db.sample_plays(n=50, filters=filters)
```

-----

## 4\. Preparing Data for Tensors

Tracking data comes in "Long Format" (one row per player per frame). ML Models (PyTorch/TF) usually expect "Wide Format" tensors: `(Batch, Frames, Players, Features)`.

Here is a recipe to transform a `batch_df` into a Tensor:

```python
import torch
import numpy as np

def to_tensor(batch_df: pl.DataFrame, max_frames=100):
    """
    Converts Polars DF -> PyTorch Tensor (Batch, Frames, Players, Features)
    Features: [x, y, s, a, o, dir]
    """
    # 1. Pivot or Group to organize data
    # We need to ensure consistent ordering: Home 1-11, Away 1-11, Ball
    
    # Example: Filter to just x, y, s
    features = ["x", "y", "s"]
    
    # 2. Group by Play and Convert to Numpy
    # This is a simplified example. You typically pad frames here.
    games = batch_df["gameId"].unique()
    plays = batch_df["playId"].unique()
    
    tensor_list = []
    
    for g, p in zip(games, plays):
        # Extract single play
        play = batch_df.filter((pl.col("gameId")==g) & (pl.col("playId")==p))
        
        # Matrix logic here (Pivot players to columns)
        # ...
        
        # tensor_list.append(play_matrix)
        pass

    return torch.stack(tensor_list)
```

-----

## 5\. FAQ & Best Practices

### Q: Should I use `schema.yaml` for validation during querying?

**No.**
The schema is strictly for **Ingestion** (writing data).

  * **Why?** Parquet files are "self-describing." They already know that `x` is a `Float32` and `team` is a `Categorical`.
  * **When to use it:** You might read `configs/schema.yaml` if you want to programmatically get a list of all available feature columns to build your Neural Network input layer dynamically.

### Q: How do I add new features (e.g., "Distance to QB")?

Do **not** edit the raw parquet files.

1.  Load a batch using `GridironQuery`.
2.  Calculate the feature in memory using Polars (it's incredibly fast).
3.  Feed that into your model.

*If the feature is computationally expensive (e.g., complex physics derivatives)*:
Create a **Feature Store** (a parallel directory `data/features/`) and join it to the raw data using `gameId` + `playId` + `frameId`.

### Q: It's saying "Memory Error" when I collect?

You are likely calling `.collect()` on the whole pool without filtering.

  * **Bad:** `db.get_pool().collect()` (Tries to load 50GB into RAM).
  * **Good:** `db.get_pool().filter(pl.col("gameId") == 2022091100).collect()` (Loads just one game).

### Q: How to handle the ball?

The ball usually has `nflId = null`.

```python
# Filter for the ball
ball_df = df.filter(pl.col("nflId").is_null())

# Filter for players
player_df = df.filter(pl.col("nflId").is_not_null())
```

-----

## 6\. Cheat Sheet

| Goal | Code |
| :--- | :--- |
| **Connect** | `db = GridironQuery("./data/raw_pool")` |
| **Get Lazy Frame** | `q = db.get_pool()` |
| **Get Unique Games** | `q.select("gameId").unique().collect()` |
| **Filter by Team** | `q.filter(pl.col("team") == "home")` |
| **Get 1 Specific Play** | `q.filter((pl.col("gameId")==X) & (pl.col("playId")==Y)).collect()` |

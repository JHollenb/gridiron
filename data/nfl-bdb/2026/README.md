# NFL Big Data Bowl 2026 - Prediction Dataset README

## Overview
This is the official dataset for the **NFL Big Data Bowl 2026 - Prediction** Kaggle competition.  
The goal is to **predict the exact (x, y) position of players after a forward pass is thrown**, while the ball is still in the air, using NFL Next Gen Stats tracking data.

The competition has two phases:
1. **Training phase** – Model on historical 2023 season data (weeks 1–18)
2. **Forecasting phase** – Predict on unseen live games for the remainder of the 2025 NFL season

## Task
Given tracking data **up to the moment the ball is thrown**, predict the future `(x, y)` coordinates of selected players for a variable number of frames (typically 10–30 frames ≈ 1–3 seconds) after the pass is released.

Evaluation metric: **Mean Absolute Error (MAE)** on predicted `x` and `y` for scored players only.

## Data Structure

### Main Training Files (in `/train/`)

- `input_2023_w[01-18].csv` → Data **before** the pass is thrown (input features)
- `output_2023_w[01-18].csv` → Data **after** the pass is thrown (target variables)

Both input and output files share the same key columns for joining:
```text
game_id + play_id + nfl_id + frame_id
```

### Key Columns

| Column                    | Description                                                                                   | Type      |
|---------------------------|-------------------------------------------------------------------------------------------------|-----------|
| `game_id`                 | Unique game identifier                                                                        | numeric   |
| `play_id`                 | Play identifier (unique within a game)                                                        | numeric   |
| `nfl_id`                  | Unique player ID                                                                              | numeric   |
| `frame_id`                | Frame number (starts at 1 for each play/input or output)                                      | numeric   |
| `player_to_predict`       | **(Input only)** True if this player's future positions will be scored                        | bool      |
| `num_frames_output`       | **(Input only)** How many future frames exist for this player in the corresponding output file| numeric   |
| `x`, `y`                  | Player position on field (yards). **Target to predict in output files**                        | numeric   |
| `s`                       | Speed (yards/second)                                                                          | numeric   |
| `a`                       | Acceleration (yards/second²)                                                                  | numeric   |
| `dir`                     | Direction of motion (degrees)                                                                 | numeric   |
| `o`                       | Player orientation (degrees)                                                                  | numeric   |
| `play_direction`          | Offense direction ("left" or "right")                                                         | text      |
| `absolute_yardline_number`| Distance from opponent's end zone for the possession team                                     | numeric   |
| `player_position`         | Official position (e.g., QB, WR, CB)                                                          | text      |
| `player_side`             | Offense or Defense                                                                            | text      |
| `player_role`             | Role on this play (Targeted Receiver, Defensive Coverage, Passer, Other Route Runner)        | text      |
| `ball_land_x`, `ball_land_y` | Where the ball lands (or would land if incomplete)                                        | numeric   |
| `player_name`, `player_height`, `player_weight`, `player_birth_date` | Player metadata                                      | text / date |

### Test / Submission Files

- `test_input.csv` – Example of real test input structure (provided for convenience)
- `test.csv` – Mock submission file listing every `(game_id, play_id, nfl_id, frame_id)` that needs a prediction (contains column `id` for submission)

During the live phase, real test data is served via the **Kaggle Evaluation API** (see `kaggle_evaluation/` folder and demo notebook).

### Data Size
- Total: **~865 MB**
- 49 CSV files (18 weeks × input + output + test files)
- Public leaderboard test set: ~60k rows to predict
- Final private test set (live games): roughly same size

### License
**CC BY-NC 4.0** – Attribution-NonCommercial 4.0 International  
You may use the data for research and this competition, but not for commercial purposes without permission.

## How to Use

```bash
# Download all data
kaggle competitions download -c nfl-big-data-bowl-2026-prediction
unzip nfl-big-data-bowl-2026-prediction.zip -d nfl_bdb_2026
```

### Basic Loading Example (Python)

```python
import pandas as pd
import glob

# Load all input files
input_files = glob.glob("train/input_*.csv")
inputs = pd.concat([pd.read_csv(f) for f in input_files], ignore_index=True)

# Load all output files (targets)
output_files = glob.glob("train/output_*.csv")
outputs = pd.concat([pd.read_csv(f) for f in output_files], ignore_index=True)

# Merge input + future positions for modeling
data = inputs.merge(outputs, 
                    on=['game_id', 'play_id', 'nfl_id', 'frame_id'], 
                    suffixes=('_input', '_future'))
```

Only rows where `player_to_predict == True` in the input files will be scored.

## Supplementary Resources
- A separate **analytics-focused dataset** from previous Big Data Bowls is linked on the competition page and provides play-level info (e.g., pass result, personnel, etc.) that can be joined on `game_id` + `play_id`.

## Links
- Competition page: https://www.kaggle.com/competitions/nfl-big-data-bowl-2026-prediction
- Discussion forum: https://www.kaggle.com/competitions/nfl-big-data-bowl-2026-prediction/discussion

Good luck, and happy modeling!

# Polar Usage
# Example usage in Python
"""
import polars as pl
from pathlib import Path

schema = pl.Config.load("schema.yaml")["input_schema"]  # or output_schema

df = pl.read_csv(
    "train/input_2023_w01.csv",
    schema_overrides=schema,
    **pl.Config.load("schema.yaml")["read_csv_options"]
)
"""


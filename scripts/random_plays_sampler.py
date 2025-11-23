#!/usr/bin/env python
"""
random_plays_sampler.py

Sample N complete plays (all frames) from a large tracking CSV and write them to a new file.

Usage:
    python random_plays_sampler.py input_file.csv output_file.csv 1000 --seed 42

or from inside Python:
    from random_plays_sampler import sample_random_plays
    sample_random_plays("big_tracking.csv", "sample_500_plays.csv", n=500)
"""

import argparse
import pandas as pd
import numpy as np
from pathlib import Path


_2018_GAME_ID = "gameId"
_2018_PLAY_ID = 'playId'
_2018_FRAME_ID = 'frameId'
_2018_NFL_ID = 'nflId'

_2023_PLAY_ID = "play_id"
_2023_GAME_ID = 'game_id'
_2023_FRAME_ID = 'frame_id'
_2023_NFL_ID = 'nfl_id'


PLAY_ID=_2018_PLAY_ID
GAME_ID=_2018_GAME_ID
FRAME_ID=_2018_FRAME_ID


def sample_random_plays(
    input_path: str | Path,
    output_path: str | Path,
    year: int = 2023,
    n: int = 1000,
    seed: int | None = None,
) -> None:
    """
    Parameters
    ----------
    input_path : str or Path
        Path to the original tracking CSV.
    output_path : str or Path
        Where to save the sampled plays.
    n : int
        Number of distinct plays (play_id) to sample.
    seed : int, optional
        Random seed for reproducibility.
    """
    input_path = Path(input_path)
    output_path = Path(output_path)

    if not input_path.exists():
        raise FileNotFoundError(f"Input file not found: {input_path}")

    if year == 2023:
        FRAME_ID = _2023_FRAME_ID
        GAME_ID = _2023_GAME_ID
        PLAY_ID = _2023_PLAY_ID
        NFL_ID = _2023_NFL_ID
    else:
        FRAME_ID = _2018_FRAME_ID
        GAME_ID = _2018_GAME_ID
        PLAY_ID = _2018_PLAY_ID
        NFL_ID = _2018_NFL_ID

    print(f"Loading {input_path} ...")
    # Using chunksize + usecols is optional for huge files; here we load everything because we need play_id grouping
    df = pd.read_csv(input_path, dtype={PLAY_ID: int})  # play_id is integer in your sample

    total_plays = df[PLAY_ID].nunique()
    print(f"Found {total_plays:,} unique plays in the dataset.")

    if n > total_plays:
        print(f"Warning: Requested {n} plays but only {total_plays} exist. Using all plays.")
        n = total_plays

    rng = np.random.default_rng(seed)
    selected_play_ids = rng.choice(
        df[PLAY_ID].unique(), size=n, replace=False
    )

    print(f"Selected {len(selected_play_ids):,} random play_id values.")

    # Boolean mask for the selected plays
    mask = df[PLAY_ID].isin(selected_play_ids)
    sampled_df = df[mask].copy()

    # Optional: sort by game_id -> play_id -> frame_id for nicer output
    sampled_df = sampled_df.sort_values([GAME_ID, PLAY_ID, FRAME_ID, NFL_ID]).reset_index(drop=True)

    print(f"Writing {len(sampled_df):,} rows ({n} plays) to {output_path} ...")
    sampled_df.to_csv(output_path, index=False)
    print("Done!")


def main():
    parser = argparse.ArgumentParser(
        description="Sample N random complete plays from a tracking CSV."
    )
    parser.add_argument("input", type=str, help="Path to input CSV file")
    parser.add_argument("output", type=str, help="Path to output CSV file")
    parser.add_argument("--year", type=int, default=2023, help="NFL BDB Year, (2018, 2023)")
    parser.add_argument(
        "--n",
        type=int,
        nargs="?",
        default=1000,
        help="Number of plays to sample (default: 1000)",
    )
    parser.add_argument(
        "--seed",
        type=int,
        default=None,
        help="Random seed for reproducible sampling",
    )

    args = parser.parse_args()

    sample_random_plays(args.input, args.output, args.year, n=args.n, seed=args.seed)


if __name__ == "__main__":
    main()

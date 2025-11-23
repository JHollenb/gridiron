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


def sample_random_plays(
    input_path: str | Path,
    output_path: str | Path,
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

    print(f"Loading {input_path} ...")
    # Using chunksize + usecols is optional for huge files; here we load everything because we need play_id grouping
    df = pd.read_csv(input_path, dtype={"play_id": int})  # play_id is integer in your sample

    total_plays = df["play_id"].nunique()
    print(f"Found {total_plays:,} unique plays in the dataset.")

    if n > total_plays:
        print(f"Warning: Requested {n} plays but only {total_plays} exist. Using all plays.")
        n = total_plays

    rng = np.random.default_rng(seed)
    selected_play_ids = rng.choice(
        df["play_id"].unique(), size=n, replace=False
    )

    print(f"Selected {len(selected_play_ids):,} random play_id values.")

    # Boolean mask for the selected plays
    mask = df["play_id"].isin(selected_play_ids)
    sampled_df = df[mask].copy()

    # Optional: sort by game_id -> play_id -> frame_id for nicer output
    sampled_df = sampled_df.sort_values(["game_id", "play_id", "frame_id"]).reset_index(drop=True)

    print(f"Writing {len(sampled_df):,} rows ({n} plays) to {output_path} ...")
    sampled_df.to_csv(output_path, index=False)
    print("Done!")


def main():
    parser = argparse.ArgumentParser(
        description="Sample N random complete plays from a tracking CSV."
    )
    parser.add_argument("input", type=str, help="Path to input CSV file")
    parser.add_argument("output", type=str, help="Path to output CSV file")
    parser.add_argument(
        "n",
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

    sample_random_plays(args.input, args.output, n=args.n, seed=args.seed)


if __name__ == "__main__":
    main()

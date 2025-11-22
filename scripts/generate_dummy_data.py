import pandas as pd
import numpy as np
import os
from pathlib import Path

# Config
OUTPUT_DIR = "./data/raw_downloads/dummy_season"
NUM_GAMES = 2
PLAYS_PER_GAME = 5
FRAMES_PER_PLAY = 50

def generate_play_data(game_id, play_id):
    """Generates 22 players + 1 ball for 50 frames of movement."""
    frames = []
    
    # 22 Players + Ball
    # We cheat and just make them move in a straight line for the visual
    identities = []
    
    # Home Team (11)
    for i in range(1, 12):
        identities.append({"nflId": 1000 + i, "team": "home", "jerseyNumber": i, "position": "WR"})
    # Away Team (11)
    for i in range(1, 12):
        identities.append({"nflId": 2000 + i, "team": "away", "jerseyNumber": i, "position": "CB"})
    # Ball (1)
    identities.append({"nflId": None, "team": "football", "jerseyNumber": None, "position": None})

    for frame_idx in range(1, FRAMES_PER_PLAY + 1):
        for entity in identities:
            # Random starting spot + movement
            # In a real generator, we'd use physics. Here we use noise.
            start_x = 50 + np.random.normal(0, 10)
            start_y = 26 + np.random.normal(0, 10)
            
            # Move 0.1 yards per frame (roughly 20mph)
            x = start_x + (frame_idx * 0.1) 
            y = start_y + (np.random.normal(0, 0.1)) # Jitter
            
            row = {
                "gameId": game_id,
                "playId": play_id,
                "frameId": frame_idx,
                "time": "2023-09-10T13:00:00.000Z", # Dummy ISO time
                "nflId": entity["nflId"],
                "team": entity["team"],
                "jerseyNumber": entity["jerseyNumber"],
                "position": entity["position"],
                "playDirection": "right",
                "x": x,
                "y": y,
                "s": 5.5,  # Speed
                "a": 2.1,  # Accel
                "dis": 0.1, # Dist
                "o": 90.0, # Orientation
                "dir": 90.0, # Direction
                "event": "pass_forward" if frame_idx == 25 else None,
                "route": "GO" if entity["position"] == "WR" else None
            }
            frames.append(row)
            
    return frames

def main():
    Path(OUTPUT_DIR).mkdir(parents=True, exist_ok=True)
    
    all_rows = []
    
    print(f"Generating {NUM_GAMES} games with {PLAYS_PER_GAME} plays each...")
    
    for g in range(NUM_GAMES):
        game_id = 2023090000 + g
        for p in range(PLAYS_PER_GAME):
            play_id = (p + 1) * 50 # Play IDs are usually skipped integers
            
            play_frames = generate_play_data(game_id, play_id)
            all_rows.extend(play_frames)
            
    # Convert to DataFrame
    df = pd.DataFrame(all_rows)
    
    # Save as CSV (simulating the raw Big Data Bowl download)
    output_path = f"{OUTPUT_DIR}/tracking2023.csv"
    print(f"Writing {len(df)} rows to {output_path}...")
    df.to_csv(output_path, index=False)
    print("Done! You can now run 'make ingest'")

if __name__ == "__main__":
    main()

import polars as pl
from pathlib import Path
import os

class GridironQuery:
    def __init__(self, data_pool_path="./data/raw_pool"):
        # Force absolute path resolution immediately
        self.path = Path(data_pool_path).resolve()
        
        if not self.path.exists():
            raise FileNotFoundError(f"Data pool not found at absolute path: {self.path}")
            
    
    def get_pool(self):
        """Returns a LazyFrame of the entire data lake."""
        # Use the absolute path for the glob string
        # We use os.path.join to ensure OS-agnostic separators
        pattern = str(self.path / "**" / "*.parquet")
        
        # Debug print (visible in Streamlit server logs)
        print(f"DEBUG: Scanning parquet pattern: {pattern}")
        
        try:
            return pl.scan_parquet(pattern)
        except Exception as e:
            # Fallback: If glob fails, maybe try direct directory (some Polars versions prefer this)
            print(f"DEBUG: Glob failed ({e}), trying direct directory scan...")
            return pl.scan_parquet(str(self.path))


    def sample_plays(self, n=10, filters=None, seed=42):
        q = self.get_pool()
        
        if filters:
            for f in filters:
                q = q.filter(f)
        
        # Get Unique Keys
        valid_keys = (
            q.select(["gameId", "playId"])
            .unique()
            .collect()
        )
        
        if valid_keys.height == 0:
            print("Warning: No plays found matching criteria.")
            return pl.DataFrame() # Return empty
        
        if valid_keys.height < n:
            selected_keys = valid_keys
        else:
            selected_keys = valid_keys.sample(n=n, seed=seed)

        final_df = q.join(selected_keys.lazy(), on=["gameId", "playId"], how="inner").collect()
        return final_df

    def _sample_plays(self, n=10, filters=None, seed=42):
        """
        Returns a materialized DataFrame of N random plays meeting criteria.
        """
        q = self.get_pool()
        
        # 1. Apply Filters
        if filters:
            for f in filters:
                q = q.filter(f)
        
        # 2. Get Unique Play Keys (GameId, PlayId) meeting criteria
        # We optimize by selecting only keys first
        valid_keys = (
            q.select(["gameId", "playId"])
            .unique()
            .collect() # Must collect to sample
        )
        
        if valid_keys.height < n:
            print(f"Warning: Only found {valid_keys.height} valid plays. Returning all.")
            selected_keys = valid_keys
        else:
            selected_keys = valid_keys.sample(n=n, seed=seed)

        # 3. Join back to get full tracking data for only those plays
        # We cast selected_keys back to lazy to join efficiently
        final_df = q.join(selected_keys.lazy(), on=["gameId", "playId"], how="inner").collect()
        
        return final_df

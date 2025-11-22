import polars as pl
from pathlib import Path

class GridironQuery:
    def __init__(self, data_pool_path="./data/raw_pool"):
        self.path = Path(data_pool_path)
    
    def get_pool(self):
        """Returns a LazyFrame of the entire data lake."""
        return pl.scan_parquet(self.path / "**/*.parquet")

    def sample_plays(self, n=10, filters=None, seed=42):
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

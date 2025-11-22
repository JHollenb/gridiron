import argparse
import glob
import os
import yaml
import polars as pl
from pathlib import Path

class NGSIngestor:
    def __init__(self, schema_path, output_dir):
        with open(schema_path, "r") as f:
            self.config = yaml.safe_load(f)
        self.output_dir = Path(output_dir)
        self.target_cols = [c['name'] for c in self.config['target_columns']]
        self.aliases = self.config.get('aliases', {})

    def load_and_normalize(self, file_path):
        """
        Reads a CSV, applies aliases, casts types, and ensures schema compliance.
        """
        print(f"Processing {file_path}...")
        
        # Scan CSV (Lazy)
        q = pl.scan_csv(file_path, ignore_errors=True)
        
        # 1. Rename columns based on alias map
        # Create a mapping dict where key exists in schema
        available_cols = q.columns
        rename_map = {old: new for old, new in self.aliases.items() if old in available_cols}
        q = q.rename(rename_map)

        # 2. Select only target columns (fill missing with Null)
        expressions = []
        for col_def in self.config['target_columns']:
            name = col_def['name']
            dtype = getattr(pl, col_def['dtype'])
            
            if name in q.columns:
                expressions.append(pl.col(name).cast(dtype))
            else:
                # Create null column if missing
                expressions.append(pl.lit(None).cast(dtype).alias(name))
        
        q = q.select(expressions)
        return q

    def generate_summary(self, df: pl.DataFrame):
        """Generates stat summary for approval."""
        print("\n--- Ingestion Summary ---")
        print(f"Total Rows: {df.height}")
        print(f"Unique Games: {df['gameId'].n_unique()}")
        print(f"Unique Plays: {df['playId'].n_unique()}")
        
        # Check for speed anomalies
        max_speed = df['s'].max()
        print(f"Max Speed seen: {max_speed} (Check if > 15 yards/sec)")
        
        print("-------------------------")

    def run(self, input_dir, dry_run=False):
        # 1. Gather files
        files = glob.glob(os.path.join(input_dir, "*.csv"))
        if not files:
            raise FileNotFoundError(f"No CSVs found in {input_dir}")

        # 2. Process Loop
        # We process eagerly here to partition safely, but could stay lazy for bigger RAM
        for f in files:
            lz_df = self.load_and_normalize(f)
            df = lz_df.collect() # Materialize to memory for splitting
            
            self.generate_summary(df)

            if dry_run:
                print("Dry Run: Skipping write.")
                continue

            # 3. Partitioned Write
            # We write: output_dir / gameId=XXXX / tracking.parquet
            # Note: Often BDB data is one season per file. We partition by GameId.
            print(f"Writing partition to {self.output_dir}...")
            
            # Polars partition write
            df.write_parquet(
                self.output_dir,
                use_pyarrow=True,
                partition_by=["gameId"] # Creates subfolders automatically
            )
            print("Write Complete.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True, help="Directory of raw CSVs")
    parser.add_argument("--schema", default="./configs/schema.yaml", help="Path to schema yaml")
    parser.add_argument("--output", default="./data/raw_pool", help="Output Data Lake path")
    parser.add_argument("--dry-run", action="store_true", help="Process without writing")
    
    args = parser.parse_args()
    
    ingestor = NGSIngestor(args.schema, args.output)
    ingestor.run(args.input, args.dry_run)

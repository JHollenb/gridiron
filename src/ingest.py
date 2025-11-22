# src/ingest.py
import argparse
import glob
import os
import yaml
import polars as pl
from pathlib import Path
from tqdm import tqdm

class NGSIngestor:
    def __init__(self, schema_path, output_dir):
        # Force absolute path resolution
        self.output_dir = Path(output_dir).resolve()
        self.schema_path = Path(schema_path).resolve()
        
        with open(self.schema_path, "r") as f:
            self.config = yaml.safe_load(f)

    def load_and_normalize(self, file_path):
        """
        Reads a CSV, applies aliases, casts types, and ensures schema compliance.
        """
        q = pl.scan_csv(file_path, infer_schema_length=10000, ignore_errors=True)
        actual_columns = q.collect_schema().names()
        # actual_columns = q.columns
        selected_exprs = []
        for col_def in self.config['columns']:
            target_name = col_def['name']
            dtype = getattr(pl, col_def['dtype'])
            candidates = [target_name] + col_def.get('aliases', [])
            source_col = next((c for c in candidates if c in actual_columns), None)
            
            if source_col:
                expr = pl.col(source_col).cast(dtype).alias(target_name)
            else:
                if 'default' in col_def:
                    expr = pl.lit(col_def['default']).cast(dtype).alias(target_name)
                elif col_def.get('allow_null', False):
                    expr = pl.lit(None).cast(dtype).alias(target_name)
                else:
                    print(f"Warning: Missing required col '{target_name}' in {file_path}")
                    continue
            selected_exprs.append(expr)
        return q.select(selected_exprs)

    def generate_summary(self, df: pl.DataFrame):
        """Generates stat summary for approval."""
        print("\n--- Ingestion Summary ---")
        print(f"Total Rows: {df.height}")
        print(f"Unique Games: {df['gameId'].n_unique()}")
        print(f"Unique Plays: {df['playId'].n_unique()}")
        
        # Check for speed anomalies
        max_frame = df['frameId'].max()
        print(f"Max frame id seen: {max_frame}")
        
        print("-------------------------")

    def write_partitioned(self, df: pl.DataFrame):
        """
        Manually partitions data by GameId and writes to Season/Game folders.
        Safe for appending.
        """
        # unique games in this batch
        # We group by gameId to write distinct files
        # partitioning: data/raw_pool/season=2023/gameId=2023090100/tracking.parquet
        
        # Iterate over each game in this CSV
        for (game_id,), game_df in df.group_by(["gameId"]):
            if game_id is None: 
                continue

            # Derive season from first 4 chars of gameId (e.g. 2022090800 -> 2022)
            season = str(game_id)[:4]
            
            # Construct Target Directory
            # Structure: raw_pool / season=XXXX / gameId=YYYY
            target_dir = self.output_dir / f"season={season}" / f"gameId={game_id}"
            target_dir.mkdir(parents=True, exist_ok=True)
            
            # Construct Filename
            # We use a fixed name "tracking.parquet". 
            # If you process the SAME game twice, this overwrite ensures we update the data.
            target_file = target_dir / "tracking.parquet"
            
            # Write (Eagerly)
            game_df.write_parquet(target_file)

    def run(self, input_dir, dry_run=False):
        input_path = Path(input_dir).resolve()
        files = glob.glob(str(input_path / "*.csv"))
        
        if not files:
            raise FileNotFoundError(f"No CSVs found in {input_path}")

        print(f"📂 Output Pool: {self.output_dir}")
        
        for f in tqdm(files, desc="Processing Files"):
            try:
                lz_df = self.load_and_normalize(f)
                df = lz_df.collect() 
                
                if df.height == 0:
                    print(f"⚠️ Warning: {f} resulted in 0 rows. Skipping.")
                    continue

                if dry_run:
                    print(f"Dry Run: Parsed {df.height} rows from {Path(f).name}")
                    continue

                # Use the new explicit writer
                self.write_partitioned(df)
                
            except Exception as e:
                print(f"❌ Error processing {f}: {e}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True, help="Directory of raw CSVs")
    parser.add_argument("--schema", default="./configs/schema.yaml", help="Path to schema yaml")
    parser.add_argument("--output", default="./data/raw_pool", help="Output Data Lake path")
    parser.add_argument("--dry-run", action="store_true", help="Process without writing")
    
    args = parser.parse_args()
    
    ingestor = NGSIngestor(args.schema, args.output)
    ingestor.run(args.input, args.dry_run)

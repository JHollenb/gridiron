# src/ingest.py
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

    def load_and_normalize(self, file_path):
        """
        Reads a CSV, applies aliases, casts types, and ensures schema compliance.
        """
        print(f"Processing {file_path}...")
        
        # 1. Lazy Scan
        # We don't provide schema_overrides here yet because names might be wrong
        q = pl.scan_csv(file_path, infer_schema_length=10000, ignore_errors=True)
        
        # Get actual columns in this file
        # actual_columns = q.columns
        actual_columns = q.collect_schema().names()
        
        selected_exprs = []
        
        # 2. Iterate through our Master Schema
        for col_def in self.config['columns']:
            target_name = col_def['name']
            dtype_str = col_def['dtype']
            dtype = getattr(pl, dtype_str)
            
            # A. Find the source column using aliases
            source_col = None
            # Check the target name itself first, then aliases
            candidates = [target_name] + col_def.get('aliases', [])
            
            for candidate in candidates:
                if candidate in actual_columns:
                    source_col = candidate
                    break
            
            # B. Build the Expression
            if source_col:
                # Column exists: Rename -> Cast
                expr = pl.col(source_col).cast(dtype).alias(target_name)
            else:
                # Column missing: Generate Default
                if 'default' in col_def:
                    # Use provided default value
                    default_val = col_def['default']
                    expr = pl.lit(default_val).cast(dtype).alias(target_name)
                elif col_def.get('allow_null', False):
                    # Fill with Null
                    expr = pl.lit(None).cast(dtype).alias(target_name)
                else:
                    # Skip or Error (depending on strictness preference)
                    # For now, we skip, but you could raise ValueError here
                    print(f"Warning: Missing required col '{target_name}' in {file_path}")
                    continue

            selected_exprs.append(expr)

        # 3. Apply Transformations
        q = q.select(selected_exprs)
        
        return q

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

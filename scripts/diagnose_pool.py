# scripts/diagnose_pool.py
import os
import glob
from pathlib import Path
import polars as pl
import sys

def diagnose(pool_path="./data/raw_pool"):
    print(f"🔍 DIAGNOSING POOL AT: {pool_path}")
    
    # 1. Resolve Absolute Path
    abs_path = Path(pool_path).resolve()
    print(f"   Absolute Path:     {abs_path}")
    
    if not abs_path.exists():
        print("❌ ERROR: Directory does not exist.")
        return

    # 2. Physical File Check (Python Glob)
    # Look for ANY parquet file recursively
    pattern = str(abs_path / "**" / "*.parquet")
    found_files = glob.glob(pattern, recursive=True)
    
    print(f"   Files found (glob): {len(found_files)}")
    
    if len(found_files) == 0:
        print("❌ ERROR: Directory exists but contains no .parquet files.")
        print(f"   Checked pattern: {pattern}")
        print("   Directory listing:")
        for root, dirs, files in os.walk(abs_path):
            level = root.replace(str(abs_path), '').count(os.sep)
            indent = ' ' * 4 * (level)
            print(f"{indent}{os.path.basename(root)}/")
            subindent = ' ' * 4 * (level + 1)
            for f in files:
                print(f"{subindent}{f}")
        return

    print(f"   ✅ Found {len(found_files)} parquet files.")
    print(f"   First file: {found_files[0]}")

    # 3. Polars Scan Check
    print("\n🧪 Testing Polars Scan...")
    try:
        # We use the absolute path in the glob string for Polars to be safe
        scan_pattern = str(abs_path / "**" / "*.parquet")
        q = pl.scan_parquet(scan_pattern)
        
        # Try to fetch schema (lightweight)
        schema = q.schema
        print("   ✅ Polars Schema detected:")
        for name, dtype in list(schema.items())[:5]: # Show first 5 cols
            print(f"      - {name}: {dtype}")
            
        # Try to fetch 1 row (heavyweight)
        row = q.fetch(1)
        print(f"   ✅ Successfully read data row. Shape: {row.shape}")
        
    except Exception as e:
        print(f"❌ Polars Error: {e}")

if __name__ == "__main__":
    target = sys.argv[1] if len(sys.argv) > 1 else "./data/raw_pool"
    diagnose(target)

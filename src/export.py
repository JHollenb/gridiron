import argparse

from query import GridironQuery

def export(pool="./data/raw_pool", num_plays=50, out_file="exported_pool.csv", dry_run=False):
    # Connect to the pool
    db = GridironQuery("./data/raw_pool")

    # Get a Tensor-ready dataset of 50 random pass plays
    df = db.sample_plays(
        n=50, 
        filters=[ ]
    )
    if not dry_run:
        df.to_csv(out_file, index=False)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--num-plays", default=50, help="Path to schema yaml")
    parser.add_argument("--pool", default="./data/raw_pool", help="Data Lake path")
    parser.add_argument("--output", default="./output.csv", help="Name of output file")
    parser.add_argument("--dry-run", action="store_true", help="Process without writing")
    
    args = parser.parse_args()
    
    export(args.pool, args.num_plays, args.output, args.dry_run)

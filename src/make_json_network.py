import json
from pathlib import Path

import polars as pl

INPUT_DIR_OC = Path("data/occurences/")
INPUT_DIR_COOC = Path("data/cooccurence/")
OUTPUT_FILE = Path("temp.json")


def main() -> None:
    # TODO: have fixed schema for this JSON dict using better types
    res = {}

    files = list(INPUT_DIR_OC.glob("*.parquet"))
    print(f"Character occurence files discovered - {len(files):,}")

    edges_df = (
        pl.concat([pl.read_parquet(fn) for fn in files])
        .with_columns(
            id=pl.concat_str("series", "name", separator="_"),
        )
        .group_by("id", "series", "name")
        .agg(pl.len().alias("occurrence"))
        .sort("id", "series", "name")
    )
    # print(nodes_df.sort("occurrence"))
    res["nodes"] = edges_df.to_dicts()
    print("-" * 30)

    files = list(INPUT_DIR_COOC.glob("*.parquet"))
    print(f"Character co-occurence files discovered - {len(files):,}")
    edges_df = (
        pl.concat([pl.read_parquet(fn) for fn in files])
        .with_columns(
            pl.concat_str(pl.col("series"), pl.col("char1"), separator="_").alias("source"),
            pl.concat_str(pl.col("series"), pl.col("char2"), separator="_").alias("target"),
        )
        .group_by("source", "target", "series")
        .agg(pl.len().alias("weight"))
        .filter(pl.col("weight") >= 5)
        .filter(pl.col("source") != pl.col("target"))
        .sort("source", "target", "series")
    )
    # print(edges_df.sort("weight"))
    res["edges"] = []  # edges_df.to_dicts()
    print("-" * 30)

    print(f"Saving network data w/ {len(res['nodes']):,} nodes and {len(res['edges']):,} edges")
    print("-" * 30)
    with OUTPUT_FILE.open("w") as f:
        json.dump(res, f, indent=2)


if __name__ == "__main__":
    main()

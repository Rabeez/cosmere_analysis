import json
from pathlib import Path

import polars as pl

CHAR_FILE = Path("data/all_cosmere_characters.parquet")
INPUT_DIR_OC = Path("data/occurences/")
INPUT_DIR_COOC = Path("data/cooccurence/")
OUTPUT_FILE = Path("temp.json")


def main() -> None:
    char_df = pl.read_parquet(CHAR_FILE)
    # TODO: have fixed schema for this JSON dict using better types
    res = {}

    files = list(INPUT_DIR_OC.glob("*.parquet"))
    print(f"Character occurence files discovered - {len(files):,}")

    nodes_df = (
        pl.concat([pl.read_parquet(fn) for fn in files])
        .with_columns(id=pl.col("name"))
        .join(char_df, on=["name"], how="left")
        .group_by("id", "name", "homeworld")
        .agg(pl.len().alias("occurrence"))
        # .filter(pl.col("occurrence") > 1)
        .sort("id")
    )
    # print(nodes_df.sort("occurrence"))
    res["nodes"] = nodes_df.to_dicts()
    print("-" * 30)

    files = list(INPUT_DIR_COOC.glob("*.parquet"))
    print(f"Character co-occurence files discovered - {len(files):,}")
    edges_df = (
        pl.concat([pl.read_parquet(fn) for fn in files])
        .with_columns(
            source=pl.col("char1"),
            target=pl.col("char2"),
        )
        .group_by("source", "target")
        .agg(pl.len().alias("weight"))
        # .filter(pl.col("weight") >= 5)
        .sort("source", "target")
    )
    # print(edges_df.sort("weight"))
    res["links"] = edges_df.to_dicts()
    print("-" * 30)

    print(f"Saving network data w/ {len(res['nodes']):,} nodes and {len(res['links']):,} edges")
    print("-" * 30)
    with OUTPUT_FILE.open("w") as f:
        json.dump(res, f, indent=2)


if __name__ == "__main__":
    main()

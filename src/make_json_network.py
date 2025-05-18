import json
from pathlib import Path

import polars as pl
from tqdm import tqdm

INPUT_DIR = Path("data/cooccurence/")
INPUT_FILE = Path("data/cooccurence/Mistborn_ The Final Empire - Brandon Sanderson.parquet")
OUTPUT_FILE = Path("temp.json")


def main() -> None:
    res = None
    files = list(INPUT_DIR.glob("*.parquet"))
    print(f"Character co-occurence files discovered - {len(files):,}")
    print("-" * 30)

    for file in tqdm(files, desc="Files", total=len(files)):
        main_df = pl.read_parquet(file)
        # Combine 'char1' and 'char2' into a single 'character' column
        characters = pl.concat(
            [
                main_df.select(["series", pl.col("char1").alias("character")]),
                main_df.select(["series", pl.col("char2").alias("character")]),
            ],
        )

        # Count occurrences for each character
        node_data = (
            characters.group_by(["series", "character"])
            .agg(pl.len().alias("occurrence"))
            .with_columns([pl.col("character").alias("id"), pl.col("character").alias("name")])
            .select(["series", "id", "name", "occurrence"])
        )
        # print(node_data.sort("occurrence"))

        # Create links by counting interactions
        # Ensure consistent ordering (char1 < char2) to avoid duplicate links
        links = (
            main_df.with_columns(
                [
                    pl.when(pl.col("char1") < pl.col("char2"))
                    .then(pl.col("char1"))
                    .otherwise(pl.col("char2"))
                    .alias("source"),
                    pl.when(pl.col("char1") < pl.col("char2"))
                    .then(pl.col("char2"))
                    .otherwise(pl.col("char1"))
                    .alias("target"),
                ],
            )
            .group_by(["series", "source", "target"])
            .agg(pl.len().alias("weight"))
            .filter(pl.col("source") != pl.col("target"))
        )
        # print(links.sort("weight"))

        if res is None:
            res = {"nodes": node_data.to_dicts(), "links": links.to_dicts()}
        else:
            res["nodes"].extend(node_data.to_dicts())
            res["links"].extend(links.to_dicts())
        # print(json_data)
        # break

    with OUTPUT_FILE.open("w") as f:
        json.dump(res, f)


if __name__ == "__main__":
    main()

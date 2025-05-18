import json
from pathlib import Path

import polars as pl

INPUT_FILE = Path("data/cooccurence/Mistborn_ The Final Empire - Brandon Sanderson.parquet")
OUTPUT_FILE = Path("temp.json")


def main() -> None:
    main_df = pl.read_parquet(INPUT_FILE)

    # Combine 'char1' and 'char2' into a single 'character' column
    characters = pl.concat(
        [
            main_df.select(pl.col("char1").alias("character")),
            main_df.select(pl.col("char2").alias("character")),
        ],
    )

    # Count occurrences for each character
    node_data = (
        characters.group_by("character")
        .agg(pl.count().alias("occurrence"))
        .with_columns(
            [
                pl.col("character").alias("id"),
                pl.col("character").alias("name"),
            ],
        )
        .select(["id", "name", "occurrence"])
    )
    print(node_data.sort("occurrence"))

    # Create links by counting interactions
    # Ensure consistent ordering (char1 < char2) to avoid duplicate links
    links = (
        main_df.with_columns(
            source=pl.when(pl.col("char1") < pl.col("char2"))
            .then(pl.col("char1"))
            .otherwise(pl.col("char2")),
            target=pl.when(pl.col("char1") < pl.col("char2"))
            .then(pl.col("char2"))
            .otherwise(pl.col("char1")),
        )
        .group_by(["source", "target"])
        .agg(weight=pl.col("source").count())
        .filter(pl.col("source") != pl.col("target"))
    )
    print(links.sort("weight"))

    json_data = {"nodes": node_data.to_dicts(), "links": links.to_dicts()}
    # print(json_data)

    with OUTPUT_FILE.open("w") as f:
        json.dump(json_data, f)


if __name__ == "__main__":
    main()

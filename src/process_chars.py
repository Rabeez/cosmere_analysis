from pathlib import Path

import polars as pl

INPUT_FILE = Path("data/all_characters.parquet")
OUTPUT_FILE = Path("data/all_cosmere_characters.parquet")


def main() -> None:
    main_df = pl.read_parquet(INPUT_FILE)
    res_df = main_df.filter(pl.col("universe") == "Cosmere")

    # NOTE: manual cleanup to ensure Aliases are unique across characters
    res_df = (
        res_df.filter(pl.col("name") != "Icy Ben Oldson")
        .filter(pl.col("name") != "Chanasha Hasareh")
        .with_columns(
            pl.when(pl.col("name") == "Spook")
            .then(pl.col("aliases").list.set_difference(["Jedal"]))
            .otherwise(pl.col("aliases"))
            .alias("aliases"),
        )
        .with_columns(
            pl.when(pl.col("name") == "S")
            .then(pl.col("aliases").list.set_difference(["Jedal"]))
            .otherwise(pl.col("aliases"))
            .alias("aliases"),
        )
    )

    OUTPUT_FILE.unlink(missing_ok=True)
    res_df.write_parquet(OUTPUT_FILE)


if __name__ == "__main__":
    main()

from pathlib import Path

import polars as pl
from tqdm import tqdm

INPUT_DIR = Path("data/occurences/")
OUTPUT_DIR = Path(".")


def generate_pairs(names: list[str]) -> list[tuple[str, str]]:
    # Create all unique pairs of characters, ensuring char1 < char2 lexicographically
    pairs = [(names[i], names[j]) for i in range(len(names)) for j in range(i + 1, len(names))]
    return pairs


def main() -> None:
    char_occurence_files = list(INPUT_DIR.glob("*.parquet"))
    print(f"Discovered character occurence files - {len(char_occurence_files):,}")
    print("-" * 30)

    for char_occurence_file in tqdm(
        char_occurence_files,
        total=len(char_occurence_files),
        desc="Files",
    ):
        occurence_df = pl.read_parquet(char_occurence_file)
        unique_chars = occurence_df.group_by(["series", "chapter_id"]).agg(
            names=pl.col("name").unique().sort(),
        )
        cooccurence_df = (
            unique_chars.with_columns(
                pairs=pl.col("names").map_elements(
                    generate_pairs,
                    return_dtype=pl.List(pl.List(pl.Utf8)),
                ),
            )
            .explode("pairs")
            .filter(pl.col("pairs").is_not_null())
            .with_columns(
                char1=pl.col("pairs").list.get(0),
                char2=pl.col("pairs").list.get(1),
            )
            .select(["series", "chapter_id", "char1", "char2"])
            .sort(["series", "chapter_id"])
        )
        output_file = OUTPUT_DIR / "temp.json"
        output_file.unlink(missing_ok=True)
        cooccurence_df.write_parquet(output_file)
        break


if __name__ == "__main__":
    main()

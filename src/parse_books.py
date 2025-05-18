import os
import re
from collections.abc import Iterator
from functools import reduce
from pathlib import Path
from typing import Literal

import polars as pl
import spacy
from dotenv import load_dotenv
from tqdm import tqdm

from data_types import Series

load_dotenv()

_BOOKS_DIR = os.getenv("BOOKS_DIR")
assert _BOOKS_DIR
BOOKS_DIR = Path(_BOOKS_DIR)

CHAR_DIR = Path("data/characters/")
OUTPUT_DIR = Path("data/occurences/")

BOOK2SERIES: dict[str, Series] = {
    "Mistborn_ The Final Empire - Brandon Sanderson": Series.MISTBORN,
    "Warbreaker - Brandon Sanderson": Series.WARBREAKER,
    "Elantris - Brandon Sanderson": Series.ELANTRIS,
}
PLANET2SERIES: dict[str, Series] = {
    "First of the Sun": Series.SIXTH_OF_THE_DUSK,
    "Nalthas": Series.WARBREAKER,
    "Roshar": Series.STORMLIGHT,
    "Scadrial": Series.MISTBORN,
    "Sel": Series.ELANTRIS,
    "Taldain": Series.WHITE_SAND,
    "Threnody": Series.SHADOWS_FOR_SILENCE,
}
SERIES2MODE: dict[Series, Literal["break_asterisks", "chapter_x"]] = {
    Series.MISTBORN: "break_asterisks",
    Series.WARBREAKER: "break_asterisks",
    Series.ELANTRIS: "chapter_x",
}

# def stream_tokens(
#     nlp: spacy.language.Language,
#     file_path: Path,
# ) -> Iterator[str]:
#     chunk_size = 50_000
#     batch_size = 1000
#
#     # Read the file in chunks
#     def read_chunks() -> Iterator[str]:
#         with open(file_path, "r", encoding="utf-8") as file:
#             while True:
#                 chunk = file.read(chunk_size)
#                 if not chunk:
#                     break
#                 yield chunk
#
#     # Process chunks in batches through nlp.pipe
#     for doc in nlp.pipe(read_chunks(), batch_size=batch_size):
#         for token in doc:
#             yield token.text


def stream_lines_w_metadata(
    nlp: spacy.language.Language,
    file_path: Path,
    marker_type: Literal["chapter_x", "break_asterisks"],
) -> Iterator[tuple[int, str]]:
    if marker_type == "chapter_x":
        chapter_pattern = r"^[Cc][Hh][Aa][Pp][Tt][Ee][Rr]\s+(\d+|[A-Za-z]+)$"
    else:
        chapter_pattern = r"^\* \* \*$"
    chapter_regex = re.compile(chapter_pattern, re.MULTILINE | re.IGNORECASE)

    chapter_count = 0
    with file_path.open("r") as f:
        for line in f:
            m = chapter_regex.match(line.strip())
            if m:
                chapter_count += 1
            doc = nlp(line.strip())
            for token in doc:
                if token.is_alpha:
                    yield (chapter_count, token.text)


def prepare_chars() -> dict[tuple[Series, str], str]:
    res: list[dict[tuple[Series, str], str]] = []
    char_files = list(CHAR_DIR.rglob("*.parquet"))
    print(f"Character files discovered - {len(char_files):,}")
    print("-" * 30)

    for char_file in char_files:
        series = PLANET2SERIES[char_file.stem]
        loc_df = pl.read_parquet(char_file)
        # TODO: Do i need to remove leading/trailing punctuations?
        # e.g. `'Ene` might be `Ene` in book
        # TODO: Do i need to remove punctuations in middle of words
        # e.g. `OreSeur` might be `Ore-Seur` in book
        res1: list[dict[str, str]] = (
            loc_df.with_columns(pl.col("aliases").list.concat(pl.col("name")))
            .select("name", "aliases")
            .explode("aliases")
            .to_dicts()
        )
        res2 = ({(series, x["aliases"]): x["name"]} for x in res1)
        res3 = reduce(lambda x, y: x | y, res2, {})
        res.append(res3)
    fin = reduce(lambda x, y: x | y, res, {})
    return fin


def main() -> None:
    nlp = spacy.load(
        "en_core_web_sm",
        enable=["tokenizer"],
    )

    txt_files = list(BOOKS_DIR.rglob("*.txt"))
    print(f"Book txt files discovered - {len(txt_files):,}")
    print("-" * 30)

    chars_name_mapping = prepare_chars()
    print(f"Character name mapping ready - {len(chars_name_mapping):,}")
    print("-" * 30)

    records = []
    for book_filename in tqdm(txt_files, total=len(txt_files), desc="Book Files"):
        series = BOOK2SERIES[book_filename.stem]
        mode = SERIES2MODE[series]
        for ch_id, word in tqdm(
            stream_lines_w_metadata(nlp, book_filename, mode),
            desc="Book lines",
            leave=False,
        ):
            if canonical_char_name := chars_name_mapping.get((series, word)):
                records.append(
                    {
                        "series": series.value,
                        "chapter_id": ch_id,
                        "name": canonical_char_name,
                    },
                )
        output_file = OUTPUT_DIR / f"{book_filename.stem}.parquet"
        output_file.unlink(missing_ok=True)
        chars_df = pl.DataFrame(records)
        chars_df.write_parquet(output_file)
        # break


if __name__ == "__main__":
    main()

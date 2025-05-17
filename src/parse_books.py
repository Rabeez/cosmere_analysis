import os
import re
from collections.abc import Iterator
from pathlib import Path
from typing import Literal

import spacy
from dotenv import load_dotenv
from tqdm import tqdm

load_dotenv()

_BOOKS_DIR = os.getenv("BOOKS_DIR")
assert _BOOKS_DIR
BOOKS_DIR = Path(_BOOKS_DIR)


def stream_tokens(
    nlp: spacy.language.Language,
    file_path: Path,
) -> Iterator[str]:
    chunk_size = 50_000
    batch_size = 1000

    # Read the file in chunks
    def read_chunks() -> Iterator[str]:
        with open(file_path, "r", encoding="utf-8") as file:
            while True:
                chunk = file.read(chunk_size)
                if not chunk:
                    break
                yield chunk

    # Process chunks in batches through nlp.pipe
    for doc in nlp.pipe(read_chunks(), batch_size=batch_size):
        for token in doc:
            yield token.text


def stream_tokens_by_chapter(
    nlp: spacy.language.Language,
    file_path: Path,
    marker_type: Literal["chapter_x", "break_asterisks"],
) -> Iterator[tuple[int, str]]:
    chunk_size = 10_000

    if marker_type == "chapter_x":
        chapter_pattern = r"^[Cc][Hh][Aa][Pp][Tt][Ee][Rr]\s+(\d+|[A-Za-z]+)$"
    else:
        chapter_pattern = r"^\* \* \*$"
    chapter_regex = re.compile(chapter_pattern, re.MULTILINE | re.IGNORECASE)

    chapter_count = 0
    current_chapter = chapter_count
    current_chunk = ""

    with file_path.open("r") as file:
        while True:
            chunk = file.read(chunk_size)
            if not chunk:
                # Process any remaining data in buffer at EoF
                if current_chunk:
                    lines = current_chunk.splitlines(keepends=True)
                    for line in lines:
                        m = chapter_regex.match(line.strip())
                        if m:
                            chapter_count += 1
                            current_chapter = chapter_count
                            continue  # Skip marker line
                        if line.strip():
                            doc = nlp(line)
                            for token in doc:
                                if token.is_alpha:
                                    yield (current_chapter, token.text)
                break

            # Append chunk to current buffer
            current_chunk += chunk
            lines = current_chunk.splitlines(keepends=True)

            # Process all complete lines
            for _, line in enumerate(lines[:-1]):  # Exclude last (potentially incomplete) line
                m = chapter_regex.match(line.strip())
                if m:
                    chapter_count += 1
                    current_chapter = chapter_count
                    continue  # Skip marker line
                if line.strip():
                    doc = nlp(line)
                    for token in doc:
                        if token.is_alpha:
                            yield (current_chapter, token.text)

            # Keep the last line (may be incomplete) for the next chunk
            current_chunk = lines[-1] if lines else ""


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


def main() -> None:
    nlp = spacy.load(
        "en_core_web_sm",
        disable=["parser", "ner", "lemmatizer", "textcat"],
    )
    # nlp.max_length = 500_000

    txt_files = BOOKS_DIR.rglob("*.txt")
    txt_files = list(txt_files)
    # print(txt_files)

    c = 0
    rec = {}
    for book_filename in tqdm(txt_files, total=len(txt_files), desc="Book Files"):
        # if c < 2:
        #     continue
        # for ch_id, word in stream_tokens_by_chapter(nlp, book_filename, "chapter_x"):
        mode = "chapter_x" if "Elantris" in book_filename.name else "break_asterisks"
        for ch_id, word in stream_lines_w_metadata(nlp, book_filename, mode):
            rec[book_filename.name] = ch_id
            # print(ch_id, word)
            # if c == 1000:
            #     break
            c += 1
        # break
    print(rec)


if __name__ == "__main__":
    main()

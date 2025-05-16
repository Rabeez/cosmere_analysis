import os
from collections.abc import Iterator
from pathlib import Path

import spacy
from dotenv import load_dotenv
from tqdm import tqdm

load_dotenv()

_BOOKS_DIR = os.getenv("BOOKS_DIR")
assert _BOOKS_DIR
BOOKS_DIR = Path(_BOOKS_DIR)


def stream_tokens(
    file_path: Path,
    chunk_size: int = 50_000,
    batch_size: int = 1000,
) -> Iterator[str]:
    nlp = spacy.load(
        "en_core_web_sm",
        disable=["parser", "ner", "lemmatizer", "textcat"],
    )
    nlp.max_length = chunk_size + 10000  # Slightly larger than chunk size

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


def main() -> None:
    txt_files = BOOKS_DIR.rglob("*.txt")
    txt_files = list(txt_files)

    for book_file in tqdm(txt_files, total=len(txt_files), desc="Book Files"):
        for word in stream_tokens(book_file):
            print(word)
            break
        break


if __name__ == "__main__":
    main()

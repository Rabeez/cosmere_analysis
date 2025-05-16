from pathlib import Path

import httpx
import polars as pl
from selectolax.parser import HTMLParser, Node
from tqdm import tqdm

OUTPUT_DIR = Path("data/characters/")
SERIES_PAGES = {
    "Sel": "https://coppermind.net/wiki/Category:Selish",
    "Scadrial": "https://coppermind.net/wiki/Category:Scadrians",
    "Threnody": "https://coppermind.net/wiki/Category:Threnodite",
    "First of the Sun": "https://coppermind.net/wiki/Category:Eelakin",
    "Roshar": "https://coppermind.net/wiki/Category:Rosharans",
    "Nalthas": "https://coppermind.net/wiki/Category:Nalthians",
    "Taldain": "https://coppermind.net/wiki/Category:Taldaini",
}
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36",
}


def series_chars(link: str) -> list[Node]:
    with httpx.Client(headers=HEADERS) as client:
        response = client.get(link)
        try:
            response.raise_for_status()
        except httpx.HTTPStatusError as e:
            raise ValueError("failed to fetch series page") from e

    tree = HTMLParser(response.text)
    container_elem = tree.css_first("#mw-pages > div:nth-child(3)")
    if not container_elem:
        raise ValueError("Container element not found")

    character_list = container_elem.css("a")
    if not character_list:
        raise ValueError("No character links found")

    return character_list


def char_info(link_suffix: str) -> list[str]:
    with httpx.Client(headers=HEADERS) as client:
        response = client.get(f"https://coppermind.net{link_suffix}")
        try:
            response.raise_for_status()
        except httpx.HTTPStatusError as e:
            raise ValueError("failed to fetch character page") from e

    tree = HTMLParser(response.text)
    container_elem = tree.css_first("#Character")
    if not container_elem:
        tqdm.write("Container table element not found")
        return []

    aliases = ""
    for tr in container_elem.css("tr"):
        th = tr.css_first("th")
        if th and th.text(strip=True) == "Aliases":
            td = tr.css_first("td")
            aliases = td.text(strip=True) if td else ""
            break
    aliases = [a.strip() for a in aliases.split(",")]
    return aliases


def main() -> None:
    for planet, main_link in tqdm(SERIES_PAGES.items(), total=len(SERIES_PAGES), desc="Series"):
        output_file = OUTPUT_DIR / f"{planet}.parquet"
        output_file.unlink(missing_ok=True)

        characters = []
        character_list = series_chars(main_link)

        for link in tqdm(character_list, desc="Characters", leave=False):
            char_name = link.text(deep=False, strip=True)
            char_link_suffix = link.attributes["href"]
            assert char_link_suffix

            aliases = char_info(char_link_suffix)

            characters.append(
                {
                    "planet": planet,
                    "name": char_name,
                    "aliases": aliases,
                },
            )

        planet_df = pl.DataFrame(characters)
        planet_df.write_parquet(output_file)


if __name__ == "__main__":
    main()

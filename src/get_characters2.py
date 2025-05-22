from pathlib import Path

import httpx
import polars as pl
from selectolax.parser import HTMLParser, Node
from tqdm import tqdm

OUTPUT_DIR = Path("data/characters/")
OUTPUT_FILE = Path("data/all_characters.parquet")
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


def get_char_more_aliases(container_elem: Node) -> str:
    print("here")
    character_list = container_elem.css("li")
    if not character_list:
        raise ValueError("No character alias li found")

    res = []
    for cl in character_list:
        # TODO: this will fail for :
        # Imperial Fool of the Rose Empire
        # since "Rose Empire" is a link child
        content = cl.text(deep=False, strip=True)
        res.append(content)

    return ", ".join(res)


def char_info(link_suffix: str) -> tuple[str, str, list[str]]:
    with httpx.Client(headers=HEADERS) as client:
        response = client.get(f"https://coppermind.net{link_suffix}")
        try:
            response.raise_for_status()
        except httpx.HTTPStatusError as e:
            raise ValueError("failed to fetch character page") from e

    tree = HTMLParser(response.text)

    if tree.css_first(".mw-redirectedfrom") is not None:
        raise ValueError("Found character dupe page, skipping")

    container_elem = tree.css_first("#Character")
    if not container_elem:
        tqdm.write(f"Container table element not found on page: {link_suffix}")
        return "", "", []

    # TODO: Extract full character table as 'metadata'
    done_aliases = False
    done_universe = False
    done_homeworld = False
    aliases = ""
    universe = ""
    homeworld = ""
    for tr in container_elem.css("tr"):
        th = tr.css_first("th")
        if th and th.text(strip=True) == "Universe":
            td = tr.css_first("td")
            if not td:
                done_universe = True
                continue
            universe = td.text(strip=True, deep=True) if td else ""
            done_universe = True
        if th and th.text(strip=True) == "Homeworld":
            td = tr.css_first("td")
            if not td:
                done_homeworld = True
                continue
            homeworld = td.text(strip=True, deep=True) if td else ""
            done_homeworld = True
        if th and th.text(strip=True) == "Aliases":
            td = tr.css_first("td")
            if not td:
                done_aliases = True
                continue
            aliases = td.text(strip=True, deep=False) if td else ""
            link_here = td.css_first("a")
            if link_here and link_here.attributes["href"] == "#Known_Aliases":
                elem = tree.css_first(".pillars")
                assert elem
                more_aliases = get_char_more_aliases(elem)
                aliases += ", " + more_aliases
            done_aliases = True
        if done_aliases and done_universe and done_homeworld:
            break
    aliases = [a.strip() for a in aliases.split(",") if a.strip() != ""]
    if len(aliases) == 1 and aliases[0] == "":
        aliases = []
    aliases = list(set(aliases))
    return universe, homeworld, aliases


def main() -> None:
    with httpx.Client(headers=HEADERS) as client:
        response = client.get("https://coppermind.net/wiki/Category:Characters")
        try:
            response.raise_for_status()
        except httpx.HTTPStatusError as e:
            raise ValueError("failed to fetch main characters page") from e

    tree = HTMLParser(response.text)
    container_elem = tree.css_first("#mw-pages > div:nth-child(3) > div:nth-child(1)")
    if not container_elem:
        raise ValueError("Container element element not found")

    character_list = container_elem.css("li")
    if not character_list:
        raise ValueError("No character links found")

    # c = 0
    characters = []
    for char_elem in tqdm(character_list, desc="Character links", leave=False):
        link = char_elem.css_first("a")
        if not link:
            raise ValueError("no link found for char")

        char_name = link.text(deep=False, strip=True)
        char_link_suffix = link.attributes["href"]
        assert char_link_suffix

        try:
            universe, homeworld, aliases = char_info(char_link_suffix)
        except ValueError as e:
            if str(e) == "Found character dupe page, skipping":
                continue
            raise e from None
        characters.append(
            {
                "char_link_suffix": char_link_suffix,
                "universe": universe,
                "homeworld": homeworld,
                "name": char_name,
                "aliases": aliases,
            },
        )
    #     c += 1
    #     if c > 2:
    #         break
    # print(characters)

    OUTPUT_FILE.unlink(missing_ok=True)
    all_chars_df = pl.DataFrame(characters)
    all_chars_df.write_parquet(OUTPUT_FILE)


if __name__ == "__main__":
    main()

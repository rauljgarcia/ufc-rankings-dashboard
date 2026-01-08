"""
ufc_rankings_update.py

This module is a tool to scrape the UFC rankings page, parse current rankings into a
tidy table, and append new snapshots to a local CSV history file only when the UFC
rankings have been updated.

Data model:
- One row per fighter per division per UFC update.
- The `snapshot_date` is the day you ran the script.
- The `ufc_last_updated` date comes from the UFC page ("Last updated: ...") and
  is used to detect whether rankings have changed since the last saved update.

Typical usage:
    python ufc_rankings_update.py
"""

import os
import re
from datetime import datetime
from urllib.parse import urljoin

import pandas as pd
import requests
from bs4 import BeautifulSoup

URL = "https://www.ufc.com/rankings"
HISTORY_CSV = "ufc_rankings_history.csv"
HEADERS = {"User-Agent": "Mozilla/5.0"}

COLS = [
    "snapshot_date",
    "ufc_last_updated",
    "division",
    "champion",
    "fighter",
    "fighter_url",
    "rank",
    "rank_change",
]


def fetch_soup(url: str) -> BeautifulSoup:
    """
    Fetch a URL and return a BeautifulSoup DOM for HTML parsing.

    Args:
        url: Web address to fetch.

    Returns:
        BeautifulSoup: Parsed HTML document.

    Raises:
        requests.HTTPError: If the request returns a non-2xx status code.
        requests.RequestException: For network-related errors (timeouts, etc.).
    """
    r = requests.get(url, headers=HEADERS, timeout=30)
    r.raise_for_status()
    return BeautifulSoup(r.text, "html.parser")


def parse_last_updated(soup: BeautifulSoup) -> datetime.date:
    """
    Parse the UFC page's "Last updated" date.

    The UFC rankings page includes a label like:
        "Last updated: Tuesday, Dec. 16"
    This function converts it into a real date by attaching the current year,
    then handling year rollovers (e.g., running in January when the update was
    in December of the previous year).

    Args:
        soup: BeautifulSoup DOM for the UFC rankings page.

    Returns:
        date: The UFC "last updated" date.

    Raises:
        RuntimeError: If the expected HTML elements are not found.
        ValueError: If the date string cannot be parsed.
    """

    # getting div (singular because there's only one)
    last_up_div = soup.find("div", class_="list-denotions")
    if not last_up_div:
        raise RuntimeError("Could not find last-updated div (list-denotions).")

    # get all <p> tags inside it
    p = last_up_div.find("p")
    if not p:
        raise RuntimeError("Could not find last-updated <p> tag.")

    raw = p.get_text()

    # use regex to replace any run of whitespace (newlines, tabs, multiple spaces) with one space:
    clean = re.sub(r"\s+", " ", raw).strip()

    # parse last updated into actual date and remove label
    last_updated_str = clean.replace("Last updated:", "").strip()

    # adding current year first
    current_year = datetime.now().year
    date_str_with_year = f"{last_updated_str}, {current_year}"
    dt = datetime.strptime(date_str_with_year, "%A, %b. %d, %Y").date()

    # handle year rollover (e.g. Jan scrape, Dec update)
    today = datetime.now().date()
    if dt > today:
        dt = dt.replace(year=dt.year - 1)

    return dt


def parse_rankings(
    soup: BeautifulSoup, snapshot_date, ufc_last_updated
) -> pd.DataFrame:
    """
    Parse all division ranking tables from the UFC rankings page.

    The UFC rankings page contains multiple "view-grouping" blocks, each with a
    single table. Each table caption includes:
    - h4: division name (sometimes includes "Top Rank" suffix)
    - h5: champion name

    The table body includes ranked fighters (#1-#15). Champion is stored in
    every row to preserve a complete snapshot per division.

    Args:
        soup: BeautifulSoup DOM for the UFC rankings page.
        snapshot_date: The local date when the script ran.
        ufc_last_updated: The UFC "last updated" date parsed from the page.

    Returns:
        pd.DataFrame: Tidy rankings data in a consistent column order.

    Raises:
        RuntimeError: If no rows are parsed (page structure likely changed).
    """

    # Blocks that hold each weight division/grouping
    blocks = soup.find_all("div", class_="view-grouping")

    # Store all dictionaries/rows in this list
    all_rows = []

    # Access tables and captions
    for group in blocks:
        table = group.find("table")
        if not table:
            continue

        caption = table.find("caption")
        if not caption:
            continue

        # Access pound-for-pound/Weight divisions
        h4 = caption.find("h4")
        # Access champion of pound-for-pound/Weight divisions
        h5 = caption.find("h5")
        if not (h4 and h5):
            continue

        # Division name, remove Top Rank suffix
        division = h4.get_text(" ", strip=True).replace(" Top Rank", "")
        # Champion name
        champion = h5.get_text(" ", strip=True)

        # Access the body of the table to parse
        tbody = table.find("tbody")
        if not tbody:
            continue

        # Access all 15 table rows, i.e. all 15 fighters from this table
        # Loop through table rows (tr) in the table body (tr)
        for tr in tbody.find_all("tr"):
            # Access fighter rank
            rank_td = tr.find("td", class_="views-field-weight-class-rank")
            if not rank_td:
                continue
            fighter_rank = int(rank_td.get_text(" ", strip=True))

            # Access hyperlink anchor
            a = tr.select_one("td.views-field-title a")
            if not a:
                continue

            # Access fighter name
            fighter_name = a.get_text(" ", strip=True)

            # Access fighter url
            fighter_url = urljoin("https://www.ufc.com", a.get("href", ""))

            # Access rank change div
            rank_change_td = tr.find(
                "td", class_="views-field-weight-class-rank-change"
            )

            # Change labels to either +/-
            rank_change = ""
            if rank_change_td:
                text = re.sub(
                    r"\s+", " ", rank_change_td.get_text(" ", strip=True)
                ).strip()
                inc = re.search(r"Rank increased by (\d+)", text)
                dec = re.search(r"Rank decreased by (\d+)", text)
                if inc:
                    rank_change = f"+{inc.group(1)}"
                elif dec:
                    rank_change = f"-{dec.group(1)}"
                elif text == "NR":
                    rank_change = "NR"

            # Populate columns
            all_rows.append(
                {
                    "snapshot_date": snapshot_date,
                    "ufc_last_updated": ufc_last_updated,
                    "division": division,
                    "champion": champion,
                    "fighter": fighter_name,
                    "fighter_url": fighter_url,
                    "rank": fighter_rank,
                    "rank_change": rank_change,
                }
            )

    df = pd.DataFrame(all_rows)
    if df.empty:
        raise RuntimeError("Parsed 0 rows - UFC page structure  may have changed.")

    return df[COLS]


def append_history(df_new: pd.DataFrame, history_csv: str):
    """
    Append a new UFC update snapshot to the history CSV if it is not already saved.

    The history file is append-only. A new run is only appended when the UFC
    "last updated" date is not already present in the history file.

    Args:
        df_new: Newly parsed rankings DataFrame for the current UFC update.
        history_csv: Path to the history CSV file.

    Returns:
        None
    """
    # If history exists, check the last stored UFC update date
    if os.path.exists(history_csv):
        hist = pd.read_csv(
            history_csv, parse_dates=["snapshot_date", "ufc_last_updated"]
        )

        # last saved update date (most recent)
        saved_updates = set(hist["ufc_last_updated"].dt.date)

        new_update = pd.to_datetime(df_new["ufc_last_updated"]).dt.date.iloc[0]
        if new_update in saved_updates:
            print(f"No UFC update since {new_update}. Skipping append.")
            return

        df_new.to_csv(history_csv, mode="a", header=False, index=False)
        print(f"Appended {len(df_new)} rows for UFC update date {new_update}.")
    else:
        # first run: write header
        df_new.to_csv(history_csv, index=False)
        print(f"Created {history_csv} with {len(df_new)} rows.")


def main():
    """
    Orchestrate scraping, parsing, sanity checks, and history persistence.

    Returns:
        None

    Raises:
        RuntimeError: If parsing produces an unexpectedly small dataset.
    """
    soup = fetch_soup(URL)

    # Create snapshot date
    snapshot_date = datetime.now().date()
    ufc_last_updated = parse_last_updated(soup)

    df_new = parse_rankings(soup, snapshot_date, ufc_last_updated)

    # ---- sanity check ----
    print(
        f"Parsed {df_new.shape[0]} rows, "
        f"{df_new['division'].nunique()} divisions, "
        f"UFC updated {ufc_last_updated}"
    )
    if df_new.shape[0] < 150:
        raise RuntimeError(
            f"Parsed only {df_new.shape[0]} rows — page structure may have changed."
        )

    append_history(df_new, HISTORY_CSV)


if __name__ == "__main__":
    main()

# UFC Rankings Dashboard (Automated)

This project automates my workflow for tracking UFC rankings.

The UFC updates rankings irregularly, and manually copying rankings into a formatted spreadsheet after each update was time-consuming and error-prone. This pipeline replaces that manual process with a repeatable script while preserving a print-ready Excel dashboard layout.

---

## Overview

The project consists of three layers:

1. **Data ingestion (Python)**
   - Scrapes the official UFC rankings page
   - Targets the current Meta UFC Rankings
   - Parses all 11 divisions and ranked fighters
   - Checks whether the UFC has actually updated the rankings
   - Appends new data only when an update occurs
   - Validates the expected divisions and row count before writing data

2. **Historical storage (CSV)**
   - Append-only rankings history
   - One row per fighter per division per UFC update
   - Enables tracking of rankings over time
   - Preserves historical rankings from before the transition to the Meta ranking system

3. **Presentation (Excel)**
   - A formatted, print-ready dashboard
   - Automatically updates when new data is added and the data connection is refreshed
   - Displays eight men's divisions and three women's divisions
   - Designed for consistent front/back printing
   - No manual cell editing required

The emphasis is on reliability and stability: the formatting remains consistent, and new data is added only when the UFC rankings have been updated.

---

## Repository contents

- **`ufc_rankings_update.py`**  
  Main automation script. Fetches the current Meta UFC Rankings, parses and validates the data, and appends new snapshots to history when appropriate.

- **`ufc_rankings_dashboard.xlsx`**  
  Excel dashboard that reads from the rankings history and renders formatted men's and women's views for printing.

- **`requirements.txt`**  
  Python dependencies required to run the scraper.

The historical CSV and development notebooks are maintained locally and are not included in the public repository.

---

## Data model

Each row in the history file represents one fighter in one division for a single UFC rankings update.

Key fields:

- `snapshot_date` – date the script was run
- `ufc_last_updated` – the "Last updated" date shown for the Meta UFC Rankings
- `division` – weight class
- `champion` – champion of that division
- `fighter` – ranked fighter name
- `fighter_url` – UFC athlete profile URL
- `rank` – numerical rank (1–15)
- `rank_change` – rank movement (`+N`, `-N`, `NR`, or blank)

The `ufc_last_updated` field is used to determine whether an update has already been saved.

---

## How it works

At a high level:

1. Fetch the UFC rankings page
2. Locate the Meta UFC Rankings section
3. Scrape and parse the 11 divisions using Python (`requests` + `BeautifulSoup`)
4. Normalize the rankings into a structured table
5. Validate the expected divisions and 165 ranked fighters
6. Append the update to the CSV history if it has not already been saved
7. Refresh the Excel dashboard from the CSV

The validation checks are intended to stop the script before writing data if the UFC changes the page structure in a way that produces unexpected results.

---

## Running the script

From the project directory:

```bash
python ufc_rankings_update.py
```

If the UFC has **not** updated the rankings since the last saved snapshot:

```text
Parsed 165 rows, 11 divisions, UFC updated YYYY-MM-DD
No UFC update since YYYY-MM-DD. Skipping append.
```

If the UFC **has** updated the rankings:

```text
Parsed 165 rows, 11 divisions, UFC updated YYYY-MM-DD
Appended 165 rows for UFC update date YYYY-MM-DD.
```

---

## Excel dashboard usage

Open `ufc_rankings_dashboard.xlsx` after running the script and refresh the external data connection.

The workbook uses the most recent UFC update date in the history to populate the men's and women's rankings views automatically while preserving the layout, spacing, formatting, and print settings.

The dashboard is designed to be printed as two pages (front and back) after each rankings update.

---

## Ranking system transition

In July 2026, the UFC rankings page changed from the previous media-based ranking structure to the Meta UFC Rankings system.

The previous page structure contained 13 ranking categories, including men's and women's Pound-for-Pound rankings, for a total of 195 ranked entries per update. The current Meta structure contains 11 weight divisions with 15 ranked fighters each, for a total of 165 entries per update.

The scraper was updated to target the Meta-specific rankings container and Meta update date. The historical data collected under the previous ranking structure is preserved, while new snapshots use the current Meta rankings.

The Excel dashboard was also updated to reflect the current 11-division structure and no longer displays Pound-for-Pound rankings.

---

## Notes

- This project depends on the current HTML structure of the UFC rankings page. If the UFC changes the markup, the parsing logic may need to be updated.
- The script validates the expected 11 divisions and 165 ranked fighters before appending a new snapshot.
- The script can be run repeatedly without appending the same UFC update more than once.
- The focus of this project is automation, reproducibility, historical tracking, and presentation stability rather than interactive visualization.
- The included Excel dashboard contains sample rankings data for demonstration and preserves the formatting used for the printable rankings views.
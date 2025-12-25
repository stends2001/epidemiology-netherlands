"""
CSV cleaning script for vaccination data.

- Reads a semicolon-separated CSV file
- Cleans and normalizes columns
- Writes cleaned output to a new file
"""

from typing import Optional, List
from pathlib import Path

# -------------------------------------------------------------------
# Configuration
# -------------------------------------------------------------------

RAW_DATA_PATH = "data/raw/epidemiological/vaxdata.csv"
CLEAN_DATA_PATH = "data/processed/epidemiological/vaxdata.csv"

COLUMN_NAMES = ["vaccine", "region", "year", "coverage"]
EXPECTED_COLUMNS = 4
SEPARATOR = ";"


# -------------------------------------------------------------------
# Row-level cleaning
# -------------------------------------------------------------------

def clean_row(line: str, sep: str, expected_columns: int) -> Optional[List[str]]:
    """
    Clean a single CSV row.

    Parameters
    ----------
    line : str
        Raw line from the CSV file.
    sep : str
        Column separator.
    expected_columns : int
        Expected number of columns.

    Returns
    -------
    list[str] | None
        Cleaned list of columns, or None if the row should be skipped.
    """

    line = line.strip()
    cols = line.split(sep)

    if len(cols) != expected_columns:
        raise ValueError(f"Unexpected number of columns: {len(cols)}")

    # Basic cleanup
    cols = [
        c.replace('"', "")
         .replace("*", "")
         .replace("\ufeff", "")
        for c in cols
    ]

    # Clean last column (coverage)
    value = cols[-1]

    if value == "":
        return None  # skip rows without data

    if value.endswith(","):
        value += "0"

    cols[-1] = value.replace(",", ".")

    return cols


# -------------------------------------------------------------------
# File-level pipeline
# -------------------------------------------------------------------

def main() -> None:
    cleaned_lines: List[str] = []

    with open(RAW_DATA_PATH, "r", encoding="utf-8-sig") as infile:
        for line_number, line in enumerate(infile):

            # Header
            if line_number == 0:
                cleaned_lines.append(SEPARATOR.join(COLUMN_NAMES))
                continue

            try:
                cols = clean_row(line, SEPARATOR, EXPECTED_COLUMNS)
            except ValueError as err:
                print(f"Line {line_number}: {err}")
                continue

            if cols is None:
                continue

            cleaned_lines.append(SEPARATOR.join(cols))

    output_path = Path(CLEAN_DATA_PATH)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    with open(CLEAN_DATA_PATH, "w", encoding="utf-8", newline="") as outfile:
        for line in cleaned_lines:
            outfile.write(line + "\n")


# -------------------------------------------------------------------
# Entry point
# -------------------------------------------------------------------

if __name__ == "__main__":
    main()

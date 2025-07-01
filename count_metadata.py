import csv
import re
from pathlib import Path

CSV_PATH = Path("/mnt/c/Users/vagrawal/OneDrive - Altair Engineering, Inc/Documents/Personal/Code/metadata_manifest.csv")
PATTERN = re.compile(r"\_conv.\b", re.IGNORECASE)

def count_rows(csv_path):
    count = 0
    with csv_path.open("r", encoding="utf-8", newline="") as f:
        reader = csv.reader(f)
        headers = next(reader, None)
        for row in reader:
            if any(PATTERN.search(cell or "") for cell in row):
                count += 1

    print(f"Rows in CSV: {count}")

if __name__ == "__main__":
    count_rows(CSV_PATH)

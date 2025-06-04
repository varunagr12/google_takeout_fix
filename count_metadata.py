import csv
import re
from pathlib import Path
from collections import Counter

CSV_PATH = Path(r"C:\Users\vagrawal\OneDrive - Altair Engineering, Inc\Documents\Personal\Code\metadata_manifest.csv")
COLUMNS = ["action_taken", "notes"]

PATTERNS = [
    (re.compile(r"^Moved to failed", re.IGNORECASE), "Moved to failed"),
    (re.compile(r"^; Converted HEIC to JPEG", re.IGNORECASE), "Converted HEIC to JPEG"),
    (re.compile(r"^Renamed ", re.IGNORECASE), "Renamed file"),
    (re.compile(r"^JSON rename failed:", re.IGNORECASE), "JSON rename failed"),
    (re.compile(r"^Timestamp updated$", re.IGNORECASE), "Timestamp updated"),
]

def simplify_message(value):
    for pattern, label in PATTERNS:
        if pattern.search(value):
            return label
    return value if value else "(blank)"

def count_column_values(csv_path, columns):
    counts = {col: Counter() for col in columns}
    total = {col: 0 for col in columns}

    with csv_path.open("r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            for col in columns:
                value = (row.get(col) or "").strip()
                simplified = simplify_message(value)
                counts[col][simplified] += 1
                total[col] += 1

    for col in columns:
        print(f"\n{col.capitalize()}:")
        for val, cnt in counts[col].most_common():
            print(f"{val} {cnt}/{total[col]}")

count_column_values(CSV_PATH, COLUMNS)

import csv
import re
from pathlib import Path
from collections import Counter

CSV_PATH = Path("/mnt/c/Users/vagrawal/OneDrive - Altair Engineering, Inc/Documents/Personal/Code/metadata_manifest.csv")
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

'''
**Usage (1 sentence)**
Run `python count_metadata.py` at any point to generate a quick, human-readable tally of how many rows in `metadata_manifest.csv` contain each common “action\_taken” or “notes” event—e.g., “Converted HEIC to JPEG,” “Timestamp updated,” “Moved to failed,” etc.—so you can gauge pipeline activity and error prevalence at a glance.

---

### Tools / Technologies employed

* **Python 3.x standard library** – `csv` for manifest reading, `re` for regex pattern matching, `collections.Counter` for frequency counts, and `pathlib` for path handling.
* **Simplification map** – configurable `PATTERNS` list that collapses verbose log strings into concise, analyst-friendly labels.
* **Terminal-friendly output** – prints counts in “value count/total” format with no external dependencies.

---

### Idea summary (what it does & why it matters)

`count_metadata.py` is the pipeline’s analytics heartbeat: by scanning the manifest’s `action_taken` and `notes` columns, normalizing each entry to a handful of canonical categories, and displaying ranked counts, it offers an instant snapshot of pipeline progress (“how many files got timestamps?”), conversion workload (“how many HEIC→JPEG”), and outstanding issues (“JSON rename failed”). This lightweight audit helps you spot systemic problems, measure the impact of each processing stage, and present clear metrics in your white-paper without firing up a full BI tool.
'''
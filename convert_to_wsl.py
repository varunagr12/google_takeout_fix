import csv
import re
from pathlib import Path

# === CONFIGURATION ===
INPUT_CSV = Path("/mnt/c/Users/vagrawal/OneDrive - Altair Engineering, Inc/Documents/Personal/Code/metadata_manifest.csv")
OUTPUT_CSV = INPUT_CSV  # Overwrite; or use INPUT_CSV.with_name("metadata_manifest_wsl.csv") for a backup
PATH_COLUMNS = ['media_path', 'json_path', 'corrected_path', 'visual_review_path', 'duplicate_of']

def to_wsl_path(path_str: str) -> str:
    """
    Convert Windows-style path to WSL path.
    If already WSL or not a valid path, return as-is.
    """
    if not path_str:
        return path_str

    path_str = path_str.strip().replace("\\", "/")
    
    # Match Windows drive-style path: e.g., C:/Users/...
    match = re.match(r"^([a-zA-Z]):/(.+)", path_str)
    if match:
        drive = match.group(1).lower()
        rest = match.group(2)
        return f"/mnt/{drive}/{rest}"
    
    return path_str  # Already WSL or not applicable

def convert_csv_paths(input_path: Path, output_path: Path, columns: list):
    rows = []
    with input_path.open('r', encoding='utf-8', newline='') as f:
        reader = csv.DictReader(f)
        fieldnames = reader.fieldnames
        for row in reader:
            for col in columns:
                if col in row:
                    row[col] = to_wsl_path(row[col])
            rows.append(row)

    with output_path.open('w', encoding='utf-8', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)

    print(f"✅ Paths converted and saved to: {output_path}")

if __name__ == "__main__":
    convert_csv_paths(INPUT_CSV, OUTPUT_CSV, PATH_COLUMNS)


# **Usage (1 sentence)**
# Run `python convert_to_wsl.py` to rewrite the path-bearing columns of `metadata_manifest.csv`, converting every Windows drive path like `C:\Users\…` into its WSL-compatible form `/mnt/c/Users/…`, and save the updated CSV in-place.

# ---

# ### Tools / Technologies employed

# * **Python 3.x standard library** – `csv` for manifest parsing/writing, `re` for drive-letter regex conversion, and `pathlib` for cross-platform file handling.
# * **Hard-coded column whitelist** – (`media_path`, `json_path`, `corrected_path`, `visual_review_path`, `duplicate_of`) guarantees only relevant fields are modified.

# ---

# ### Idea summary (what it does & why it matters)

# `convert_to_wsl.py` bridges the Windows↔WSL divide in one sweep: it detects any `X:\…` style path, swaps backslashes for forward slashes, injects the proper `/mnt/x/` mount prefix, and leaves already-Linux paths untouched. By normalising every location string inside the manifest, the script prevents “file not found” errors when you run the processing pipeline from within WSL2, ensuring that subsequent Python or Bash tools can access the media seamlessly regardless of which OS authored the CSV.

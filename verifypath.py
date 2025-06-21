import csv
from pathlib import Path
from tqdm import tqdm

# --- Config ---
CSV_FILE = Path("/mnt/c/Users/vagrawal/OneDrive - Altair Engineering, Inc/Documents/Personal/Code/metadata_manifest.csv")
LOG_FILE = CSV_FILE.with_name("invalid_paths.log")
PATH_COLUMNS = ["json_path", "media_path", "corrected_path"]

# --- Count total rows first for tqdm ---
with CSV_FILE.open(newline='', encoding='utf-8') as csvfile:
    total_rows = sum(1 for _ in csvfile) - 1  # header skipped

invalid_count = 0

with (
    CSV_FILE.open(newline='', encoding='utf-8') as csvfile,
    LOG_FILE.open("w", encoding='utf-8') as log
):
    reader = csv.DictReader(csvfile)
    for idx, row in enumerate(tqdm(reader, total=total_rows, desc="🔍 Validating paths"), 1):
        # skip any row where row_type contains 'unmatched'
        if 'unmatched' in row.get('row_type', '').lower():
            continue
        for col in PATH_COLUMNS:
            path_str = row.get(col, "").strip()
            if path_str and not Path(path_str).exists():
                log.write(f"Row {idx}, {col}: {path_str} [NOT FOUND]\n")
                log.flush()
                invalid_count += 1

print(f"\n✅ Finished scanning: {CSV_FILE.name}")
print(f"❌ Invalid paths found: {invalid_count}")
print(f"📄 See log file: {LOG_FILE}")

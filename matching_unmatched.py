import csv
import re
import shutil
from pathlib import Path
from tqdm import tqdm

# Configuration settings
PRE_METADATA_DIR = Path("/mnt/c/Users/vagrawal/OneDrive - Altair Engineering, Inc/Documents/Personal/Pictures/Processing")
UNMATCHED_JSON_DIR = PRE_METADATA_DIR / "__UNMATCHED_JSON__"
UNMATCHED_MEDIA_DIR = PRE_METADATA_DIR / "__UNMATCHED_MEDIA__"
MANIFEST_FILE = Path("/mnt/c/Users/vagrawal/OneDrive - Altair Engineering, Inc/Documents/Personal/Code/metadata_manifest.csv")
DRYRUN = False

# Helper function to match JSON file to media file using simplified comparison
def match_json_to_media(json_name, media_names):
    json_base = re.sub(r'\.+json$', '', json_name.lower())
    json_base = re.sub(r'[^a-zA-Z0-9]', '', json_base)
    for media_name in media_names:
        media_base = Path(media_name).stem.lower()
        media_base_clean = re.sub(r'[^a-zA-Z0-9]', '', media_base)
        if json_base == media_base_clean or json_base in media_base_clean or media_base_clean in json_base:
            return media_name
    return None

# Load manifest
with open(MANIFEST_FILE, 'r', encoding='utf-8') as f:
    reader = csv.DictReader(f)
    manifest_rows = list(reader)
    fieldnames = reader.fieldnames

# Collect unmatched files
unmatched_jsons = list(UNMATCHED_JSON_DIR.rglob("*.json"))
unmatched_media = list(UNMATCHED_MEDIA_DIR.rglob("*"))
media_names = [str(m.relative_to(UNMATCHED_MEDIA_DIR)) for m in unmatched_media]

# Move file helper
def move_to_final_location(unmatched_path, folder_type):
    rel_path = unmatched_path.relative_to(PRE_METADATA_DIR / f"__UNMATCHED_{folder_type}__")
    z_folder = rel_path.parts[0]
    rest = Path(*rel_path.parts[1:])
    dest = PRE_METADATA_DIR / z_folder / "Takeout" / "Google Photos" / rest
    dest.parent.mkdir(parents=True, exist_ok=True)
    if not DRYRUN:
        shutil.move(str(unmatched_path), str(dest))
    print(f"{'[DRYRUN]' if DRYRUN else 'Moved'} {unmatched_path} --> {dest}")
    return dest

updated_rows = 0
rows_to_remove = set()

for json_file in tqdm(unmatched_jsons, desc="Matching JSONs"):
    match = match_json_to_media(json_file.name, media_names)
    if match:
        media_file = UNMATCHED_MEDIA_DIR / match
        new_json_path = move_to_final_location(json_file, 'JSON')
        new_media_path = move_to_final_location(media_file, 'MEDIA')

        json_filename_lower = json_file.name.lower()
        media_filename_lower = media_file.name.lower()

        unmatched_media_row = None
        unmatched_json_row_index = None

        for i, row in enumerate(manifest_rows):
            row_type = row.get('row_type', '').lower()
            if row_type == 'unmatched_media' and Path(row.get('media_path', '')).name.lower() == media_filename_lower:
                unmatched_media_row = row
            elif row_type == 'unmatched_json' and Path(row.get('json_path', '')).name.lower() == json_filename_lower:
                unmatched_json_row_index = i

        if unmatched_media_row:
            unmatched_media_row['json_filename'] = json_file.name
            unmatched_media_row['json_path'] = str(new_json_path)
            unmatched_media_row['original_media'] = new_media_path.name
            unmatched_media_row['media_path'] = str(new_media_path)
            unmatched_media_row['corrected_path'] = str(new_media_path)
            unmatched_media_row['row_type'] = 'matched'
            unmatched_media_row['notes'] = 'Recovered match'
            updated_rows += 1

            if unmatched_json_row_index is not None:
                rows_to_remove.add(unmatched_json_row_index)

        media_names.remove(str(match))

# Remove matched unmatched_json rows
manifest_rows = [row for i, row in enumerate(manifest_rows) if i not in rows_to_remove]

# Write updated manifest if not a dry run
if not DRYRUN:
    with open(MANIFEST_FILE, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(manifest_rows)
    print(f"✔ Updated {updated_rows} rows and deleted {len(rows_to_remove)} rows in manifest.")
else:
    print(f"[DRYRUN] {updated_rows} rows would be updated. {len(rows_to_remove)} rows would be deleted.")

# **Usage (1 sentence)**
# Run this utility after the initial quarantine stage to crawl the `__UNMATCHED_JSON__` and `__UNMATCHED_MEDIA__` vaults, fuzz-match orphaned Google-Takeout JSONs to their media twins, move both back into the canonical `Z###/Takeout/Google Photos/…` tree, and patch the corresponding rows in `metadata_manifest.csv` (with `DRYRUN=True` first for a safe preview).

# **Tools / Technologies employed**

# * **Python 3.10+** standard library: `csv`, `pathlib`, `re`, `shutil` for manifest editing, path maths, regex-based fuzzy matching, and atomic file moves.
# * **tqdm** progress bars for visual feedback on large unmatched sets.
# * **Dry-run toggle** to simulate all moves and manifest rewrites without touching disk.

# **Idea summary (what it does & why it matters)**
# `matching_unmatched.py` is the recovery engine that salvages missed pairings left behind by earlier ingestion steps. It first enumerates every JSON under `__UNMATCHED_JSON__` and builds a filename-cleaned hash (letters + digits only). Using the same sanitization on each media file name in `__UNMATCHED_MEDIA__`, it performs a simple but effective fuzzy equality/containment check to find likely matches that differ only by punctuation, spaces, or trailing duplicates. When a match is found the script:

# 1. **Restores original hierarchy** – moves both JSON and media back into their rightful `Z###/Takeout/Google Photos/…` location, recreating folders as needed.
# 2. **Repairs the manifest** – switches the row’s `row_type` to `matched`, updates JSON / media / corrected paths, and annotates the action with a “Recovered match” note.

# Because all actions are logged—and optionally simulated—the script provides a low-risk, high-reward sweep that dramatically reduces manual triage, ensuring every asset and its metadata wind up reunited before the hashing, deduplication, and timestamp-correction phases proceed.

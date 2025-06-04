import csv
import re
import shutil
from pathlib import Path
from tqdm import tqdm

# Configuration settings used by this script.
# PRE_METADATA_DIR = Path(r"D:\pre-metadata")
PRE_METADATA_DIR = Path(r"C:\Users\vagrawal\OneDrive - Altair Engineering, Inc\Documents\Personal\Pictures\Processing")
UNMATCHED_JSON_DIR = PRE_METADATA_DIR / "__UNMATCHED_JSON__"
UNMATCHED_MEDIA_DIR = PRE_METADATA_DIR / "__UNMATCHED_MEDIA__"
# MANIFEST_FILE = PRE_METADATA_DIR / "metadata_manifest.csv"
MANIFEST_FILE = Path(r"C:\Users\vagrawal\OneDrive - Altair Engineering, Inc\Documents\Personal\Code\metadata_manifest.csv")

DRYRUN = True  

# This helper function tries to match a JSON file with a media file using fuzzy logic.
def match_json_to_media(json_name, media_names):
    json_base = re.sub(r'\.+json$', '', json_name.lower())
    json_base = re.sub(r'[^a-zA-Z0-9]', '', json_base)  

    for media_name in media_names:
        media_base = Path(media_name).stem.lower()
        media_base_clean = re.sub(r'[^a-zA-Z0-9]', '', media_base)
        if json_base == media_base_clean or json_base in media_base_clean or media_base_clean in json_base:
            return media_name
    return None

# Load the manifest CSV file into memory.
with open(MANIFEST_FILE, 'r', encoding='utf-8') as f:
    reader = csv.DictReader(f)
    manifest_rows = list(reader)

# Recursively look for unmatched JSON and media files.
unmatched_jsons = list(UNMATCHED_JSON_DIR.rglob("*.json"))
unmatched_media = list(UNMATCHED_MEDIA_DIR.rglob("*"))
media_names = [str(m.relative_to(UNMATCHED_MEDIA_DIR)) for m in unmatched_media]

print(f"Found {len(unmatched_jsons)} JSONs and {len(media_names)} media files.")

# Helper function that moves a file to its final destination.
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

# Process each unmatched JSON file to find its media match.
updated_rows = 0
for json_file in tqdm(unmatched_jsons, desc="Matching JSONs"):
    match = match_json_to_media(json_file.name, media_names)
    if match:
        media_file = UNMATCHED_MEDIA_DIR / match

        new_json_path = move_to_final_location(json_file, 'JSON')
        new_media_path = move_to_final_location(media_file, 'MEDIA')

        for row in manifest_rows:
            if row['json_filename'].lower() == json_file.name.lower() or Path(row['json_path']).name.lower() == json_file.name.lower():
                row['json_path'] = str(new_json_path)
                row['original_media'] = new_media_path.name
                row['media_path'] = str(new_media_path)
                row['corrected_path'] = str(new_media_path)
                row['row_type'] = 'matched'
                row['notes'] = 'Recovered match'
                updated_rows += 1

        media_names.remove(str(match))

if not DRYRUN:
    with open(MANIFEST_FILE, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=manifest_rows[0].keys())
        writer.writeheader()
        writer.writerows(manifest_rows)
    print(f"Updated {updated_rows} rows in manifest.")
else:
    print(f"[DRYRUN] {updated_rows} rows would be updated.")

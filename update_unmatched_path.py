import csv
from pathlib import Path
from tqdm import tqdm

# Configuration settings
MANIFEST_FILE = Path("/mnt/c/Users/vagrawal/OneDrive - Altair Engineering, Inc/Documents/Personal/Code/metadata_manifest.csv")
PROCESSING_ROOT = Path("/mnt/c/Users/vagrawal/OneDrive - Altair Engineering, Inc/Documents/Personal/Pictures/Processing")
UNMATCHED_PREFIX = PROCESSING_ROOT / "__UNMATCHED_MEDIA__"
DRY_RUN = True 

def fix_media_path(row):
    media_path = Path(row.get("media_path", ""))
    if not media_path.exists() and "unmatched_media" in row.get("row_type", "").lower():
        parts = media_path.parts
        try:
            # Find the index where the part starts with "Z"
            z_index = next(i for i, p in enumerate(parts) if p.upper().startswith("Z"))
            # Start building the relative path with the "Z" directory
            rel_parts = [parts[z_index]]
            # Continue with subdirectories, skipping unwanted folders
            rel_parts += [p for p in parts[z_index + 1:] if p.lower() not in {'takeout', 'google photos'}]
            # Add the file name at the end if it is not already present
            if media_path.name != rel_parts[-1]:
                rel_parts.append(media_path.name)
            new_path = UNMATCHED_PREFIX.joinpath(*rel_parts)
            return str(new_path)
        except StopIteration:
            return None
    return None

def update_manifest():
    with MANIFEST_FILE.open("r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        rows = list(reader)

    updated_count = 0
    for row in tqdm(rows, desc="Fixing media_path"):
        new_path = fix_media_path(row)
        if new_path:
            row["notes"] = f"media_path updated to {new_path}"
            if not DRY_RUN:
                row["media_path"] = new_path
            updated_count += 1

    if not DRY_RUN:
        with MANIFEST_FILE.open("w", encoding="utf-8", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=rows[0].keys())
            writer.writeheader()
            writer.writerows(rows)
        print(f"✅ Manifest updated: {updated_count} media_paths corrected.")
    else:
        print(f"✅ Dry-run: {updated_count} media_paths would be updated.")

if __name__ == "__main__":
    update_manifest()

'''
**Usage (1 sentence)**
Run this helper after any bulk moves to `__UNMATCHED_MEDIA__` to sweep `metadata_manifest.csv`, rebuild broken `media_path`s for rows flagged *unmatched\_media*, and—when you flip `DRY_RUN = False`—persist those fixes in-place.

**Tools / Technologies employed**

* **Python 3.10+ standard library** – `csv`, `pathlib`, built-in iteration for manifest parsing and path reconstruction.
* **tqdm** – progress-bar visualization over tens-of-thousands of manifest rows.
* **Dry-run toggle** – lightweight safety valve to preview edits before committing.

**Idea summary (what it does & why it matters)**
`update_unmatched_path.py` is the manifest-maintenance mop-up: after earlier steps shove orphaned media into a flattened `__UNMATCHED_MEDIA__` vault, the CSV can still contain stale absolute paths. This script programmatically rebuilds each broken path by slicing from the first *Z###* directory, discarding noisy “Takeout/Google Photos” segments, and grafting the remainder onto the central unmatched prefix. It tags every would-be change in the `notes` column for transparency, then—outside dry-run mode—writes the corrections back to disk. The result is a self-healing manifest where every record, even those awaiting manual review, cleanly resolves to a real file, eliminating 404-style hiccups in downstream analytics or UI tooling.
'''
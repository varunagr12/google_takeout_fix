import csv
from pathlib import Path
from tqdm import tqdm

# Configuration settings
MANIFEST_FILE = Path("/mnt/c/Users/vagrawal/OneDrive - Altair Engineering, Inc/Documents/Personal/Code/metadata_manifest.csv")
PROCESSING_ROOT = Path("/mnt/c/Users/vagrawal/OneDrive - Altair Engineering, Inc/Documents/Personal/Pictures/Processing")
UNMATCHED_MEDIA_PREFIX = PROCESSING_ROOT / "__UNMATCHED_MEDIA__"
UNMATCHED_JSON_PREFIX = PROCESSING_ROOT / "__UNMATCHED_JSON__"
DRY_RUN = False

def rewrite_path(base_path: Path, target_prefix: Path):
    parts = base_path.parts
    try:
        z_index = next(i for i, p in enumerate(parts) if p.upper().startswith("Z"))
        rel_parts = [parts[z_index]]
        rel_parts += [p for p in parts[z_index + 1:] if p.lower() not in {'takeout', 'google photos'}]
        return target_prefix.joinpath(*rel_parts)
    except StopIteration:
        return None

def fix_unmatched_paths(row):
    row_type = row.get("row_type", "").lower()
    notes = []

    # Fix unmatched_media: media_path and corrected_path
    if "unmatched_media" in row_type:
        for col in ["media_path", "corrected_path"]:
            old_path = Path(row.get(col, "").strip())
            new_path = rewrite_path(old_path, UNMATCHED_MEDIA_PREFIX)
            if new_path and new_path != old_path:
                notes.append(f"{col} updated to {new_path}")
                if not DRY_RUN:
                    row[col] = str(new_path)

    # Fix unmatched_json: json_path
    if "unmatched_json" in row_type:
        col = "json_path"
        old_path = Path(row.get(col, "").strip())
        new_path = rewrite_path(old_path, UNMATCHED_JSON_PREFIX)
        if new_path and new_path != old_path:
            notes.append(f"{col} updated to {new_path}")
            if not DRY_RUN:
                row[col] = str(new_path)

    return notes

def update_manifest():
    with MANIFEST_FILE.open("r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        rows = list(reader)

    updated_count = 0
    for row in tqdm(rows, desc="🛠 Fixing unmatched paths"):
        note_updates = fix_unmatched_paths(row)
        if note_updates:
            existing_note = row.get("notes", "").strip()
            full_note = "; ".join(note_updates)
            row["notes"] = f"{existing_note}; {full_note}".strip("; ")
            updated_count += 1

    if not DRY_RUN:
        with MANIFEST_FILE.open("w", encoding="utf-8", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=rows[0].keys())
            writer.writeheader()
            writer.writerows(rows)
        print(f"✅ Manifest updated: {updated_count} unmatched paths corrected.")
    else:
        print(f"✅ Dry-run: {updated_count} unmatched paths would be updated.")

if __name__ == "__main__":
    update_manifest()

import csv
import os
from pathlib import Path
from tqdm import tqdm

MANIFEST_FILE = "metadata_manifest.csv"
DELETION_LOG = "dedup_deletion_log.txt"

def log_deletion(msg):
    with open(DELETION_LOG, "a", encoding="utf-8") as log:
        log.write(msg + "\n")

def delete_flagged_files():
    with open(MANIFEST_FILE, "r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        rows = list(reader)

    deleted_count = 0
    modified = False

    for row in tqdm(rows, desc="Deleting flagged files", unit="file"):
        if str(row.get("delete_flag", "")).strip().lower() != "true":
            continue

        media_path = row.get("media_path")
        json_path = row.get("json_path")
        deleted = False

        if media_path and os.path.exists(media_path):
            try:
                os.remove(media_path)
                log_deletion(f"Deleted media: {media_path}")
                deleted = True
                deleted_count += 1
            except Exception as e:
                log_deletion(f"❌ Failed to delete media {media_path}: {e}")

        if json_path and os.path.exists(json_path):
            try:
                os.remove(json_path)
                log_deletion(f"Deleted JSON: {json_path}")
            except Exception as e:
                log_deletion(f"❌ Failed to delete JSON {json_path}: {e}")

        row["deletion_status"] = "deleted" if deleted else "not_deleted"
        modified = True

    if modified:
        fieldnames = list(rows[0].keys())
        if "deletion_status" not in fieldnames:
            fieldnames.append("deletion_status")

        with open(MANIFEST_FILE, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(rows)

    print(f"✅ Deletion stage complete. {deleted_count} files deleted.")
    print(f"📝 Log saved to: {DELETION_LOG}")

if __name__ == "__main__":
    delete_flagged_files()

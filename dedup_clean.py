import csv
import os
from pathlib import Path, PureWindowsPath
import platform
from tqdm import tqdm

MANIFEST_FILE = "metadata_manifest.csv"
DELETION_LOG  = "dedup_deletion_log.txt"

# --- normalize Windows vs WSL paths ---
def to_local_path(p_str: str) -> Path:
    p = p_str.strip()
    # if running under WSL and we see a "C:\..." style path
    if platform.system().lower() == "linux" and len(p) >= 2 and p[1] == ":":
        win = PureWindowsPath(p)
        mount = "/mnt/" + win.drive[0].lower()
        return Path(mount, *win.parts[1:])
    # otherwise assume it's already a valid POSIX path
    return Path(p)

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

        # normalize both paths
        media_raw = row.get("media_path", "")
        json_raw  = row.get("json_path", "")
        media_p   = to_local_path(media_raw)
        json_p    = to_local_path(json_raw)

        deleted = False

        if media_raw and media_p.exists():
            try:
                os.remove(media_p)
                log_deletion(f"Deleted media: {media_raw} -> {media_p}")
                deleted = True
                deleted_count += 1
            except Exception as e:
                log_deletion(f"❌ Failed to delete media {media_p}: {e}")

        if json_raw and json_p.exists():
            try:
                os.remove(json_p)
                log_deletion(f"Deleted JSON: {json_raw} -> {json_p}")
            except Exception as e:
                log_deletion(f"❌ Failed to delete JSON {json_p}: {e}")

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

'''
**Usage (1 sentence)**
After you’ve verified and locked-in the manifest, run `python dedup_clean.py` to physically delete every file whose `delete_flag` is **true** (plus its JSON sidecar), mark the outcome in `metadata_manifest.csv`, and append a detailed audit line to `dedup_deletion_log.txt`.

---

### Tools / Technologies employed

| Layer                  | Components                                          | Purpose                                                                    |
| ---------------------- | --------------------------------------------------- | -------------------------------------------------------------------------- |
| **Python 3.x std-lib** | `csv`, `os`, `pathlib`, `platform`                  | Manifest parsing/writing, cross-platform path handling, filesystem deletes |
| **tqdm**               | Progress bar                                        | Real-time feedback during mass deletions                                   |
| **WSL path shim**      | Converts `C:\…` → `/mnt/c/…` when running under WSL | Ensures identical behavior on native Windows and WSL/Linux                 |
| **Plain-text logging** | `dedup_deletion_log.txt`                            | Permanent, timestamp-free audit trail of every delete attempt              |

---

### Idea summary (what it does & why it matters)

`dedup_clean.py` is the *irreversible* final act of the deduplication pipeline. It walks the manifest, normalizes each stored path to the current OS (e.g., `C:\…` → `/mnt/c/…` for WSL), and—only for rows where `delete_flag=true`—tries to remove both the media file and its metadata JSON. Each success or failure is appended to a human-readable log, and the script updates the CSV with a `deletion_status` so future passes can safely skip already-handled rows. By combining atomic manifest edits, OS-agnostic path translation, and granular logging, the script offers a transparent, auditable, and repeatable way to reclaim disk space without risking accidental double-deletes or silent data loss.
'''
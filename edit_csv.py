import csv
import argparse
import platform
from pathlib import Path, PureWindowsPath
from tqdm import tqdm

# --- CONFIGURATION FOR WSL ---
INPUT_CSV  = Path("/mnt/c/Users/vagrawal/OneDrive - Altair Engineering, Inc/Documents/Personal/Code/metadata_manifest.csv")
BACKUP_CSV = INPUT_CSV.with_suffix(".bak.csv")

def is_wsl() -> bool:
    return "microsoft" in platform.uname().release.lower()

def to_local_path(path_str: str) -> Path:
    """Converts Windows-style paths to WSL paths if needed."""
    p = path_str.strip()
    if is_wsl() and len(p) >= 2 and p[1] == ":":
        win = PureWindowsPath(p)
        mount = "/mnt/" + win.drive[0].lower()
        return Path(mount, *win.parts[1:])
    return Path(p)

def parse_args():
    p = argparse.ArgumentParser(description="Clean or clear tags in a CSV manifest")
    p.add_argument(
        "--backup", "-b", action="store_true",
        help="create a backup of the CSV file before making changes"
    )
    p.add_argument(
        "--dry-run", action="store_true",
        help="preview which rows would be removed without saving changes"
    )
    grp = p.add_mutually_exclusive_group(required=True)
    grp.add_argument(
        "--remove-rows", nargs=2, metavar=("KEY", "VALUE"),
        help="remove any row where KEY == VALUE (case-insensitive)"
    )
    grp.add_argument(
        "--clear-column", metavar="KEY",
        help="clear the value in column KEY for every row"
    )
    grp.add_argument(
        "--prune-missing", action="store_true",
        help="remove rows where media_path or json_path does not point to existing files"
    )
    return p.parse_args()

def main():
    args = parse_args()

    print("🔍 Loading CSV from:", INPUT_CSV)
    with INPUT_CSV.open("r", newline="", encoding="utf-8") as f:
        rows = list(csv.DictReader(f))
    if not rows:
        print("⚠️  No rows found; exiting.")
        return
    fieldnames = list(rows[0].keys())

    if args.backup and not args.dry_run:
        print(f"📝 Writing backup to: {BACKUP_CSV}")
        with BACKUP_CSV.open("w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(tqdm(rows, desc="Writing backup"))
        print(f"📝 Backup written to: {BACKUP_CSV}")

    if args.remove_rows:
        key, val = args.remove_rows
        val_lo = val.lower()
        before = len(rows)
        cleaned = [
            r for r in tqdm(rows, desc=f"Removing rows where {key}='{val}'")
            if r.get(key, "").strip().lower() != val_lo
        ]
        removed = before - len(cleaned)
        print(f"✅ Would remove {removed} rows where {key} == '{val}'" if args.dry_run else f"✅ Removed {removed} rows")
        output_rows = cleaned

    elif args.clear_column:
        key = args.clear_column
        cleared = 0
        for r in tqdm(rows, desc=f"Clearing column '{key}'"):
            if key in r and r[key].strip():
                if not args.dry_run:
                    r[key] = ""
                cleared += 1
        print(f"✅ Would clear '{key}' in {cleared} rows" if args.dry_run else f"✅ Cleared '{key}' in {cleared} rows")
        output_rows = rows

    elif args.prune_missing:
        before = len(rows)
        cleaned = []
        removed_paths = []
        for r in tqdm(rows, desc="Pruning missing files"):
            media_str = r.get("media_path", "").strip()
            json_str  = r.get("json_path", "").strip()

            if not media_str or not json_str:
                cleaned.append(r)
                continue

            media_path = to_local_path(media_str)
            json_path  = to_local_path(json_str)
            media_exists = media_path.exists()
            json_exists  = json_path.exists()

            if media_exists and json_exists:
                cleaned.append(r)
            else:
                removed_paths.append((media_str, json_str))

        print(f"✅ Would remove {len(removed_paths)} rows with missing files:" if args.dry_run else f"✅ Removed {len(removed_paths)} rows")
        output_rows = cleaned

    else:
        print("❌ No valid action provided.")
        return

    if args.dry_run:
        print("💡 Dry-run mode: No changes were written.")
        return

    print(f"📄 Writing updated CSV to: {INPUT_CSV}")
    with INPUT_CSV.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(tqdm(output_rows, desc="Writing updated CSV"))
    print(f"📄 Updated CSV saved to: {INPUT_CSV}")

if __name__ == "__main__":
    main()

'''
**Usage (1 sentence)**
Run `python edit_csv.py --remove-rows KEY VALUE | --clear-column KEY | --prune-missing [--backup] [--dry-run]` to batch-edit `metadata_manifest.csv`, either deleting rows that match a value, blanking an entire column, or pruning entries whose media/JSON files no longer exist, with optional WSL-aware backups and preview mode.

---

### Tools / Technologies employed

| Layer                            | Components                                       | Purpose                                                                |
| -------------------------------- | ------------------------------------------------ | ---------------------------------------------------------------------- |
| **Python 3.x std-lib**           | `argparse`, `csv`, `pathlib`, `platform`, `tqdm` | CLI parsing, manifest I/O, Windows↔WSL path translation, progress bars |
| **Mutually-exclusive CLI group** | `argparse.add_mutually_exclusive_group`          | Ensures the user picks exactly one edit mode                           |
| **WSL path shim**                | `to_local_path()`                                | Converts `C:\…` to `/mnt/c/…` when validating file existence in WSL    |
| **Backup & dry-run switches**    | `--backup`, `--dry-run`                          | Safe experimentation and rollback before committing edits              |

---

### Idea summary (what it does & why it matters)

`edit_csv.py` is the manifest’s Swiss-army editor, giving you three high-impact cleanup operations in one command:

1. **Row removal** – strip out every entry where a chosen column equals a specific value (case-insensitive), e.g., delete all rows tagged `row_type=unmatched_media`.
2. **Column clearing** – blank a single column across all rows, handy for resetting status fields like `notes` after a pipeline iteration.
3. **Missing-file pruning** – verify that both `media_path` and `json_path` still point to real files (with Windows paths converted to WSL on the fly) and drop rows whose assets have vanished.

The script writes a time-stamped backup when requested, streams progress via `tqdm`, and supports a dry-run preview that reports exactly how many rows would change without touching disk—making large-scale manifest surgery both safe and transparent.
'''
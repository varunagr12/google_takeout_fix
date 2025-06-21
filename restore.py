#!/usr/bin/env python3
# fix this. 
import shutil
import argparse
from pathlib import Path
from tqdm import tqdm

# Base path in WSL for Processing
BASE_PATH = Path("/mnt/c/Users/vagrawal/OneDrive - Altair Engineering, Inc/Documents/Personal/Pictures/Processing")

def move_failed_files_back(failed_folder: Path, dry_run: bool = False):
    all_files = [f for f in failed_folder.rglob("*") if f.is_file()]
    print(f"Found {len(all_files)} files to move back from {failed_folder.name}... (dry run={dry_run})")

    for src in tqdm(all_files, desc=f"Restoring from {failed_folder.name}"):
        parts = list(src.parts)
        if failed_folder.name not in parts:
            print(f"Skipping (no {failed_folder.name} in path): {src}")
            continue

        idx = parts.index(failed_folder.name)
        # base before subfolder, group code, then insert 'Takeout/Google Photos', then the rest
        pre   = parts[:idx]
        group = parts[idx + 1]
        post  = parts[idx + 2:]
        restored_parts = pre + [group, 'Takeout', 'Google Photos'] + post
        dest = Path(*restored_parts)

        if dry_run:
            print(f"→ DRY RUN: would create dir {dest.parent}")
            print(f"→ DRY RUN: would move {src} → {dest}")
        else:
            dest.parent.mkdir(parents=True, exist_ok=True)
            shutil.move(str(src), str(dest))

    print("✅ Done.")

def main():
    parser = argparse.ArgumentParser(
        description="Restore files from a subfolder under Processing back to original locations."
    )
    parser.add_argument(
        "--subfolder",
        default="__UNMATCHED_MEDIA__",
        help="Name of the subfolder under Processing to restore from (default: __UNMATCHED_MEDIA__)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be moved without actually moving files",
    )
    args = parser.parse_args()

    failed_folder = BASE_PATH / args.subfolder
    if not failed_folder.is_dir():
        parser.error(f"Subfolder not found: {failed_folder}")

    move_failed_files_back(failed_folder, dry_run=args.dry_run)

if __name__ == "__main__":
    main()

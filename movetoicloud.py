#!/usr/bin/env python3
"""
Copy media files from processing folder to iCloud Drive, skipping __UNMATCHED__ subfolders.
"""
import argparse
import shutil
from pathlib import Path
from tqdm import tqdm

# Default processing folder (WSL path)
DEFAULT_PROCESSING = Path("/mnt/c/Users/vagrawal/OneDrive - Altair Engineering, Inc/Documents/Personal/Pictures/Processing")
# Supported media extensions
MEDIA_EXTS = {'.jpg', '.jpeg', '.png', '.heic', '.mp4', '.mov', '.avi', '.mkv'}

def main():
    parser = argparse.ArgumentParser(description="Copy media from processing to iCloud folder")
    parser.add_argument(
        '--processing-dir', '-p',
        type=Path,
        default=DEFAULT_PROCESSING,
        help="Root Processing directory to scan"
    )
    parser.add_argument(
        '--icloud-dir', '-i',
        type=Path,
        default=Path("/mnt/c/Users/vagrawal/iCloudPhotos/Photos"),
        help="Destination iCloud Drive folder"
    )
    parser.add_argument(
        '--test', '-t',
        action='store_true',
        help="Test mode: copy only a limited number of files"
    )
    parser.add_argument(
        '--limit', '-n',
        type=int,
        default=5,
        help="Number of files to copy in test mode (default: 5)"
    )
    args = parser.parse_args()

    src_root = args.processing_dir
    dest_root = args.icloud_dir

    # Gather all media files, excluding __UNMATCHED__ folders
    all_files = [f for f in src_root.rglob('*')
                 if f.is_file()
                 and '__UNMATCHED' not in f.parts
                 and f.suffix.lower() in MEDIA_EXTS]
    total = len(all_files)
    to_copy = all_files[:args.limit] if args.test else all_files

    print(f"{'[TEST]' if args.test else '[COPY]'} {len(to_copy)}/{total} files to process")

    for file_path in tqdm(to_copy, desc="Copying files", unit="file"):
        rel = file_path.relative_to(src_root)
        target = dest_root / rel
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(file_path, target)

    print("✅ Done.")

if __name__ == '__main__':
    main()
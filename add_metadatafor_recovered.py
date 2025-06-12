#!/usr/bin/env python3
"""
Update missing timestamps in a manifest CSV from Google-Takeout-style JSON files
and, at the same time, fill the `new_ext` column with the media file's extension.

Criteria for update on each row
--------------------------------
• `timestamp_unix` is empty          AND
• `row_type`.lower() == "matched"

Actions performed
-----------------
1. Read `json_path`, grab meta["photoTakenTime"]["timestamp"] → write
   `timestamp_unix` + human-readable `formatted_time`.
2. If `new_ext` is blank, copy the extension of the media file
   (taken from `media_path` if present, else from `json_path` sibling).

Run:
    python update_manifest_timestamps.py --backup   # keep *.bak
    python update_manifest_timestamps.py --dry-run  # preview only
"""

import argparse, csv, json, sys, shutil
from pathlib import Path
from datetime import datetime
from tempfile import NamedTemporaryFile
from tqdm import tqdm

# ---------------------------------------------------------------------------

def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Fill in missing timestamps + new_ext")
    p.add_argument("--backup", "-b", action="store_true",
                   help="Write <csv>.bak before modifying")
    p.add_argument("--dry-run", action="store_true",
                   help="Preview changes without touching the file")
    return p.parse_args()

# ---------------------------------------------------------------------------

def extract_timestamp(json_path: Path) -> tuple[int, str]:
    """Return (unix_timestamp, formatted_time) or (0, '') on failure."""
    try:
        with json_path.open("r", encoding="utf-8") as f:
            meta = json.load(f)
        ts = int(meta.get("photoTakenTime", {}).get("timestamp", 0))
    except Exception:
        ts = 0
    fmt = datetime.utcfromtimestamp(ts).strftime("%Y:%m:%d %H:%M:%S") if ts else ""
    return ts, fmt

# ---------------------------------------------------------------------------

def main() -> None:
    args = parse_args()

    # >>>>  EDIT THIS PATH OR make it a positional arg if you prefer  <<<<
    csv_path = Path(r"/mnt/c/Users/vagrawal/OneDrive - Altair Engineering, Inc/Documents/Personal/Code/metadata_manifest.csv")

    if not csv_path.is_file():
        sys.exit(f"ERROR: CSV not found → {csv_path}")

    # Optional backup
    if args.backup and not args.dry_run:
        bak = csv_path.with_suffix(csv_path.suffix + ".bak")
        bak.write_bytes(csv_path.read_bytes())
        print(f"Backup written → {bak}")

    # Read manifest
    with csv_path.open("r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        rows = list(reader)
        fieldnames = reader.fieldnames or []

    # Ensure new_ext column exists
    if "new_ext" not in fieldnames:
        fieldnames.append("new_ext")

    # Verify required columns exist
    required = {"timestamp_unix", "formatted_time", "row_type", "json_path"}
    missing = required - set(fieldnames)
    if missing:
        sys.exit(f"ERROR: CSV missing column(s): {', '.join(sorted(missing))}")

    updated_rows, missing_json = 0, []

    for row in tqdm(rows, desc="Scanning rows", unit="row"):
        if row["timestamp_unix"].strip() or row["row_type"].strip().lower() != "matched":
            continue

        jp = Path(row["json_path"].strip())
        if not jp.is_file():
            missing_json.append(str(jp))
            continue

        ts, fmt = extract_timestamp(jp)
        if ts:
            row["timestamp_unix"] = str(ts)
            row["formatted_time"] = fmt
            updated_rows += 1

            # --------- new_ext handling ---------
            if not row.get("new_ext", "").strip():
                # Prefer media_path's extension if present
                media_path = row.get("media_path", "").strip()
                if media_path:
                    ext = Path(media_path).suffix.lower()
                else:
                    # fall back: assume media file has same stem as JSON
                    sibling_candidates = list(jp.parent.glob(jp.stem + ".*"))
                    ext = sibling_candidates[0].suffix.lower() if sibling_candidates else ""
                row["new_ext"] = ext  # store without dot
            # ------------------------------------

    print(f"\nRows updated: {updated_rows}")
    if missing_json:
        print(f"⚠️  JSON not found for {len(missing_json)} rows "
              f"(first missing: {missing_json[0]})")

    if args.dry_run:
        print("Dry-run: no file written.")
        return

    # ----- WRITE UPDATED MANIFEST (same filesystem → avoids cross-device rename) -----
    with NamedTemporaryFile("w",
                             delete=False,
                             encoding="utf-8",
                             newline="",
                             dir=csv_path.parent) as tmp:
        writer = csv.DictWriter(tmp, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)
        tmp_path = Path(tmp.name)

    try:
        tmp_path.replace(csv_path)          # atomic, same mount point
    except OSError:                          # exotic FS edge-case -> fallback
        shutil.move(tmp_path, csv_path)

    print(f"✔️  Manifest updated in-place → {csv_path}")

# ---------------------------------------------------------------------------

if __name__ == "__main__":
    main()

'''
**Usage (1 sentence)**
Run this script once the *recovered* rows are back in the manifest—`python add_metadatafor_recovered.py --backup` (or `--dry-run` first)—to pull missing Unix timestamps from each photo-metadata JSON, convert them to human-readable EXIF time, and back-fill the `new_ext` column with the media file’s true extension.

**Tools / Technologies employed**

* **Python 3.10+** standard libraries: `csv`, `json`, `argparse`, `datetime`, `pathlib`, `tempfile`, `shutil`, `sys` for CLI parsing, timestamp conversion, atomic file swaps, and manifest editing.
* **tqdm** for live row-processing progress bars.
* **NamedTemporaryFile + `Path.replace()`** for same-mount, atomic in-place CSV updates—avoids half-written manifests on interruption.
* **Safety switches**: `--backup` automatic `.bak` creation and `--dry-run` mode for preview-only execution.

**Idea summary (what it does & why it matters)**
`add_metadatafor_recovered.py` closes the metadata loop by enriching every “matched” row that still lacks a timestamp: it reads the Google-Takeout JSON, extracts `photoTakenTime.timestamp`, stores both the raw Unix value and a `YYYY:MM:DD HH:MM:SS` string, and—while it’s there—sets `new_ext` to the definitive lowercase extension of the actual media file (favoring `media_path`, falling back to sibling sniffing). The script supports dry-runs and automatic backups, performs all edits in memory, then atomically swaps in the new CSV to guarantee consistency even on abrupt exits. This final polish ensures that the manifest now contains complete temporal metadata and correct file-type indicators, enabling accurate EXIF repair, chronological sorting, and downstream archival without any manual patch-ups.
'''
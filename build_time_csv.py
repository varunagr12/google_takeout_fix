import csv
from pathlib import Path

IN_CSV  = Path("metadata_manifest.csv")
OUT_CSV = Path("times.csv")

with IN_CSV.open(newline="", encoding="utf-8") as fin, \
     OUT_CSV.open("w", newline="", encoding="utf-8") as fout:

    reader = csv.DictReader(fin)
    fieldnames = [
        "SourceFile",
        "DateTimeOriginal", "CreateDate", "ModifyDate",
        "MediaCreateDate", "TrackCreateDate"
    ]
    writer = csv.DictWriter(fout, fieldnames=fieldnames)
    writer.writeheader()

    for row in reader:
        src = row["media_path"]
        t   = row["formatted_time"]
        ext = Path(src).suffix.lower()

        # Build the CSV row for ExifTool
        out = {"SourceFile": src}
        if ext in (".jpg", ".jpeg"):
            out.update({
                "DateTimeOriginal": t,
                "CreateDate":       t,
                "ModifyDate":       t,
                # leave the quicktime tags blank
                "MediaCreateDate":  "",
                "TrackCreateDate":  ""
            })
        else:
            out.update({
                "DateTimeOriginal": "",
                "CreateDate":       "",
                "ModifyDate":       "",
                "MediaCreateDate":  t,
                "TrackCreateDate":  t
            })

        writer.writerow(out)

# **Usage (1 sentence)**
# Run `python times.py` to generate `times.csv`, a formatted sidecar manifest for ExifTool, encoding authoritative timestamps for every media file in your archive—mapped to the correct tags depending on filetype.

# ---

# ### Tools / Technologies employed

# | Layer                       | Components                                    | Purpose                                                                                     |
# | --------------------------- | --------------------------------------------- | ------------------------------------------------------------------------------------------- |
# | **Python 3.x std-lib**      | `csv`, `pathlib`                              | Reads input manifest and writes output CSV in ExifTool-friendly format                     |
# | **ExifTool (CLI)**          | Expects output to be passed with `-csv=times.csv` | Propagates timestamp metadata into image and video headers                                  |

# ---

# ### Idea summary (what it does & why it matters)

# `times.py` converts your internal metadata manifest (`metadata_manifest.csv`) into a **minimal, ExifTool-compatible CSV** for bulk timestamp injection.

# 1. **Reads manifest** – loads each row from `metadata_manifest.csv`, extracting `media_path` and `formatted_time`.
# 2. **Branch by filetype** – for JPEGs, fills `DateTimeOriginal`, `CreateDate`, and `ModifyDate`; for all others (videos, HEIC, PNG, etc.), fills `MediaCreateDate` and `TrackCreateDate` only.
# 3. **Writes to `times.csv`** – ready to be passed directly to ExifTool via `-csv=times.csv`, keeping your EXIF and QuickTime timestamps in sync with manifest data.
# 
# This script ensures your timestamps are **universally embedded at the file level**, not just tracked in metadata. That means:
# • Correct sorting in photo apps
# • Accurate preview timelines in macOS, Windows, and cloud platforms
# • Clean downstream EXIF reads during search, sync, or dedup

# In short: it's the bridge between your structured manifest and the media headers themselves—one of the final, crucial polishing steps before archive delivery.

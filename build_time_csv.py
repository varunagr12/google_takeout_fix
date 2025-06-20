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

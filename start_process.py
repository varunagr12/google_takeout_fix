import os
import subprocess
import shutil
import re
from pathlib import Path

SEVEN_ZIP_EXE = r"C:\Program Files\7-Zip\7z.exe"
ZIPPED_DIR = Path(r"D:\Zipped")
PROCESSING_DIR = Path(r"C:\Users\vagrawal\OneDrive - Altair Engineering, Inc\Documents\Personal\Pictures\Processing")
TRACKER_DIR = Path(r"C:\Users\vagrawal\OneDrive - Altair Engineering, Inc\Documents\Personal\Pictures\Tracker")

def is_photos_year_folder(name):
    return re.fullmatch(r"Photos from \d{4}", name.strip())

def find_deepest_photos_root(root_dir):
    for dirpath, dirnames, filenames in os.walk(root_dir):
        matching = [d for d in dirnames if is_photos_year_folder(d)]
        if len(matching) >= 1:
            return Path(dirpath)
    return None

def delete_non_year_folders(photos_root):
    for item in Path(photos_root).iterdir():
        if item.is_dir() and not is_photos_year_folder(item.name):
            print(f"🗑️ Deleting: {item}")
            shutil.rmtree(item)

def extract_and_process_zip(zip_file: Path):
    # Extract the numeric suffix from the ZIP filename
    match = re.search(r"-(\d{3})\.zip$", zip_file.name)
    if not match:
        print(f"⚠️ Skipping: Cannot extract number from {zip_file.name}")
        return

    number = match.group(1)
    standard_name = f"Z{number}.zip"
    extract_dir = PROCESSING_DIR / f"Z{number}"

    print(f"📦 Extracting: {zip_file.name} → {extract_dir}")
    extract_dir.mkdir(parents=True, exist_ok=True)

    extract_command = [
        SEVEN_ZIP_EXE,
        "x",
        str(zip_file),
        f"-o{extract_dir}",
        "-y"
    ]

    try:
        subprocess.run(extract_command, check=True)
        print(f"✅ Extracted to {extract_dir}")

        # Identify the folder that follows the 'Photos from YYYY' pattern and remove other folders in that directory
        photos_root = find_deepest_photos_root(extract_dir)
        if photos_root:
            print(f"📂 Identified 'Photos from YYYY' root at: {photos_root}")
            delete_non_year_folders(photos_root)
        else:
            print("⚠️ No 'Photos from YYYY' folders found. Skipping cleanup.")

        # Create a log entry noting that this ZIP has been processed
        TRACKER_DIR.mkdir(parents=True, exist_ok=True)
        tracker_file = TRACKER_DIR / (zip_file.stem + ".txt")
        tracker_file.write_text(f"Extracted: {zip_file.name}\n", encoding="utf-8")
        print(f"📄 Created tracker log at: {tracker_file}")

        # Rename the ZIP file to follow the standard format with a 'Z' prefix
        new_zip_path = zip_file.with_name(standard_name)
        if not new_zip_path.exists():
            zip_file.rename(new_zip_path)
            print(f"🔄 Renamed ZIP to: {standard_name}")
        else:
            print(f"⚠️ Skipped rename: {new_zip_path.name} already exists")

    except subprocess.CalledProcessError as e:
        print(f"❌ Extraction failed for {zip_file.name}: {e}")

def main():
    zip_files = sorted([
        f for f in ZIPPED_DIR.iterdir()
        if f.is_file() and f.suffix.lower() == ".zip" and not f.name.startswith("Z")
    ])

    if not zip_files:
        print("✅ No new ZIP files to process.")
        return

    print(f"📁 Found {len(zip_files)} zip(s) to extract...\n")
    for zip_file in zip_files:
        extract_and_process_zip(zip_file)

if __name__ == "__main__":
    main()
'''
**Usage (1-sentence)**
Run this script on a Windows workstation to automatically pull every new Google-Takeout ZIP in `D:\Zipped`, extract it into a standardized `Z###` sub-folder under your Processing directory, prune irrelevant folders, and log/rename the original archive so you always know what has already been ingested.

**Tools / Technologies employed**

* **Python 3.10+** – `pathlib`, `os`, `subprocess`, `shutil`, `re` for cross-platform filesystem manipulation, process execution, and regex parsing.
* **7-Zip CLI (`7z.exe`)** – fast, scriptable extraction of large ZIP archives.
* **Windows-style absolute paths** and WSL-compatible UNC handling (Path objects).
* Lightweight **tracker logs** as plain-text files to mark processed archives.

**Idea summary (what it does & why it matters)**
`start_process.py` forms the first stage of the media-cleanup pipeline: it scans a designated “incoming” folder for raw Google Takeout ZIPs that haven’t yet been prefixed with a `Z###` code, then (1) extracts each archive via 7-Zip into a uniquely named processing sub-tree, (2) dives down to locate the deepest “Photos from YYYY” directory so that only year-structured content is retained, deleting any extraneous folders that clutter the export, (3) creates a tracker file so the pipeline never double-processes the same archive, and finally (4) renames the original ZIP to the canonical `Z###.zip` pattern to signal completion. This automated normalization and bookkeeping step ensures the downstream hashing, deduplication, and metadata-repair stages receive clean, consistently organized inputs, eliminating manual oversight and dramatically reducing the risk of missed or duplicate photo sets.

'''
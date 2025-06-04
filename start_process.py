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
    # Extract suffix number
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

        # Clean up non-year folders
        photos_root = find_deepest_photos_root(extract_dir)
        if photos_root:
            print(f"📂 Identified 'Photos from YYYY' root at: {photos_root}")
            delete_non_year_folders(photos_root)
        else:
            print("⚠️ No 'Photos from YYYY' folders found. Skipping cleanup.")

        # Log extraction
        TRACKER_DIR.mkdir(parents=True, exist_ok=True)
        tracker_file = TRACKER_DIR / (zip_file.stem + ".txt")
        tracker_file.write_text(f"Extracted: {zip_file.name}\n", encoding="utf-8")
        print(f"📄 Created tracker log at: {tracker_file}")

        # Rename to Z###
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

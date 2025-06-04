import os
import shutil
from pathlib import Path
from tqdm import tqdm
from PIL import Image
import imagehash
import piexif
import cv2

from pillow_heif import register_heif_opener
register_heif_opener()

# Configurations
UNMATCHED_ROOT = Path(r"C:\Users\vagrawal\OneDrive - Altair Engineering, Inc\Documents\Personal\Pictures\Processing\__UNMATCHED_MEDIA__")
PROCESSING_ROOT = Path(r"C:\Users\vagrawal\OneDrive - Altair Engineering, Inc\Documents\Personal\Pictures\Processing")
HASH_THRESHOLD = 15  # Adjust as needed
DRY_RUN = True  # Set to False to apply changes
processed_files = []
duplicates_found = []

# Supported file types
IMAGE_EXTS = {'.jpg', '.jpeg', '.png', '.heic'}
VIDEO_EXTS = {'.mp4', '.mov', '.avi', '.mkv'}


def compute_hash(path):
    ext = path.suffix.lower()
    try:
        if ext in IMAGE_EXTS:
            with Image.open(path) as img:
                return imagehash.phash(img)
        elif ext in VIDEO_EXTS:
            cap = cv2.VideoCapture(str(path))
            success, frame = cap.read()
            if success:
                frame = cv2.cvtColor(frame, cv2.COLOR_BGR2GRAY)
                img = Image.fromarray(frame)
                hash_val = imagehash.phash(img)
                cap.release()
                return hash_val
    except Exception as e:
        print(f"Hash error for {path}: {e}")
    return None


def get_year_from_path(path):
    for parent in path.parents:
        if parent.name.lower().startswith("photos from "):
            return parent.name
    return None


def get_all_year_folders(year_name):
    year_folders = []
    for zfolder in PROCESSING_ROOT.iterdir():
        if zfolder.is_dir() and zfolder.name.lower().startswith("z") and not zfolder.name.startswith("_"):
            year_path = zfolder / "Takeout" / "Google Photos" / year_name
            if year_path.exists():
                year_folders.append(year_path)
    return year_folders


def get_timestamp_from_exif(image_path):
    try:
        exif = piexif.load(image_path)
        ts = exif["Exif"].get(piexif.ExifIFD.DateTimeOriginal) or exif["0th"].get(piexif.ImageIFD.DateTime)
        if ts:
            return ts.decode("utf-8")
    except Exception:
        return None
    return None


def set_timestamp_to_exif(image_path, timestamp):
    try:
        exif = piexif.load(image_path)
        encoded_ts = timestamp.encode("utf-8")
        exif["Exif"][piexif.ExifIFD.DateTimeOriginal] = encoded_ts
        exif["Exif"][piexif.ExifIFD.DateTimeDigitized] = encoded_ts
        exif["0th"][piexif.ImageIFD.DateTime] = encoded_ts
        piexif.insert(piexif.dump(exif), image_path)
        return True
    except Exception as e:
        print(f"Failed to set timestamp for {image_path}: {e}")
        return False


def match_unmatched_images():
    unmatched_files = [f for f in UNMATCHED_ROOT.rglob("*") if f.is_file() and not any(p.name.startswith("_") and not p.name.startswith("__") for p in f.parents)]
    print(f"\nFound {len(unmatched_files)} unmatched files to process.\n")

    for ufile in tqdm(unmatched_files, desc="Processing unmatched files"):
        print(f"\n🔍 Processing unmatched file: {ufile}")
        uhash = compute_hash(ufile)
        if uhash is None:
            continue

        year_name = get_year_from_path(ufile)
        if not year_name:
            print(f"❌ No year folder found for {ufile}")
            continue

        candidate_folders = get_all_year_folders(year_name)
        print(f"🔎 Searching in {len(candidate_folders)} candidate folders for year: {year_name}")

        best_match = None
        best_dist = 100
        near_matches = []

        for folder in candidate_folders:
            for cfile in folder.rglob("*"):
                if cfile.is_file() and cfile.suffix.lower() in IMAGE_EXTS | VIDEO_EXTS and cfile != ufile and not any(p.name.startswith("_") for p in cfile.parents):
                    chash = compute_hash(cfile)
                    if chash is None:
                        continue
                    dist = uhash - chash
                    near_matches.append((cfile, dist))
                    if dist < best_dist:
                        best_match = cfile
                        best_dist = dist

        # Sort and log near matches
        near_matches.sort(key=lambda x: x[1])
        print(f"🔍 Near matches for {ufile.name}:")
        for nm, dist in near_matches[:5]:
            print(f"  • {nm} (distance: {dist})")

        if best_match:
            if best_dist == 0:
                # Exact duplicate: delete the unmatched file
                if DRY_RUN:
                    print(f"🗑️ Would delete {ufile} (exact duplicate of {best_match})")
                else:
                    try:
                        os.remove(ufile)
                        print(f"🗑️ Deleted exact duplicate: {ufile}")
                    except Exception as e:
                        print(f"Error deleting {ufile}: {e}")
                duplicates_found.append({"unmatched": str(ufile), "match": str(best_match)})
            elif best_dist <= HASH_THRESHOLD:
                # Near match: move and update timestamp
                timestamp = get_timestamp_from_exif(str(best_match)) if best_match.suffix.lower() in IMAGE_EXTS else None
                new_name = f"{ufile.stem}_sim{ufile.suffix}"
                dest_path = best_match.parent / new_name

                processed_files.append({
                    "unmatched": str(ufile),
                    "match": str(best_match),
                    "dest": str(dest_path),
                    "timestamp": timestamp,
                    "distance": best_dist
                })

                if not DRY_RUN:
                    try:
                        shutil.move(str(ufile), str(dest_path))
                        if timestamp and dest_path.suffix.lower() in IMAGE_EXTS:
                            set_timestamp_to_exif(str(dest_path), timestamp)
                        print(f"✅ Moved and timestamp set: {dest_path}")
                    except Exception as e:
                        print(f"Error processing {ufile}: {e}")

            else:
                print(f"❌ No good match for {ufile} (best distance {best_dist})")

    print_summary()


def print_summary():
    if DRY_RUN:
        print(f"\n📋 Dry-run summary:")
    else:
        print(f"\n✅ Update summary:")

    print(f"Total files that would be updated: {len(processed_files)}")
    for entry in processed_files:
        print(f"- {entry['unmatched']} -> {entry['dest']} (match: {entry['match']}, distance: {entry['distance']}, timestamp: {entry['timestamp']})")

    print(f"\n🗑️ Exact duplicates found: {len(duplicates_found)}")
    for entry in duplicates_found:
        print(f"- {entry['unmatched']} (duplicate of {entry['match']})")


if __name__ == "__main__":
    match_unmatched_images()

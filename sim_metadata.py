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

# Load perceptual hashes from manifest
import csv
MANIFEST_FILE = Path("/mnt/c/Users/vagrawal/OneDrive - Altair Engineering, Inc/Documents/Personal/Code/metadata_manifest.csv")
phash_map = {}
global_candidates = []
# Basic settings and configuration options with updated paths for WSL
UNMATCHED_ROOT = Path("/mnt/c/Users/vagrawal/OneDrive - Altair Engineering, Inc/Documents/Personal/Pictures/Processing/__UNMATCHED_MEDIA__")
PROCESSING_ROOT = Path("/mnt/c/Users/vagrawal/OneDrive - Altair Engineering, Inc/Documents/Personal/Pictures/Processing")
HASH_THRESHOLD = 10  # Adjust this value to tweak similarity matching
DRY_RUN = False 
SEARCH_WITHIN_YEAR_ONLY = False  # Set to True to restrict search to same "Photos from XXXX" folder
with MANIFEST_FILE.open(newline='', encoding='utf-8') as mf:
    for row in csv.DictReader(mf):
        ph = row.get('phash64', '').strip()
        path_str = row.get('media_path', '').strip()
        if ph and path_str:
            try:
                ph_int = int(ph, 16)
                phash_map[path_str] = ph_int

                path_obj = Path(path_str)
                if UNMATCHED_ROOT not in path_obj.parents:
                    global_candidates.append((path_obj, ph_int))
            except ValueError:
                continue

processed_files = []
duplicates_found = []

# File type definitions for images and videos
IMAGE_EXTS = {'.jpg', '.jpeg', '.png', '.heic'}
VIDEO_EXTS = {'.mp4', '.mov', '.avi', '.mkv'}

# New folder for similar matches
SIM_ROOT = PROCESSING_ROOT / "__SIM__"

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
    # Traverse the parent directories to find a folder starting with "photos from "
    for parent in path.parents:
        if parent.name.lower().startswith("photos from "):
            return parent.name
    return None

def get_all_year_folders(year_name):
    # Look for folders matching the pattern within the processing root
    year_folders = []
    for zfolder in PROCESSING_ROOT.iterdir():
        if zfolder.is_dir() and zfolder.name.lower().startswith("z") and not zfolder.name.startswith("_"):
            year_path = zfolder / "Takeout" / "Google Photos" / year_name
            if year_path.exists():
                year_folders.append(year_path)
    return year_folders

def get_timestamp_from_exif(image_path):
    # Try to extract the original timestamp from the image's EXIF data
    try:
        exif = piexif.load(image_path)
        ts = exif["Exif"].get(piexif.ExifIFD.DateTimeOriginal) or exif["0th"].get(piexif.ImageIFD.DateTime)
        if ts:
            return ts.decode("utf-8")
    except Exception:
        return None
    return None

def set_timestamp_to_exif(image_path, timestamp):
    # Update the image's EXIF data with a new timestamp
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
    print("🔁 Search mode:", "Year-restricted" if SEARCH_WITHIN_YEAR_ONLY else "Global")

    # Find all unmatched files in the specified directory, excluding any in masked folders
    unmatched_files = [f for f in UNMATCHED_ROOT.rglob("*") if f.is_file() and not any(p.name.startswith("_") and not p.name.startswith("__") for p in f.parents)]
    print(f"\nFound {len(unmatched_files)} unmatched files to process.\n")

    for ufile in tqdm(unmatched_files, desc="Processing unmatched files"):
        print(f"\n🔍 Processing unmatched file: {ufile}")
        uhash = phash_map.get(str(ufile))
        if uhash is None:
            continue

        if SEARCH_WITHIN_YEAR_ONLY:
            year_name = get_year_from_path(ufile)
            if not year_name:
                print(f"❌ No year folder found for {ufile}")
                continue

            candidate_folders = get_all_year_folders(year_name)
            print(f"🔎 Searching in {len(candidate_folders)} candidate folders for year: {year_name}")

            candidates = []
            for folder in candidate_folders:
                for cfile in folder.rglob("*"):
                    if (
                        cfile.is_file()
                        and cfile.suffix.lower() in IMAGE_EXTS | VIDEO_EXTS
                        and cfile != ufile
                        and not any(p.name.startswith("_") for p in cfile.parents)
                    ):
                        chash = phash_map.get(str(cfile))
                        if chash is not None:
                            candidates.append((cfile, chash))
        else:
            print(f"🌐 Global search enabled — using all {len(global_candidates)} candidates.")
            candidates = global_candidates

        best_match = None
        best_dist = 100
        near_matches = []

        for cfile, chash in candidates:
            if str(cfile) == str(ufile):
                continue
            dist = (uhash ^ chash).bit_count()
            near_matches.append((cfile, dist))
            if dist < best_dist:
                best_match = cfile
                best_dist = dist

        # Organize and display the closest matches
# Sort by visual similarity (Hamming distance)
        near_matches.sort(key=lambda x: x[1])

        # Extract the year from the unmatched file
        unmatched_year = get_year_from_path(ufile)
        if unmatched_year:
            # Filter to same-year matches
            near_matches = [(nm, d) for nm, d in near_matches if get_year_from_path(nm) == unmatched_year]

        # Display top filtered matches
        print(f"🔍 Near matches for {ufile.name}:")
        if near_matches:
            for nm, dist in near_matches[:5]:
                print(f"  • {nm} (distance: {dist})")
        else:
            print("  • No matches from the same year remain after filtering.")

        # Update best match after year filtering
        if near_matches:
            best_match, best_dist = near_matches[0]
        else:
            best_match, best_dist = None, 999

        if best_match:
            if best_dist == 0:
                # If the file is an exact duplicate, remove the unmatched one
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
                # If the file is a near match, prepare to move it under SIM_ROOT and adjust timestamp
                timestamp = get_timestamp_from_exif(str(best_match)) if best_match.suffix.lower() in IMAGE_EXTS else None
                new_name = f"{ufile.stem}_sim{ufile.suffix}"
                # retain subfolder structure from UNMATCHED_ROOT
                rel = ufile.relative_to(UNMATCHED_ROOT)
                dest_dir = SIM_ROOT / rel.parent
                dest_path = dest_dir / new_name

                processed_files.append({
                    "unmatched": str(ufile),
                    "match": str(best_match),
                    "dest": str(dest_path),
                    "timestamp": timestamp,
                    "distance": best_dist
                })

                if not DRY_RUN:
                    # ensure destination directory exists
                    dest_dir.mkdir(parents=True, exist_ok=True)
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
    # Show a summary report of what was processed or what would be processed
    if DRY_RUN:
        print(f"\n📋 Dry-run summary:")
    else:
        print(f"\n✅ Update summary:")

    print(f"Total files that would be updated: {len(processed_files)}")
    for entry in processed_files:
        print(f"- {entry['unmatched']} -> {entry['dest']} (match: {entry['match']}, distance: {entry['distance']}, timestamp: {entry['timestamp']})")

    print(f"\n🗑️ Exact duplicates found: {len(duplicates_found)}")
    # for entry in duplicates_found:
    #     print(f"- {entry['unmatched']} (duplicate of {entry['match']})")

if __name__ == "__main__":
    match_unmatched_images()


'''
**Usage (1 sentence)**
Run `python sim_metadata.py [edit DRY_RUN flag]` to scan every file stranded in `__UNMATCHED_MEDIA__`, compute perceptual hashes, hunt for the closest match inside the proper *Photos from YYYY* folders, and—if the distance is 0 or below the adjustable threshold—either delete the duplicate or move it next to its twin while propagating the timestamp into its EXIF.

---

### Tools / Technologies employed

| Layer                       | Components                                  | Purpose                                          |
| --------------------------- | ------------------------------------------- | ------------------------------------------------ |
| **Python 3.x std-lib**      | `pathlib`, `shutil`, `os`, `subprocess`     | Cross-platform paths, safe moves, deletions      |
| **Pillow (+ pillow\_heif)** | Image loading for JPEG/PNG/HEIC             | Uniform RGB ingestion across formats             |
| **imagehash (pHash)**       | 64-bit perceptual hashes + Hamming distance | Measures visual similarity between files         |
| **OpenCV**                  | Grabs first frame of each video             | Extends perceptual hashing to video content      |
| **piexif**                  | EXIF read/write                             | Copies original timestamp into new *\_sim* image |
| **tqdm**                    | Progress bars                               | Feedback during large scans                      |
| **Config flags**            | `DRY_RUN`, `HASH_THRESHOLD`                 | Risk-free preview + tunable sensitivity          |

---

### Idea summary (what it does & why it matters)

`sim_metadata.py` is the intelligent reconciliation pass that cleans up whatever the earlier manifest-driven pipeline still couldn’t place:

1. **Deep search arena** – for each unmatched asset it derives its *“Photos from YYYY”* year from the path and limits the hash search to that year, making the process fast and context-aware.
2. **Cross-media hashing** – uses a perceptual hash for images and a grayscale hash on the first video frame, yielding a uniform 0-to-64 Hamming distance metric for both photos and clips.
3. **Exact duplicates (distance 0)** – the unmatched file is simply deleted (or “would be” in dry-run), logging the action.
4. **Near matches (≤ threshold)** – the file is renamed with a `_sim` suffix, moved into the matched photo’s folder, and—if it’s an image—its EXIF timestamp is copied from the canonical file so it slots perfectly into chronological views.
5. **Detailed reporting** – both exact duplicates and near-match moves are summarized at the end (or previewed in dry-run), letting the user validate actions before committing.

By combining perceptual hashing, year-scoped searches, and automatic timestamp propagation, this script salvages burst shots, slight edits, or format conversions that escaped the main deduplication manifest, ensuring every surviving file has a logical home and coherent metadata in the archive.
'''
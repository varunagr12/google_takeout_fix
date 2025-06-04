import csv
import os
import json
import shutil
import subprocess
import re
from pathlib import Path
from datetime import datetime, timezone
from tqdm import tqdm
import piexif
from PIL import Image
from pillow_heif import register_heif_opener
import rawpy
import imageio

# Initialize HEIF opener
register_heif_opener()

# Configuration: Set paths and folder names
MANIFEST_PATH = Path(r"C:/Users/vagrawal/OneDrive - Altair Engineering, Inc/Documents/Personal/Code/metadata_manifest.csv")
PROCESSING_ROOT = Path(r"C:/Users/vagrawal/OneDrive - Altair Engineering, Inc/Documents/Personal/Pictures/Processing")
FAILED_DIR_NAME = "__FAILED_FILES__"


def log(msg: str):
    """Log a message to standard output."""
    print(msg)


def move_to_failed(file_path: str, reason: str = None):
    """
    Move the given file and all its related variants into a "__FAILED_FILES__" folder.
    The original folder hierarchy (starting from a directory whose name starts with 'Z')
    is preserved.
    
    Returns a tuple of (moved_paths_str, reason).
    """
    try:
        src = Path(file_path).resolve()
        failed_root = (PROCESSING_ROOT / FAILED_DIR_NAME).resolve()
        # Find the index of the first directory starting with "Z" (case insensitive)
        parts = src.parts
        z_index = next(i for i, p in enumerate(parts) if p.upper().startswith('Z'))
        rel = parts[z_index:]
        # Remove generic folder names from the path
        rel = [rel[0]] + [p for p in rel[1:] if p.lower() not in {'takeout', 'google photos'}]
        target_dir = failed_root.joinpath(*rel[:-1])
        target_dir.mkdir(parents=True, exist_ok=True)

        base = src.stem
        moved = []
        for variant in src.parent.glob(base + '.*'):
            dst = target_dir / variant.name
            if dst.exists():
                stem, suf = dst.stem, dst.suffix
                i = 1
                while (target_dir / f"{stem}_{i}{suf}").exists():
                    i += 1
                dst = target_dir / f"{stem}_{i}{suf}"
            shutil.move(str(variant), str(dst))
            moved.append(str(dst))

        reason = reason or 'Failure during processing'
        return '; '.join(moved), reason
    except Exception as e:
        log(f"Failed to move {file_path} to failed: {e}")
        return None, f"Move error: {e}"


def correct_file_extension(file_path: str):
    """
    Identify the actual file type based on its signature and correct the file extension
    if it is mislabeled (works for JPEG and QuickTime formats).
    
    Returns a tuple of (new_path_str, new_ext).
    """
    try:
        p = Path(file_path)
        with open(p, 'rb') as f:
            sig = f.read(12)
        ext = p.suffix.lower()
        new_path = None
        # Check for JPEG signature
        if sig.startswith(b'\xFF\xD8\xFF') and ext != '.jpg':
            new_path = p.with_suffix('.jpg')
        # Check for QuickTime / MOV signature
        elif sig[4:8] == b'ftyp' and b'qt' in sig and ext != '.mov':
            new_path = p.with_suffix('.mov')
        if new_path:
            new_path = get_safe_conversion_path(new_path)  # ✅ ADD THIS LINE
            os.rename(file_path, str(new_path))
            return str(new_path), new_path.suffix
    except Exception as e:
        log(f"Could not correct extension for {file_path}: {e}")
    return file_path, Path(file_path).suffix


def convert_heic_to_jpg(heic_path: str):
    """Convert a HEIC image into JPEG format."""
    try:
        img = Image.open(heic_path).convert('RGB')
        jpg_path = get_safe_conversion_path(Path(heic_path).with_suffix('.jpg'))
        img.save(jpg_path, 'JPEG')
        log(f"Converted HEIC to JPEG: {jpg_path}")
        return str(jpg_path)
    except Exception as e:
        log(f"HEIC->JPEG conversion failed for {heic_path}: {e}")
        move_to_failed(heic_path, f"HEIC conversion error: {e}")
        return None


def convert_dng_to_jpg(dng_path: str):
    """Convert a DNG image into JPEG format."""
    try:
        jpg_path = get_safe_conversion_path(Path(dng_path).with_suffix('.jpg'))

        with rawpy.imread(dng_path) as raw:
            rgb = raw.postprocess()
        imageio.imwrite(str(jpg_path), rgb)
        log(f"Converted DNG to JPEG: {jpg_path}")
        return str(jpg_path)
    except Exception as e:
        log(f"DNG->JPEG conversion failed for {dng_path}: {e}")
        move_to_failed(dng_path, f"DNG conversion error: {e}")
        return None


def update_timestamp(file_path: str, formatted_time: str):
    """
    Update the timestamp metadata in the media file.
    For JPEG files, use the piexif library; for other files, use exiftool.
    
    Returns a tuple of (success, message).
    """
    try:
        ext = Path(file_path).suffix.lower()
        if ext in ('.jpg', '.jpeg'):
            exif = piexif.load(file_path)
            ts = formatted_time.encode('utf-8')
            exif['Exif'][piexif.ExifIFD.DateTimeOriginal] = ts
            exif['Exif'][piexif.ExifIFD.DateTimeDigitized] = ts
            exif['0th'][piexif.ImageIFD.DateTime] = ts
            piexif.insert(piexif.dump(exif), file_path)
        else:
            cmd = [
                'exiftool',
                f'-DateTimeOriginal={formatted_time}',
                f'-CreateDate={formatted_time}',
                f'-ModifyDate={formatted_time}',
                '-overwrite_original',
                file_path
            ]
            res = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
            if res.returncode != 0:
                raise RuntimeError(res.stderr.strip())
        return True, 'Timestamp updated'
    except Exception as e:
        moved, reason = move_to_failed(file_path, f"Timestamp update failed: {e}")
        return False, reason


def rename_json_file(old_json: Path, old_media: str, new_media: str):
    """Rename the JSON file so that its name matches the new media filename."""
    try:
        new_name = old_json.name.replace(old_media, new_media)
        new_path = old_json.with_name(new_name)
        new_path = get_safe_conversion_path(new_path)
        os.rename(str(old_json), str(new_path))
        return new_name, str(new_path), None, None
    except Exception as e:
        moved, reason = move_to_failed(str(old_json), f"JSON rename failed: {e}")
        return old_json.name, str(old_json), moved, reason


def get_safe_conversion_path(original_path: Path) -> Path:
    """
    Generate a new filename for a conversion by appending a '_conv' suffix.
    If a file with that name already exists, add extra counters (_1, _2, etc.) to prevent overwriting.
    """
    stem = original_path.stem
    suffix = original_path.suffix
    parent = original_path.parent

    # Append '_conv' to the file stem
    conv_stem = f"{stem}_conv"
    candidate = parent / f"{conv_stem}{suffix}"

    i = 1
    while candidate.exists():
        candidate = parent / f"{conv_stem}_{i}{suffix}"
        i += 1

    if i > 1 or '_conv' in conv_stem:
        log(f"Generated safe path: {candidate}")
    return candidate


def main():
    # Read the manifest CSV file
    rows = []
    with open(MANIFEST_PATH, 'r', newline='', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for r in reader:
            rows.append(r)

    for row in tqdm(rows, desc='Processing manifest entries'):
        # Set an initial empty action_taken field if not present
        if not row.get('action_taken'):
            row['action_taken'] = ''

        # Skip rows flagged for deletion or that are marked as deleted
        if row.get('delete_flag', '').strip().lower() == 'true' or row.get('status', '').strip().lower() == 'deleted':
            row['notes'] = 'Skipped (delete_flag or status=deleted)'
            continue
        # Only process rows that are marked as matched
        if row.get('row_type', '').strip().lower() != 'matched':
            row['notes'] = "Skipped (row_type != 'matched')"
            continue

        media_path = Path(row['media_path'])
        json_path = Path(row['json_path'])

        # Check if the media file exists; if not, move it to the failed folder and record the issue
        if not media_path.exists():
            moved, reason = move_to_failed(str(media_path), 'Media missing')
            row['action_taken'] = f"Moved to failed: {moved}" if moved else ''
            row['notes'] = reason
            continue
        # Check if the JSON file exists; if not, move it to the failed folder and record the issue
        if not json_path.exists():
            moved, reason = move_to_failed(str(json_path), 'JSON missing')
            row['action_taken'] = f"Moved to failed: {moved}" if moved else ''
            row['notes'] = reason
            continue

        # Fix the file extension if it's mislabeled
        orig_name = media_path.name
        corrected_str, new_ext = correct_file_extension(str(media_path))
        if corrected_str != str(media_path):
            new_name = Path(corrected_str).name
            row['media_path'] = corrected_str
            row['corrected_path'] = corrected_str
            row['new_ext'] = new_ext
            row['action_taken'] = f"Renamed {orig_name} to {new_name}"
            # Rename the corresponding JSON file to match the new media filename
            new_json_fn, new_json_p, act, rsn = rename_json_file(Path(row['json_path']), orig_name, new_name)
            row['json_filename'] = new_json_fn
            row['json_path'] = new_json_p
            if act:
                row['action_taken'] += f"; {act}"
            if rsn:
                row['notes'] = rsn
            media_path = Path(corrected_str)
            json_path = Path(new_json_p)

        # Handle file conversion based on the file type
        ext = media_path.suffix.lower()
        if ext == '.heic':
            old_nm = media_path.name
            jpg = convert_heic_to_jpg(str(media_path))
            if jpg:
                os.remove(str(media_path))
                log(f"Deleted original HEIC: {media_path}")
                media_path = Path(jpg)
                row['media_path'] = jpg
                row['corrected_path'] = jpg
                row['new_ext'] = media_path.suffix
                row['action_taken'] += f"; Converted HEIC to JPEG"
                # Rename the JSON file after converting the media
                new_json_fn, new_json_p, act, rsn = rename_json_file(json_path, old_nm, media_path.name)
                row['json_filename'] = new_json_fn
                row['json_path'] = new_json_p
                if act:
                    row['action_taken'] += f"; {act}"
                if rsn:
                    row['notes'] = rsn
        elif ext == '.dng':
            old_nm = media_path.name
            jpg = convert_dng_to_jpg(str(media_path))
            if jpg:
                os.remove(str(media_path))
                log(f"Deleted original DNG: {media_path}")
                media_path = Path(jpg)
                row['media_path'] = jpg
                row['corrected_path'] = jpg
                row['new_ext'] = media_path.suffix
                row['action_taken'] += f"; Converted DNG to JPEG"
                # Rename the JSON file after converting the media
                new_json_fn, new_json_p, act, rsn = rename_json_file(json_path, old_nm, media_path.name)
                row['json_filename'] = new_json_fn
                row['json_path'] = new_json_p
                if act:
                    row['action_taken'] += f"; {act}"
                if rsn:
                    row['notes'] = rsn

        # Update the timestamp metadata in the media file
        success, msg = update_timestamp(str(media_path), row['formatted_time'])
        if success:
            row['action_taken'] += f"; {msg}" if row['action_taken'] else msg
        else:
            moved, reason = move_to_failed(str(media_path), msg)
            row['action_taken'] = f"Moved to failed: {moved}" if moved else ''
            row['notes'] = reason

    # Write the updated data back to the manifest CSV file
    fieldnames = [
        'row_type', 'json_filename', 'json_path', 'original_media', 'media_path', 'corrected_path',
        'timestamp_unix', 'formatted_time', 'new_ext', 'action_taken', 'notes',
        'dedup_group_id', 'delete_flag', 'dedup_reason', 'visual_review_path', 'duplicate_of', 'deletion_status'
    ]
    with open(MANIFEST_PATH, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)

    print("\n✅ Manifest updated, conversions applied, and CSV saved.")


if __name__ == '__main__':
    main()

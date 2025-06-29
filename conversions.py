#!/usr/bin/env python3
# exiftool -progress -r -overwrite_original -ext jpg -ext jpeg -ext mp4 -ext mov -csv=times.csv     /mnt/c/Users/vagrawal/OneDrive\ -\ Altair\ Engineering,\ Inc/Documents/Personal/Pictures/Processing/
'''
Results of test:
MTS works
3pg works
jfif worked
jpg worked
avi worked
tif worked
nef worked
thm sort of worked? it is very small but has right metadata
heic worked
mov worked
dng works
webp worked
jpeg worked
tiff worked
mpg worked
mp4 worked
png worked
gif worked
Everything works!!!
'''

import argparse
import csv
import os
import shutil
import subprocess
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor
from tqdm import tqdm

# Third-party imports
import piexif
from PIL import Image, ImageFile
import rawpy
import imageio
from pillow_heif import register_heif_opener
from typing import Optional   

# Ensure PIL can load truncated JPEGs
ImageFile.LOAD_TRUNCATED_IMAGES = True
# Initialize HEIC opener
register_heif_opener()

# Simple logger for error messages
def log(msg):
    print(msg)

# Configuration
MANIFEST_PATH = Path(r"/mnt/c/Users/vagrawal/OneDrive - Altair Engineering, Inc/Documents/Personal/Code/metadata_manifest.csv")
PROCESSING_ROOT = Path(r"/mnt/c/Users/vagrawal/OneDrive - Altair Engineering, Inc/Documents/Personal/Pictures/Processing")
FAILED_DIR_NAME = "__FAILED_FILES__"
VIDEO_TARGET_EXTS = {".avi", ".mpg", ".mpeg", ".mts", ".3gp"}

# Extensions to sample in --test mode
SAMPLE_EXTS = [
    # photos
    '.jpg', '.gif', '.png', '.heic', '.jpeg', '.tif', '.webp', '.jfif', '.tiff',
    # videos
    '.3gp', '.avi', '.mpg', '.mov', '.mp4', '.mts',
    # others
    '.thm', '.dng', '.nef'
]

# ----------------------------------------------------------------------------
# Helper functions
# ----------------------------------------------------------------------------

def append_action(row: dict, text: str):
    """
    Safely append to row['action_taken']:
      - if empty       -> "text"
      - otherwise      -> existing + "; text"
    """
    prev = row.get('action_taken', '').strip()
    if prev:
        row['action_taken'] = f"{prev}; {text}"
    else:
        row['action_taken'] = text

from pathlib import Path
from typing import Optional   # make sure this import is present once at the top



def get_safe_conversion_path(original_path: Path,
                             tag: str = None,
                             allow_numbering: bool = False) -> Path:
    stem   = original_path.stem
    suffix = original_path.suffix
    parent = original_path.parent

    if tag and not allow_numbering:
        # ✔ embed the tag before '_conv'
        return parent / f"{stem}_{tag}_conv{suffix}"

    base = f"{stem}_conv{suffix}"
    candidate = parent / base
    if not tag:                       # tag-less: keep counting until free
        i = 1
        while candidate.exists():
            candidate = parent / f"{stem}_conv_{i}{suffix}"
            i += 1
    return candidate

def rename_json_file(old_json: Path, old_media_name: str, new_media_name: str):
    """
    Rename sidecar JSON to match a media filename change.
    Returns (new_json_filename, new_json_path, moved_info, reason).
    """
    try:
        new_name = old_json.name.replace(old_media_name, new_media_name)
        new_path = old_json.with_name(new_name)
        # ensure no overwrite
        new_path = get_safe_conversion_path(new_path)
        old_json.rename(new_path)
        return new_name, str(new_path), None, None
    except Exception as e:
        moved, reason = move_to_failed(str(old_json), f"JSON rename failed: {e}")
        return old_json.name, str(old_json), moved, reason


def move_to_failed(file_path: str, reason: str = None):
    try:
        src = Path(file_path).resolve()
        failed_root = (PROCESSING_ROOT / FAILED_DIR_NAME).resolve()
        parts = src.parts
        z_index = next((i for i, p in enumerate(parts) if p.upper().startswith('Z')), 0)
        rel = parts[z_index:]
        target_dir = failed_root.joinpath(*rel[:-1])
        target_dir.mkdir(parents=True, exist_ok=True)
        moved = []
        for variant in src.parent.glob(src.stem + '.*'):
            dst = target_dir / variant.name
            if dst.exists():
                base, suf = dst.stem, dst.suffix
                j = 1
                while (target_dir / f"{base}_{j}{suf}").exists():
                    j += 1
                dst = target_dir / f"{base}_{j}{suf}"
            shutil.move(str(variant), str(dst))
            moved.append(str(dst))
        return '; '.join(moved), reason or 'Failed during processing'
    except Exception as e:
        return None, f"Move error: {e}"


def correct_file_extension(file_path: str):
    """
    Identify the real file type by magic bytes, and if mislabeled:
      • rename to the right suffix, 
      • embed the old suffix as a tag so we never collide.
    """
    p = Path(file_path)
    try:
        sig = p.open('rb').read(12)
        old_ext = p.suffix.lower()
        new_ext = None

        if sig.startswith(b'\xFF\xD8\xFF') and old_ext != '.jpg':
            new_ext = '.jpg'
        elif sig[4:8] == b'ftyp' and b'qt' in sig and old_ext != '.mov':
            new_ext = '.mov'

        if new_ext:
            # tag with the old extension (no dot)
            safe = get_safe_conversion_path(p.with_suffix(new_ext), tag=old_ext.lstrip('.'))
            p.rename(safe)
            return str(safe), safe.suffix

    except Exception:
        pass

    return file_path, p.suffix


def convert_png_to_jpg(png_path: str) -> str:
    orig = Path(png_path)
    # print(f"[PNG->JPG] Starting conversion: {orig}")
    try:
        im = Image.open(orig).convert('RGBA')
        # print(f"[PNG->JPG] Opened image: {orig.name}, mode: {im.mode}, size: {im.size}")
        bg = Image.new('RGB', im.size, (255, 255, 255))
        bg.paste(im, mask=im.split()[3])
        jpg = orig.with_suffix('.jpg')
        safe = get_safe_conversion_path(jpg, tag='png')
        # print(f"[PNG->JPG] Saving as: {safe}")
        bg.save(safe, 'JPEG', quality=95)
        orig.unlink()
        # print(f"[PNG->JPG] Successfully converted and deleted original: {orig}")
        return str(safe)
    except Exception as e:
        # print(f"[PNG->JPG][ERROR] {e}")
        move_to_failed(png_path, f"PNG->JPEG error: {e}")
        return png_path

def convert_heic_to_jpg(heic_path: str) -> str:
    orig = Path(heic_path)
    # print(f"[HEIC->JPG] Starting conversion: {orig}")
    try:
        img = Image.open(orig).convert('RGB')
        # print(f"[HEIC->JPG] Opened image: {orig.name}, mode: {img.mode}, size: {img.size}")
        jpg = orig.with_suffix('.jpg')
        safe = get_safe_conversion_path(jpg, tag='heic')
        # print(f"[HEIC->JPG] Saving as: {safe}")
        img.save(safe, 'JPEG')
        orig.unlink()
        # print(f"[HEIC->JPG] Successfully converted and deleted original: {orig}")
        return str(safe)
    except Exception as e:
        # print(f"[HEIC->JPG][ERROR] {e}")
        move_to_failed(heic_path, f"HEIC->JPEG error: {e}")
        return heic_path

def convert_dng_to_jpg(dng_path: str) -> str:
    orig = Path(dng_path)
    if orig.suffix.lower() != '.dng':
        # print(f"[DNG->JPG] Skipping non-DNG file: {orig}")
        return str(orig)
    # print(f"[DNG->JPG] Starting conversion: {orig}")
    try:
        # build the target name: IMG_0499_dng_conv.jpg
        final_path = get_safe_conversion_path(orig.with_suffix('.jpg'), tag='dng')
        # print(f"[DNG->JPG] Final save path: {final_path}")

        import uuid, tempfile
        # make a temp file next to the original
        tmp_fd, tmp_name = tempfile.mkstemp(
            suffix='.jpg',
            prefix=f"tmp_{uuid.uuid4().hex}_",
            dir=str(orig.parent)
        )
        os.close(tmp_fd)
        # print(f"[DNG->JPG] Temporary file created: {tmp_name}")

        # *** THE ONLY CHANGE BELOW: pass str(orig) ***
        with rawpy.imread(str(orig)) as raw:
            rgb = raw.postprocess()
        # print(f"[DNG->JPG] DNG image read and postprocessed")

        Image.fromarray(rgb).save(tmp_name, 'JPEG', quality=95)
        # print(f"[DNG->JPG] Saved temporary JPEG")

        # move into place (overwrites if needed)
        os.replace(tmp_name, str(final_path))
        # print(f"[DNG->JPG] Renamed temp file to: {final_path}")

        orig.unlink()
        # print(f"[DNG->JPG] Deleted original DNG: {orig}")

        return str(final_path)

    except Exception as e:
        # print(f"[DNG->JPG][ERROR] {e}")
        move_to_failed(str(dng_path), f"DNG->JPEG error: {e}")
        return dng_path

def convert_tif_to_jpg(input_path: str) -> str:
    orig = Path(input_path)
    if orig.suffix.lower() not in ('.tiff', '.tif', '.gif'):
        # print(f"[TIFF/GIF->JPG] Skipping non-TIFF/GIF file: {orig}")
        return str(orig)
    # print(f"[TIFF/GIF->JPG] Starting conversion: {orig}")
    try:
        im = Image.open(orig).convert('RGB')
        # print(f"[TIFF/GIF->JPG] Opened image: {orig.name}, mode: {im.mode}, size: {im.size}")
        jpg = orig.with_suffix('.jpg')
        safe = get_safe_conversion_path(jpg, tag=orig.suffix.lstrip('.'))
        # print(f"[TIFF/GIF->JPG] Saving as: {safe}")
        im.save(safe, 'JPEG', quality=95)
        orig.unlink()
        # print(f"[TIFF/GIF->JPG] Successfully converted and deleted original: {orig}")
        return str(safe)
    except Exception as e:
        # print(f"[TIFF/GIF->JPG][ERROR] {e}")
        move_to_failed(input_path, f"TIFF/GIF->JPEG error: {e}")
        return input_path

def convert_to_mov(input_path: Path, output_path: Path, formatted_time: str = None):
    cmd = [
        "ffmpeg", "-y", "-loglevel", "error",
        # GPU‐accelerated decode & encode
        "-hwaccel", "cuda", "-hwaccel_output_format", "cuda",
        "-i", str(input_path),
        "-c:v", "h264_nvenc",   # replace x264 CPU encode
        "-preset", "p1",        # p1=fastest; adjust for quality/speed
        "-c:a", "aac", "-b:a", "192k",
        "-movflags", "+faststart",
    ]
    # (optional) carry over timestamp metadata here as before...
    cmd.append(str(output_path))
    return subprocess.run(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL).returncode == 0


def handle_video_conversion(media_path: Path, json_path: Path, row: dict):
    old_ext = media_path.suffix.lower().lstrip('.')
    if f".{old_ext}" not in VIDEO_TARGET_EXTS:
        return media_path, json_path

    old_name = media_path.name
    # embed old_ext so .avi and .mpg don’t collide
    mov = get_safe_conversion_path(media_path.with_suffix('.mp4'), tag=old_ext)
    if not convert_to_mov(media_path, mov, row['formatted_time']):
        moved, reason = move_to_failed(str(media_path), 'Video conversion failed')
        row['notes'] = reason
        return media_path, json_path

    media_path.unlink()
    append_action(row, f"Converted .{old_ext} -> .mp4")
    row.update(media_path=str(mov), corrected_path=str(mov), new_ext='.mp4')

    # rename JSON
    new_json_name = json_path.name.replace(old_name, mov.name)
    candidate = json_path.with_name(new_json_name)
    safe_json = get_safe_conversion_path(candidate, tag=old_ext)
    try:
        json_path.rename(safe_json)
        row['json_path']     = str(safe_json)
        row['json_filename'] = safe_json.name
    except:
        move_to_failed(str(json_path), 'JSON rename failed')

    return mov, Path(row['json_path'])


def write_manifest(rows, path=MANIFEST_PATH):
    import csv
    # 1) read original header order
    with path.open('r', newline='', encoding='utf-8') as f:
        orig = csv.DictReader(f).fieldnames or []
    # 2) append any new keys (preserves orig order)
    fieldnames = orig.copy()
    for r in rows:
        for k in r:
            if k not in fieldnames:
                fieldnames.append(k)
    # 3) write out with stable ordering
    with path.open('w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for r in tqdm(rows, desc="Writing manifest", unit="row"):
            writer.writerow(r)

# ----------------------------------------------------------------------------
# Pipeline steps
# ----------------------------------------------------------------------------

def convert_media(row: dict) -> dict:
    path = Path(row['media_path'])
    ext = path.suffix.lower()
    row.setdefault('action_taken', '')
    # extension-correction
    if ext in ('.jpg', '.jpeg', '.mp4', '.mov'):
        # print(f"[convert_media] Skipping {path.name} (already correct extension)")
        return row
    corrected, new_ext = correct_file_extension(str(path))
    if corrected != str(path):
        old = path.name
        path = Path(corrected)
        row.update(media_path=str(path), corrected_path=str(path), new_ext=new_ext)
        append_action(row, f"Renamed {old} -> {path.name}")
    # image-specific conversions
    if ext == '.png':  new = convert_png_to_jpg(str(path))
    elif ext == '.heic': new = convert_heic_to_jpg(str(path))
    elif ext == '.dng': new = convert_dng_to_jpg(str(path))
    elif ext in ('.tif', '.tiff', '.gif'): new = convert_tif_to_jpg(str(path))
    else: new = str(path)
    # if we got a new file, update row and rename JSON
    if new != str(path):
        new_suffix = Path(new).suffix
        append_action(row, f"Converted {ext} -> {new_suffix}")
        old_name = path.name
        row['media_path'] = new
        # rename JSON sidecar
        old_json = Path(row['json_path'])
        new_media_name = Path(new).name
        new_json_fn, new_json_p, moved, reason = rename_json_file(old_json, old_name, new_media_name)
        row['json_filename'] = new_json_fn
        row['json_path'] = new_json_p
        if moved:
            append_action(row, f"JSON moved -> {Path(new_json_p).name}")
        if reason:
            row['notes'] = reason
    return row


def convert_videos(row: dict) -> dict:
    path = Path(row['media_path'])
    jsonp = Path(row['json_path'])
    new_path, new_json = handle_video_conversion(path, jsonp, row)
    row['media_path'] = str(new_path)
    row['json_path'] = str(new_json)
    return row


# ----------------------------------------------------------------------------
# Orchestration
# ----------------------------------------------------------------------------

from concurrent.futures import ProcessPoolExecutor

def run_in_parallel(fn, rows, workers, desc):
    with ProcessPoolExecutor(max_workers=workers) as ex:
        return list(tqdm(ex.map(fn, rows), total=len(rows), desc=desc))


def main():
    p = argparse.ArgumentParser(description="Parallel media-processing pipeline")
    p.add_argument('--workers', type=int, default=8, help='Number of parallel workers')
    p.add_argument('--test', action='store_true', help='Run one-sample-per-extension test mode')
    p.add_argument('--skip-media', action='store_true', help='Skip image conversions')
    p.add_argument('--skip-video', action='store_true', help='Skip video conversions')
    args = p.parse_args()

    # Test-mode setup
    if args.test:
        test_root = PROCESSING_ROOT / "__test_files__"
        if test_root.exists():
            shutil.rmtree(test_root)
        test_root.mkdir(parents=True)
        globals()['PROCESSING_ROOT'] = test_root
        print(f"🔍 Test mode: using {test_root}")

    # Load manifest
    with MANIFEST_PATH.open('r', newline='', encoding='utf-8') as f:
        rows = list(csv.DictReader(f))

    # Sample one-per-extension if test-mode
    if args.test:
        sampled, seen = [], set()
        for row in tqdm(rows, desc="Sampling test files", unit="file"):
             ext = Path(row['media_path']).suffix.lower()
             if ext in SAMPLE_EXTS and ext not in seen:
                 seen.add(ext)
                 orig_m, orig_j = Path(row['media_path']), Path(row['json_path'])
                 dst_m = PROCESSING_ROOT / orig_m.name
                 dst_j = PROCESSING_ROOT / orig_j.name
                 if not orig_m.exists() or not orig_j.exists():
                     continue
                 shutil.copy2(orig_m, dst_m)
                 shutil.copy2(orig_j, dst_j)
                 row['media_path'], row['json_path'] = str(dst_m), str(dst_j)
                 sampled.append(row)
                 if len(seen) == len(SAMPLE_EXTS): break
        rows = sampled
        print(f"🔍 Test mode: selected {len(rows)} samples.")

    # Step 1: media
    if not args.skip_media:
        rows = run_in_parallel(convert_media, rows, args.workers, 'Converting media')

    # Step 2: videos
    if not args.skip_video:
        rows = run_in_parallel(convert_videos, rows, args.workers, 'Converting videos')

    # Write updated manifest
    write_manifest(rows)

    print("\n✅ Stage complete!")

def run():
    main()

if __name__ == '__main__':
    import sys
    if '--profile' in sys.argv:
        sys.argv.remove('--profile')
        import cProfile
        # run the entire pipeline under cProfile (single worker, test mode, etc.)
        cProfile.run('run()', 'profile.prof')
        print('▶ Profile written to profile.prof')
    else:
        run()

'''
**Usage (1 sentence)**
Run `python metadata.py [--workers N] [--test] [--skip-media] [--skip-video] [--skip-timestamp]` to launch the **core media-normalisation pipeline** that batch-converts oddball images/videos to mainstream formats, repairs file extensions, copies timestamps into EXIF, and writes every action back into `metadata_manifest.csv`, quarantining any failures along the way.

---

### Tools / Technologies employed

| Layer                       | Components                                                                 | Purpose                                                                                      |
| --------------------------- | -------------------------------------------------------------------------- | -------------------------------------------------------------------------------------------- |
| **Python 3.x std-lib**      | `argparse`, `csv`, `pathlib`, `shutil`, `subprocess`, `concurrent.futures` | CLI flags, manifest I/O, safe renames/moves, FFmpeg + ExifTool calls, parallel orchestration |
| **Pillow (+ pillow\_heif)** | RGB decoding (JPEG, PNG, HEIC), EXIF-aware orientation                     | Uniform image loading and format conversion                                                  |
| **piexif**                  | Low-level EXIF read/write                                                  | Injects `DateTimeOriginal` into JPEG headers                                                 |
| **rawpy + imageio**         | DNG/RAW -> RGB pipeline                                                     | High-fidelity conversion of camera RAWs to JPEG                                              |
| **FFmpeg (CLI)**            | Lossless or H.264/AAC remux of legacy videos -> `.mov`                      | Modernises AVI/MPG/MTS while preserving quality                                              |
| **ExifTool (CLI)**          | Timestamp writing for non-JPEG formats                                     | Consistent metadata across all media types                                                   |
| **ThreadPoolExecutor**      | User-tunable worker pool                                                   | Parallelism that saturates I/O and CPU cores                                                 |
| **tqdm**                    | Progress bars for each stage                                               | Real-time pipeline feedback                                                                  |
| **Robust failure-handling** | `__FAILED_FILES__` quarantine + detailed notes                             | Keeps bad files out of the happy path without data loss                                      |

---

### Idea summary (what it does & why it matters)

`metadata.py` is the linchpin that transforms a messy, heterogeneous Google-Takeout dump into a **clean, metadata-rich, archival-grade library**:

1. **File-extension sanity check** – detects mismatches by inspecting magic bytes, renames `.png` masquerading as JPEGs, MOVs mis-labelled as MP4s, etc.
2. **Image conversions** – PNGs gain white-matte JPEGs, HEICs become JPEGs for universal viewing, and DNG/RAW files are demosaiced to high-quality RGB.
3. **Video modernisation** – legacy AVI/MPG/MTS containers are remuxed (or re-encoded when needed) to fast-start `.mov`, with sidecar JSONs renamed in lock-step.
4. **EXIF timestamp injection** – pulls the authoritative Google timestamp from the manifest and writes it into every photo/video, ensuring chronological integrity in any viewer.
5. **Parallel, test-friendly architecture** – configurable worker count, optional one-sample-per-extension mode, and granular `--skip-*` flags let you iterate quickly and safely.
6. **Bullet-proof logging & recovery** – every action is appended to `action_taken`, any hiccup moves the offending files (and all variants) to `__FAILED_FILES__`, and detailed notes capture the reason.

By automating all these tedious, error-prone chores in a single multithreaded run, the script elevates the dataset from *dump* to *curated archive*, paving the way for flawless deduplication, sharing, and long-term preservation—exactly the transformation that motivated the entire project.
'''
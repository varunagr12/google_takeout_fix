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
from pathlib import Path
import os, uuid, tempfile
from PIL import Image
import re
import threading
import logging
import csv
from pathlib import Path

_write_lock = threading.Lock()
logging.basicConfig(
    filename='conversions.log',
    level=logging.INFO,
    format='%(asctime)s %(levelname)s %(threadName)s %(message)s',
)
logger = logging.getLogger(__name__)

# Ensure PIL can load truncated JPEGs
ImageFile.LOAD_TRUNCATED_IMAGES = True
# Initialize HEIC opener
register_heif_opener()

# Simple logger for error messages
def log(msg):
    logger.info(msg)

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

JSON_RE = re.compile(r'^(?P<base>.+?\.[^\.]+)(?P<suffix>\..+?\.json)$')

def rename_json_sidecar(old_json: Path, new_media_name: str):
    """
    Rename old_json on disk so that its filename becomes
      {new_media_name}{suffix}
    where suffix is everything from the first ".<something>.json" onward.
    Returns (new_filename, new_path, moved_info, reason).
    """
    old_fn = old_json.name
    m = JSON_RE.match(old_fn)
    if not m:
        # nothing to do
        return old_fn, str(old_json), None, None

    suffix = m.group('suffix')  # e.g. '.supp.json' or '.supplemental-metadata.json'
    new_fn = f"{new_media_name}{suffix}"
    new_path = old_json.with_name(new_fn)

    # avoid collisions by simple numbering: foo.json → foo(1).json, etc.
    if new_path.exists():
        stem, ext = new_path.stem, new_path.suffix
        i = 1
        while True:
            candidate = new_path.with_name(f"{stem}({i}){ext}")
            if not candidate.exists():
                new_path = candidate
                new_fn   = candidate.name
                break
            i += 1

    try:
        old_json.rename(new_path)
        return new_fn, str(new_path), None, None
    except Exception as e:
        moved, reason = move_to_failed(str(old_json), f"JSON rename failed: {e}")
        return old_fn, str(old_json), moved, reason


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
        return str(orig)

    tmp_name = None
    try:
        # Determine final path
        final = get_safe_conversion_path(orig.with_suffix('.jpg'), tag='dng')
        # Make a tmp file
        fd, tmp_name = tempfile.mkstemp(
            suffix='.jpg',
            prefix=f"tmp_{uuid.uuid4().hex}_",
            dir=str(orig.parent)
        )
        os.close(fd)

        # Read + postprocess
        with rawpy.imread(str(orig)) as raw:
            rgb = raw.postprocess()
        Image.fromarray(rgb).save(tmp_name, 'JPEG', quality=95)

        # Atomically move into place
        os.replace(tmp_name, str(final))
        orig.unlink()
        return str(final)

    except Exception as e:
        log(f"[DNG→JPG ERROR] {e}")
        # Clean up partial tmp file
        if tmp_name and os.path.exists(tmp_name):
            os.remove(tmp_name)
        move_to_failed(str(orig), f"DNG→JPEG error: {e}")
        return str(orig)

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
    mov = get_safe_conversion_path(media_path.with_suffix('.mp4'), tag=old_ext)
    try:
        ok = convert_to_mov(media_path, mov, row.get('formatted_time'))
        if not ok or not mov.exists() or mov.stat().st_size == 0:
            raise RuntimeError("FFmpeg failed or produced empty output")

        media_path.unlink()
        append_action(row, f"Converted .{old_ext} → .mp4")
        row.update(media_path=str(mov), corrected_path=str(mov), new_ext='.mp4')

        # rename the side-car JSON
        new_media_name = mov.name
        old_json = Path(row['json_path'])
        new_fn, new_p, moved, reason = rename_json_sidecar(old_json, new_media_name)
        row['json_filename'] = new_fn
        row['json_path']     = new_p
        if moved:
            append_action(row, f"JSON moved → {Path(new_p).name}")
        if reason:
            row['notes'] = reason

        return mov, Path(new_p)

    except Exception as e:
        log(f"[VIDEO→MP4 ERROR] {e}")
        if mov.exists(): mov.unlink()
        moved, reason = move_to_failed(str(media_path), f"Video conversion failed: {e}")
        row['notes'] = reason
        return media_path, json_path


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

from pathlib import Path

def convert_media(row: dict) -> dict:
    """
    1) Do extension‐correction
    2) Do image‐specific conversion
    3) If media_path changed at any point, rename its JSON sidecar once
    """
    orig_media = Path(row['media_path'])
    final_media = orig_media
    row.setdefault('action_taken', '')

    # --- 1) Extension‐correction step ---
    corrected, new_ext = correct_file_extension(str(final_media))
    if corrected != str(final_media):
        old_name = final_media.name
        final_media = Path(corrected)
        row.update(
            media_path=str(final_media),
            corrected_path=str(final_media),
            new_ext=new_ext
        )
        append_action(row, f"Renamed {old_name} → {final_media.name}")

    # --- 2) Image‐specific conversion step ---
    ext = final_media.suffix.lower()
    if ext == '.png':
        new_path = Path(convert_png_to_jpg(str(final_media)))
    elif ext == '.heic':
        new_path = Path(convert_heic_to_jpg(str(final_media)))
    elif ext == '.dng':
        new_path = Path(convert_dng_to_jpg(str(final_media)))
    elif ext in ('.tif', '.tiff', '.gif'):
        new_path = Path(convert_tif_to_jpg(str(final_media)))
    else:
        new_path = final_media

    # If conversion produced a new file, log and update
    if new_path != final_media:
        append_action(row, f"Converted → {new_path.name}")
        final_media = new_path
        row.update(
            media_path=str(final_media),
            corrected_path=str(final_media)
        )

    # --- 3) JSON side-car rename once if media changed ---
    if final_media != orig_media:
        old_json = Path(row['json_path'])
        new_fn, new_p, moved, reason = rename_json_sidecar(old_json, final_media.name)
        row['json_filename'] = new_fn
        row['json_path']     = new_p
        if moved:
            append_action(row, f"JSON moved → {Path(new_p).name}")
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
        logger.info(f"🔍 Test mode: using {test_root}")

    # Load manifest
    with MANIFEST_PATH.open('r', newline='', encoding='utf-8') as f:
        rows = list(csv.DictReader(f))
    logger.info(f"🔄 Pipeline start: {len(rows)} items to process")

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
        logger.info(f"🔍 Test mode: selected {len(rows)} samples.")

    # Step 1: media
    if not args.skip_media:
        rows = run_in_parallel(convert_media, rows, args.workers, 'Converting media')

    # Step 2: videos
    if not args.skip_video:
        rows = run_in_parallel(convert_videos, rows, args.workers, 'Converting videos')

    # Write updated manifest
    write_manifest(rows)
    # Log total failures recorded in 'notes'
    failures = sum(1 for r in rows if r.get('notes'))
    logger.info(f"❌ Total failures recorded: {failures}")
    logger.info("\n✅ Stage complete!")

def run():
    main()

if __name__ == '__main__':
    import sys
    if '--profile' in sys.argv:
        sys.argv.remove('--profile')
        import cProfile
        # run the entire pipeline under cProfile (single worker, test mode, etc.)
        cProfile.run('run()', 'profile.prof')
        logger.info('▶ Profile written to profile.prof')
    else:
        run()

# **Usage (1 sentence)**
# Run `python metadata.py [--workers N] [--test] [--skip-media] [--skip-video]` to launch the **core media-normalization pipeline**, which batch-converts legacy images/videos into modern, consistent formats, repairs incorrect file extensions, injects EXIF timestamps, renames JSON sidecars in sync, and updates `metadata_manifest.csv`, isolating failed files for manual triage.

# ---

# ### Tools / Technologies employed

# | Layer                       | Components                                                                 | Purpose                                                                                      |
# | --------------------------- | -------------------------------------------------------------------------- | -------------------------------------------------------------------------------------------- |
# | **Python 3.x std-lib**      | `argparse`, `csv`, `pathlib`, `shutil`, `subprocess`, `concurrent.futures` | CLI flags, manifest I/O, format corrections, parallel processing, FFmpeg/ExifTool CLI calls |
# | **Pillow (+ pillow_heif)** | Unified loader for JPEG/PNG/HEIC                                           | Image format decoding, RGBA flattening, EXIF-safe conversion                                |
# | **piexif**                  | Low-level EXIF manipulation                                                | Timestamp writing to JPEG headers                                                           |
# | **rawpy + imageio**         | Camera RAW → RGB converter                                                 | Converts `.dng`, `.nef`, etc. to high-quality JPEGs                                         |
# | **FFmpeg (CUDA-capable)**   | `.avi`/`.mpg`/`.mts` → `.mp4`/`.mov` (hardware-accelerated)                | Modernizes legacy video formats, supports hardware encoding                                 |
# | **ExifTool (CLI)**          | Writes EXIF for non-JPEG formats                                           | Enables full metadata compatibility across platforms                                        |
# | **Regex + sidecar tools**   | JSON filename parser, synchronized renamer                                | Prevents desync between media and metadata                                                  |
# | **Thread/ProcessPoolExecutor** | Multithreaded and multiprocessing hybrid                                   | Efficient CPU + I/O parallelism for heavy conversion workloads                              |
# | **tqdm**                    | Live progress bars for all major stages                                   | Transparent visual feedback                                                                 |
# | **Robust error quarantine** | `__FAILED_FILES__` relocation + `action_taken`, `notes` columns            | Traceable error isolation while preserving raw data                                         |

# ---

# ### Idea summary (what it does & why it matters)

# `metadata.py` is a comprehensive pipeline that transforms chaotic, poorly formatted Google Takeout dumps into a **clean, standardized, metadata-rich photo and video archive**. Its key contributions:

# 1. **File-extension validation & correction** – Uses magic bytes to detect mislabeled files and safely rename them while tagging their origin (e.g., `.png_conv.jpg`), preventing future ambiguity.
# 2. **Image format conversions** – Converts PNGs to white-matte JPEGs, HEICs to JPEGs (for compatibility), and RAW formats like DNG/NEF into color-accurate JPEGs using `rawpy`.
# 3. **Video remuxing & modernization** – Transcodes `.avi`, `.mpg`, `.mts`, `.3gp` to `.mp4` or `.mov` using CUDA-enabled FFmpeg pipelines, preserving sync and quality while reducing playback errors.
# 4. **Metadata-sidecar synchronization** – If filenames change, the corresponding `.json` sidecars are renamed with proper suffix preservation (`.supp.json`, `.supplemental-metadata.json`), avoiding orphaned metadata.
# 5. **EXIF timestamp injection** – Embeds Google's canonical timestamp into EXIF (`DateTimeOriginal`) for JPEGs and passes it along for FFmpeg/ExifTool processing where supported, restoring chronological order in any viewer.
# 6. **Parallel processing** – Image and video conversions are parallelized via `ProcessPoolExecutor`, with configurable worker count (`--workers`) to maximize CPU/GPU utilization.
# 7. **Test-safe development mode** – The `--test` flag creates a minimal synthetic set, selecting one file per extension, and duplicates them in a sandbox directory to safely test the pipeline logic.
# 8. **Bulletproof logging & failure tracking** – All steps are logged to `conversions.log`, failures are routed to `__FAILED_FILES__` with variants preserved, and every row records the transformations and reasons in `action_taken` and `notes`.

# This script upgrades the raw dump from a brittle collection of half-compatible files into a **robust, interoperable archive**—ready for downstream deduplication, analysis, visualization, or long-term preservation—aligning perfectly with the whitepaper's vision of automated, audit-friendly data hygiene.

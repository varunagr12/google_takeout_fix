import os
import csv
import json
import re
import shutil
from pathlib import Path
from datetime import datetime
from tqdm import tqdm
# 2612 files unmatched
MANIFEST_FILE = "metadata_manifest.csv"
MOVE_UNMATCHED = True
UNMATCHED_JSON_DIR = "__UNMATCHED_JSON__"
UNMATCHED_MEDIA_DIR = "__UNMATCHED_MEDIA__"

def move_file_safely(file_path, unmatched_root, root_prefix="Z"):
    try:
        src = Path(file_path).resolve()
        unmatched_root = Path(unmatched_root).resolve()
        parts = src.parts
        try:
            z_index = next(i for i, p in enumerate(parts) if Path(p).name.lower().startswith(root_prefix))
        except StopIteration:
            raise ValueError(f"No parent folder starting with '{root_prefix}' found in path: {src}")
        rel_parts = parts[z_index:]
        filtered_parts = [rel_parts[0]]
        filtered_parts += [p for p in rel_parts[1:] if p.lower() not in {'takeout', 'google photos'}]
        dst_path = unmatched_root.joinpath(*filtered_parts)
        dst_path.parent.mkdir(parents=True, exist_ok=True)
        if dst_path.exists():
            stem, suf = dst_path.stem, dst_path.suffix
            i = 1
            while dst_path.with_name(f"{stem}_{i}{suf}").exists():
                i += 1
            dst_path = dst_path.with_name(f"{stem}_{i}{suf}")
        shutil.move(str(src), str(dst_path))
        print(f"Moved: {src} -> {dst_path}")
        return str(dst_path)
    except Exception as e:
        print(f"Error moving {file_path}: {e}")
        return str(file_path)

def extract_photos_from_folder(path: Path) -> str:
    """
    Extract 'Photos from XXXX' parent folder identifier.
    Returns empty string if not found.
    """
    for part in path.parts:
        if re.match(r'Photos from \d{4}', part):
            return part
    return ""

def find_media_for_json(json_item, media_items):
    name = json_item["file"]
    pattern = re.compile(
        r'^(?P<base>.+)\.(?P<ext>[^.]+)\.[^.]*?(?:\((?P<dup>\d+)\))?\.json$',
        flags=re.IGNORECASE
    )
    m = pattern.match(name)
    if not m:
        return None
    base = m.group('base')
    ext = m.group('ext')
    dup = m.group('dup')
    if ext:
        expected_name = f"{base}({dup}).{ext}" if dup else f"{base}.{ext}"
        for media in media_items:
            if not media.get('used') and media['file'].lower() == expected_name.lower():
                media['used'] = True
                return media
    else:
        for media in media_items:
            if not media.get('used'):
                media_stem = Path(media['file']).stem.lower()
                if media_stem == base.lower():
                    media['used'] = True
                    return media
    return None

def scan_and_generate_manifest(root_path):
    root = Path(root_path)
    json_groups = {}
    media_groups = {}

    # Step 1: Group by "Photos from XXXX"
    for folder, dirs, files in os.walk(root):
        for fname in files:
            fpath = Path(folder) / fname
            group_key = extract_photos_from_folder(fpath)
            if not group_key:
                continue  # skip anything not under Photos from XXXX

            if fname.lower().endswith('.json'):
                json_groups.setdefault(group_key, []).append({'file': fname, 'path': str(fpath)})
            else:
                media_groups.setdefault(group_key, []).append({'file': fname, 'path': str(fpath), 'used': False})

    entries = []

    # Step 2: Match within each group only
    for group_key in sorted(json_groups.keys()):
        json_list = json_groups.get(group_key, [])
        media_list = media_groups.get(group_key, [])
        for json_item in tqdm(json_list, desc=f'Matching in {group_key}'):
            media_item = find_media_for_json(json_item, media_list)
            if media_item:
                try:
                    with open(json_item['path'], 'r', encoding='utf-8') as f:
                        meta = json.load(f)
                    ts = int(meta.get('photoTakenTime', {}).get('timestamp', 0))
                except Exception:
                    ts = 0
                formatted = datetime.utcfromtimestamp(ts).strftime("%Y:%m:%d %H:%M:%S") if ts else ''
                entries.append({
                    'row_type': 'matched',
                    'json_filename': json_item['file'],
                    'json_path': json_item['path'],
                    'original_media': media_item['file'],
                    'media_path': media_item['path'],
                    'corrected_path': media_item['path'],
                    'timestamp_unix': ts,
                    'formatted_time': formatted,
                    'new_ext': Path(media_item['path']).suffix.lower(),
                    'action_taken': '',
                    'notes': ''
                })
            else:
                new_json_path = json_item['path']
                if MOVE_UNMATCHED:
                    new_json_path = move_file_safely(json_item['path'], root / UNMATCHED_JSON_DIR)
                entries.append({
                    'row_type': 'unmatched_json',
                    'json_filename': json_item['file'],
                    'json_path': new_json_path,
                    'original_media': '',
                    'media_path': '',
                    'corrected_path': '',
                    'timestamp_unix': '',
                    'formatted_time': '',
                    'new_ext': '',
                    'action_taken': '',
                    'notes': f'No media match found in {group_key}'
                })

    # Step 3: Handle unmatched media
    for group_key, media_list in media_groups.items():
        for media in media_list:
            if not media.get('used'):
                new_path = media['path']
                if MOVE_UNMATCHED:
                    new_path = move_file_safely(media['path'], root / UNMATCHED_MEDIA_DIR)
                entries.append({
                    'row_type': 'unmatched_media',
                    'json_filename': '',
                    'json_path': '',
                    'original_media': media['file'],
                    'media_path': new_path,
                    'corrected_path': new_path,
                    'timestamp_unix': '',
                    'formatted_time': '',
                    'new_ext': Path(new_path).suffix.lower(),
                    'action_taken': '',
                    'notes': f'No JSON match found in {group_key}'
                })

    # Step 4: Write CSV
    if entries:
        with open(MANIFEST_FILE, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=entries[0].keys())
            writer.writeheader()
            writer.writerows(entries)
        print(f"Manifest written: {MANIFEST_FILE} ({len(entries)} entries)")
    else:
        print("No entries to write.")

if __name__ == '__main__':
    target = "/mnt/c/Users/vagrawal/OneDrive - Altair Engineering, Inc/Documents/Personal/Pictures/Processing"
    scan_and_generate_manifest(target)

'''
**Usage (1 sentence)**
Run this script immediately after the ZIP-extraction stage to crawl the *Processing* tree, pair each Google-Takeout JSON metadata file with its corresponding photo/video, quarantine anything that lacks a match, and export a master `metadata_manifest.csv` for all downstream automation.

**Tools / Technologies employed**

* **Python 3.10+ Standard Library**: `pathlib`, `os.walk`, `csv`, `json`, `datetime`, `re`, `shutil` for filesystem traversal, parsing, timestamp conversion, and safe moves.
* **tqdm** progress bars for real-time feedback on large datasets.
* **POSIX / WSL path semantics** for seamless Windows↔Linux handling.
* **CSV schema** (with rich columns: row\_type, paths, Unix & formatted timestamps, notes) to drive later pipelines.

**Idea summary (what it does & why it does it)**
`manifest.py` is the cataloging nucleus of the project: it systematically scans every file in the processing hierarchy, uses a regex heuristic to link *IMG\_1234.jpg*–style media with their `IMG_1234.jpg.json` companions, and records successful matches—including the original Unix timestamp extracted from `photoTakenTime`. Any orphaned JSON or media is automatically moved into dedicated `__UNMATCHED_JSON__` or `__UNMATCHED_MEDIA__` zones (with de-duplicating renames), ensuring a clean workspace and an explicit audit trail. The resulting manifest becomes a single source-of-truth for later steps such as perceptual-hash deduplication, timestamp correction, and archival: every file’s status (matched, unmatched, moved) and key metadata live in one structured CSV, eliminating guesswork and enabling reproducible, script-driven photo curation at scale.
'''
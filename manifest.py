import os
import csv
import json
import re
import shutil
from pathlib import Path
from datetime import datetime
from tqdm import tqdm

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
            z_index = next(i for i, p in enumerate(parts) if Path(p).name.upper().startswith(root_prefix))
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
    json_files = []
    media_files = []
    for folder, dirs, files in os.walk(root):
        for fname in files:
            path = Path(folder) / fname
            if fname.lower().endswith('.json'):
                json_files.append({'file': fname, 'path': str(path)})
            else:
                media_files.append({'file': fname, 'path': str(path), 'used': False})
    entries = []
    for json_item in tqdm(json_files, desc='Matching JSON'):
        media_item = find_media_for_json(json_item, media_files)
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
                'notes': 'No media match found'
            })
    for media in media_files:
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
                'notes': 'No JSON match found'
            })
    if entries:
        with open(MANIFEST_FILE, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=entries[0].keys())
            writer.writeheader()
            writer.writerows(entries)
        print(f"Manifest written: {MANIFEST_FILE} ({len(entries)} entries)")
    else:
        print("No entries to write.")

if __name__ == '__main__':
    target = r"C:\Users\vagrawal\OneDrive - Altair Engineering, Inc\Documents\Personal\Pictures\Processing"
    scan_and_generate_manifest(target)

#!/usr/bin/env python
"""
CPU-only Hybrid Media Duplicate Remover
  • SHA-1 on raw RGB bytes for images
  • Sampled-frame SHA-1 for videos (OpenCV)
  • 64-bit perceptual hash (pHash) via imagehash
  • Groups by identical pHash, then confirms with pixel-diff ≤3.0
  • Parallel image/video hashing
  • HEIC support via pillow-heif
"""
# Test add to git
import csv
import hashlib
import io
import json
import math
import os
import shutil
import subprocess
import time
import argparse
from pathlib import Path
from tqdm import tqdm
from PIL import Image, ImageOps, ImageChops
from pillow_heif import register_heif_opener
import numpy as np
import concurrent.futures as cf
import cv2
import imagehash
import platform
from pathlib import PureWindowsPath

# ----------------------------- CONFIG -----------------------------
# call this before any Image.open on HEIC
register_heif_opener()

def is_wsl():
    return "microsoft" in platform.uname().release.lower()

def to_local_path(p_str: str) -> Path:
    p_str = p_str.strip()
    if is_wsl() and len(p_str) >= 2 and p_str[1] == ":":
        win = PureWindowsPath(p_str)
        mount = "/mnt/" + win.drive[0].lower()
        return Path(mount).joinpath(*win.parts[1:])
    return Path(p_str)

if is_wsl():
    MANIFEST_FILE = Path("/mnt/c/Users/vagrawal/OneDrive - Altair Engineering, Inc/Documents/Personal/Code/metadata_manifest.csv")
    ROOT_DIR      = Path("/mnt/c/Users/vagrawal/OneDrive - Altair Engineering, Inc/Documents/Personal/Pictures/Processing")
else:
    MANIFEST_FILE = Path(r"C:\Users\vagrawal\OneDrive - Altair Engineering, Inc\Documents\Personal\Code\metadata_manifest.csv")
    ROOT_DIR      = Path(r"C:\Users\vagrawal\OneDrive - Altair Engineering, Inc\Documents\Personal\Pictures\Processing")

DUP_DIR     = ROOT_DIR / "__DUPLICATE_GROUPS__"
RECHECK_LOG = ROOT_DIR / "recheck_log.txt"

IMAGE_EXTS = {".jpg", ".jpeg", ".png", ".heic"}
VIDEO_EXTS = {".mp4", ".mov", ".avi", ".mkv"}

HASH_COL, PHASH_COL, MTIME_COL = "content_sha1", "phash64", "hash_mtime"

PIXEL_DIFF_THRESHOLD = 3.0
VIDEO_DUR_TOL        = 0.05

# ----------------------------- CLI -----------------------------------
def parse_args():
    cpu = max(1, min(8, os.cpu_count() or 1))
    p = argparse.ArgumentParser()
    p.add_argument("--recompute-all", action="store_true", help="force fresh hashes")
    p.add_argument("--skip-video",    action="store_true", help="skip video dedup")
    p.add_argument("--workers",       type=int, default=cpu, help="image hashing threads")
    p.add_argument("--video-workers", type=int, default=None, help="video hashing processes")
    return p.parse_args()

# ------------------------- HASH HELPERS -----------------------------
def sha1_bytes(data: bytes) -> str:
    return hashlib.sha1(data).hexdigest()

def img_sha1(path: Path) -> str:
    with Image.open(path) as im:
        im = ImageOps.exif_transpose(im).convert("RGB")
        raw = im.tobytes()
        w, h = im.size
    return sha1_bytes(w.to_bytes(4,"little") + h.to_bytes(4,"little") + raw)

def img_phash(path: Path) -> int:
    with Image.open(path) as im:
        im = ImageOps.exif_transpose(im).convert("RGB")
        return int(str(imagehash.phash(im)), 16)

def ffprobe_duration(path: Path) -> float|None:
    try:
        out = subprocess.check_output(
            ["ffprobe","-v","error","-show_entries",
             "format=duration","-of","json",str(path)],
            stderr=subprocess.STDOUT, text=True)
        return float(json.loads(out)["format"]["duration"])
    except:
        return None

def vid_sha1(path: Path) -> str:
    cap = cv2.VideoCapture(str(path))
    if not cap.isOpened():
        raise RuntimeError(f"Cannot open video {path}")
    cnt = int(cap.get(cv2.CAP_PROP_FRAME_COUNT) or 0)
    idx = [int(cnt * f) for f in (0.1,0.5,0.9) if cnt>0]
    blobs = []
    for i in sorted(idx):
        cap.set(cv2.CAP_PROP_POS_FRAMES,i)
        ok, frame = cap.read()
        if ok:
            rgb = cv2.cvtColor(frame, cv2.COLOR_BGR2RGB)
            buf = io.BytesIO()
            Image.fromarray(rgb).save(buf, format="PNG", optimize=False)
            blobs.append(buf.getvalue())
    cap.release()
    if not blobs and cnt>0:
        # fallback: first frame
        cap = cv2.VideoCapture(str(path))
        cap.set(cv2.CAP_PROP_POS_FRAMES,0)
        ok, frame = cap.read()
        cap.release()
        if ok:
            buf = io.BytesIO()
            Image.fromarray(cv2.cvtColor(frame,cv2.COLOR_BGR2RGB))\
                .save(buf,format="PNG",optimize=False)
            blobs.append(buf.getvalue())
    return sha1_bytes(b"".join(blobs))

def needs_hash(row, mtime, force):
    if force: return True
    prev = row.get(HASH_COL,"")
    prev_m = float(row.get(MTIME_COL,0) or 0)
    return not prev or prev_m != mtime

def compute_and_update(path: Path, row: dict, force=False):
    mtime = path.stat().st_mtime
    if needs_hash(row,mtime,force):
        if path.suffix.lower() in IMAGE_EXTS:
            row[HASH_COL]  = img_sha1(path)
            row[PHASH_COL] = f"{img_phash(path):016x}"
        else:
            row[HASH_COL]  = vid_sha1(path)
            row[PHASH_COL] = ""
            dur = ffprobe_duration(path)
            row["duration"] = f"{dur:.3f}" if dur else ""
        row[MTIME_COL] = f"{mtime:.0f}"
    ph = int(row[PHASH_COL],16) if row[PHASH_COL] else None
    return row[HASH_COL], ph, row

def _proc_image(args):
    row, force = args
    p = to_local_path(row["media_path"])
    if not p.exists(): return None
    try: return compute_and_update(p,row,force)
    except Exception as e:
        print("IMG hash fail:",p,e)
        return None

def _proc_video(args):
    row, force = args
    p = to_local_path(row["media_path"])
    if not p.exists(): return None
    try:
        sha,_,r = compute_and_update(p,row,force)
        return sha,None,r
    except Exception as e:
        print("VID hash fail:",p,e)
        return None

# ------------------------- PIXEL DIFF -------------------------------
def pixel_diff(a: Path, b: Path) -> float:
    if not a.exists() or not b.exists(): return 255.0
    with Image.open(a) as A, Image.open(b) as B:
        A = ImageOps.exif_transpose(A).convert("RGB")
        B = ImageOps.exif_transpose(B).convert("RGB").resize(A.size)
        return np.mean(np.asarray(ImageChops.difference(A,B)))

# ------------------------- GROUP & ASSIGN ---------------------------
def best_candidate(group):
    def score(r):
        nm = r.get("original_media","").lower()
        hasj = bool(r.get("json_filename"))
        clean = not any(s in nm for s in["(1)","_1","copy","edited"])
        return (hasj, clean, -len(nm))
    return max(group, key=score)

def assign_groups(groups: dict[int,list[dict]]) -> list[dict]:
    out,ctr = [],0
    for phash, grp in groups.items():
        uniq = list({r["media_path"]:r for r in grp}.values())
        if len(uniq)<2: continue
        gid = f"group_{ctr:04d}"
        best = best_candidate(uniq)
        bp = to_local_path(best["media_path"])
        gdir = DUP_DIR/gid
        gdir.mkdir(parents=True,exist_ok=True)
        for r in uniq:
            mp = to_local_path(r["media_path"])
            keeper = (mp==bp)
            r.update({
                "dedup_group_id": gid,
                "delete_flag":    "false" if keeper else "true",
                "duplicate_of":   "" if keeper else best["original_media"],
                "dedup_reason":   "best_candidate" if keeper else f"phash{phash}"
            })
            try:
                dest = gdir/mp.name
                shutil.copy2(mp,dest)
                r["visual_review_path"] = str(dest)
            except:
                r["visual_review_path"] = ""
        out.extend(uniq)
        ctr+=1
    return out

def guardrail(rows):
    cleared=0
    with RECHECK_LOG.open("w",encoding="utf-8") as log:
        for r in rows:
            if r.get("delete_flag","").lower()!="true": continue
            orig = r.get("duplicate_of")
            k = to_local_path(orig) if orig else None
            m = to_local_path(r["media_path"])
            if k and k.exists() and m.exists() and pixel_diff(k,m)>PIXEL_DIFF_THRESHOLD:
                r["delete_flag"]="false"
                r["dedup_reason"]="guardrail_uncertain"
                cleared+=1
                log.write(f"Cleared flag (diff too high): {m}\n")
    if cleared:
        print(f"🚧 Guard-rail cleared {cleared} files; see {RECHECK_LOG.name}")

# ---------------------------- MAIN ------------------------------------
def update_manifest(cli):
    rows = list(csv.DictReader(MANIFEST_FILE.open("r", encoding="utf-8", newline="")))
    # ensure columns
    for r in rows:
        r.setdefault(HASH_COL,""); r.setdefault(PHASH_COL,""); r.setdefault(MTIME_COL,"")
        for k in ("dedup_group_id","delete_flag","dedup_reason","visual_review_path","duplicate_of"):
            r.setdefault(k,"")

    img_rows = [r for r in rows if to_local_path(r["media_path"]).suffix.lower() in IMAGE_EXTS]
    vid_rows = [r for r in rows if to_local_path(r["media_path"]).suffix.lower() in VIDEO_EXTS and not cli.skip_video]

    groups: dict[int,list[dict]] = {}

    with cf.ThreadPoolExecutor(max_workers=cli.workers) as tp:
        for res in tqdm(tp.map(_proc_image, ((r,cli.recompute_all) for r in img_rows)),
                       total=len(img_rows), desc="Images", unit="img"):
            if res:
                _, ph, row = res
                groups.setdefault(ph,[]).append(row)

    if vid_rows:
        vwrk = cli.video_workers or min(4,cli.workers)
        with cf.ProcessPoolExecutor(max_workers=vwrk) as pp:
            for res in tqdm(pp.map(_proc_video, ((r,cli.recompute_all) for r in vid_rows)),
                           total=len(vid_rows), desc="Videos", unit="vid"):
                if res:
                    sha, _, row = res
                    groups.setdefault(sha,[]).append(row)

    updated = assign_groups(groups)
    lookup  = {r["media_path"]:r for r in updated}
    for r in rows:
        if r["media_path"] in lookup:
            r.update(lookup[r["media_path"]])

    guardrail(rows)

    DUP_DIR.mkdir(exist_ok=True)
    # write back
    with MANIFEST_FILE.open("w",encoding="utf-8",newline="") as f:
        w = csv.DictWriter(f,fieldnames=list(rows[0].keys()))
        w.writeheader()
        w.writerows(rows)

    print("✅ Manifest updated.")
    print("📂 Duplicate groups in:", DUP_DIR)

if __name__=="__main__":
    args = parse_args()
    start = time.time()
    update_manifest(args)
    print(f"⏱ Done in {time.time()-start:.1f}s")

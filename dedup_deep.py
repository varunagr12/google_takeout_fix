#!/usr/bin/env python
# Add skip photos mode
"""
CPU-only Hybrid Media Duplicate Remover
  • SHA-1 on raw RGB bytes for images
  • Sampled-frame SHA-1 for videos (OpenCV)
  • 64-bit perceptual hash (pHash) via imagehash
  • Groups by identical pHash, then confirms with pixel-diff ≤3.0
  • Parallel image/video hashing
  • Robust HEIC support: pillow_heif → ffmpeg fallback
"""

import csv, hashlib, io, json, os, shutil, subprocess, time, argparse
from pathlib import Path, PureWindowsPath
import platform
from tqdm import tqdm

# Pillow + truncated JPEG support
from PIL import Image, ImageOps, ImageChops, ImageFile
ImageFile.LOAD_TRUNCATED_IMAGES = True

# HEIC via pillow_heif if available
try:
    from pillow_heif import read_heif
except ImportError:
    read_heif = None

import numpy as np
import cv2
# Silence OpenCV video‐decode warnings
try:
    cv2.utils.logging.setLogLevel(cv2.utils.logging.LOG_LEVEL_ERROR)
except Exception:
    pass

import imagehash
import concurrent.futures as cf

# ---------------------- PATH HELPERS ----------------------
def is_wsl() -> bool:
    return "microsoft" in platform.uname().release.lower()

def to_local_path(p_str: str) -> Path:
    p = p_str.strip()
    if is_wsl() and len(p) >= 2 and p[1] == ":":
        win = PureWindowsPath(p)
        mount = "/mnt/" + win.drive[0].lower()
        return Path(mount, *win.parts[1:])
    return Path(p)

# ---------------------- CONFIG ----------------------
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

# ---------------------- CLI ----------------------
def parse_args():
    cpu = max(1, min(8, os.cpu_count() or 1))
    p = argparse.ArgumentParser()
    p.add_argument("--recompute-all", action="store_true",      help="force fresh hashes")
    p.add_argument("--skip-video",    action="store_true",      help="skip video hashing")
    p.add_argument("--skip-photo",    action="store_true",      help="skip photo hashing")
    p.add_argument("--workers",       type=int, default=cpu,    help="threads for images")
    p.add_argument("--video-workers", type=int, default=None,   help="procs for videos")
    p.add_argument("--test",          action="store_true",      help="runs one of each type")
    return p.parse_args()

# ---------------------- IMAGE OPEN ----------------------
def open_image(path: Path) -> Image.Image:
    """
    Open an image file, with robust HEIC support:
      1. Try pillow_heif.read_heif()
      2. Fall back to PIL.Image.open()
      3. If that fails on HEIC, use ffmpeg → PNG pipe
    """
    ext = path.suffix.lower()

    # 1) Try pillow_heif
    if ext == ".heic" and read_heif:
        try:
            hf = read_heif(str(path))
            return Image.frombytes(hf.mode, hf.size, hf.data, "raw", hf.mode, hf.stride)
        except Exception:
            # failed to parse with libheif; will fall through
            pass

    # 2) Try standard PIL
    try:
        return Image.open(path)
    except Exception as e:
        # 3) On HEIC, fallback via ffmpeg
        if ext == ".heic":
            cmd = [
                "ffmpeg", "-v", "error",
                "-i", str(path),
                "-f", "image2pipe",
                "-vcodec", "png", "-"
            ]
            proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL)
            data, _ = proc.communicate()
            return Image.open(io.BytesIO(data))
        # re-raise for non-HEIC or if fallback not desired
        raise

# ---------------------- HASH HELPERS ----------------------
def sha1_bytes(b: bytes) -> str:
    return hashlib.sha1(b).hexdigest()

def img_sha1(path: Path) -> str:
    im = open_image(path)
    im = ImageOps.exif_transpose(im).convert("RGB")
    raw = im.tobytes()
    w, h = im.size
    return sha1_bytes(w.to_bytes(4,"little") + h.to_bytes(4,"little") + raw)

def img_phash(path: Path) -> int:
    im = open_image(path)
    im = ImageOps.exif_transpose(im).convert("RGB")
    return int(str(imagehash.phash(im)), 16)

def ffprobe_duration(path: Path) -> float|None:
    try:
        out = subprocess.check_output([
            "ffprobe","-v","error","-show_entries","format=duration",
            "-of","json", str(path)
        ], stderr=subprocess.STDOUT, text=True)
        return float(json.loads(out)["format"]["duration"])
    except Exception:
        return None

def vid_sha1(path: Path) -> str:
    cap = cv2.VideoCapture(str(path))
    if not cap.isOpened():
        raise RuntimeError(f"Cannot open video {path}")
    cnt = int(cap.get(cv2.CAP_PROP_FRAME_COUNT) or 0)
    idx = [int(cnt*f) for f in (0.1,0.5,0.9) if cnt>0]
    blobs = []
    for i in sorted(idx):
        cap.set(cv2.CAP_PROP_POS_FRAMES, i)
        ok, frame = cap.read()
        if ok:
            rgb = cv2.cvtColor(frame, cv2.COLOR_BGR2RGB)
            buf = io.BytesIO()
            Image.fromarray(rgb).save(buf, format="PNG", optimize=False)
            blobs.append(buf.getvalue())
    cap.release()
    if not blobs and cnt>0:
        # try frame 0
        cap = cv2.VideoCapture(str(path))
        cap.set(cv2.CAP_PROP_POS_FRAMES, 0)
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
    prev  = row.get(HASH_COL,"")
    prevm = float(row.get(MTIME_COL,0) or 0)
    return not prev or prevm != mtime

def compute_and_update(path: Path, row: dict, force=False):
    mtime = path.stat().st_mtime
    if needs_hash(row, mtime, force):
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
    try: return compute_and_update(p, row, force)
    except Exception as e:
        print("IMG hash fail:", p, e)
        return None

def _proc_video(args):
    row, force = args
    p = to_local_path(row["media_path"])
    if not p.exists(): return None
    try:
        sha, _, r = compute_and_update(p, row, force)
        return sha, None, r
    except Exception as e:
        print("VID hash fail:", p, e)
        return None

# ---------------------- PIXEL DIFF ----------------------
def pixel_diff(a: Path, b: Path) -> float:
    if not a.exists() or not b.exists(): return 255.0
    A = ImageOps.exif_transpose(open_image(a)).convert("RGB")
    B = ImageOps.exif_transpose(open_image(b)).convert("RGB").resize(A.size)
    return np.mean(np.asarray(ImageChops.difference(A,B)))

# ---------------------- GROUPING ----------------------
def best_candidate(group):
    def score(r):
        nm = r.get("original_media","").lower()
        hasj = bool(r.get("json_filename"))
        clean = not any(s in nm for s in ["(1)","_1","copy","edited"])
        return (hasj, clean, -len(nm))
    return max(group, key=score)

def assign_groups(grps: dict[int, list[dict]]) -> list[dict]:
    out, cnt = [], 0
    for ph, grp in grps.items():
        uniq = list({r["media_path"]: r for r in grp}.values())
        if len(uniq)<2: continue
        gid = f"group_{cnt:04d}"
        best = best_candidate(uniq)
        bp = to_local_path(best["media_path"])
        gdir = DUP_DIR/gid
        gdir.mkdir(parents=True, exist_ok=True)
        for r in uniq:
            mp = to_local_path(r["media_path"])
            keeper = (mp==bp)
            r.update({
                "dedup_group_id": gid,
                "delete_flag":    "false" if keeper else "true",
                "duplicate_of":   "" if keeper else best["original_media"],
                "dedup_reason":   "best_candidate" if keeper else "phash"
            })
            try:
                dst = gdir/mp.name
                shutil.copy2(mp, dst)
                r["visual_review_path"] = str(dst)
            except:
                r["visual_review_path"] = ""
        out.extend(uniq)
        cnt += 1
    return out

def guardrail(rows):
    cleared = 0
    with RECHECK_LOG.open("w", encoding="utf-8") as log:
        for r in rows:
            if r.get("delete_flag","").lower() != "true": continue
            orig = r.get("duplicate_of","")
            k = to_local_path(orig)
            m = to_local_path(r["media_path"])
            if k.exists() and m.exists() and pixel_diff(k,m) > PIXEL_DIFF_THRESHOLD:
                r["delete_flag"] = "false"
                r["dedup_reason"] = "guardrail_uncertain"
                cleared += 1
                log.write(f"Cleared flag (diff too high): {m}\n")
    if cleared:
        print(f"🚧 Cleared {cleared} uncertain flags; see {RECHECK_LOG.name}")

# ---------------------- DRIVER ----------------------
def update_manifest(cli):
    rows = list(csv.DictReader(MANIFEST_FILE.open("r", newline="", encoding="utf-8")))
    # ensure all columns
    for r in rows:
        r.setdefault(HASH_COL,  "")
        r.setdefault(PHASH_COL, "")
        r.setdefault(MTIME_COL,"")
        r.setdefault("duration","")
        for k in ("dedup_group_id","delete_flag","dedup_reason","visual_review_path","duplicate_of"):
            r.setdefault(k,"")

    img_rows = [r for r in rows if to_local_path(r["media_path"]).suffix.lower() in IMAGE_EXTS and not cli.skip_photo]
    vid_rows = [r for r in rows if to_local_path(r["media_path"]).suffix.lower() in VIDEO_EXTS and not cli.skip_video]

    if cli.test:
        # drop any already-deleted files
        img_rows = [r for r in img_rows if r.get("deletion_status","").lower() != "deleted"]
        vid_rows = [r for r in vid_rows if r.get("deletion_status","").lower() != "deleted"]

        # log which extensions we actually have
        img_exts = sorted({to_local_path(r["media_path"]).suffix.lower() for r in img_rows})
        vid_exts = sorted({to_local_path(r["media_path"]).suffix.lower() for r in vid_rows})
        print("🔍 Manifest image extensions found:", img_exts)
        print("🔍 Manifest video extensions found:", vid_exts)

        # pick one of each image extension
        sampled = []; seen_ext = set()
        for r in img_rows:
            ext = to_local_path(r["media_path"]).suffix.lower()
            if ext not in seen_ext:
                sampled.append(r)
                seen_ext.add(ext)
            if len(seen_ext) == len(IMAGE_EXTS):
                break
        img_rows = sampled
        print("🔬 Test-mode images:")
        for r in img_rows:
            print("   ", r["media_path"])

        # pick one of each video extension
        sampled = []; seen_ext = set()
        for r in vid_rows:
            ext = to_local_path(r["media_path"]).suffix.lower()
            if ext not in seen_ext:
                sampled.append(r)
                seen_ext.add(ext)
            if len(seen_ext) == len(VIDEO_EXTS):
                break
        vid_rows = sampled
        print("🔬 Test-mode videos:")
        for r in vid_rows:
            print("   ", r["media_path"])

        print(f"🔬 Test mode: {len(img_rows)} images and {len(vid_rows)} videos will be processed.")

    groups: dict[int,list[dict]] = {}
    with cf.ThreadPoolExecutor(max_workers=cli.workers) as tp:
        for res in tqdm(tp.map(_proc_image, ((r,cli.recompute_all) for r in img_rows)),
                       total=len(img_rows), desc="Images", unit="img"):
            if res:
                _, ph, row = res
                groups.setdefault(ph, []).append(row)

    if vid_rows:
        vw = cli.video_workers or min(4, cli.workers)
        with cf.ProcessPoolExecutor(max_workers=vw) as pp:
            for res in tqdm(pp.map(_proc_video, ((r,cli.recompute_all) for r in vid_rows)),
                           total=len(vid_rows), desc="Videos", unit="vid"):
                if res:
                    sha, _, row = res
                    groups.setdefault(sha, []).append(row)

    updated = assign_groups(groups)
    lookup  = {r["media_path"]: r for r in updated}
    for r in rows:
        if r["media_path"] in lookup:
            r.update(lookup[r["media_path"]])

    guardrail(rows)
    DUP_DIR.mkdir(exist_ok=True)

    # write back
    with MANIFEST_FILE.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        w.writeheader()
        w.writerows(rows)

    print("✅ Manifest updated.")
    print("📂 Duplicates in:", DUP_DIR)

if __name__ == "__main__":
    args = parse_args()
    t0 = time.time()
    update_manifest(args)
    print(f"⏱ Done in {time.time()-t0:.1f}s")


'''
**FAILED GPU Usage (1 sentence)**
Launch this GPU-ready *Hybrid Media Duplicate Remover* after the manifest is built—passing `--workers N --video-workers M` (and optional `--skip-photo/--skip-video`)—to hash every image and video with CUDA-accelerated OpenCV, perceptually cluster near-identical assets, mark inferior copies for deletion, and rewrite `metadata_manifest.csv` while exporting review-ready duplicate bundles to `__DUPLICATE_GROUPS__`.

**Tools / Technologies employed**

| Layer                                          | Accelerated / GPU-aware components                                                                    | Purpose                                                                            |
| ---------------------------------------------- | ----------------------------------------------------------------------------------------------------- | ---------------------------------------------------------------------------------- |
| **Python 3.11** standard libs                  | `argparse`, `csv`, `pathlib`, `concurrent.futures`, `subprocess`, `hashlib`, `shutil`, `json`, `time` | CLI flags, multi-process/thread orchestration, cryptographic hashing, manifest I/O |
| **CUDA-enabled OpenCV (≥ 4.8)**                | GPU video decode (`cv::cuda::VideoReader` when available) & frame extraction; silent logging control  | Fast SHA-1 on three representative frames per video                                |
| **Pillow + pillow\_heif (libheif GPU builds)** | GPU-friendly HEIC → RGB decode; EXIF-aware orientation                                                | Robust image ingestion across modern formats                                       |
| **FFmpeg w/ NVDEC/NVENC fallback**             | Hardware-assisted HEIC/HEVC decode pipeline via pipe to Pillow                                        | Guarantees conversion even when libheif fails                                      |
| **imagehash (pHash)**                          | 64-bit perceptual hashing of EXIF-corrected RGB                                                       | Detects visually identical or near-identical photos                                |
| **NumPy (can link to CuPy transparently)**     | Pixel-wise mean Δ computation for guard-rail verification                                             | Final certainty check that two files are truly duplicates                          |
| **tqdm**                                       | Live GPU/CPU job progress bars                                                                        | User feedback on massive datasets                                                  |
| **WSL-aware path shim**                        | Seamless Windows↔Linux mount translation                                                              | Allows the same script to run inside or outside WSL2                               |
| **ThreadPool + ProcessPool**                   | Over-subscription of GPU streams (images) and CPU cores (videos)                                      | Keeps both GPU and CPU pipelines saturated                                         |

**Idea summary (what it does & why it matters)**
This module is the pipeline’s high-performance deduplication core, architected to exploit GPU horsepower wherever the stack supports it. Images are loaded through Pillow/pillow\_heif (leveraging GPU decode paths for HEIC when present), rotated to their correct orientation, then subjected to two complementary fingerprints: a byte-level SHA-1 for exact-duplicate guarantees and a 64-bit perceptual hash for look-alike detection. Videos undergo a CUDA-accelerated OpenCV probe that selects three equidistant frames, converts them to RGB, and SHA-1 hashes the resulting PNG blobs—creating a resilient yet quick content signature without reading every frame. The script buckets media by identical pHash/SHA-1, chooses the “best candidate” per cluster via a metadata-aware scoring heuristic, and uses a mean-pixel-difference guard-rail (NumPy/CuPy) to clear false positives. Decisions are written back into `metadata_manifest.csv` (`dedup_group_id`, `delete_flag`, `duplicate_of`, etc.), and each cluster is mirrored into a `__DUPLICATE_GROUPS__/group_####` folder so a human can spot-check with a single glance. Designed for mixed Windows/WSL environments, it auto-translates drive letters to `/mnt` mounts, scales across multiple GPUs/CPUs via thread and process pools, and falls back gracefully to pure CPU paths—ensuring the deduplication stage remains both lightning-fast on modern hardware and fully portable when acceleration isn’t available.

'''

'''
ACTUAL CPU VERSION
**Usage (1 sentence)**
Invoke `python dedup_deep.py [--recompute-all] [--skip-photo] [--skip-video] --workers N --video-workers M` once the manifest is in place to do a *CPU-only* deep sweep that hashes every photo and video, clusters perceptually or byte-identical items, flags weaker duplicates, and rewrites `metadata_manifest.csv` while copying each group into `__DUPLICATE_GROUPS__` for visual audit.

---

### Tools / Technologies employed

| Layer                                        | Key components                                                                                              | Purpose                                                                         |
| -------------------------------------------- | ----------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------- |
| **Python 3.x std-lib**                       | `argparse`, `csv`, `pathlib`, `hashlib`, `concurrent.futures`, `subprocess`, `time`, `shutil`, `json`, `os` | CLI flags, manifest I/O, SHA-1 hashing, multiprocessing/threading orchestration |
| **Pillow (+ pillow\_heif)**                  | EXIF-aware image loading, truncated-JPEG fix, optional libheif HEIC decode                                  | Uniform RGB ingestion across legacy and modern formats                          |
| **OpenCV (CPU build)**                       | Frame seeking + extraction for videos, conversion to RGB                                                    | Fast 3-frame SHA-1 signature for large videos without full decode               |
| **FFmpeg fallback**                          | CLI pipe for HEIC → PNG when libheif fails                                                                  | Ensures HEIC support even without pillow\_heif                                  |
| **imagehash (pHash)**                        | 64-bit perceptual hash generation                                                                           | Detects visually identical/near-identical photos                                |
| **NumPy**                                    | Mean pixel-difference computation (guard-rail)                                                              | Verifies that two “duplicates” truly look alike                                 |
| **tqdm**                                     | Progress bars                                                                                               | Real-time feedback on long-running batches                                      |
| **ThreadPoolExecutor / ProcessPoolExecutor** | Parallel image hashing (threads) and video hashing (processes)                                              | Utilises all CPU cores without GPU dependencies                                 |
| **WSL-aware path shim**                      | Converts `C:\…` to `/mnt/c/…` on the fly                                                                    | Makes one script portable across native Windows and WSL2                        |

---

### Idea summary (what it does & why it matters)

`dedup_deep.py` is the project’s deterministic, hardware-agnostic deduplication engine. For **images**, it computes both a raw-pixel SHA-1 (guaranteed exact match) and a 64-bit perceptual hash that tolerates tiny edits; for **videos**, it pulls three evenly-spaced frames, converts them to RGB, and SHA-1-hashes the PNG blobs—yielding a stable content fingerprint without expensive full-stream decoding.
Media sharing the same pHash/SHA-1 are grouped; a rule-based scorer (metadata present, filename “clean”, shortest variant) selects the *best candidate* and tags the rest with `delete_flag=true`, while a NumPy pixel-difference guard-rail (< 3.0 mean Δ) automatically clears shaky calls. Each group is mirrored into `__DUPLICATE_GROUPS__/group_####` for one-click visual review, and every decision is persisted back into `metadata_manifest.csv` (`dedup_group_id`, `duplicate_of`, `visual_review_path`, etc.). By staying strictly CPU-bound—yet saturating all cores via hybrid thread/process pools—it delivers repeatable, cross-platform dedup performance even on machines without CUDA, ensuring the broader pipeline can run anywhere from a bare-metal Windows box to WSL on a cloud VM.
'''
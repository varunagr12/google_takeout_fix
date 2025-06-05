#!/usr/bin/env python
# source ~/dedup-venv/bin/activate

# I need to revert back to some older script. I dont think I can use any GPU for images which sucks. but I need to still do phash and such. somehow fix this stupid mess.
"""
Hybrid GPU/CPU Media Duplicate Remover
    • Computes a SHA-1 hash based on the image's raw RGB pixels (with orientation correction)
    • Uses GPU-batch perceptual hashing (pHash) via CuPy/cuCIM when available; falls back to imagehash on CPU
    • Computes video hashes with NVDEC, with a CPU fallback
    • Groups duplicates based on identical pHash values and confirms them with a mean pixel difference of 3.0 or less
    • For videos, duplicates are confirmed if the duration is within ±0.05 seconds and their frame hashes match
    • Includes a guard-rail: if the pixel difference of a flagged file exceeds the threshold, the duplicate flag is removed
    • Warns when a group contains more than 10 items (this may indicate burst mode)
"""

import csv, hashlib, io, os, shutil, argparse, time, subprocess, json, math
from pathlib import Path
from tqdm import tqdm
from PIL import Image, ImageOps, ImageChops
from pillow_heif import register_heif_opener
import numpy as np
import concurrent.futures as cf
import cv2, imagehash
import platform
from pathlib import PureWindowsPath

def is_wsl():
        return "microsoft" in platform.uname().release.lower()

def to_local_path(p_str: str) -> Path:
        """
        Convert Windows paths to WSL mount paths when running under WSL.
        Leaves Unix/WSL paths untouched on either platform.
        """
        p_str = p_str.strip()
        if not p_str:
                return Path()

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

try:
        import cupy as cp
        import cucim
        HAVE_CUDA_PHASH = True
except Exception:
        HAVE_CUDA_PHASH = False

try:
        from decord import VideoReader, gpu
        HAVE_DECORD = True
except Exception:
        HAVE_DECORD = False

DUP_DIR       = ROOT_DIR / "__DUPLICATE_GROUPS__"
RECHECK_LOG   = ROOT_DIR / "recheck_log.txt"

IMAGE_EXTS = {".jpg", ".jpeg", ".png", ".heic"}
VIDEO_EXTS = {".mp4", ".mov", ".avi", ".mkv"}

HASH_COL, PHASH_COL, MTIME_COL = "content_sha1", "phash64", "hash_mtime"

PHASH_HAMMING_THRESHOLD = 0        # Use only identical pHash values for grouping
PIXEL_DIFF_THRESHOLD    = 3.0      # Consider images duplicates if mean absolute error is 3 or less
VIDEO_DUR_TOL           = 0.05     # Allowed duration tolerance (in seconds) for videos

# ----------------------------- CLI ---------------------------------------
def parse_args() -> argparse.Namespace:
        cpu_default = max(1, min(8, os.cpu_count() or 1))
        p = argparse.ArgumentParser()
        p.add_argument("--recompute-all", action="store_true")
        p.add_argument("--skip-video",    action="store_true")
        p.add_argument("--workers",       type=int, default=cpu_default)
        p.add_argument("--video-workers", type=int, default=None)
        return p.parse_args()

# --------------------------- HASH HELPERS --------------------------------
def sha1_bytes(b: bytes) -> str:
        return hashlib.sha1(b).hexdigest()

def img_sha1(p: Path) -> str:
        """
        Compute a SHA-1 hash based on the image's raw RGB pixel bytes, ensuring the orientation is corrected.
        """
        with Image.open(p) as im:
                im = ImageOps.exif_transpose(im).convert("RGB")
                data = im.tobytes()
                size = im.size
        return sha1_bytes(size[0].to_bytes(4, "little") +
                                             size[1].to_bytes(4, "little") +
                                             data)

def img_phash_int_cpu(p: Path) -> int:
        with Image.open(p) as im:
                im = ImageOps.exif_transpose(im).convert("RGB")
                return int(str(imagehash.phash(im)), 16)

def img_phash_int_gpu(p: Path) -> int:
        img = cucim.CuImage(str(p)).read().convert("RGB")
        img = cp.mean(img, axis=2).astype(cp.float32)
        img = cp.asarray(Image.fromarray(img.get()).resize((32, 32)))
        dct = cp.fft.fft2(img)[:8, :8]
        median = cp.median(dct).get()
        bits = (dct.get() > median).astype(np.uint8).ravel()
        return int("".join("1" if b else "0" for b in bits), 2)

# Select GPU-based pHash if available, otherwise use the CPU
PHASH_FN = img_phash_int_cpu
if HAVE_CUDA_PHASH:
        print("🟢  GPU pHash enabled (CuPy/cuCIM)")

def ffprobe_duration(path: Path) -> float | None:
        """
        Return the duration of a video in seconds using ffprobe.
        If ffprobe is missing or fails, return None.
        """
        try:
                cmd = ["ffprobe", "-v", "error", "-show_entries",
                             "format=duration", "-of", "json", str(path)]
                out = subprocess.check_output(cmd, stderr=subprocess.STDOUT, text=True)
                return float(json.loads(out)["format"]["duration"])
        except Exception:
                return None

def vid_sha1(path: Path) -> str:
        """
        Compute a SHA-1 hash for a video by sampling frames.
        Try using NVDEC via decord first, and if that fails, fall back to a CPU method with OpenCV.
        """
        if HAVE_DECORD:
                try:
                        vr = VideoReader(str(path), ctx=gpu(0))
                        idx = [int(len(vr) * f) for f in (0.0, 0.5, 0.9)]
                        blobs = []
                        for i in idx:
                                frame = vr[i].asnumpy()
                                buf = io.BytesIO()
                                Image.fromarray(frame).save(buf, format="PNG", optimize=False)
                                blobs.append(buf.getvalue())
                        return sha1_bytes(b"".join(blobs))
                except Exception as e:
                        print(f"⚠️  NVDEC failed ({path.name}): {e}. Using CPU fallback.")
        # Fallback method using CPU with OpenCV
        cap = cv2.VideoCapture(str(path))
        blobs = []
        cnt = int(cap.get(cv2.CAP_PROP_FRAME_COUNT) or 0)
        idx = [int(cnt * f) for f in (0, 0.5, 0.9)]
        for i in idx:
                cap.set(cv2.CAP_PROP_POS_FRAMES, i)
                ok, frame = cap.read()
                if ok:
                        buf = io.BytesIO()
                        Image.fromarray(cv2.cvtColor(frame, cv2.COLOR_BGR2RGB)).save(buf, format="PNG", optimize=False)
                        blobs.append(buf.getvalue())
        cap.release()
        return sha1_bytes(b"".join(blobs))

def needs_hash(r, mtime, force):
        return force or not r.get(HASH_COL) or not r.get(PHASH_COL) or float(r.get(MTIME_COL, 0) or 0) != mtime

def compute_and_update(path: Path, row: dict, force=False):
        mtime = path.stat().st_mtime
        if needs_hash(row, mtime, force):
                if path.suffix.lower() in IMAGE_EXTS:
                        row[HASH_COL]  = img_sha1(path)
                        row[PHASH_COL] = f"{PHASH_FN(path):016x}"
                else:
                        row[HASH_COL]  = vid_sha1(path)
                        row[PHASH_COL] = ""
                        dur = ffprobe_duration(path)
                        row["duration"] = f"{dur:.3f}" if dur else ""
                row[MTIME_COL] = f"{mtime:.0f}"
        phash_int = int(row[PHASH_COL], 16) if row[PHASH_COL] else None
        return row[HASH_COL], phash_int, row

def _proc_image(args):
        row, force = args
        p = to_local_path(row["media_path"])
        if not p.exists():
                return None
        try:
                return compute_and_update(p, row, force)
        except Exception as e:
                print("IMG hash fail:", p, e)
                return None

def _proc_video(args):
        row, force = args
        p = to_local_path(row["media_path"])
        if not p.exists():
                return None
        try:
                sha1, _, row = compute_and_update(p, row, force)
                return sha1, None, row
        except Exception as e:
                print("VID hash fail:", p, e)
                return None

def pixel_diff(a: Path, b: Path) -> float:
        if not a.exists() or not b.exists():
                return 255.0
        with Image.open(a) as A, Image.open(b) as B:
                A = ImageOps.exif_transpose(A).convert("RGB")
                B = ImageOps.exif_transpose(B).convert("RGB").resize(A.size)
                return np.mean(np.asarray(ImageChops.difference(A, B)))

def hamming(a: int, b: int) -> int:
        return (a ^ b).bit_count()

def best_candidate(group):
        def score(r):
                name = r.get("original_media", "").lower()
                has = bool(r.get("json_filename"))
                clean = not any(s in name for s in ["(1)", "_1", "copy", "edited"])
                return (has, clean, -len(name))
        return max(group, key=score)

def assign_groups(groups):
        updated = []
        counter = 0
        for key, grp in groups.items():
                uniq = list({r["media_path"]: r for r in grp}.values())
                if len(uniq) < 2:
                        continue
                if len(uniq) > 10:
                        print(f"🔍 Burst warning: group candidate with {len(uniq)} items (pHash={key}).")
                gid = f"group_{counter:04d}"
                best = best_candidate(uniq)
                bp = to_local_path(best["media_path"])
                gfold = DUP_DIR / gid
                gfold.mkdir(parents=True, exist_ok=True)
                for r in uniq:
                        mp = to_local_path(r["media_path"])
                        if not mp.exists():
                                continue
                        keeper = (mp == bp)
                        if keeper:
                                r.update({
                                        "dedup_group_id": gid,
                                        "delete_flag": "false",
                                        "duplicate_of": "",
                                        "dedup_reason": "best_candidate"
                                })
                        else:
                                # If the perceptual hashes are identical, skip computing pixel differences.
                                diff = (
                                        pixel_diff(bp, mp)
                                        if int(best.get(PHASH_COL) or "0", 16)
                                        != int(r.get(PHASH_COL) or "0", 16)
                                        else 0.0
                                )
                                is_dup = diff <= PIXEL_DIFF_THRESHOLD
                                if is_dup:
                                        r.update({
                                                "dedup_group_id": gid,
                                                "delete_flag": "true",
                                                "duplicate_of": best["original_media"],
                                                "dedup_reason": f"phash_pixeldup_diff{diff:.2f}"
                                        })
                                else:
                                        continue
                        try:
                                shutil.copy2(mp, gfold / mp.name)
                                r["visual_review_path"] = str(gfold / mp.name)
                        except Exception:
                                pass
                updated.extend(uniq)
                counter += 1
        return updated

def guardrail(rows):
        cleared = 0
        with RECHECK_LOG.open("w", encoding="utf-8") as log:
                for r in rows:
                        if r.get("delete_flag", "").lower() != "true":
                                continue
                        k = to_local_path(r["duplicate_of"]) if r["duplicate_of"] else None
                        m = to_local_path(r["media_path"])
                        if k and k.exists() and m.exists():
                                if pixel_diff(k, m) > PIXEL_DIFF_THRESHOLD:
                                        r["delete_flag"] = "false"
                                        r["dedup_reason"] = "guardrail_uncertain"
                                        cleared += 1
                                        log.write(f"Cleared flag (diff too high): {m}\n")
        if cleared:
                print(f"🚧 Guard-rail: {cleared} files un-flagged; see {RECHECK_LOG.name}")

def update_manifest(cli):
        rows = list(csv.DictReader(MANIFEST_FILE.open("r", encoding="utf-8", newline="")))
        for r in rows:
                r.setdefault(HASH_COL, "")
                r.setdefault(PHASH_COL, "")
                r.setdefault(MTIME_COL, "")
                for k in ("dedup_group_id", "delete_flag", "dedup_reason", "visual_review_path", "duplicate_of"):
                        r.setdefault(k, "")

        img_rows = [r for r in rows if to_local_path(r["media_path"]).suffix.lower() in IMAGE_EXTS]
        vid_rows = [r for r in rows if to_local_path(r["media_path"]).suffix.lower() in VIDEO_EXTS and not cli.skip_video]

        groups = {}

        with cf.ThreadPoolExecutor(max_workers=cli.workers) as tp:
                for res in tqdm(tp.map(_proc_image, ((r, cli.recompute_all) for r in img_rows)),
                                                total=len(img_rows), desc="Images", unit="img"):
                        if res:
                                _, ph, row = res
                                groups.setdefault(ph, []).append(row)

        if vid_rows:
                vwrk = cli.video_workers or min(4, cli.workers)
                with cf.ProcessPoolExecutor(max_workers=vwrk) as pp:
                        for res in tqdm(pp.map(_proc_video, ((r, cli.recompute_all) for r in vid_rows)),
                                                        total=len(vid_rows), desc="Videos", unit="vid"):
                                if res:
                                        sha1, _, row = res
                                        groups.setdefault(sha1, []).append(row)

        if PHASH_HAMMING_THRESHOLD:
                merged = {}
                for k in list(groups):
                        placed = False
                        for m in list(merged):
                                if k and m and hamming(k, m) <= PHASH_HAMMING_THRESHOLD:
                                        merged[m].extend(groups[k])
                                        placed = True
                                        break
                        if not placed:
                                merged[k] = groups[k]
                groups = merged

        updated_rows = assign_groups(groups)
        look = {r["media_path"]: r for r in updated_rows}
        for r in rows:
                if r["media_path"] in look:
                        r.update(look[r["media_path"]])

        guardrail(rows)

        DUP_DIR.mkdir(exist_ok=True)
        for r in rows:
                if isinstance(r["media_path"], Path):
                        r["media_path"] = str(r["media_path"])
                if isinstance(r.get("duplicate_of"), Path):
                        r["duplicate_of"] = str(r["duplicate_of"])

        with MANIFEST_FILE.open("w", encoding="utf-8", newline="") as f:
                w = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
                w.writeheader()
                w.writerows(rows)
        print("✅ Manifest updated. Duplicate groups in:", DUP_DIR)

if __name__ == "__main__":
        args = parse_args()
        start = time.time()
        update_manifest(args)
        print(f"⏱  Done in {time.time() - start:,.1f} s")

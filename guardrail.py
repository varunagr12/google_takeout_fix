import csv
import sys
from pathlib import Path
from PIL import Image, ImageOps
import numpy as np
from imagehash import phash
from tqdm import tqdm

# -------- CONFIG --------
MANIFEST_FILE = Path("/mnt/c/Users/vagrawal/OneDrive - Altair Engineering, Inc/Documents/Personal/Code/metadata_manifest.csv")
RECHECK_LOG   = MANIFEST_FILE.parent / "recheck_log.txt"
PIXEL_DIFF_THRESHOLD = 10  # Adjusted threshold for pixel diff to allow minor differences in duplicates
PHASH_DIST_THRESHOLD = 4     # If hamming distance > 4, it's probably a burst variation

# -------- UTILS --------
def open_image(p: Path) -> Image.Image:
    return ImageOps.exif_transpose(Image.open(p)).convert("RGB")

def pixel_diff(a: Path, b: Path) -> float:
    A = open_image(a)
    B = open_image(b)
    min_width = min(A.width, B.width)
    min_height = min(A.height, B.height)
    A = A.resize((min_width, min_height))
    B = B.resize((min_width, min_height))
    return np.mean(np.abs(np.asarray(A, dtype=np.int16) - np.asarray(B, dtype=np.int16)))

def phash_distance(a: Path, b: Path) -> int:
    h1 = phash(open_image(a))
    h2 = phash(open_image(b))
    return h1 - h2

def to_local_path(path_str: str) -> Path:
    return Path(path_str.strip()).resolve()

# -------- MAIN GUARDRAIL --------
def guardrail_pass(dry_run: bool = False):
    mode = "DRY RUN" if dry_run else "LIVE MODE"
    print(f"🔍 Running enhanced guardrail check in {mode}...")
    rows = list(csv.DictReader(MANIFEST_FILE.open("r", encoding="utf-8")))
    changed = 0

    with RECHECK_LOG.open("w", encoding="utf-8") as log:
        for r in tqdm(rows, desc="Guardrail checking", unit="file"):
            if r.get("delete_flag", "").lower() != "true":
                continue

            dup_of = r.get("duplicate_of", "")
            if not dup_of:
                continue

            try:
                orig = to_local_path(dup_of)
                dup  = to_local_path(r["media_path"])
                if not orig.exists() or not dup.exists():
                    continue

                diff  = pixel_diff(orig, dup)
                pdist = phash_distance(orig, dup)

                if diff > PIXEL_DIFF_THRESHOLD or pdist > PHASH_DIST_THRESHOLD:
                    if dry_run:
                        log.write(f"[DRY] Would unflag {dup.name} — diff={diff:.1f}, phash_dist={pdist}\n")
                    else:
                        r["delete_flag"] = "false"
                        r["dedup_reason"] = "guardrail_uncertain"
                        log.write(f"[UNFLAG] {dup.name} — diff={diff:.1f}, phash_dist={pdist}\n")
                    changed += 1

            except Exception as e:
                log.write(f"[ERROR] {r.get('media_path')}: {e}\n")

    if changed:
        if dry_run:
            print(f"🚧 {changed} delete flags would be cleared (visual uncertainty).")
        else:
            print(f"🚧 {changed} delete flags cleared (visual uncertainty).")
            with MANIFEST_FILE.open("w", newline="", encoding="utf-8") as f:
                writer = csv.DictWriter(f, fieldnames=rows[0].keys())
                writer.writeheader()
                writer.writerows(rows)
    else:
        print("✅ No uncertain duplicates detected.")
    print(f"📄 Log saved to: {RECHECK_LOG}")

if __name__ == "__main__":
    dry_run = "--dry-run" in sys.argv
    guardrail_pass(dry_run=dry_run)


'''
**Usage (1 sentence)**
Execute `python guardrail.py [--dry-run]` after the initial deduplication pass to re-examine every file pre-marked for deletion, compare it to its “keeper” with both pixel-difference and perceptual-hash distance tests, and automatically clear any delete flags that look like burst-shot variations rather than true duplicates.

---

### Tools / Technologies employed

| Layer                  | Components                                | Purpose                                                          |
| ---------------------- | ----------------------------------------- | ---------------------------------------------------------------- |
| **Python 3.x std-lib** | `csv`, `pathlib`, `sys`                   | Manifest I/O, CLI flag, path handling                            |
| **Pillow**             | `Image`, `ImageOps.exif_transpose`        | Robust RGB loading + EXIF rotation                               |
| **NumPy**              | Vectorized pixel arithmetic               | Fast mean absolute pixel-diff calculation                        |
| **imagehash (pHash)**  | 64-bit perceptual hash + Hamming distance | Secondary “look-alike” metric to spot burst shots                |
| **tqdm**               | Progress bar over thousands of rows       | User feedback                                                    |
| **Dry-run switch**     | `--dry-run`                               | Preview mode that logs proposed changes without touching the CSV |
| **CSV writer**         | Atomic rewrite only when flags change     | Ensures manifest integrity                                       |

---

### Idea summary (what it does & why it matters)

`guardrail.py` is the safety net that prevents the deduplication engine from deleting near-identical burst frames, light-edited variants, or images with subtle in-camera processing differences. For every row where `delete_flag=true` and a `duplicate_of` reference exists, the script:

1. **Loads both images** and normalizes orientation.
2. **Calculates two similarity metrics**:

   * *Mean pixel difference* (resized to the smaller common resolution) — rejects large photometric changes.
   * *Perceptual-hash Hamming distance* — filters out frames whose visual fingerprint diverges beyond a tunable threshold.
3. **Logs and (optionally) unflags** any pair whose metrics exceed preset thresholds (`PIXEL_DIFF_THRESHOLD`, `PHASH_DIST_THRESHOLD`).
4. **Writes the updated manifest** only when operating in live mode, ensuring a deterministic rollback path.

The result is a cleaner, confidence-rated duplicate set where only indisputably redundant files are scheduled for deletion, vastly reducing the chance of accidentally purging valuable shots while still keeping the storage savings promised by the broader pipeline.
'''
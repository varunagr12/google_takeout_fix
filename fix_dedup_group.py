import csv
from pathlib import Path
from collections import defaultdict

# --- Config ---
DRY_RUN = False  # Set to False to apply changes
MANIFEST_PATH = Path(r"/mnt/c/Users/vagrawal/OneDrive - Altair Engineering, Inc/Documents/Personal/Code/metadata_manifest.csv")

# --- Helper: Score function to decide best candidate ---
def score(row):
    name = row.get("original_media", "").lower()
    has_json = bool(row.get("json_filename"))
    clean_name = not any(x in name for x in ["(1)", "_1", "copy", "edited"])
    return (has_json, clean_name, -len(name))

# --- Load CSV ---
with MANIFEST_PATH.open("r", encoding="utf-8", newline="") as f:
    reader = csv.DictReader(f)
    rows = list(reader)

# --- Group rows by dedup_group_id ---
groups = defaultdict(list)
for row in rows:
    gid = row.get("dedup_group_id", "").strip()
    if gid:
        groups[gid].append(row)

# --- Fix groups with multiple best candidates ---
fix_count = 0
for gid, group in groups.items():
    bests = [r for r in group if r.get("dedup_reason", "").strip() == "best_candidate"]
    if len(bests) <= 1:
        continue  # skip if OK

    fix_count += 1
    print(f"⚠️  Group {gid} has {len(bests)} best candidates — fixing...")

    # Pick the true best
    true_best = max(group, key=score)
    true_best_path = true_best.get("original_media", "").strip()

    for r in group:
        if r is true_best:
            r["delete_flag"] = "FALSE"
            r["dedup_reason"] = "best_candidate"
            r["duplicate_of"] = ""
        else:
            r["delete_flag"] = "TRUE"
            r["dedup_reason"] = "phash"
            r["duplicate_of"] = true_best_path

if DRY_RUN:
    print(f"✅ Dry run: {fix_count} groups would have been updated.")
else:
    # --- Save corrected CSV ---
    with MANIFEST_PATH.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=rows[0].keys())
        writer.writeheader()
        writer.writerows(rows)
    print(f"✅ Done. Fixed {fix_count} groups with multiple best candidates.")

'''
**Usage (1 sentence)**
Run `python quick_fix_dedup.py` (toggle `DRY_RUN = True/False`) after a manifest verification to automatically resolve any duplicate-groups that accidentally contain more than one “best candidate,” ensuring each group has exactly one keeper and correctly flagged duplicates.

---

### Tools / Technologies employed

* **Python 3.x** standard library only – `csv` for reading/writing the manifest, `pathlib` for cross-OS paths, and `collections.defaultdict` for in-memory grouping.
* Simple **inline score heuristic** — prioritises rows that have JSON metadata, a “clean” filename, and shorter length to pick the canonical file.
* **Dry-run flag** for safe preview of changes before committing.

---

### Idea summary (what it does & why it matters)

`quick_fix_dedup.py` is an automated patcher that cleans up logical mistakes introduced during deduplication. It groups manifest rows by `dedup_group_id`, identifies cases where multiple files are tagged as `best_candidate`, and then recalculates a single *true best* using a deterministic score: presence of JSON metadata → filename cleanliness → shortest name. That row retains `delete_flag=FALSE`, while every other member is flipped to `delete_flag=TRUE`, `dedup_reason=phash`, and `duplicate_of=<canonical path>`. With an instant CSV rewrite (or a harmless dry-run report), the script guarantees consistency—one keeper per group—before any irreversible delete operation is triggered, eliminating another manual quality-control step in the pipeline.
'''
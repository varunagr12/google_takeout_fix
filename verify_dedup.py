import csv
from collections import defaultdict

MANIFEST_FILE = "metadata_manifest.csv"

def check_dedup_groups(manifest_path):
    with open(manifest_path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        rows = [row for row in reader if row.get("deletion_status", "").strip().lower() != "deleted"]

    groups = defaultdict(list)

    # Group by dedup_group_id
    for row in rows:
        group_id = row.get("dedup_group_id", "").strip()
        if group_id:
            groups[group_id].append(row)

    issues_found = 0
    for group_id, group_rows in groups.items():
        bests = [r for r in group_rows if r.get("delete_flag", "").lower() == "false"]
        dups = [r for r in group_rows if r.get("delete_flag", "").lower() == "true"]

        if len(bests) != 1:
            print(f"❌ Group {group_id} has {len(bests)} best candidates (expected exactly 1).")
            issues_found += 1

        if len(group_rows) < 2:
            print(f"⚠️ Group {group_id} has less than 2 members — not a valid duplicate group?")
            issues_found += 1

    if issues_found == 0:
        print("✅ All groups have exactly one best candidate.")
    else:
        print(f"⚠️ Verification complete: {issues_found} issues found.")

if __name__ == "__main__":
    check_dedup_groups(MANIFEST_FILE)

'''
**Usage (1 sentence)**
Run `python verify_dedup.py` after any deduplication or guard-rail pass to sanity-check that every `dedup_group_id` in `metadata_manifest.csv` contains at least two members and **exactly one** “best” copy (i.e., `delete_flag=false`) before you proceed to irreversible deletes.

---

### Tools / Technologies employed

* **Python 3.x** standard library only – `csv` for manifest reading, `collections.defaultdict` for fast in-memory grouping, lightweight CLI via the `__main__` guard.
* **Unicode console emojis / print statements** – human-readable issue reporting without external dependencies.

---

### Idea summary (what it does & why it matters)

`verify_dedup.py` acts as the final integrity gate in the deduplication pipeline. It loads the manifest, ignores rows already physically deleted, groups remaining records by `dedup_group_id`, and flags three critical anomalies:

1. **Multiple or zero “keepers”** – if a group has anything other than one `delete_flag=false`, it means the earlier scoring logic failed to pick a single canonical file.
2. **Singleton groups** – a “duplicate” set with fewer than two files is suspicious and likely an error.
3. **Aggregate issue count** – the script prints a concise summary so you know whether it’s safe to trigger the deletion script or if further review is needed.

By catching logical inconsistencies early, this verifier prevents catastrophic mass-deletes and ensures the manifest’s dedup decisions are internally coherent before any destructive operation is executed.
'''
import csv
from collections import defaultdict

MANIFEST_FILE = "metadata_manifest.csv"

def check_dedup_groups(manifest_path):
    with open(manifest_path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        rows = list(reader)

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

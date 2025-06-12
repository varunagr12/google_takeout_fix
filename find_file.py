import os
import sys
import zipfile
from pathlib import Path

SEARCH_LOCATIONS = [
    Path("D:\\"),
    Path(r"C:\Users\vagrawal\OneDrive - Altair Engineering, Inc\Documents\Personal\Pictures\Processing")
]

def search_file(pattern, exact=False):
    pattern = pattern.lower()
    found_paths = []

    for base_dir in SEARCH_LOCATIONS:
        print(f"🔍 Searching in: {base_dir}")
        for root, dirs, files in os.walk(base_dir):
            for name in files:
                name_lower = name.lower()
                full_path = os.path.join(root, name)

                if (name_lower == pattern) if exact else (pattern in name_lower):
                    found_paths.append(full_path)

                if name_lower.endswith(".zip"):
                    try:
                        with zipfile.ZipFile(full_path, 'r') as zipf:
                            for zipinfo in zipf.infolist():
                                zipname = zipinfo.filename.lower()
                                if (zipname == pattern) if exact else (pattern in zipname):
                                    found_paths.append(f"{full_path} ▶ {zipinfo.filename}")
                    except zipfile.BadZipFile:
                        print(f"⚠️ Skipped corrupted zip: {full_path}")

    return found_paths

def main():
    if len(sys.argv) < 2:
        print("Usage: python find_file.py <pattern> [--exact]")
        return

    pattern = sys.argv[1]
    exact = '--exact' in sys.argv

    results = search_file(pattern, exact=exact)

    if results:
        print(f"\n✅ Found {len(results)} result(s):")
        for path in results:
            print(path)
    else:
        print("\n❌ File not found in any specified location.")

if __name__ == '__main__':
    main()

'''
**Usage (1 sentence)**
Run `python find_file.py <substring_or_filename> [--exact]` to recursively search both the external `D:\` drive and the main *Processing* tree—plus the contents of every ZIP it encounters—for any file whose name matches (or exactly equals) the given pattern.

---

### Tools / Technologies employed

* **Python 3.x standard library** – `os.walk` for directory traversal, `pathlib` for OS-agnostic paths, `zipfile` for reading archive contents, and `sys` for minimal CLI parsing.
* **Windows-aware search roots** – pre-defined list of absolute paths so the script can sweep a whole drive and your curated photo archive in one pass.
* **Corrupted-ZIP handling** – graceful skip with a warning to avoid crashes when an archive is damaged.

---

### Idea summary (what it does & why it matters)

`find_file.py` is a Swiss-army locator that answers “Where did that file go?” across millions of photos and archives. By checking both live directories **and** drilling into every `.zip`, it can track down originals, renamed copies, or stray metadata without unpacking everything first. The optional `--exact` flag switches between substring discovery and strict filename equality, while clear emoji-tagged logs show progress and alert you to unreadable archives. This quick command-line utility saves minutes (or hours) of manual digging whenever a file referenced in the manifest or log can’t be found, tightening the feedback loop during debugging and data recovery.
'''
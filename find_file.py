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

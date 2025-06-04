import os
import shutil
from pathlib import Path

# --- Setting Up Configuration ---
# Define the main directory for processing files.
PROCESSING_DIR = Path(r"C:\Users\vagrawal\OneDrive - Altair Engineering, Inc\Documents\Personal\Pictures\Processing")
# Directory to hold JSON files that didn’t have a matching entry.
UNMATCHED_JSON_DIR = PROCESSING_DIR / "__UNMATCHED_JSON__"
# Directory to hold media files that didn’t have a matching entry.
UNMATCHED_MEDIA_DIR = PROCESSING_DIR / "__UNMATCHED_MEDIA__"
# Suffix used to rebuild the original folder structure during restoration.
RESTORE_SUFFIX = Path("Takeout/Google Photos")

# --- Function to Create a Collision-Free Restore Path ---
def get_safe_restore_path(dest_path):
    # Use the desired destination if it doesn't already exist.
    if not dest_path.exists():
        return dest_path
    # If the path exists, append '_restored' and additional numbers if necessary.
    stem = dest_path.stem + "_restored"
    suffix = dest_path.suffix
    parent = dest_path.parent
    candidate = parent / f"{stem}{suffix}"
    i = 1
    while candidate.exists():
        candidate = parent / f"{stem}_{i}{suffix}"
        i += 1
    return candidate

# --- Function to Restore Files from Unmatched Folders ---
def restore_files_from_unmatched(unmatched_dir, processing_root, restore_suffix):
    # If the unmatched directory isn't there, notify and exit.
    if not unmatched_dir.exists():
        print(f"Directory not found: {unmatched_dir}")
        return

    # Walk through every folder and file inside the unmatched directory.
    for folder, _, files in os.walk(unmatched_dir):
        for file in files:
            src_path = Path(folder) / file
            rel_path = src_path.relative_to(unmatched_dir)  # e.g., Z008/Photos from 2017/...

            # Expect at least two elements: one for the identifier (like Z008) and the rest of the path.
            parts = rel_path.parts
            if len(parts) < 2:
                print(f"Skipping: Unexpected structure {src_path}")
                continue

            # The first element (like Z008) remains separate from the rest of the original path.
            z_folder = parts[0]
            rest = Path(*parts[1:])

            # Rebuild the intended destination in the structure:
            # processing_root/Z008/Takeout/Google Photos/<rest>
            intended_dest = processing_root / z_folder / restore_suffix / rest
            final_dest = get_safe_restore_path(intended_dest)

            # Ensure the destination directory exists before moving the file.
            final_dest.parent.mkdir(parents=True, exist_ok=True)
            try:
                shutil.move(str(src_path), str(final_dest))
                print(f"Restored: {src_path} -> {final_dest}")
            except Exception as e:
                print(f"Error restoring {src_path}: {e}")

# --- Main Process Execution ---
def main():
    # Restore any unmatched JSON files.
    print("Restoring unmatched JSON files...")
    restore_files_from_unmatched(UNMATCHED_JSON_DIR, PROCESSING_DIR, RESTORE_SUFFIX)

    # Restore any unmatched media files.
    print("Restoring unmatched media files...")
    restore_files_from_unmatched(UNMATCHED_MEDIA_DIR, PROCESSING_DIR, RESTORE_SUFFIX)

    # Remove the unmatched directories after processing.
    print("Cleanup: Removing empty unmatched directories...")
    for dir_path in [UNMATCHED_JSON_DIR, UNMATCHED_MEDIA_DIR]:
        try:
            shutil.rmtree(dir_path)
            print(f"Deleted: {dir_path}")
        except Exception as e:
            print(f"Failed to delete {dir_path}: {e}")

if __name__ == '__main__':
    main()

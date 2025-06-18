import subprocess
from pathlib import Path

TARGET_DIR = Path(r"/mnt/c/Users/vagrawal/OneDrive - Altair Engineering, Inc/Documents/Personal/Pictures/Processing/__test_files__")
MEDIA_EXTS = {
    # photos
    '.jpg', '.gif', '.png', '.heic', '.jpeg', '.tif', '.webp', '.jfif', '.tiff',
    # videos
    '.3gp', '.avi', '.mpg', '.mov', '.mp4', '.mts',
    # others
    '.thm', '.dng', '.nef'
}

def extract_metadata(file_path: Path):
    try:
        result = subprocess.run(
            ['exiftool', str(file_path)],
            stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True
        )
        output = result.stdout
        lines = output.splitlines()
        wanted = {
            "Track Create Date": None,
            "Media Create Date": None,
            "Date/Time Original": None
        }

        for line in lines:
            for key in wanted:
                if line.strip().startswith(key):
                    wanted[key] = line.split(":", 1)[1].strip()

        print(f"\n📄 {file_path.name}")
        for k, v in wanted.items():
            if v:
                print(f"   {k}: {v}")

    except Exception as e:
        print(f"❌ Error reading {file_path.name}: {e}")

def main():
    for f in sorted(TARGET_DIR.glob("*")):
        if f.suffix.lower() in MEDIA_EXTS and f.is_file():
            extract_metadata(f)

if __name__ == "__main__":
    main()

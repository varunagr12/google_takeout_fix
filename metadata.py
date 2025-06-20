import subprocess
import re
from tqdm import tqdm
from pathlib import Path
import csv

def estimate_total_from_csv(csv_path: Path) -> int:
    with csv_path.open('r', encoding='utf-8') as f:
        return sum(1 for _ in f) - 1  # subtract header

def exiftool_times():
    times_csv = Path("times.csv").resolve()
    input_dir = Path("/mnt/c/Users/vagrawal/OneDrive - Altair Engineering, Inc/Documents/Personal/Pictures/Processing/__test_files__").resolve()

    total_estimate = estimate_total_from_csv(times_csv)

    cmd = [
        "exiftool",
        "-progress",
        "-r",
        "-overwrite_original",
        f"-csv={str(times_csv)}",
        str(input_dir)
    ]

    proc = subprocess.Popen(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1
    )

    bar = tqdm(total=total_estimate, desc="ExifTool Updating", unit="files", dynamic_ncols=True)
    progress_re = re.compile(r'\[(\d+)/(\d+)\]$')

    summary_lines = []
    last_count = 0

    for raw_line in proc.stdout:
        line = raw_line.strip()

        match = progress_re.search(line)
        if match:
            current = int(match.group(1))
            bar.update(current - last_count)
            last_count = current
        elif any(word in line for word in ("directories", "files updated", "skipped", "error", "warning")):
            summary_lines.append(line)

    proc.wait()
    bar.close()

    print("\n📋 ExifTool Summary:")
    for l in summary_lines:
        print("  " + l)

if __name__ == "__main__":
    exiftool_times()

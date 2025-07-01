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
    input_dir = Path("/mnt/c/Users/vagrawal/OneDrive - Altair Engineering, Inc/Documents/Personal/Pictures/Processing/").resolve()

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

# **Usage (1 sentence)**
# Run `python metadata.py` to apply all timestamps in `times.csv` to the actual media files inside *Processing*, using ExifTool in recursive mode with live progress tracking.

# ---

# ### Tools / Technologies employed

# | Layer               | Components                                        | Purpose                                                                                 |
# | ------------------ | ------------------------------------------------- | --------------------------------------------------------------------------------------- |
# | **Python 3.x std-lib** | `subprocess`, `re`, `csv`, `pathlib`              | Shell interaction, parsing progress, resolving file paths, counting media from CSV     |
# | **ExifTool (CLI)** | `-csv=times.csv`, `-r`, `-overwrite_original`, etc | Inject timestamps into EXIF and QuickTime metadata headers                             |
# | **tqdm**            | Progress bar                                       | Tracks and displays real-time progress while ExifTool runs                             |
# | **Regex**           | `\[N/M\]` pattern                                  | Parses live output from ExifTool to update the progress bar dynamically                |

# ---

# ### Idea summary (what it does & why it matters)

# `metadata.py` is a lightweight, terminal-driven wrapper around the final timestamp-application step in the pipeline:

# 1. **Estimates workload** – Parses `times.csv` to determine the number of entries for progress tracking.
# 2. **Spawns ExifTool** – Runs it with `-csv=times.csv -r -overwrite_original`, applying timestamps to every file listed.
# 3. **Tracks live progress** – Uses regex on ExifTool’s verbose output to show a true progress bar.
# 4. **Summarizes outcome** – Collects and displays useful end-of-run stats like directories scanned, files updated, skipped, or errored.

# Without this script, `exiftool -csv=times.csv …` works silently and slowly. This wrapper brings **visibility, reliability, and accountability** to the most delicate operation—*modifying media metadata in-place*. It's the final checkpoint before declaring the library archive-ready.

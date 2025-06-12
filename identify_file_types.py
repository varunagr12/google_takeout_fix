import pandas as pd
import os

def identify_file_types(csv_path, column_name='new_ext'):
    # Read the CSV file
    try:
        df = pd.read_csv(csv_path)
    except Exception as e:
        print(f"Error reading CSV file: {e}")
        return None
    
    # Check if column exists
    if column_name not in df.columns:
        print(f"Column '{column_name}' not found in CSV. Available columns: {', '.join(df.columns)}")
        return None
    
    # Get unique file extensions
    unique_extensions = df[column_name].dropna().unique()
    
    # Define common photo and video extensions
    photo_extensions = {
        '.jpg', '.jpeg', '.png', '.gif', '.bmp', '.tiff', '.tif', '.webp', 
        '.raw', '.heic', '.heif', '.jfif'
    }
    
    video_extensions = {
        '.mp4', '.avi', '.mov', '.wmv', '.flv', '.mkv', '.webm', '.m4v', 
        '.3gp', '.mpg', '.mpeg', '.mts'
    }
    
    # Sort extensions into categories
    photos = []
    videos = []
    others = []
    
    for ext_value in unique_extensions:
        # Ensure the extension starts with a dot
        ext = str(ext_value).lower()
        if not ext.startswith('.'):
            ext = '.' + ext
            
        if ext in photo_extensions:
            photos.append(ext)
        elif ext in video_extensions:
            videos.append(ext)
        else:
            others.append(ext)
    
    return {
        'photos': photos,
        'videos': videos,
        'others': others
    }

def main():
    # Hardcoded CSV path for WSL environment
    csv_path = "/mnt/c/Users/vagrawal/OneDrive - Altair Engineering, Inc/Documents/Personal/Code/metadata_manifest.csv"
    
    # Process the CSV file
    result = identify_file_types(csv_path)
    if not result:
        return
    
    # Print results
    print(f"\nFound {len(result['photos'])} photo files:")
    for photo in result['photos']:
        print(f"  {photo}")
    
    print(f"\nFound {len(result['videos'])} video files:")
    for video in result['videos']:
        print(f"  {video}")
    
    if result['others']:
        print(f"\nFound {len(result['others'])} other files:")
        for other in result['others']:
            print(f"  {other}")

if __name__ == "__main__":
    main()

'''
**Usage (1 sentence)**
Run `python identify_file_types.py` to scan the `new_ext` column of `metadata_manifest.csv`, list every distinct file extension it contains, and classify them into *photos*, *videos*, and *others* for a quick sanity-check of your dataset’s media mix.

---

### Tools / Technologies employed

| Layer                   | Components                   | Purpose                                                         |
| ----------------------- | ---------------------------- | --------------------------------------------------------------- |
| **pandas**              | `read_csv`, Series filtering | Fast CSV ingestion and unique-value extraction                  |
| **Python std-lib**      | `os`, built-in set logic     | Path handling, extension categorization                         |
| **Hard-coded WSL path** | `/mnt/c/Users/vagrawal/...`  | Ensures the script points at the canonical manifest inside WSL2 |

---

### Idea summary (what it does & why it matters)

`identify_file_types.py` offers a one-shot inventory of the formats still present in your curated archive. By reading the manifest’s `new_ext` column (populated during earlier conversion steps), it tallies which extensions are recognized as photo formats (JPEG, HEIC, RAW, etc.), video formats (MP4, MOV, MTS, etc.), or miscellaneous “other” types that may need special handling. The concise report helps you verify that unusual extensions (e.g., stray `.dng` after a failed conversion) are either gone or explicitly accounted for, and provides a neat metric for the white-paper on how diverse the original dump was versus the streamlined, standardized result.
'''
"""Inspect merged CSV for duplicate rows per case and print case dir contents for offenders."""
from pathlib import Path
import pandas as pd

OUT = Path('data/output_csv')
CSV = OUT / 'all_cases_original.csv'

if not CSV.exists():
    print('No merged CSV found at', CSV)
    raise SystemExit(1)

print('Reading', CSV)
df = pd.read_csv(CSV)

# Normalize FileName as string
if 'FileName' not in df.columns:
    print('No FileName column in merged CSV')
    raise SystemExit(1)

counts = df.groupby(['ProjectID', 'FileName']).size().reset_index(name='count')
mult = counts[counts['count'] > 1].sort_values('count', ascending=False)

if mult.empty:
    print('No duplicate FileName entries per ProjectID found in merged CSV')
else:
    print('Duplicates found (ProjectID, FileName, count):')
    print(mult.to_string(index=False))

# Also check FileName duplicates ignoring ProjectID
fn_counts = df['FileName'].value_counts()
dups = fn_counts[fn_counts > 1]
if dups.empty:
    print('\nNo duplicate FileName values overall')
else:
    print('\nFileName values appearing multiple times:')
    for fn, c in dups.items():
        print(f"{fn}: {c}")
    # inspect first offender
    first_fn = dups.index[0]
    print('\nInspecting case directory for first offender:', first_fn)
    stem = Path(first_fn).stem
    cand_dirs = list(Path('data/dicom_cases').glob(f"{stem}*"))
    if cand_dirs:
        for d in cand_dirs:
            if d.is_dir():
                files = list(d.rglob('*'))
                print(f"Directory: {d} -> {len([f for f in files if f.is_file()])} files")
                print('Top-level files:', [p.name for p in sorted(d.iterdir()) if p.is_file()][:50])
    else:
        print('No candidate case directory found for', stem)

# Also report how many rows per ProjectID
pid_counts = df['ProjectID'].value_counts().sort_values(ascending=False)
print('\nRows per ProjectID (top 20):')
print(pid_counts.head(20).to_string())

print('\nFinished inspection')

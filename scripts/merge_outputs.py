import pandas as pd
from pathlib import Path

OUT = Path('data/output_csv')
FIXED_COLUMNS = [
    "ProjectID",
    "FileName",
    "PatientName",
    "PatientID",
    "StudyDate",
    "PatientBirthDate",
    "PatientAge",
    "PatientSex",
    "StudyInstanceUID",
    "SeriesInstanceUID",
    "Modality",
    "Manufacturer",
    "Rows",
    "Columns",
    "ImageCount",
    "SeriesCount",
]

# collect original per-case CSVs (exclude merged files)
orig_files = sorted([p for p in OUT.glob('*.csv') if p.name.endswith('.desensitized.csv') is False and not p.name.startswith('all_cases_')])
des_files = sorted([p for p in OUT.glob('*.desensitized.csv')])

if orig_files:
    dfs = []
    for f in orig_files:
        try:
            dfs.append(pd.read_csv(f))
        except Exception as e:
            print('Failed to read', f, e)
    if dfs:
        big = pd.concat(dfs, ignore_index=True)
        for c in FIXED_COLUMNS:
            if c not in big.columns:
                big[c] = None
        big = big.reindex(columns=FIXED_COLUMNS)
        big.to_csv(OUT / 'all_cases_original.csv', index=False)
        print('Wrote', OUT / 'all_cases_original.csv')
else:
    print('No original per-case CSVs found')

if des_files:
    dfs = []
    for f in des_files:
        try:
            dfs.append(pd.read_csv(f))
        except Exception as e:
            print('Failed to read', f, e)
    if dfs:
        big = pd.concat(dfs, ignore_index=True)
        for c in FIXED_COLUMNS:
            if c not in big.columns:
                big[c] = None
        big = big.reindex(columns=FIXED_COLUMNS)
        big.to_csv(OUT / 'all_cases_desensitized.csv', index=False)
        print('Wrote', OUT / 'all_cases_desensitized.csv')
else:
    print('No desensitized per-case CSVs found')

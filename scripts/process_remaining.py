"""Process only cases that do not yet have a per-case CSV in data/output_csv.
This skips cases already processed and continues until all are done.
"""
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.dcm_extractor.extractor import extract_case_metadata, iter_case_dirs


def main():
    data_root = Path('data/dicom_cases')
    out_dir = Path('data/output_csv')
    out_dir.mkdir(parents=True, exist_ok=True)

    cases = list(iter_case_dirs(data_root))
    total = len(cases)
    print(f"Found {total} cases; checking for missing outputs...")

    missing = []
    for case in cases:
        csv_path = out_dir / f"{case.name}.csv"
        if not csv_path.exists():
            missing.append(case)

    print(f"Missing cases: {len(missing)}")

    succeeded = []
    failed = []

    for idx, case in enumerate(missing, start=1):
        print(f"Processing missing ({idx}/{len(missing)}): {case.name}")
        try:
            # project_id will be assigned based on full ordering of cases
            # compute project_id as position in full sorted list
            project_id = [c.name for c in cases].index(case.name) + 1
            extract_case_metadata(case, out_dir, desensitize=False, project_id=project_id)
            # also write desensitized copy here to match extractor behaviour
            import pandas as pd
            csv_path = out_dir / f"{case.name}.csv"
            if csv_path.exists():
                df = pd.read_csv(csv_path)
                des_df = df.copy()
                if 'PatientName' in des_df.columns:
                    from src.dcm_extractor.extractor import desensitize_name
                    des_df['PatientName'] = des_df['PatientName'].apply(lambda v: desensitize_name(v) if v else v)
                des_df.to_csv(out_dir / f"{case.name}.desensitized.csv", index=False)
            succeeded.append(case.name)
            print(f"  OK: {case.name}")
        except Exception as e:
            print(f"  ERROR: {case.name} -> {e}")
            failed.append(case.name)

    print("\nSummary:")
    print(f"  Newly succeeded: {len(succeeded)}")
    print(f"  Failed: {len(failed)}")
    if failed:
        print('Failed cases:\n' + '\n'.join(failed))

    return 0


if __name__ == '__main__':
    raise SystemExit(main())

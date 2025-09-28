"""Process all case subdirectories using the extractor module, assigning ProjectID sequentially.
This script is tolerant to errors and will continue if a case fails.
"""
import sys
from pathlib import Path
import logging

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
    print(f"Found {total} cases to process")

    succeeded = []
    failed = []

    for idx, case in enumerate(cases, start=1):
        print(f"Processing ({idx}/{total}): {case}")
        try:
            extract_case_metadata(case, out_dir, desensitize=False, project_id=idx)
            succeeded.append(case.name)
            print(f"  OK: wrote {out_dir / (case.name + '.csv')}")
        except Exception as e:
            print(f"  ERROR processing {case}: {e}")
            failed.append(case.name)

    print("\nSummary:")
    print(f"  Succeeded: {len(succeeded)}")
    print(f"  Failed: {len(failed)}")
    if failed:
        print("Failed cases:\n" + '\n'.join(failed))

    return 0


if __name__ == '__main__':
    raise SystemExit(main())

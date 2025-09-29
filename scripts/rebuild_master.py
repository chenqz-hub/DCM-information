#!/usr/bin/env python3
"""Rebuild the master merged CSVs from processed/dicom_cases folders.

Usage: python scripts/rebuild_master.py [--backup] [--parallel N] [--dry-run]

Options:
  --backup           Create timestamped backups of existing master CSVs before overwriting.
  --parallel N       Number of worker processes to use. Default 1 (no parallelism).
  --dry-run          Do not write CSVs, just report what would be processed and any missing cases.
  --projectid-map    Path to the case->ProjectID JSON (default data/output_csv/case_projectid_map.json)
  --out-dir          Output directory for CSVs (default data/output_csv)

This script uses the project's extractor module: src/dcm_extractor/extractor.py
"""

import argparse
import json
import os
import shutil
import time
from functools import partial
from multiprocessing import Pool

import pandas as pd


def find_case_path(case_name, processed_dir, dicom_dir):
    candidates = [
        os.path.join(processed_dir, case_name),
        os.path.join(processed_dir, case_name + '.dir'),
        os.path.join(processed_dir, case_name + '.zip'),
        os.path.join(dicom_dir, case_name),
        os.path.join(dicom_dir, case_name + '.dir'),
        os.path.join(dicom_dir, case_name + '.zip'),
    ]
    for c in candidates:
        if os.path.exists(c):
            return c
    # fuzzy search in processed
    if os.path.isdir(processed_dir):
        for entry in os.listdir(processed_dir):
            if case_name in entry:
                return os.path.join(processed_dir, entry)
    return None


def process_case(pid, case_name, processed_dir, dicom_dir, out_dir):
    """Process a single case and return (pid, case_name, (df_orig, df_des), error).

    This function imports the extractor module locally so it can be used inside
    multiprocessing child processes on Windows (avoids pickling module objects).
    """
    path = find_case_path(case_name, processed_dir, dicom_dir)
    if not path:
        return pid, case_name, None, 'missing'
    try:
        # Import extractor inside worker to avoid passing module objects to multiprocessing
        import importlib, sys
        root = os.getcwd()
        if os.path.join(root, 'src') not in sys.path:
            sys.path.insert(0, os.path.join(root, 'src'))
        extractor = importlib.import_module('dcm_extractor.extractor')

        df_orig = extractor.extract_case_metadata(path, out_dir=out_dir, desensitize=False, project_id=pid, only_merged=True)
        df_des = extractor.extract_case_metadata(path, out_dir=out_dir, desensitize=True, project_id=pid, only_merged=True)
        # ensure ProjectID column
        if df_orig is not None and not df_orig.empty:
            df_orig['ProjectID'] = pid
        if df_des is not None and not df_des.empty:
            df_des['ProjectID'] = pid
        return pid, case_name, (df_orig, df_des), None
    except Exception as e:
        return pid, case_name, None, str(e)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--backup', action='store_true', help='Backup existing master CSVs before overwriting')
    parser.add_argument('--parallel', type=int, default=1, help='Number of parallel workers (default 1)')
    parser.add_argument('--dry-run', action='store_true', help="Don't write CSVs; just report what's missing")
    parser.add_argument('--projectid-map', default=os.path.join('data', 'output_csv', 'case_projectid_map.json'))
    parser.add_argument('--out-dir', default=os.path.join('data', 'output_csv'))
    args = parser.parse_args()

    root = os.getcwd()
    processed_dir = os.path.join(root, 'data', 'processed')
    dicom_dir = os.path.join(root, 'data', 'dicom_cases')
    out_dir = args.out_dir
    os.makedirs(out_dir, exist_ok=True)


    # load mapping
    with open(args.projectid_map, 'r', encoding='utf-8') as f:
        mapping = json.load(f)
    proj_to_case = {int(v): k for k, v in mapping.items()}

    # backup
    orig_csv = os.path.join(out_dir, 'all_cases_original.csv')
    des_csv = os.path.join(out_dir, 'all_cases_desensitized.csv')
    timestamp = time.strftime('%Y%m%d_%H%M%S')
    if args.backup:
        for p in (orig_csv, des_csv):
            if os.path.exists(p):
                shutil.copy2(p, p + f'.bak.{timestamp}')

    cases = sorted(proj_to_case.items(), key=lambda x: x[0])

    # Build argument list for workers: (pid, case_name, processed_dir, dicom_dir, out_dir)
    case_args = [(pid, case_name, processed_dir, dicom_dir, out_dir) for pid, case_name in cases]

    results = []
    if args.parallel and args.parallel > 1:
        with Pool(args.parallel) as pool:
            # Use starmap to call process_case(pid, case_name, processed_dir, dicom_dir, out_dir)
            results = pool.starmap(process_case, case_args)
    else:
        results = [process_case(pid, case_name, processed_dir, dicom_dir, out_dir) for pid, case_name in cases]

    rows = []
    des_rows = []
    missing = []
    errors = []

    for pid, case_name, data, err in results:
        if err:
            if err == 'missing':
                missing.append((pid, case_name))
            else:
                errors.append((pid, case_name, err))
            continue
        df_orig, df_des = data
        if df_orig is None or df_orig.empty:
            missing.append((pid, case_name))
        else:
            rows.append(df_orig)
        if df_des is None or df_des.empty:
            # allow original present but des missing (unlikely)
            pass
        else:
            des_rows.append(df_des)

    if args.dry_run:
        print('DRY RUN:')
        print('total_cases', len(cases))
        print('missing_count', len(missing))
        print('errors_count', len(errors))
        if missing:
            print('missing_list', missing)
        if errors:
            print('errors_list', errors)
        return

    if not rows:
        print('No rows extracted; aborting')
        return

    big = pd.concat(rows, ignore_index=True)
    big_des = pd.concat(des_rows, ignore_index=True) if des_rows else None

    # coerce and sort
    big['ProjectID'] = pd.to_numeric(big['ProjectID'], errors='coerce')
    big = big.sort_values(by='ProjectID', na_position='last')
    big.to_csv(orig_csv, index=False)
    if big_des is not None:
        big_des['ProjectID'] = pd.to_numeric(big_des['ProjectID'], errors='coerce')
        big_des = big_des.sort_values(by='ProjectID', na_position='last')
        big_des.to_csv(des_csv, index=False)

    print('WROTE', orig_csv, des_csv if big_des is not None else '(no desensitized rows)')
    print('SUMMARY: total', len(cases), 'written', len(big), 'missing', len(missing), 'errors', len(errors))
    if missing:
        print('MISSING_LIST', missing)
    if errors:
        print('ERRORS_LIST', errors)


if __name__ == '__main__':
    main()

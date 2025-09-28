"""Process each case directory with a per-case timeout and write merged CSVs only.

This script imports functions from the extractor module and runs each case in a child
process (multiprocessing) to allow per-case timeouts. Successful case DataFrames are
collected and concatenated into merged CSV outputs.
"""
from __future__ import annotations

import argparse
import logging
import multiprocessing as mp
import os
import sys
import tempfile
from pathlib import Path

import pandas as pd

# ensure src is importable
ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from dcm_extractor import extractor  # type: ignore

LOGGER = logging.getLogger("process_cases_timeout")


def _worker(case_dir: str, out_dir: str, project_id: int, tmp_csv: str) -> None:
    try:
        df = extractor.extract_case_metadata(Path(case_dir), Path(out_dir), desensitize=False, project_id=project_id, only_merged=True)
        if isinstance(df, pd.DataFrame):
            df.to_csv(tmp_csv, index=False)
    except Exception:
        LOGGER.exception("Worker failed for case: %s", case_dir)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Process cases with per-case timeout and produce merged CSVs only")
    parser.add_argument("--data-root", "-d", required=True)
    parser.add_argument("--out", "-o", default="data/output_csv")
    parser.add_argument("--timeout", "-t", type=int, default=300, help="Per-case timeout in seconds")
    parser.add_argument("--move-top-level-zips", action="store_true")
    args = parser.parse_args(argv)

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    data_root = Path(args.data_root)
    out_dir = Path(args.out)
    out_dir.mkdir(parents=True, exist_ok=True)

    if args.move_top_level_zips:
        try:
            extractor.move_top_level_zips(data_root)
        except Exception:
            LOGGER.exception("Failed moving top-level zips")

    merged_parts = []

    cases = list(extractor.iter_case_dirs(data_root))
    for idx, case in enumerate(cases, start=1):
        LOGGER.info("Processing case %d/%d: %s", idx, len(cases), case)
        with tempfile.NamedTemporaryFile(delete=False, suffix=".csv", prefix=f"case_{idx}_", dir=out_dir) as tf:
            tmp_path = Path(tf.name)

        p = mp.Process(target=_worker, args=(str(case), str(out_dir), idx, str(tmp_path)))
        p.start()
        p.join(args.timeout)
        if p.is_alive():
            LOGGER.warning("Timeout reached for case %s (pid %s); terminating", case, p.pid)
            p.terminate()
            p.join()
            try:
                tmp_path.unlink(missing_ok=True)
            except Exception:
                pass
            continue

        # worker finished; read tmp csv if produced
        if tmp_path.exists():
            try:
                df = pd.read_csv(tmp_path)
                merged_parts.append(df)
            except Exception:
                LOGGER.exception("Failed to read temporary CSV for case: %s", case)
            finally:
                try:
                    tmp_path.unlink(missing_ok=True)
                except Exception:
                    pass

    if not merged_parts:
        LOGGER.info("No case produced data; exiting")
        return 0

    big = pd.concat(merged_parts, ignore_index=True)
    for c in extractor.FIXED_COLUMNS:
        if c not in big.columns:
            big[c] = None
    big = big.reindex(columns=extractor.FIXED_COLUMNS)

    big_path = out_dir / "all_cases_original.csv"
    big.to_csv(big_path, index=False)
    LOGGER.info("Wrote merged CSV (original): %s", big_path)

    try:
        des_big = big.copy()
        if "PatientName" in des_big.columns:
            des_big["PatientName"] = des_big["PatientName"].apply(lambda v: extractor.desensitize_name(v) if v else v)
        des_path = out_dir / "all_cases_desensitized.csv"
        des_big.to_csv(des_path, index=False)
        LOGGER.info("Wrote merged CSV (desensitized): %s", des_path)
    except Exception:
        LOGGER.exception("Failed to write desensitized merged CSV")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())

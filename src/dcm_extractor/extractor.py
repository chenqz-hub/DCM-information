"""Extract DICOM metadata from case folders and write per-file CSVs."""
from __future__ import annotations

import argparse
import csv
import logging
import tempfile
import zipfile
from pathlib import Path
from typing import Iterable, Dict, Any

import pydicom
import pandas as pd
import shutil
import hashlib
import json


LOGGER = logging.getLogger("dcm_extractor")


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


def read_dicom_metadata(dcm_path: Path) -> Dict[str, Any]:
    """Read a DICOM file and return a flat dict of selected metadata.

    The function extracts common tags; missing tags will be present with None.
    """
    ds = pydicom.dcmread(str(dcm_path), stop_before_pixels=True, force=True)
    # Some DICOM fields (like PatientName) are specialized types; cast to str when present
    patient_name = getattr(ds, "PatientName", None)
    if patient_name is not None:
        try:
            patient_name = str(patient_name)
        except Exception:
            patient_name = None

    def parse_age(age_val: Any, birth: Any, study: Any) -> Any:
        # If age_val is like '043Y' or '043', extract digits
        if age_val:
            try:
                s = str(age_val)
                digits = "".join(ch for ch in s if ch.isdigit())
                if digits:
                    return int(digits)
            except Exception:
                pass

        # fallback: compute from birth and study dates if possible (YYYYMMDD)
        if birth and study:
            try:
                from datetime import datetime

                b = datetime.strptime(str(birth), "%Y%m%d")
                s = datetime.strptime(str(study), "%Y%m%d")
                years = s.year - b.year - ((s.month, s.day) < (b.month, b.day))
                return years
            except Exception:
                pass

        return None

    fields = {
        "FileName": dcm_path.name,
        "PatientName": patient_name,
        "PatientID": getattr(ds, "PatientID", None),
        "PatientBirthDate": getattr(ds, "PatientBirthDate", None),
        # normalize PatientAge to integer where possible
        "PatientAge": None,
        "PatientSex": getattr(ds, "PatientSex", None),
        "StudyInstanceUID": getattr(ds, "StudyInstanceUID", None),
        "SeriesInstanceUID": getattr(ds, "SeriesInstanceUID", None),
        "StudyDate": getattr(ds, "StudyDate", None),
        "Modality": getattr(ds, "Modality", None),
        "Manufacturer": getattr(ds, "Manufacturer", None),
        "Rows": getattr(ds, "Rows", None),
        "Columns": getattr(ds, "Columns", None),
    }
    # compute normalized age
    raw_age = getattr(ds, "PatientAge", None)
    birth = fields.get("PatientBirthDate")
    study_date = getattr(ds, "StudyDate", None)
    fields["PatientAge"] = parse_age(raw_age, birth, study_date)

    return fields


def desensitize_name(name: Any) -> Any:
    if not name:
        return name
    try:
        s = str(name)
        h = hashlib.sha256(s.encode("utf-8")).hexdigest()
        return f"hash:{h[:16]}"
    except Exception:
        return None


def extract_case_metadata(case_dir: Path, out_dir: Path, desensitize: bool = False, project_id: int | None = None, only_merged: bool = False):
    """Traverse a case directory, read each DICOM file, and save a CSV with metadata.

    Args:
        case_dir: directory containing DICOM files (or nested folders).
        out_dir: directory to write the CSV file. CSV will be named same as case_dir.

    Returns:
        Path to the written CSV file.
    """
    case_dir = Path(case_dir)
    out_dir = Path(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    rows = []
    for path in case_dir.rglob("*"):
        if path.is_file():
            try:
                if path.suffix.lower() == ".zip":
                    # extract zip to a temp directory and aggregate contained DICOM files into one row
                    with tempfile.TemporaryDirectory() as td:
                        try:
                            with zipfile.ZipFile(path, "r") as zf:
                                zf.extractall(td)
                        except Exception as e:
                            LOGGER.warning("Failed to extract zip %s: %s", path, e)
                            continue

                        agg: dict[str, Any] = {}
                        image_count = 0
                        series_uids: set[str] = set()

                        for inner in Path(td).rglob("*"):
                            if inner.is_file():
                                try:
                                    meta = read_dicom_metadata(inner)
                                    image_count += 1
                                    # collect series uid
                                    sid = meta.get("SeriesInstanceUID")
                                    if sid:
                                        series_uids.add(sid)

                                    # for each field, keep first non-empty value
                                    for k, v in meta.items():
                                        if k == "FileName":
                                            continue
                                        if k not in agg or not agg[k]:
                                            agg[k] = v
                                except Exception as e:
                                    LOGGER.warning("Failed to read inner file %s in %s: %s", inner, path, e)

                        if image_count == 0:
                            LOGGER.info("No DICOM found in archive %s", path)
                            continue

                        # prepare aggregated row
                        agg_row: Dict[str, Any] = {
                            "ProjectID": project_id,
                            "FileName": path.name,
                            "PatientName": agg.get("PatientName"),
                            "PatientID": agg.get("PatientID"),
                            "PatientBirthDate": agg.get("PatientBirthDate"),
                            "PatientAge": agg.get("PatientAge"),
                            "PatientSex": agg.get("PatientSex"),
                            "StudyInstanceUID": agg.get("StudyInstanceUID"),
                            "SeriesInstanceUID": None,
                            "StudyDate": agg.get("StudyDate"),
                            "Modality": agg.get("Modality"),
                            "Manufacturer": agg.get("Manufacturer"),
                            "Rows": None,
                            "Columns": None,
                            "ImageCount": image_count,
                            "SeriesCount": len(series_uids),
                        }

                        # try to fill representative SeriesInstanceUID, Rows, Columns from agg
                        if agg.get("SeriesInstanceUID"):
                            agg_row["SeriesInstanceUID"] = agg.get("SeriesInstanceUID")
                        if agg.get("Rows"):
                            agg_row["Rows"] = agg.get("Rows")
                        if agg.get("Columns"):
                            agg_row["Columns"] = agg.get("Columns")

                        # do not mutate original metadata here; write desensitized copy later if requested

                        rows.append(agg_row)
                else:
                    meta = read_dicom_metadata(path)
                    # attach project id placeholder; actual project id set by caller
                    meta["ProjectID"] = project_id
                    rows.append(meta)
            except Exception as e:
                LOGGER.warning("Failed to read %s: %s", path, e)

    if not rows:
        LOGGER.info("No DICOM files found under %s", case_dir)

    df = pd.DataFrame(rows)

    # enforce column order: put StudyDate immediately after PatientID
    cols = list(df.columns)
    if "PatientID" in cols and "StudyDate" in cols:
        # build desired order
        desired = []
        for c in cols:
            if c == "PatientID":
                desired.append("PatientID")
                desired.append("StudyDate")
            elif c == "StudyDate":
                continue
            else:
                if c not in desired:
                    desired.append(c)
        # reorder DataFrame if possible
        try:
            df = df.reindex(columns=desired)
        except Exception:
            pass

    # ensure the final DataFrame has the fixed columns (add missing as None)
    for c in FIXED_COLUMNS:
        if c not in df.columns:
            df[c] = None

    # ensure ProjectID is first column
    if "ProjectID" in df.columns:
        cols = list(df.columns)
        if cols[0] != "ProjectID":
            cols.remove("ProjectID")
            cols.insert(0, "ProjectID")
            df = df.reindex(columns=cols)

    # reorder columns to the fixed template
    try:
        df = df.reindex(columns=FIXED_COLUMNS)
    except Exception:
        pass

    if not only_merged:
        csv_name = f"{case_dir.name}.csv"
        out_path = out_dir / csv_name
        df.to_csv(out_path, index=False)
        LOGGER.info("Wrote metadata CSV: %s", out_path)

        # also write a desensitized variant of the per-case CSV where PatientName is hashed
        try:
            des_df = df.copy()
            if "PatientName" in des_df.columns:
                des_df["PatientName"] = des_df["PatientName"].apply(lambda v: desensitize_name(v) if v else v)
            des_path = out_dir / f"{case_dir.name}.desensitized.csv"
            des_df.to_csv(des_path, index=False)
            LOGGER.info("Wrote desensitized CSV: %s", des_path)
        except Exception:
            LOGGER.exception("Failed to write desensitized CSV for case: %s", case_dir)

        return out_path

    # when only_merged is True, return the DataFrame for merging and do not write per-case CSVs
    return df


def iter_case_dirs(data_root: Path) -> Iterable[Path]:
    """Yield immediate subdirectories of data_root (each is a case)."""
    for p in sorted(Path(data_root).iterdir()):
        if p.is_dir():
            yield p


def move_top_level_zips(data_root: Path) -> None:
    """Move any .zip files directly under data_root into a subdirectory named after the zip (stem).

    If the subdirectory already exists, the zip will be moved into it. If a file with the
    same name exists in the destination, a numeric suffix will be appended to avoid
    overwriting.
    """
    data_root = Path(data_root)
    for z in sorted(data_root.glob("*.zip")):
        try:
            dest_dir = data_root / z.stem
            if dest_dir.exists() and not dest_dir.is_dir():
                LOGGER.warning("Skipping move: destination exists and is not a directory: %s", dest_dir)
                continue
            dest_dir.mkdir(parents=True, exist_ok=True)
            dest = dest_dir / z.name
            if dest.exists():
                # find a non-colliding filename
                base = z.stem
                ext = z.suffix
                i = 1
                while True:
                    candidate = dest_dir / f"{base}_{i}{ext}"
                    if not candidate.exists():
                        dest = candidate
                        break
                    i += 1

            shutil.move(str(z), str(dest))
            LOGGER.info("Moved top-level zip %s -> %s", z, dest)
        except Exception:
            LOGGER.exception("Failed to move top-level zip: %s", z)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Extract DICOM metadata to CSV per case")
    parser.add_argument("--data-root", "-d", required=True, help="Root folder containing case subfolders")
    parser.add_argument("--out", "-o", default="data/output_csv", help="Output folder for CSV files")
    parser.add_argument("--log", default="logs/extractor.log", help="Log file")
    parser.add_argument("--merge-all", action="store_true", help="Produce a merged CSV 'all_cases.csv' in the output folder")
    parser.add_argument("--export-json", action="store_true", help="Also export per-case JSON files alongside CSV")
    parser.add_argument("--desensitize", action="store_true", help="Desensitize PatientName by hashing before output")
    parser.add_argument(
        "--move-top-level-zips",
        action="store_true",
        help="If set, move any .zip files directly under the data root into per-case subdirectories before processing",
    )
    parser.add_argument(
        "--only-merged",
        action="store_true",
        help="Do not write per-case CSV files; only produce merged all_cases_original.csv and all_cases_desensitized.csv in the output folder",
    )
    args = parser.parse_args(argv)

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s", filename=args.log)
    logging.getLogger().addHandler(logging.StreamHandler())

    data_root = Path(args.data_root)
    out_dir = Path(args.out)

    if not data_root.exists():
        LOGGER.error("Data root does not exist: %s", data_root)
        return 2

    # optionally move any top-level zip files into per-case folders before processing
    if getattr(args, "move_top_level_zips", False) or getattr(args, "move-top-level-zips", False):
        try:
            move_top_level_zips(data_root)
        except Exception:
            LOGGER.exception("Failed during moving top-level zips")

    merged_rows = []
    # enumerate cases and assign sequential ProjectID starting at 1
    for idx, case in enumerate(iter_case_dirs(data_root), start=1):
        LOGGER.info("Processing case: %s", case)
        try:
            result = extract_case_metadata(
                case, out_dir, desensitize=args.desensitize, project_id=idx, only_merged=args.only_merged
            )

            # if only_merged, extract_case_metadata returns a DataFrame; otherwise it returns the CSV path
            if args.only_merged:
                if isinstance(result, pd.DataFrame):
                    merged_rows.append(result)
                else:
                    LOGGER.warning("Expected DataFrame for merging but got: %s", type(result))
            else:
                csv_path = result
                if args.export_json:
                    # also write JSON of the CSV rows for this case
                    try:
                        df = pd.read_csv(csv_path)
                        json_path = out_dir / f"{case.name}.json"
                        df.to_json(json_path, orient="records", force_ascii=False)
                    except Exception:
                        LOGGER.exception("Failed to write JSON for case: %s", case)

                if args.merge_all:
                    try:
                        df = pd.read_csv(csv_path)
                        merged_rows.append(df)
                    except Exception:
                        LOGGER.exception("Failed to read CSV for merging for case: %s", case)
        except Exception:
            LOGGER.exception("Failed processing case: %s", case)

    if args.merge_all and merged_rows:
        try:
            big = pd.concat(merged_rows, ignore_index=True)
            # ensure fixed columns
            for c in FIXED_COLUMNS:
                if c not in big.columns:
                    big[c] = None
            big = big.reindex(columns=FIXED_COLUMNS)

            # write original merged CSV
            big_path = out_dir / "all_cases_original.csv"
            big.to_csv(big_path, index=False)
            LOGGER.info("Wrote merged CSV (original): %s", big_path)

            # write desensitized merged CSV
            try:
                des_big = big.copy()
                if "PatientName" in des_big.columns:
                    des_big["PatientName"] = des_big["PatientName"].apply(lambda v: desensitize_name(v) if v else v)
                des_path = out_dir / "all_cases_desensitized.csv"
                des_big.to_csv(des_path, index=False)
                LOGGER.info("Wrote merged CSV (desensitized): %s", des_path)
            except Exception:
                LOGGER.exception("Failed to write desensitized merged CSV")
        except Exception:
            LOGGER.exception("Failed to write merged CSV")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())

import sys
import os
import shutil
import tempfile
from pathlib import Path
import pandas as pd
import pydicom
from pydicom.dataset import Dataset, FileMetaDataset

# Ensure repository root is on sys.path so `src` package is importable during tests
ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from src.dcm_extractor import extractor


def make_minimal_dcm(path: Path, patient_name: str = "John^Doe", patient_id: str = "ID123", study_date: str = "20200101"):
    ds = Dataset()
    ds.PatientName = patient_name
    ds.PatientID = patient_id
    ds.StudyDate = study_date
    ds.is_little_endian = True
    ds.is_implicit_VR = True

    file_meta = FileMetaDataset()
    file_meta.MediaStorageSOPClassUID = pydicom.uid.SecondaryCaptureImageStorage
    file_meta.MediaStorageSOPInstanceUID = pydicom.uid.generate_uid()
    file_meta.TransferSyntaxUID = pydicom.uid.ImplicitVRLittleEndian

    ds.file_meta = file_meta
    ds.save_as(str(path))


def test_desensitized_and_merged_outputs(tmp_path):
    # prepare two case directories
    data_root = tmp_path / "data_root"
    out_dir = tmp_path / "out"
    data_root.mkdir()
    out_dir.mkdir()

    case1 = data_root / "Case_A"
    case2 = data_root / "Case_B"
    case1.mkdir()
    case2.mkdir()

    # create a dcm in case1
    d1 = case1 / "img1.dcm"
    make_minimal_dcm(d1, patient_name="Alice^A", patient_id="A1", study_date="20210101")

    # create a dcm in case2
    d2 = case2 / "img2.dcm"
    make_minimal_dcm(d2, patient_name="Bob^B", patient_id="B2", study_date="20210202")

    # run extractor with merge and export json off (we just want CSVs)
    argv = ["--data-root", str(data_root), "--out", str(out_dir), "--merge-all"]
    rc = extractor.main(argv)
    assert rc == 0

    # check per-case files exist
    orig1 = out_dir / "Case_A.csv"
    des1 = out_dir / "Case_A.desensitized.csv"
    orig2 = out_dir / "Case_B.csv"
    des2 = out_dir / "Case_B.desensitized.csv"

    assert orig1.exists()
    assert des1.exists()
    assert orig2.exists()
    assert des2.exists()

    # check merged files
    merged_orig = out_dir / "all_cases_original.csv"
    merged_des = out_dir / "all_cases_desensitized.csv"
    assert merged_orig.exists()
    assert merged_des.exists()

    # verify ProjectID is first column in merged_orig
    df_orig = pd.read_csv(merged_orig)
    assert df_orig.columns[0] == 'ProjectID'

    # verify desensitization: PatientName in des file should be hashed
    df_des = pd.read_csv(merged_des)
    for name in df_des['PatientName'].dropna().astype(str):
        assert name.startswith('hash:') and len(name) > 6

    # verify original names present in original merged
    names = df_orig['PatientName'].dropna().astype(str).tolist()
    assert any('Alice' in n or 'Alice' == n for n in names)
    assert any('Bob' in n or 'Bob' == n for n in names)

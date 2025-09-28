import sys
import os
import shutil
from pathlib import Path

# Ensure repository root is on sys.path so `src` package is importable during tests
ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

import pydicom
from pydicom.dataset import Dataset, FileMetaDataset

from src.dcm_extractor.extractor import extract_case_metadata


def make_minimal_dicom(path: Path):
    file_meta = FileMetaDataset()
    file_meta.MediaStorageSOPClassUID = pydicom.uid.generate_uid()
    file_meta.MediaStorageSOPInstanceUID = pydicom.uid.generate_uid()
    file_meta.TransferSyntaxUID = pydicom.uid.ImplicitVRLittleEndian

    ds = Dataset()
    ds.file_meta = file_meta
    ds.PatientID = "TEST123"
    ds.StudyInstanceUID = pydicom.uid.generate_uid()
    ds.SeriesInstanceUID = pydicom.uid.generate_uid()
    ds.Modality = "MR"
    ds.Rows = 1
    ds.Columns = 1
    ds.is_little_endian = True
    ds.is_implicit_VR = True

    ds.save_as(str(path))


def test_extract_minimal(tmp_path: Path):
    case_dir = tmp_path / "caseA"
    case_dir.mkdir()
    dicom_path = case_dir / "img1.dcm"
    make_minimal_dicom(dicom_path)

    out_dir = tmp_path / "out"
    out_path = extract_case_metadata(case_dir, out_dir)

    assert out_path.exists()
    text = out_path.read_text()
    assert "PatientID" in text
    assert "FileName" in text

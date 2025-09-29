"""Quick debug runner to extract metadata for a single case and print the DataFrame or exception."""
from pathlib import Path
import sys
ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
from dcm_extractor import extractor
import argparse

parser = argparse.ArgumentParser()
parser.add_argument("case_dir")
parser.add_argument("--out", default="data/output_csv")
parser.add_argument("--projectid", type=int, default=None)
args = parser.parse_args()

case = Path(args.case_dir)
try:
    print(f"Debugging case: {case}")
    df = extractor.extract_case_metadata(case, Path(args.out), desensitize=False, project_id=args.projectid, only_merged=True)
    if df is None or df.empty:
        print("No metadata produced (empty DataFrame)")
    else:
        print(df.to_string(index=False))
except Exception as e:
    import traceback
    traceback.print_exc()
    print('Exception:', e)

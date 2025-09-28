<#
A convenience PowerShell script to setup venv (if needed), install dependencies,
and run the timeout-based processing with mapping. It prints a short summary at the end.
#>
param(
  [int]$Timeout = 300,
  [string]$DataRoot = "data/dicom_cases",
  [string]$OutDir = "data/output_csv",
  [string]$MapPath = "data/output_csv/case_projectid_map.json"
)

Write-Host "=== Run All: DCM extraction convenience script ==="

# Create venv if missing
if (-not (Test-Path .venv)) {
  Write-Host "Creating virtual environment .venv..."
  python -m venv .venv
}

Write-Host "Activating virtual environment..."
.\.venv\Scripts\Activate.ps1

Write-Host "Installing requirements (pip install -r requirements.txt)..."
pip install -r requirements.txt

Write-Host "Running timeout processor (timeout ${Timeout}s)..."
python scripts/process_cases_with_timeout.py -d $DataRoot -o $OutDir --timeout $Timeout --move-top-level-zips --projectid-map $MapPath

Write-Host "\n=== Summary ==="
if (Test-Path "$OutDir/all_cases_original.csv") {
  $origLines = (Get-Content "$OutDir/all_cases_original.csv").Length
  Write-Host "all_cases_original.csv lines: $origLines"
} else { Write-Host "No all_cases_original.csv produced" }

if (Test-Path "$OutDir/all_cases_desensitized.csv") {
  $desLines = (Get-Content "$OutDir/all_cases_desensitized.csv").Length
  Write-Host "all_cases_desensitized.csv lines: $desLines"
} else { Write-Host "No all_cases_desensitized.csv produced" }

if (Test-Path $MapPath) {
  Write-Host "ProjectID map: $MapPath"
} else { Write-Host "No projectid map found at $MapPath" }

Write-Host "=== Finished ==="

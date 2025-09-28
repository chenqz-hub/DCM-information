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

Write-Host "=== Interactive Run All: DCM extraction convenience script ==="

# discover cases and top-level zips
$dataRootPath = Resolve-Path $DataRoot -ErrorAction SilentlyContinue
if (-not $dataRootPath) {
    Write-Host "Data root not found: $DataRoot" -ForegroundColor Red
    exit 1
}

$cases = Get-ChildItem -Path $DataRoot -Directory -ErrorAction SilentlyContinue | Select-Object -ExpandProperty Name
$topZips = Get-ChildItem -Path $DataRoot -Filter *.zip -File -ErrorAction SilentlyContinue | Select-Object -ExpandProperty Name

Write-Host "Found $($cases.Count) case directories under $DataRoot"
if ($cases.Count -gt 0) { Write-Host ($cases | Select-Object -First 10) }
Write-Host "Found $($topZips.Count) top-level zip files under $DataRoot"
if ($topZips.Count -gt 0) { Write-Host ($topZips | Select-Object -First 10) }

$go = Read-Host "Proceed with processing these cases? (Y/N)"
if ($go -notin @('Y','y')) {
    Write-Host "Aborting run by user request." -ForegroundColor Yellow
    exit 0
}

# optionally allow changing timeout
$tInput = Read-Host "Per-case timeout in seconds (press Enter to keep $Timeout)"
if ($tInput -match '^[0-9]+$') { $Timeout = [int]$tInput }

# venv/install choices
$doVenv = $true
$venvChoice = Read-Host "Create/activate virtual env and install requirements? (Y/n)"
if ($venvChoice -in @('N','n')) { $doVenv = $false }

if ($doVenv) {
    if (-not (Test-Path .venv)) {
      Write-Host "Creating virtual environment .venv..."
      python -m venv .venv
    }
    Write-Host "Activating virtual environment..."
    .\.venv\Scripts\Activate.ps1
    Write-Host "Installing requirements (pip install -r requirements.txt)..."
    pip install -r requirements.txt
} else {
    Write-Host "Skipping venv creation/activation and dependency installation as requested." -ForegroundColor Cyan
}

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
  Write-Host "ProjectID map saved: $MapPath"
  Write-Host (Get-Content $MapPath -Raw)
} else { Write-Host "No projectid map found at $MapPath" }

Write-Host "=== Finished ==="

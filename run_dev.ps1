$ErrorActionPreference = "Stop"
$scriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
Set-Location $scriptDir

Set-Location backend
python -m venv .venv
. .\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
Start-Process powershell -ArgumentList "uvicorn app:app --reload"
Set-Location ..\frontend
npm install
Start-Process powershell -ArgumentList "npm run dev"
Start-Process "http://localhost:5173"

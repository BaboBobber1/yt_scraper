#!/usr/bin/env bash
set -euo pipefail
SCRIPT_DIR=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
cd "$SCRIPT_DIR"

if ! command -v python3 >/dev/null 2>&1; then
  echo "python3 is required" >&2
  exit 1
fi

cd backend
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
uvicorn app:app --reload &
BACKEND_PID=$!
cd ../frontend
npm install
npm run dev &
FRONTEND_PID=$!
sleep 2
if command -v xdg-open >/dev/null 2>&1; then
  xdg-open http://localhost:5173 >/dev/null 2>&1 || true
elif command -v open >/dev/null 2>&1; then
  open http://localhost:5173 >/dev/null 2>&1 || true
fi
wait $BACKEND_PID $FRONTEND_PID

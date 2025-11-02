# Crypto YouTube Harvester

A local-first toolkit for discovering and enriching public cryptocurrency YouTube channels. The project ships a FastAPI backend with SQLite persistence and a Vite + React dark-mode dashboard for controlling discovery, enrichment, and CSV exports.

## Requirements

- Python 3.10+
- Node.js 18+
- ffmpeg (for audio sampling)
- yt-dlp (installed via requirements)
- Whisper compatible hardware (CPU-only works, GPU recommended)

Optional:

- `YT_API_KEY` environment variable for YouTube Data API (subscriber counts)

## Getting started

### macOS / Linux

```bash
./run_dev.sh
```

### Windows (PowerShell)

```powershell
./run_dev.ps1
```

Both scripts will install dependencies, launch the FastAPI backend on `http://localhost:8000`, start the frontend dev server on `http://localhost:5173`, and open the dashboard in your default browser.

## Environment variables

Create a `.env` file in `backend/` to override defaults:

```
YT_API_KEY=
VIDEOS_PER_CHANNEL=1
SAMPLE_SECONDS=20
WHISPER_MODEL=small
RATE_SLEEP=1.0
```

## Features

- Keyword-driven discovery using yt-dlp's `ytsearch` backend
- Subscriber enrichment via YouTube Data API (optional) or public channel pages
- Language detection from automatic captions or 20-second Whisper transcription
- Email harvesting from channel about pages and latest video metadata
- Real-time progress log via Server-Sent Events
- Responsive dark-mode table with search, sorting, and CSV export (Google Sheets ready)

## CSV Export

Use the **Export CSV (Google Sheets)** button to download `channels_enriched.csv`. The file is UTF-8 encoded with comma delimiters for straightforward Google Sheets import.

## Logging

Backend progress events stream to the UI. Extend logging by capturing FastAPI logs or persisting events to `backend/logs/` as needed.

import os
import shutil
import tempfile
import time
from pathlib import Path
from typing import Optional, Tuple

import whisper
from yt_dlp import YoutubeDL

from .util import detect_language

DEFAULT_SAMPLE_SECONDS = int(os.getenv("SAMPLE_SECONDS", "20"))
DEFAULT_VIDEOS_PER_CHANNEL = int(os.getenv("VIDEOS_PER_CHANNEL", "1"))
WHISPER_MODEL = os.getenv("WHISPER_MODEL", "small")

_MODEL_CACHE = None


def ffmpeg_available() -> bool:
    return shutil.which("ffmpeg") is not None


def _latest_videos(channel_id: str, limit: int):
    channel_url = f"https://www.youtube.com/channel/{channel_id}/videos"
    ydl_opts = {
        "quiet": True,
        "skip_download": True,
        "extract_flat": "discard_in_playlist",
    }
    with YoutubeDL(ydl_opts) as ydl:
        info = ydl.extract_info(channel_url, download=False)
    entries = info.get("entries", []) if isinstance(info, dict) else []
    for entry in entries[:limit]:
        yield entry.get("id") or entry.get("url")


def _download_auto_subtitle(video_id: str, temp_dir: Path) -> Optional[str]:
    options = {
        "quiet": True,
        "skip_download": True,
        "writesubtitles": True,
        "writeautomaticsub": True,
        "subtitlesformat": "vtt",
        "outtmpl": str(temp_dir / "%(id)s.%(ext)s"),
    }
    with YoutubeDL(options) as ydl:
        info = ydl.extract_info(video_id, download=False)
        automatic = info.get("automatic_captions", {}) if isinstance(info, dict) else {}
        if not automatic:
            return None
    subtitle_files = list(temp_dir.glob(f"{video_id}.*"))
    for subtitle_file in subtitle_files:
        if subtitle_file.suffix.lower() in {".vtt", ".srt", ".ttml"}:
            try:
                return subtitle_file.read_text(encoding="utf-8", errors="ignore")
            except Exception:
                continue
    return None


def _download_audio_sample(video_id: str, temp_dir: Path, seconds: int) -> Optional[Path]:
    options = {
        "quiet": True,
        "format": "bestaudio/best",
        "outtmpl": str(temp_dir / "%(id)s.%(ext)s"),
        "postprocessors": [
            {
                "key": "FFmpegExtractAudio",
                "preferredcodec": "wav",
                "preferredquality": "192",
            }
        ],
    }
    with YoutubeDL(options) as ydl:
        info = ydl.extract_info(video_id, download=True)
        if not info:
            return None
    audio_file = next(temp_dir.glob(f"{video_id}*.wav"), None)
    if not audio_file:
        return None
    if seconds:
        clipped_path = temp_dir / f"{video_id}_sample.wav"
        os.system(
            f"ffmpeg -y -i \"{audio_file}\" -t {seconds} -acodec copy \"{clipped_path}\" >/dev/null 2>&1"
        )
        return clipped_path if clipped_path.exists() else audio_file
    return audio_file


def _load_model():
    global _MODEL_CACHE
    if _MODEL_CACHE is None:
        _MODEL_CACHE = whisper.load_model(WHISPER_MODEL)
    return _MODEL_CACHE


def _transcribe_audio(audio_path: Path) -> Optional[str]:
    try:
        model = _load_model()
    except Exception:
        return None
    try:
        result = model.transcribe(str(audio_path), fp16=False)
    except Exception:
        return None
    return result.get("text")


def detect_channel_language(channel_id: str, rate_sleep: float) -> Tuple[Optional[str], Optional[float], int]:
    temp_dir = Path(tempfile.mkdtemp(prefix="yt-lang-"))
    videos_sampled = 0
    try:
        for video_id in _latest_videos(channel_id, DEFAULT_VIDEOS_PER_CHANNEL):
            videos_sampled += 1
            subtitle_text = _download_auto_subtitle(video_id, temp_dir)
            if subtitle_text:
                lang, prob = detect_language(subtitle_text)
                if lang:
                    return lang, prob, videos_sampled
            if not ffmpeg_available():
                continue
            audio_path = _download_audio_sample(video_id, temp_dir, DEFAULT_SAMPLE_SECONDS)
            if not audio_path:
                continue
            transcript = _transcribe_audio(audio_path)
            if transcript:
                lang, prob = detect_language(transcript)
                if lang:
                    return lang, prob, videos_sampled
            time.sleep(rate_sleep)
    finally:
        shutil.rmtree(temp_dir, ignore_errors=True)
    return None, None, videos_sampled

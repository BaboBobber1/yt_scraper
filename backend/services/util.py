import asyncio
import random
import re
import time
from typing import Iterable, List

from langdetect import detect_langs


EMAIL_REGEX = re.compile(r"([a-zA-Z0-9_.+\-]+@[a-zA-Z0-9\-]+\.[a-zA-Z0-9\-.]+)")


def now_ts() -> int:
    return int(time.time())


def normalize_emails(emails: Iterable[str]) -> List[str]:
    cleaned = set()
    for email in emails:
        email = email.strip().lower()
        if email:
            cleaned.add(email)
    return sorted(cleaned)


def deobfuscate_email(text: str) -> str:
    replacements = {
        " [at] ": "@",
        " (at) ": "@",
        " at ": "@",
        "[at]": "@",
        "(at)": "@",
        " dot ": ".",
        " (dot) ": ".",
        "[dot]": ".",
        "(dot)": ".",
    }
    for key, value in replacements.items():
        text = text.replace(key, value)
    return text.replace("{dot}", ".")


def extract_emails(text: str) -> List[str]:
    text = deobfuscate_email(text)
    return normalize_emails(match.group(1) for match in EMAIL_REGEX.finditer(text))


async def rate_limited_sleep(rate_sleep: float) -> None:
    delay = max(0.0, rate_sleep + random.uniform(-0.2, 0.2))
    await asyncio.sleep(delay)


def detect_language(text: str):
    try:
        probs = detect_langs(text)
    except Exception:
        return None, None
    if not probs:
        return None, None
    top = probs[0]
    return top.lang, top.prob

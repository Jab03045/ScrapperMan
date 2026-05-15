"""
process.py — LinkedIn CSV processor for the Render pipeline.
Called by server.py after Apify finishes. Logs stream to the UI via SSE.
"""

import os
import json
import time
import logging
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable

import pandas as pd
import requests
from bs4 import BeautifulSoup

log = logging.getLogger("process")

# ---------------------------------------------------------------------------
# Exec identity map
# ---------------------------------------------------------------------------
EXEC_URL_MAP = {
    "arvindkrishna":   ("Arvind Krishna",   "Chairman, President & CEO"),
    "james-kavanaugh": ("James Kavanaugh",  "CFO"),
    "robthomas":       ("Rob Thomas",       "SVP Software & CCO"),
    "mohamad-ali":     ("Mohamad Ali",      "SVP Consulting"),
    "matthicks":       ("Matt Hicks",       "Red Hat CEO"),
    "jaygambetta":     ("Jay Gambetta",     "VP Quantum"),
    "skyla-loomis":    ("Skyla Loomis",     "VP Z & LinuxONE"),
    "nicklela":        ("Nickle LaMoreaux", "CHRO"),
    "riclewis":        ("Ric Lewis",        "SVP Infrastructure"),
    "jonathanadashek": ("Jonathan Adashek", "CMO"),
}

def resolve_author(author_name: str, linkedin_url: str) -> tuple[str, str]:
    if author_name and author_name.lower() not in ("unknown", ""):
        return author_name, ""
    for slug, (name, role) in EXEC_URL_MAP.items():
        if slug in (linkedin_url or "").lower():
            return name, role
    return author_name or "Unknown", ""


# ---------------------------------------------------------------------------
# Classification
# ---------------------------------------------------------------------------
def classify_row(row: pd.Series) -> str:
    is_repost = pd.notna(row.get("repostId"))
    if is_repost:
        if pd.notna(row.get("repost/postVideo/videoUrl")):
            return "REPOST_VIDEO"
        elif pd.notna(row.get("repost/article/link")):
            return "REPOST_ARTICLE"
        else:
            return "REPOST_TEXT"
    else:
        if pd.notna(row.get("postVideo/videoUrl")):
            return "POST_VIDEO"
        elif pd.notna(row.get("article/link")):
            return "POST_ARTICLE"
        else:
            return "POST_TEXT"


# ---------------------------------------------------------------------------
# Video transcription
# ---------------------------------------------------------------------------
def transcribe_video(video_url: str, groq_api_key: str, label: str) -> dict:
    headers = {
        "User-Agent": "Mozilla/5.0 (compatible; IBMPipeline/1.0)",
        "Referer": "https://www.linkedin.com/",
    }
    tmp_path = None
    try:
        with tempfile.NamedTemporaryFile(suffix=".mp4", delete=False) as tmp:
            tmp_path = tmp.name
            r = requests.get(video_url, headers=headers, stream=True, timeout=90)
            r.raise_for_status()
            size = 0
            for chunk in r.iter_content(chunk_size=1024 * 1024):
                tmp.write(chunk)
                size += len(chunk)

        from groq import Groq
        client = Groq(api_key=groq_api_key)
        with open(tmp_path, "rb") as f:
            response = client.audio.transcriptions.create(
                file=("video.mp4", f, "video/mp4"),
                model="whisper-large-v3",
                response_format="verbose_json",
                timestamp_granularities=["segment"],
            )

        text = response.text or ""
        segments = [
            {"start": round(s.start, 2), "end": round(s.end, 2), "text": s.text.strip()}
            for s in (response.segments or [])
        ]
        duration = segments[-1]["end"] if segments else 0
        return {
            "transcript":    text,
            "segments":      segments,
            "duration_secs": round(duration, 1),
            "status":        "transcribed",
            "error":         None,
        }
    except Exception as e:
        return {
            "transcript":    None,
            "segments":      [],
            "duration_secs": 0,
            "status":        "transcription_failed",
            "error":         str(e),
        }
    finally:
        if tmp_path:
            Path(tmp_path).unlink(missing_ok=True)


# ---------------------------------------------------------------------------
# Article fetching
# ---------------------------------------------------------------------------
PAYWALL_DOMAINS  = ["bloomberg.com", "wsj.com", "ft.com", "nytimes.com", "economist.com"]
SKIP_JINA_DOMAINS = ["podcasts.apple.com"]

def _is_paywalled(url): return any(d in url for d in PAYWALL_DOMAINS)
def _skip_jina(url):    return any(d in url for d in SKIP_JINA_DOMAINS)

def _fetch_via_jina(url: str) -> str | None:
    try:
        r = requests.get(
            f"https://r.jina.ai/{url}",
            headers={"Accept": "text/plain", "X-Return-Format": "markdown", "X-Timeout": "20"},
            timeout=30,
        )
        return r.text.strip() if r.status_code == 200 and len(r.text.strip()) > 100 else None
    except:
        return None

def _fetch_via_requests(url: str) -> str | None:
    try:
        r = requests.get(url, headers={
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
            "Accept": "text/html,application/xhtml+xml",
        }, timeout=20, allow_redirects=True)
        r.raise_for_status()
        soup = BeautifulSoup(r.text, "html.parser")
        for tag in soup(["script","style","nav","footer","header","aside","form"]):
            tag.decompose()
        body = soup.find("article") or soup.find("main") or soup.body
        raw = body.get_text(separator="\n", strip=True) if body else ""
        lines = [l.strip() for l in raw.splitlines() if l.strip()]
        text = "\n".join(lines)
        return text if len(text.split()) > 50 else None
    except:
        return None

def fetch_article(url: str) -> dict:
    if _is_paywalled(url):
        return {"article_text": None, "status": "paywalled", "fetch_method": None, "error": "Paywalled"}
    text = None
    if not _skip_jina(url):
        text = _fetch_via_jina(url)
        if text:
            return {"article_text": text, "status": "article_fetched", "fetch_method": "jina", "error": None}
    text = _fetch_via_requests(url)
    if text:
        return {"article_text": text, "status": "article_fetched", "fetch_method": "requests", "error": None}
    return {"article_text": None, "status": "article_fetch_failed", "fetch_method": None,
            "error": "Both Jina and direct fetch returned no usable content"}


# ---------------------------------------------------------------------------
# Exec role check for reposted videos
# ---------------------------------------------------------------------------
EXEC_NAME_PATTERNS = {
    "arvind krishna":   ["arvind krishna", "arvindkrishna"],
    "james kavanaugh":  ["james kavanaugh", "james-kavanaugh"],
    "rob thomas":       ["rob thomas", "robthomas", "robert thomas"],
    "mohamad ali":      ["mohamad ali", "mohamad-ali"],
    "matt hicks":       ["matt hicks", "matthicks"],
    "jay gambetta":     ["jay gambetta", "jaygambetta"],
    "skyla loomis":     ["skyla loomis", "skyla-loomis"],
    "nickle lamoreaux": ["nickle lamoreaux", "nicklela"],
    "ric lewis":        ["ric lewis", "riclewis"],
    "jonathan adashek": ["jonathan adashek", "jonathanadashek"],
}
SPEAKER_KEYWORDS = [
    "sat down with", "joined by", "featuring", "interview with", "keynote",
    "spoke with", "in conversation with", "hear from", "join me", "join us",
    "i will be", "i'll be", "speaking at",
]

def check_video_relevance(sharer_name, sharer_comment, original_author, original_content) -> dict:
    combined = (sharer_comment + " " + original_content).lower()
    patterns = EXEC_NAME_PATTERNS.get(sharer_name.lower(), [sharer_name.lower()])
    name_hits = [p for p in patterns if p in combined]
    exec_named = bool(name_hits)
    speaker_hit = any(kw in combined for kw in SPEAKER_KEYWORDS)

    if exec_named and speaker_hit:
        return {"tier": "EXEC_FEATURED",  "confidence": "high",   "reason": f"Exec named as speaker ({name_hits[0]})"}
    if exec_named:
        return {"tier": "EXEC_MENTIONED", "confidence": "medium", "reason": f"Exec named in post text ({name_hits[0]})"}
    return {"tier": "AMPLIFIED",          "confidence": "high",   "reason": f"Exec not named — amplified from {original_author or 'original poster'}"}


# ---------------------------------------------------------------------------
# Build one record
# ---------------------------------------------------------------------------
def build_record(row: pd.Series, kind: str, groq_api_key: str | None) -> dict:
    post_id      = str(row.get("id") or row.get("entityId") or "unknown")
    raw_author   = str(row.get("author/name") or "Unknown")
    author_url   = str(row.get("author/linkedinUrl") or "")
    author, role = resolve_author(raw_author, author_url)
    posted_at    = str(row.get("postedAt/date") or "")
    post_text    = str(row.get("content") or "")
    linkedin_url = str(row.get("linkedinUrl") or "")
    repost_author     = row.get("repost/author/name")
    repost_author_url = row.get("repost/author/linkedinUrl")
    repost_content    = row.get("repost/content")

    record = {
        "author":        author,
        "role":          role,
        "posted_at":     posted_at,
        "post_url":      linkedin_url,
        "post_text":     post_text,
        "transcript":    None,
        "segments":      [],
        "duration_secs": 0,
        "status":        "pending",
        "id":            post_id,
        "content_type":  kind,
        "is_repost":     kind.startswith("REPOST"),
        "repost_author":     str(repost_author) if pd.notna(repost_author) else None,
        "repost_author_url": str(repost_author_url) if pd.notna(repost_author_url) else None,
        "repost_content":    str(repost_content) if pd.notna(repost_content) else None,
        "article_text":  None,
        "article_url":   None,
        "video_url":     None,
        "fetch_method":  None,
        "error":         None,
        "relevance_checked":    False,
        "content_author":       None,
        "exec_role":            None,
        "exec_role_confidence": None,
        "exec_role_reason":     None,
        "reposted_by":          None,
        "processed_at":  datetime.now(timezone.utc).isoformat(),
    }

    label = f"{author} / {post_id}"

    if kind == "POST_TEXT":
        record["status"] = "text_only"

    elif kind == "POST_VIDEO":
        url = row.get("postVideo/videoUrl")
        record["video_url"] = url
        if groq_api_key and pd.notna(url):
            record.update(transcribe_video(url, groq_api_key, label))
        else:
            record["status"] = "skipped_no_groq_key"

    elif kind == "POST_ARTICLE":
        url = row.get("article/link")
        record["article_url"] = url
        if pd.notna(url):
            result = fetch_article(url)
            record.update({"article_text": result["article_text"], "status": result["status"],
                           "fetch_method": result["fetch_method"], "error": result["error"]})
        else:
            record["status"] = "skipped_no_url"

    elif kind == "REPOST_TEXT":
        record["status"] = "text_only"

    elif kind == "REPOST_VIDEO":
        url = row.get("repost/postVideo/videoUrl")
        original_author_name = str(row.get("repost/author/name") or "")
        record["video_url"]      = url
        record["content_author"] = original_author_name
        attr = check_video_relevance(author, post_text, original_author_name, str(repost_content or ""))
        tier = attr["tier"]
        record["exec_role"]            = tier.lower().replace("exec_", "")
        record["exec_role_confidence"] = attr["confidence"]
        record["exec_role_reason"]     = attr["reason"]
        record["relevance_checked"]    = True
        record["reposted_by"]          = author if tier == "AMPLIFIED" else None
        if groq_api_key and pd.notna(url):
            record.update(transcribe_video(url, groq_api_key, label))
        else:
            record["status"] = "skipped_no_groq_key"

    elif kind == "REPOST_ARTICLE":
        url = row.get("repost/article/link")
        record["article_url"] = url
        if pd.notna(url):
            result = fetch_article(url)
            record.update({"article_text": result["article_text"], "status": result["status"],
                           "fetch_method": result["fetch_method"], "error": result["error"]})
        else:
            record["status"] = "skipped_no_url"

    return record


# ---------------------------------------------------------------------------
# Main entry point — called by server.py
# ---------------------------------------------------------------------------
def run_pipeline(
    csv_path: str,
    groq_api_key: str,
    emit: Callable[[str], None],   # function to stream log lines to the UI
    delay: float = 1.5,
) -> dict:
    """
    Process a CSV file and return the transcripts dict.
    emit() is called with each log line so the UI can stream progress.
    """
    def log_emit(msg: str):
        log.info(msg)
        emit(msg)

    df = pd.read_csv(csv_path)
    log_emit(f"Loaded {len(df)} rows from CSV")

    records = []
    summary = {k: 0 for k in ["POST_TEXT","POST_VIDEO","POST_ARTICLE","REPOST_TEXT","REPOST_VIDEO","REPOST_ARTICLE"]}

    for i, (_, row) in enumerate(df.iterrows()):
        kind   = classify_row(row)
        author = str(row.get("author/name") or "?")
        summary[kind] += 1
        log_emit(f"[{i+1:02d}/{len(df)}] {kind:<18} {author}")

        record = build_record(row, kind, groq_api_key)
        records.append(record)

        if kind in ("POST_VIDEO", "REPOST_VIDEO", "POST_ARTICLE", "REPOST_ARTICLE"):
            time.sleep(delay)

    output = {
        "lastRun":     datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "source":      "render",
        "count":       len(records),
        "summary":     summary,
        "transcripts": records,
    }

    log_emit(f"Done — {len(records)} records processed")
    for k, v in summary.items():
        if v:
            log_emit(f"  {k:<22} {v}")

    return output

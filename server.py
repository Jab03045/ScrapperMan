"""
server.py — IBM Executive Intelligence Pipeline
Render web service. One button triggers the full pipeline:
  1. Run Apify LinkedIn Posts Search scraper
  2. Poll until done, download CSV
  3. Process CSV (classify + transcribe + fetch articles)
  4. Push transcripts.json to Cloudflare Pages
  5. Stream live logs to the UI via SSE
"""

import os
import io
import csv
import json
import time
import queue
import logging
import tempfile
import threading
from datetime import datetime, timezone
from pathlib import Path

import requests
from flask import Flask, Response, jsonify, request, stream_with_context

from process import run_pipeline

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("server")

app = Flask(__name__)

APIFY_TOKEN      = os.environ["APIFY_TOKEN"]
APIFY_ACTOR_ID   = os.environ.get("APIFY_ACTOR_ID", "buIWk2uOUzTmcLsuB")
GROQ_API_KEY     = os.environ["GROQ_API_KEY"]
CF_API_TOKEN     = os.environ["CF_API_TOKEN"]
CF_ACCOUNT_ID    = os.environ["CF_ACCOUNT_ID"]
CF_PROJECT_NAME  = os.environ.get("CF_PROJECT_NAME", "ibm-transcripts")

# Global state — one run at a time
pipeline_state = {
    "running":    False,
    "last_run":   None,
    "last_status": None,
}
log_queue: queue.Queue = queue.Queue()


# ---------------------------------------------------------------------------
# Apify helpers
# ---------------------------------------------------------------------------

# IBM exec profiles to scrape
EXEC_PROFILES = [
    "https://www.linkedin.com/company/ibm",
    "https://www.linkedin.com/showcase/ibm-cloud",
    "https://www.linkedin.com/in/arvindkrishna",
    "https://www.linkedin.com/in/james-kavanaugh-ibm",
    "https://www.linkedin.com/in/robthomas0",
    "https://www.linkedin.com/in/mohamad-ali-3a44a",
    "https://www.linkedin.com/in/matthicks",
    "https://www.linkedin.com/in/jaygambetta",
    "https://www.linkedin.com/in/skyla-loomis",
    "https://www.linkedin.com/in/nicklela",
    "https://www.linkedin.com/in/riclewis",
    "https://www.linkedin.com/in/jonathanadashek",
]

def start_apify_run(days_back: int = 30, max_posts: int = 10, emit=None) -> str:
    """Start the Apify actor and return the run ID."""
    emit(f"  Actor ID:      {APIFY_ACTOR_ID}")
    emit(f"  Profiles:      {len(EXEC_PROFILES)} IBM exec profiles")
    emit(f"  Search terms:  IBM AI, IBM Think, agentic enterprise, hybrid cloud")
    emit(f"  Date range:    last {days_back} days")
    emit(f"  Max posts:     {max_posts} per profile")

    url = f"https://api.apify.com/v2/acts/{APIFY_ACTOR_ID}/runs"
    payload = {
        "targetUrls":    EXEC_PROFILES,
        "searchQueries": ["IBM AI", "IBM consulting", "IBM Think", "agentic enterprise", "hybrid cloud"],
        "sortBy":        "date",
        "postedLimit":   f"{days_back * 24}h" if days_back <= 7 else "month",
        "maxPosts":      max_posts,
    }
    r = requests.post(url, params={"token": APIFY_TOKEN}, json=payload, timeout=30)
    r.raise_for_status()
    run_id = r.json()["data"]["id"]
    emit(f"  ✓ Apify run created: {run_id}")
    emit(f"  View at: https://console.apify.com/actors/{APIFY_ACTOR_ID}/runs/{run_id}")
    return run_id


def poll_apify_run(run_id: str, emit, poll_interval: int = 15) -> str:
    """Poll until the run finishes. Returns dataset ID."""
    url = f"https://api.apify.com/v2/actor-runs/{run_id}"
    elapsed = 0
    emit("  Polling every 15 seconds...")
    while True:
        r = requests.get(url, params={"token": APIFY_TOKEN}, timeout=15)
        r.raise_for_status()
        data    = r.json()["data"]
        status  = data["status"]
        stats   = data.get("stats", {})
        n_items = stats.get("outputDatasetItems", 0)

        emit(f"  [{elapsed:>3}s] status={status}  posts_scraped={n_items}")

        if status == "SUCCEEDED":
            dataset_id = data["defaultDatasetId"]
            emit(f"  ✓ Scraper finished — {n_items} posts collected")
            emit(f"  Dataset ID: {dataset_id}")
            return dataset_id
        elif status in ("FAILED", "ABORTED", "TIMED-OUT"):
            raise RuntimeError(f"Apify run {status} — check https://console.apify.com/actors/{APIFY_ACTOR_ID}/runs/{run_id}")

        time.sleep(poll_interval)
        elapsed += poll_interval


def download_csv(dataset_id: str, emit) -> str:
    """Download the Apify dataset as CSV and return the local file path."""
    emit(f"  Dataset URL: https://api.apify.com/v2/datasets/{dataset_id}/items")
    url = f"https://api.apify.com/v2/datasets/{dataset_id}/items"
    r = requests.get(url, params={"token": APIFY_TOKEN, "format": "csv", "flatten": "1"}, timeout=60)
    r.raise_for_status()

    tmp = tempfile.NamedTemporaryFile(suffix=".csv", delete=False)
    tmp.write(r.content)
    tmp.close()

    size_kb    = len(r.content) / 1024
    row_count  = r.content.count(b"\n") - 1
    emit(f"  ✓ Downloaded {row_count} rows ({size_kb:.1f} KB) → {tmp.name}")
    return tmp.name


# ---------------------------------------------------------------------------
# Cloudflare Pages upload
# ---------------------------------------------------------------------------

def push_to_cloudflare(transcripts: dict, emit) -> str:
    """Upload transcripts.json to Cloudflare Pages. Returns the public URL."""
    emit("Pushing transcripts.json to Cloudflare Pages ...")

    json_bytes = json.dumps(transcripts, indent=2, ensure_ascii=False, default=str).encode("utf-8")

    # Cloudflare Pages direct upload API
    url = (
        f"https://api.cloudflare.com/client/v4/accounts/{CF_ACCOUNT_ID}"
        f"/pages/projects/{CF_PROJECT_NAME}/deployments"
    )

    r = requests.post(
        url,
        headers={"Authorization": f"Bearer {CF_API_TOKEN}"},
        files={
            "transcripts.json": ("transcripts.json", io.BytesIO(json_bytes), "application/json"),
        },
        timeout=60,
    )
    r.raise_for_status()
    public_url = f"https://{CF_PROJECT_NAME}.pages.dev/transcripts.json"
    emit(f"Published → {public_url}")
    return public_url


# ---------------------------------------------------------------------------
# Pipeline orchestrator — runs in a background thread
# ---------------------------------------------------------------------------

def run_full_pipeline(days_back: int, max_posts: int):
    global pipeline_state

    def emit(msg: str):
        ts = datetime.now().strftime("%H:%M:%S")
        line = f"[{ts}] {msg}"
        log.info(line)
        log_queue.put(line)

    try:
        pipeline_state["running"] = True
        pipeline_state["last_run"] = datetime.now(timezone.utc).isoformat()
        emit("━" * 50)
        emit("IBM Executive Intelligence Pipeline")
        emit(f"Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        emit(f"Settings: {days_back} days back  |  {max_posts} max posts per profile")
        emit("━" * 50)

        # ── Step 1: Apify scraper ─────────────────────────────────────────
        emit("")
        emit("STEP 1/4 — Scraping LinkedIn posts via Apify")
        emit("─" * 40)
        run_id = start_apify_run(days_back, max_posts, emit)

        # ── Step 2: Poll until done ───────────────────────────────────────
        emit("")
        emit("STEP 2/4 — Waiting for Apify scraper to finish")
        emit("─" * 40)
        emit("  This usually takes 5–15 minutes depending on post volume.")
        emit("  You can also watch progress at console.apify.com")
        dataset_id = poll_apify_run(run_id, emit)

        # ── Step 3: Download + process ────────────────────────────────────
        emit("")
        emit("STEP 3/4 — Downloading CSV and processing content")
        emit("─" * 40)
        emit("  Downloading CSV from Apify ...")
        csv_path = download_csv(dataset_id, emit)
        emit("  Starting content processing:")
        emit("    • Text posts  → saved as-is")
        emit("    • Articles    → fetched via Jina Reader")
        emit("    • Videos      → downloaded + transcribed via Groq Whisper")
        emit("    • Reposts     → attributed by exec role (featured/mentioned/amplified)")
        emit("")
        try:
            transcripts = run_pipeline(csv_path, GROQ_API_KEY, emit)
        finally:
            Path(csv_path).unlink(missing_ok=True)

        # ── Step 4: Push to Cloudflare ────────────────────────────────────
        emit("")
        emit("STEP 4/4 — Publishing to Cloudflare Pages")
        emit("─" * 40)
        public_url = push_to_cloudflare(transcripts, emit)

        # ── Done ──────────────────────────────────────────────────────────
        emit("")
        emit("━" * 50)
        emit("✓ PIPELINE COMPLETE")
        emit(f"  Records published:  {transcripts['count']}")
        emit(f"  Finished at:        {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        emit(f"  Live JSON:          https://{CF_PROJECT_NAME}.pages.dev/transcripts.json")
        emit("━" * 50)
        emit("  In Claude: say 'refresh dashboard' to load the new data.")
        pipeline_state["last_status"] = "success"

    except Exception as e:
        emit("")
        emit("━" * 50)
        emit(f"✗ PIPELINE FAILED: {e}")
        emit("━" * 50)
        log.exception("Pipeline failed")
        pipeline_state["last_status"] = "error"

    finally:
        pipeline_state["running"] = False
        log_queue.put("__DONE__")


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------

@app.route("/")
def index():
    last_run    = pipeline_state.get("last_run") or "Never"
    last_status = pipeline_state.get("last_status") or "—"
    running     = pipeline_state["running"]

    status_color = {"success": "#22c55e", "error": "#ef4444"}.get(last_status, "#888")

    return f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>IBM Intelligence Pipeline</title>
  <style>
    * {{ box-sizing: border-box; margin: 0; padding: 0; }}
    body {{ background: #0a0a0f; color: #e5e7eb; font-family: 'DM Mono', monospace; padding: 2rem; }}
    h1 {{ font-size: 1.4rem; color: #fff; margin-bottom: 0.25rem; letter-spacing: -0.02em; }}
    .subtitle {{ color: #6b7280; font-size: 0.8rem; margin-bottom: 2rem; }}
    .card {{ background: #13131a; border: 1px solid #1f2937; border-radius: 12px; padding: 1.5rem; margin-bottom: 1.5rem; }}
    label {{ display: block; font-size: 0.75rem; color: #9ca3af; margin-bottom: 0.4rem; }}
    input[type=number], select {{
      background: #0a0a0f; border: 1px solid #374151; border-radius: 6px;
      color: #e5e7eb; padding: 0.5rem 0.75rem; font-size: 0.85rem;
      font-family: inherit; width: 180px;
    }}
    .row {{ display: flex; gap: 1.5rem; flex-wrap: wrap; margin-bottom: 1.25rem; }}
    button {{
      background: #4f8ef7; color: #fff; border: none; border-radius: 8px;
      padding: 0.65rem 1.5rem; font-size: 0.9rem; font-family: inherit;
      cursor: pointer; transition: background 0.15s;
    }}
    button:hover {{ background: #3b7de8; }}
    button:disabled {{ background: #374151; color: #6b7280; cursor: not-allowed; }}
    .status-row {{ display: flex; gap: 2rem; font-size: 0.8rem; color: #9ca3af; margin-top: 1rem; }}
    .status-row span {{ color: #e5e7eb; }}
    .status-dot {{ display: inline-block; width: 8px; height: 8px; border-radius: 50%; background: {status_color}; margin-right: 6px; }}
    #log-box {{
      background: #0a0a0f; border: 1px solid #1f2937; border-radius: 8px;
      padding: 1rem; height: 420px; overflow-y: auto;
      font-size: 0.75rem; line-height: 1.7; color: #9ca3af;
      white-space: pre-wrap; word-break: break-all;
    }}
    #log-box .ok   {{ color: #22c55e; }}
    #log-box .err  {{ color: #ef4444; }}
    #log-box .info {{ color: #4f8ef7; }}
    #log-box .dim  {{ color: #4b5563; }}
    .running-badge {{
      display: inline-block; background: #1e3a5f; color: #4f8ef7;
      font-size: 0.7rem; padding: 2px 10px; border-radius: 20px; margin-left: 0.75rem;
      animation: pulse 1.5s infinite;
    }}
    @keyframes pulse {{ 0%,100% {{ opacity:1 }} 50% {{ opacity:0.4 }} }}
  </style>
</head>
<body>
  <h1>IBM Executive Intelligence Pipeline {"<span class='running-badge'>RUNNING</span>" if running else ""}</h1>
  <p class="subtitle">Scrape → Transcribe → Publish to Cloudflare Pages</p>

  <div class="card">
    <div class="row">
      <div>
        <label>Days back</label>
        <input type="number" id="days" value="30" min="1" max="180">
      </div>
      <div>
        <label>Max posts per profile</label>
        <input type="number" id="posts" value="10" min="1" max="50">
      </div>
    </div>
    <button id="run-btn" onclick="startPipeline()" {"disabled" if running else ""}>
      {"⏳ Pipeline running..." if running else "▶ Run pipeline"}
    </button>
    <div class="status-row">
      <div>Last run: <span>{last_run}</span></div>
      <div>Status: <span><span class="status-dot"></span>{last_status}</span></div>
    </div>
  </div>

  <div class="card">
    <label style="margin-bottom:0.75rem">Live logs</label>
    <div id="log-box">Waiting for pipeline to start...</div>
  </div>

  <script>
    function colorize(line) {{
      if (line.includes("✗") || line.includes("FAILED") || line.includes("ERROR")) return `<span class="err">${{line}}</span>`;
      if (line.includes("✓") || line.includes("COMPLETE") || line.includes("Published")) return `<span class="ok">${{line}}</span>`;
      if (line.startsWith("[") && line.includes("] STEP")) return `<span class="info" style="font-weight:bold">${{line}}</span>`;
      if (line.includes("STEP ") || line.includes("━") || line.includes("─")) return `<span class="info">${{line}}</span>`;
      if (line.includes("status=") || line.includes("[still")) return `<span class="dim">${{line}}</span>`;
      return line;
    }}

    function startPipeline() {{
      const days  = document.getElementById("days").value;
      const posts = document.getElementById("posts").value;
      const btn   = document.getElementById("run-btn");
      const box   = document.getElementById("log-box");

      btn.disabled = true;
      btn.textContent = "⏳ Pipeline running...";
      box.innerHTML = "<span style='color:#4b5563'>Starting pipeline...</span>\n";

      fetch(`/run?days=${{days}}&posts=${{posts}}`, {{ method: "POST" }})
        .then(r => {{
          if (!r.ok) throw new Error("Failed to start pipeline");
          return r.json();
        }})
        .then(() => {{
          // Only open SSE stream AFTER /run confirms it started
          box.innerHTML = "";
          const es = new EventSource("/logs");
          es.onmessage = (e) => {{
            if (e.data === "__DONE__") {{
              es.close();
              btn.disabled = false;
              btn.textContent = "▶ Run pipeline";
              return;
            }}
            box.innerHTML += colorize(e.data) + "\\n";
            box.scrollTop = box.scrollHeight;
          }};
          es.onerror = () => {{
            es.close();
            btn.disabled = false;
            btn.textContent = "▶ Run pipeline";
          }};
        }})
        .catch(e => {{
          box.innerHTML = `<span class="err">Error: ${{e.message}}</span>`;
          btn.disabled = false;
          btn.textContent = "▶ Run pipeline";
        }});
    }}
  </script>
</body>
</html>"""


@app.route("/run", methods=["POST"])
def run():
    if pipeline_state["running"]:
        return jsonify({"error": "Pipeline already running"}), 409

    days_back  = int(request.args.get("days",  30))
    max_posts  = int(request.args.get("posts", 10))

    # Clear the log queue
    while not log_queue.empty():
        try: log_queue.get_nowait()
        except: break

    thread = threading.Thread(
        target=run_full_pipeline,
        args=(days_back, max_posts),
        daemon=True,
    )
    thread.start()
    return jsonify({"status": "started"})


@app.route("/logs")
def logs():
    """SSE endpoint — streams log lines to the browser."""
    def generate():
        while True:
            try:
                msg = log_queue.get(timeout=60)
                yield f"data: {msg}\n\n"
                if msg == "__DONE__":
                    break
            except queue.Empty:
                yield "data: [still running...]\n\n"

    return Response(
        stream_with_context(generate()),
        mimetype="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "X-Accel-Buffering": "no",
        },
    )


@app.route("/status")
def status():
    return jsonify(pipeline_state)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)

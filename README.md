# IBM Executive Intelligence Pipeline

Scrapes IBM exec LinkedIn posts → transcribes videos → fetches articles → publishes to Cloudflare Pages.

## Files

| File | Purpose |
|---|---|
| `server.py` | Flask web server, pipeline orchestrator, UI |
| `process.py` | CSV processor (classify, transcribe, fetch articles) |
| `requirements.txt` | Python dependencies |
| `Procfile` | Render start command |

## Setup

### 1. Create GitHub repo
Push these 4 files to a new GitHub repo.

### 2. Create Render service
- New Web Service → connect your GitHub repo
- Runtime: Python
- Build command: `pip install -r requirements.txt`
- Start command: uses Procfile automatically

### 3. Set environment variables in Render

| Variable | Value |
|---|---|
| `APIFY_TOKEN` | Your Apify API token |
| `APIFY_ACTOR_ID` | `buIWk2uOUzTmcLsuB` |
| `GROQ_API_KEY` | Your Groq API key |
| `CF_API_TOKEN` | Cloudflare API token |
| `CF_ACCOUNT_ID` | Cloudflare account ID |
| `CF_PROJECT_NAME` | `ibm-transcripts` |

### 4. Run the pipeline
- Open your Render URL
- Set days back + max posts
- Click **Run pipeline**
- Watch live logs
- When done, `transcripts.json` is live at `https://ibm-transcripts.pages.dev/transcripts.json`

## Timeout note
Gunicorn is set to 600s (10 min) timeout. If you're scraping a large date range with many videos,
increase `--timeout` in the Procfile or run with `--timeout 1200`.

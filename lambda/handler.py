# handler.py
import json, os, hashlib, urllib.request, urllib.error
from datetime import datetime, timezone
import boto3

# =======================
# Environment variables (set these in Lambda → Configuration → Environment variables)
# =======================
DDB_TABLE = os.environ.get("DDB_TABLE", "disaster_alerts")
WEBSITE_BUCKET = os.environ["WEBSITE_BUCKET"]              # <- required (your website bucket)
OPENROUTER_API_KEY = os.environ.get("OPENROUTER_API_KEY", "")  # optional
HF_TOKEN = os.environ.get("HF_TOKEN", "")                      # optional
MODEL = os.environ.get("MODEL", "qwen/qwen-2.5-7b-instruct")   # for OpenRouter
MAX_ITEMS = int(os.environ.get("MAX_ITEMS", "40"))             # how many quakes to process per run
SUMMARIES_TO_KEEP = int(os.environ.get("SUMMARIES_TO_KEEP", "50"))  # keep on website

# =======================
# Data source: last ~30 days, magnitude >= 2.5 (worldwide)
# For even more events: use "all_month.geojson"
# =======================
USGS_URL = "https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/2.5_month.geojson"

# AWS clients
dynamodb = boto3.client("dynamodb")
s3 = boto3.client("s3")

def _id(s: str) -> str:
    """Short stable id from a string."""
    return hashlib.sha1(s.encode("utf-8")).hexdigest()[:16]

def _fetch_usgs() -> dict:
    """Download the USGS GeoJSON feed."""
    req = urllib.request.Request(USGS_URL, headers={"User-Agent": "Mozilla/5.0"})
    with urllib.request.urlopen(req, timeout=30) as r:
        return json.loads(r.read().decode("utf-8"))

def _ai_summarize(plain: dict) -> str:
    """Optional AI summary. Works with OpenRouter first, then HF as fallback."""
    text = json.dumps(plain, ensure_ascii=False)

    # 1) OpenRouter (chat completions)
    if OPENROUTER_API_KEY:
        try:
            body = {
                "model": MODEL,
                "messages": [
                    {"role": "user",
                     "content": "Summarize this earthquake for the public in 2 short sentences. "
                                "Include magnitude, nearest place, UTC time, depth (km), and if tsunami alert exists. "
                                + text}
                ]
            }
            req = urllib.request.Request(
                "https://openrouter.ai/api/v1/chat/completions",
                data=json.dumps(body).encode("utf-8"),
                headers={
                    "Content-Type": "application/json",
                    "Authorization": f"Bearer {OPENROUTER_API_KEY}",
                    "HTTP-Referer": "https://example.com",
                    "X-Title": "Disaster Summarizer"
                },
                method="POST"
            )
            with urllib.request.urlopen(req, timeout=40) as resp:
                out = json.loads(resp.read().decode("utf-8"))
                return out["choices"][0]["message"]["content"].strip()
        except Exception:
            pass  # fall through

    # 2) Hugging Face Inference (summarization)
    if HF_TOKEN:
        try:
            body = {"inputs": text, "parameters": {"max_length": 120, "min_length": 30}}
            req = urllib.request.Request(
                "https://api-inference.huggingface.co/models/facebook/bart-large-cnn",
                data=json.dumps(body).encode("utf-8"),
                headers={
                    "Content-Type": "application/json",
                    "Authorization": f"Bearer {HF_TOKEN}"
                },
                method="POST"
            )
            with urllib.request.urlopen(req, timeout=60) as resp:
                out = json.loads(resp.read().decode("utf-8"))
                if isinstance(out, list) and out and "summary_text" in out[0]:
                    return out[0]["summary_text"].strip()
                if isinstance(out, dict) and "summary_text" in out:
                    return out["summary_text"].strip()
                return str(out)[:300]
        except Exception:
            pass

    # 3) No key or both failed
    return "Summary unavailable."

def _publish_feed(feed: list) -> None:
    """Write alerts.json into the website bucket."""
    body = json.dumps({"alerts": feed}, ensure_ascii=False).encode("utf-8")
    s3.put_object(
        Bucket=WEBSITE_BUCKET,
        Key="alerts.json",
        Body=body,
        ContentType="application/json",
        CacheControl="no-cache"
    )

def handler(event, context):
    # Fetch data
    try:
        data = _fetch_usgs()
    except Exception as e:
        # Publish empty feed so UI still loads
        _publish_feed([])
        return {"error": f"USGS fetch failed: {e}"}

    features = (data.get("features", []) or [])[:MAX_ITEMS]
    now_iso = datetime.now(timezone.utc).isoformat()
    feed_for_web = []

    for f in features:
        props = f.get("properties", {}) or {}
        geom = f.get("geometry", {}) or {}
        coords = geom.get("coordinates", [None, None, None])

        mag = props.get("mag")
        place = props.get("place")
        tms = props.get("time")  # milliseconds
        time_utc = (
            datetime.fromtimestamp(tms / 1000, tz=timezone.utc).isoformat()
            if tms else now_iso
        )
        depth_km = coords[2] if len(coords) >= 3 else None
        url = props.get("url") or ""
        tsunami = bool(props.get("tsunami", 0))

        plain = {
            "magnitude": mag,
            "place": place,
            "time_utc": time_utc,
            "depth_km": depth_km,
            "tsunami": tsunami,
            "source": url
        }

        uid = _id(f"{time_utc}-{place}-{mag}")
        summary = _ai_summarize(plain)

        # Best-effort: write to DynamoDB (non-blocking if it fails)
        try:
            dynamodb.put_item(
                TableName=DDB_TABLE,
                Item={
                    "alert_id": {"S": uid},
                    "created_at": {"S": now_iso},
                    "type": {"S": "earthquake"},
                    "raw": {"S": json.dumps(plain, ensure_ascii=False)},
                    "summary": {"S": summary}
                }
            )
        except Exception:
            pass

        feed_for_web.append({
            "id": uid,
            "created_at": now_iso,
            "type": "earthquake",
            "data": plain,
            "summary": summary
        })

    # Keep the most recent N for the site
    feed_for_web = sorted(feed_for_web, key=lambda x: x["created_at"], reverse=True)[:SUMMARIES_TO_KEEP]

    # Publish to S3
    try:
        _publish_feed(feed_for_web)
    except Exception:
        # Don't crash the invocation if S3 write fails
        pass

    return {"count": len(feed_for_web)}

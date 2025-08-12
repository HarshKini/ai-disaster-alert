import json, os, hashlib, urllib.request
from datetime import datetime, timezone
import boto3

# ===== settings we pass from AWS =====
DDB_TABLE = os.environ.get("DDB_TABLE", "disaster_alerts")
WEBSITE_BUCKET = os.environ["harsh-disaster-web-001"]  
OPENROUTER_API_KEY = os.environ.get("OPENROUTER_API_KEY", "")  # optional
HF_TOKEN = os.environ.get("HF_TOKEN", "")  # optional
MODEL = os.environ.get("MODEL", "qwen/qwen-2.5-7b-instruct")
# =====================================

USGS_URL = "https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/2.5_hour.geojson"
dynamodb = boto3.client("dynamodb")
s3 = boto3.client("s3")

def _id(s):  # small helper to make a unique id
    import hashlib
    return hashlib.sha1(s.encode()).hexdigest()[:16]

def fetch_quakes():
    req = urllib.request.Request(USGS_URL, headers={"User-Agent":"Mozilla/5.0"})
    with urllib.request.urlopen(req, timeout=20) as r:
        return json.loads(r.read().decode())

def summarize(plain):
    text = json.dumps(plain, ensure_ascii=False)
    # Try OpenRouter first (if key present)
    if OPENROUTER_API_KEY:
        body = {
            "model": MODEL,
            "messages": [{"role":"user","content":
                "Summarize in 2 short sentences for the public: " + text}]
        }
        req = urllib.request.Request(
            "https://openrouter.ai/api/v1/chat/completions",
            data=json.dumps(body).encode(),
            headers={
                "Content-Type":"application/json",
                "Authorization": f"Bearer {OPENROUTER_API_KEY}"
            }, method="POST"
        )
        try:
            with urllib.request.urlopen(req, timeout=30) as resp:
                out = json.loads(resp.read().decode())
                return out["choices"][0]["message"]["content"].strip()
        except:
            pass
    # Else Hugging Face (if token present)
    if HF_TOKEN:
        body = {"inputs": text, "parameters": {"max_length": 120, "min_length": 20}}
        req = urllib.request.Request(
            "https://api-inference.huggingface.co/models/facebook/bart-large-cnn",
            data=json.dumps(body).encode(),
            headers={
                "Content-Type":"application/json",
                "Authorization": f"Bearer {HF_TOKEN}"
            }, method="POST"
        )
        try:
            with urllib.request.urlopen(req, timeout=60) as resp:
                out = json.loads(resp.read().decode())
                if isinstance(out, list) and out and "summary_text" in out[0]:
                    return out[0]["summary_text"].strip()
        except:
            pass
    return "Summary unavailable."

def handler(event, context):
    try:
        data = fetch_quakes()
    except Exception as e:
        # publish empty feed so UI doesn't break
        _publish([])
        return {"error": str(e)}

    features = data.get("features", [])
    now = datetime.now(timezone.utc).isoformat()
    feed = []

    for f in features:
        p = f.get("properties", {})
        g = f.get("geometry", {}) or {}
        coords = g.get("coordinates", [None, None, None])

        magnitude = p.get("mag")
        place = p.get("place")
        tms = p.get("time")  # ms
        when = datetime.fromtimestamp(tms/1000, tz=timezone.utc).isoformat() if tms else now
        depth_km = coords[2] if len(coords) > 2 else None
        src = p.get("url","")
        tsunami = bool(p.get("tsunami",0))

        plain = {
            "magnitude": magnitude,
            "place": place,
            "time_utc": when,
            "depth_km": depth_km,
            "tsunami": tsunami,
            "source": src
        }

        uid = _id(f"{when}-{place}-{magnitude}")
        summary = summarize(plain)

        # save one row in DynamoDB (best-effort)
        try:
            dynamodb.put_item(
                TableName=DDB_TABLE,
                Item={
                    "alert_id": {"S": uid},
                    "created_at": {"S": now},
                    "type": {"S": "earthquake"},
                    "raw": {"S": json.dumps(plain, ensure_ascii=False)},
                    "summary": {"S": summary}
                }
            )
        except:
            pass

        feed.append({"id": uid, "created_at": now, "type":"earthquake",
                     "data": plain, "summary": summary})

    # keep up to 30 recent items
    feed = sorted(feed, key=lambda x: x["created_at"], reverse=True)[:30]
    _publish(feed)
    return {"count": len(feed)}

def _publish(feed):
    body = json.dumps({"alerts": feed}, ensure_ascii=False).encode()
    s3.put_object(
        Bucket=WEBSITE_BUCKET, Key="alerts.json",
        Body=body, ContentType="application/json", CacheControl="no-cache"
    )

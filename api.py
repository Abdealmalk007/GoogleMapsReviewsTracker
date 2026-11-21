#!/usr/bin/env python3
"""REST API v40.0 — Flask API for branches, reviews, alerts, health and search telemetry."""
import sys, os, json, sqlite3, hashlib, secrets, traceback, contextlib
from pathlib import Path
from datetime import datetime, timedelta, date

APP_DIR = Path(__file__).parent
DATA_DIR = Path(os.environ.get("REVIEWS_TRACKER_DATA_DIR") or APP_DIR)
DATA_DIR.mkdir(parents=True, exist_ok=True)
DB_PATH = DATA_DIR / "reviews.db"
API_KEY_FILE = DATA_DIR / ".api_key"

try:
    from flask import Flask, request, jsonify
except ImportError:
    print("Flask not installed. Run: pip install flask", file=sys.stderr)
    sys.exit(1)


def load_api_key():
    if API_KEY_FILE.exists():
        return API_KEY_FILE.read_text(encoding="utf-8").strip()
    key = secrets.token_urlsafe(32)
    API_KEY_FILE.write_text(key, encoding="utf-8")
    return key


API_KEY = load_api_key()


@contextlib.contextmanager
def db():
    c = sqlite3.connect(str(DB_PATH), check_same_thread=False, timeout=30)
    c.row_factory = sqlite3.Row
    c.execute("PRAGMA journal_mode=WAL")
    try:
        yield c
        c.commit()
    except Exception:
        c.rollback()
        raise
    finally:
        c.close()


def require_key():
    k = request.headers.get("X-API-Key")
    if k != API_KEY:
        return jsonify({"error": "unauthorized"}), 401
    return None


app = Flask(__name__)


@app.route("/")
def root():
    return jsonify({
        "service": "Reviews Tracker API",
        "version": "43.0.0-beta.1",
        "endpoints": [
            "GET  /api/v1/branches?workspace_id=1",
            "GET  /api/v1/branches/<pk>",
            "GET  /api/v1/reviews?workspace_id=1&limit=100",
            "GET  /api/v1/reviews/<pk>",
            "GET  /api/v1/alerts?workspace_id=1",
            "GET  /api/v1/stats?workspace_id=1",
            "POST /api/v1/webhook/zapier",
            "POST /api/v1/webhook/make",
            "GET  /api/v1/search-jobs?workspace_id=1",
            "GET  /api/v1/change-events?workspace_id=1",
            "GET  /api/v1/identity-health?workspace_id=1",
            "GET  /api/v1/outbox?workspace_id=1",
            "GET  /api/v1/health",
        ]
    })


@app.route("/api/v1/health")
def health():
    try:
        with db() as c:
            n = c.execute("SELECT COUNT(*) FROM reviews").fetchone()[0]
            b = c.execute("SELECT COUNT(*) FROM companies").fetchone()[0]
            integrity = c.execute("PRAGMA quick_check").fetchone()[0]
            try:
                sv = c.execute("SELECT version FROM schema_version WHERE id=1").fetchone()
                schema = sv[0] if sv else None
            except Exception:
                schema = None
        return jsonify({"status": "ok" if integrity == "ok" else "warning", "version":"41.0.0",
                        "schema":schema,"reviews": n, "branches":b,"integrity":integrity,
                        "ts": datetime.now().isoformat()})
    except Exception as e:
        return jsonify({"status": "error", "detail": str(e)}), 500


@app.route("/api/v1/search-jobs")
def search_jobs():
    err = require_key()
    if err: return err
    ws = int(request.args.get("workspace_id", 1))
    limit = max(1, min(int(request.args.get("limit", 100)), 500))
    try:
        with db() as c:
            rows = c.execute("""SELECT COALESCE(NULLIF(job_id,''), CAST(id AS TEXT)) AS job_id,query,status,started_at,completed_at,elapsed_ms,
                results,discovered,tiles,errors FROM search_jobs WHERE workspace_id=?
                ORDER BY started_at DESC LIMIT ?""", (ws,limit)).fetchall()
        return jsonify([dict(r) for r in rows])
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/v1/identity-health")
def identity_health():
    err = require_key()
    if err: return err
    ws = int(request.args.get("workspace_id", 1))
    try:
        with db() as c:
            branches = c.execute("SELECT COUNT(*) FROM companies WHERE workspace_id=?",(ws,)).fetchone()[0]
            strong = c.execute("SELECT COUNT(*) FROM companies WHERE workspace_id=? AND (COALESCE(place_id,'')<>'' OR COALESCE(cid,'')<>'' OR COALESCE(maps_url,'')<>'')",(ws,)).fetchone()[0]
            aliases = c.execute("SELECT COUNT(*) FROM branch_aliases WHERE workspace_id=?",(ws,)).fetchone()[0]
            merges = c.execute("SELECT COUNT(*) FROM branch_merge_audit WHERE workspace_id=? AND status='merged'",(ws,)).fetchone()[0]
            pconf = c.execute("SELECT COUNT(*) FROM (SELECT place_id FROM companies WHERE workspace_id=? AND COALESCE(place_id,'')<>'' GROUP BY place_id HAVING COUNT(*)>1)",(ws,)).fetchone()[0]
            cconf = c.execute("SELECT COUNT(*) FROM (SELECT cid FROM companies WHERE workspace_id=? AND COALESCE(cid,'')<>'' GROUP BY cid HAVING COUNT(*)>1)",(ws,)).fetchone()[0]
        return jsonify({"branches":branches,"strong_identity":strong,"weak_identity":max(0,branches-strong),"aliases":aliases,"merge_history":merges,"identity_conflicts":pconf+cconf,"schema":49})
    except Exception as e:
        return jsonify({"error":str(e)}),500


@app.route("/api/v1/branches")
def branches():
    err = require_key()
    if err: return err
    ws = int(request.args.get("workspace_id", 1))
    limit = max(1, min(int(request.args.get("limit", 500)), 2000))
    with db() as c:
        rows = c.execute("""SELECT pk, name, address, rating, review_count,
            phone, mobile, email, email_source, website, category, cid, place_id, maps_url,
            latitude, longitude, branch_group, last_updated
            FROM companies WHERE workspace_id=? ORDER BY rating DESC, review_count DESC, last_updated DESC LIMIT ?""",
            (ws, limit)).fetchall()
    return jsonify([dict(r) for r in rows])


@app.route("/api/v1/branches/<pk>")
def branch_detail(pk):
    err = require_key()
    if err: return err
    ws = int(request.args.get("workspace_id", 1))
    with db() as c:
        row = c.execute("SELECT * FROM companies WHERE pk=? AND workspace_id=?", (pk, ws)).fetchone()
        if not row:
            try:
                alias = c.execute("SELECT canonical_pk FROM branch_aliases WHERE workspace_id=? AND alias_type='pk' AND alias_value=? LIMIT 1",(ws,pk)).fetchone()
                if alias:
                    pk = alias["canonical_pk"]
                    row = c.execute("SELECT * FROM companies WHERE pk=? AND workspace_id=?", (pk, ws)).fetchone()
            except Exception:
                pass
        if not row: return jsonify({"error": "not found"}), 404
        reviews = c.execute("""SELECT id, author, rating, text, review_date, language, sentiment,
            topic, aspect_primary, urgency, fake_score, responded, workflow_status,
            analysis_method, analysis_confidence, priority
            FROM reviews WHERE pk=? AND workspace_id=? ORDER BY id DESC LIMIT 200""",
            (pk, ws)).fetchall()
    return jsonify({"branch": dict(row), "reviews": [dict(r) for r in reviews]})


@app.route("/api/v1/reviews")
def reviews():
    err = require_key()
    if err: return err
    ws = int(request.args.get("workspace_id", 1))
    limit = max(1, min(int(request.args.get("limit", 100)), 1000))
    sentiment = request.args.get("sentiment")
    with db() as c:
        q = """SELECT r.id, r.pk, c.name company_name, r.author, r.rating, r.text,
            r.review_date, r.language, r.sentiment, r.topic, r.aspect_primary, r.urgency, r.fake_score,
            r.responded, r.workflow_status, r.analysis_method, r.analysis_confidence, r.priority
            FROM reviews r LEFT JOIN companies c ON c.pk=r.pk AND c.workspace_id=r.workspace_id
            WHERE r.workspace_id=?"""
        p = [ws]
        if sentiment:
            q += " AND r.sentiment=?"; p.append(sentiment)
        q += " ORDER BY r.id DESC LIMIT ?"; p.append(limit)
        rows = c.execute(q, p).fetchall()
    return jsonify([dict(r) for r in rows])


@app.route("/api/v1/alerts")
def alerts():
    err = require_key()
    if err: return err
    ws = int(request.args.get("workspace_id", 1))
    status = request.args.get("status", "new")
    with db() as c:
        q = """SELECT a.*, co.name company_name FROM alerts a
            LEFT JOIN companies co ON co.pk=a.pk AND co.workspace_id=a.workspace_id
            WHERE a.workspace_id=?"""
        p = [ws]
        if status != "all": q += " AND a.status=?"; p.append(status)
        q += " ORDER BY a.id DESC LIMIT 200"
        rows = c.execute(q, p).fetchall()
    return jsonify([dict(r) for r in rows])


@app.route("/api/v1/stats")
def stats():
    err = require_key()
    if err: return err
    ws = int(request.args.get("workspace_id", 1))
    days = max(1, min(int(request.args.get("days", 30)), 3650))
    since = (datetime.now() - timedelta(days=days)).isoformat()
    with db() as c:
        branches = c.execute("SELECT COUNT(*) FROM companies WHERE workspace_id=?", (ws,)).fetchone()[0]
        reviews = c.execute("""SELECT COUNT(*) FROM reviews WHERE workspace_id=? AND fetched_at>=?""",
                            (ws, since)).fetchone()[0]
        neg = c.execute("""SELECT COUNT(*) FROM reviews WHERE workspace_id=? AND fetched_at>=?
                          AND sentiment='negative'""", (ws, since)).fetchone()[0]
        avg = c.execute("""SELECT AVG(rating) FROM reviews WHERE workspace_id=? AND fetched_at>=?
                          AND rating IS NOT NULL""", (ws, since)).fetchone()[0]
        alerts_n = c.execute("SELECT COUNT(*) FROM alerts WHERE workspace_id=? AND status='new'",
                            (ws,)).fetchone()[0]
    return jsonify({"branches": branches, "reviews": reviews, "negative": neg,
                    "avg_rating": round(avg, 2) if avg else 0,
                    "unread_alerts": alerts_n, "days": days})


@app.route("/api/v1/webhook/zapier", methods=["POST"])
def zapier_webhook():
    return _webhook_handler("zapier")


@app.route("/api/v1/webhook/make", methods=["POST"])
def make_webhook():
    return _webhook_handler("make")


def _webhook_handler(source):
    err = require_key()
    if err: return err
    try:
        data = request.get_json(silent=True) or {}
        action = data.get("action") or "notify"
        with db() as c:
            c.execute("""INSERT INTO webhook_log(event_key,url,status,response,sent_at,error)
                VALUES(?,?,?,?,?,?)""",
                ("incoming:{}".format(source), "", 200,
                 json.dumps(data, ensure_ascii=False)[:500],
                 datetime.now().isoformat(), ""))
            if action == "notify":
                c.execute("""INSERT INTO notif_center(workspace_id,kind,severity,title,body,created_at)
                    VALUES(?,?,?,?,?,?)""",
                    (int(data.get("workspace_id", 1)), "webhook", "info",
                     data.get("title", "Webhook"), data.get("body", ""),
                     datetime.now().isoformat()))
        return jsonify({"status": "ok", "source": source, "action": action})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.errorhandler(404)
def not_found(e):
    return jsonify({"error": "endpoint not found"}), 404


@app.errorhandler(500)
def server_error(e):
    return jsonify({"error": "internal error"}), 500


if __name__ == "__main__":
    port = int(os.environ.get("API_PORT", 8502))
    bind_host = os.environ.get("REVIEWS_API_BIND_HOST", "127.0.0.1").strip() or "127.0.0.1"
    if bind_host not in {"127.0.0.1", "::1", "localhost"}:
        raise RuntimeError("Remote API listening is disabled; use a secured reverse proxy and explicit authentication.")
    print("API listening on {}:{} (credentials not displayed)".format(bind_host,port))
    app.run(host=bind_host, port=port, debug=False, use_reloader=False)

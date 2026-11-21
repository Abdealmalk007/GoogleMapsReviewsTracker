#!/usr/bin/env python3
"""Monitoring daemon v40.0 — scheduled digests, quiet hours, outbox retry, WhatsApp/Slack/Teams/Telegram/Email/Webhook."""
import sys, os, time, json, sqlite3, subprocess, smtplib, hashlib, html, re
import urllib.request, urllib.parse
from datetime import datetime, timedelta
from pathlib import Path
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

APP_DIR = Path(__file__).parent
DATA_DIR = Path(os.environ.get("REVIEWS_TRACKER_DATA_DIR") or APP_DIR)
DATA_DIR.mkdir(parents=True, exist_ok=True)
DB_PATH = DATA_DIR / "reviews.db"
WORKER = APP_DIR / "worker.py"
LOG = DATA_DIR / "daemon.log"


def log(msg):
    line = "[{}] {}".format(datetime.now().strftime("%Y-%m-%d %H:%M:%S"), msg)
    print(line, flush=True)
    try:
        with open(LOG, "a", encoding="utf-8") as f: f.write(line + "\n")
    except Exception: pass


def load_cfg():
    cfg = {}
    for p in [APP_DIR / ".streamlit" / "secrets.toml", Path.home() / ".streamlit" / "secrets.toml"]:
        if p.exists():
            for line in p.read_text(encoding="utf-8").splitlines():
                line = line.strip()
                if not line or line.startswith("#") or "=" not in line: continue
                k, v = line.split("=", 1)
                cfg[k.strip()] = v.strip().strip('"').strip("'")
    for k in ["SMTP_SERVER","SMTP_PORT","SMTP_SENDER","SMTP_PASSWORD","SMTP_RECIPIENT",
              "TELEGRAM_BOT_TOKEN","TELEGRAM_CHAT_ID","WEBHOOK_URL","SLACK_WEBHOOK_URL",
              "TEAMS_WEBHOOK_URL","WHATSAPP_TOKEN","WHATSAPP_PHONE_ID","WHATSAPP_TO"]:
        if os.environ.get(k): cfg[k] = os.environ[k]
    return cfg


def send_email(cfg, subject, body):
    if not (cfg.get("SMTP_SENDER") and cfg.get("SMTP_PASSWORD") and cfg.get("SMTP_RECIPIENT")):
        return False
    try:
        msg = MIMEMultipart("alternative")
        msg["Subject"] = subject
        msg["From"] = cfg["SMTP_SENDER"]; msg["To"] = cfg["SMTP_RECIPIENT"]
        msg.attach(MIMEText(body, "html", "utf-8"))
        with smtplib.SMTP(cfg.get("SMTP_SERVER","smtp.gmail.com"),
                          int(cfg.get("SMTP_PORT", 587)), timeout=20) as s:
            s.starttls(); s.login(cfg["SMTP_SENDER"], cfg["SMTP_PASSWORD"])
            s.send_message(msg); return True
    except Exception as e:
        log("email: " + str(e)[:120]); return False


def send_telegram(cfg, text):
    token = cfg.get("TELEGRAM_BOT_TOKEN"); chat = cfg.get("TELEGRAM_CHAT_ID")
    if not (token and chat): return False
    try:
        url = "https://api.telegram.org/bot{}/sendMessage".format(token)
        data = urllib.parse.urlencode({"chat_id": chat, "text": text,
                                        "parse_mode": "HTML"}).encode()
        urllib.request.urlopen(url, data=data, timeout=15); return True
    except Exception as e:
        log("tg: " + str(e)[:120]); return False


def send_slack(cfg, text):
    url = cfg.get("SLACK_WEBHOOK_URL")
    if not url: return False
    try:
        data = json.dumps({"text": text[:3000]}).encode("utf-8")
        req = urllib.request.Request(url, data=data,
            headers={"Content-Type": "application/json"})
        urllib.request.urlopen(req, timeout=15); return True
    except Exception as e:
        log("slack: " + str(e)[:120]); return False


def send_teams(cfg, text):
    url = cfg.get("TEAMS_WEBHOOK_URL")
    if not url: return False
    try:
        data = json.dumps({"text": text[:3000]}).encode("utf-8")
        req = urllib.request.Request(url, data=data,
            headers={"Content-Type": "application/json"})
        urllib.request.urlopen(req, timeout=15); return True
    except Exception as e:
        log("teams: " + str(e)[:120]); return False


def send_whatsapp(cfg, text):
    """Meta Cloud API v18+. Send to WHATSAPP_TO."""
    token = cfg.get("WHATSAPP_TOKEN")
    phone_id = cfg.get("WHATSAPP_PHONE_ID")
    to = cfg.get("WHATSAPP_TO")
    if not (token and phone_id and to): return False
    try:
        url = "https://graph.facebook.com/v18.0/{}/messages".format(phone_id)
        body = {"messaging_product": "whatsapp", "to": to,
                "type": "text", "text": {"body": text[:4000]}}
        data = json.dumps(body).encode("utf-8")
        req = urllib.request.Request(url, data=data,
            headers={"Authorization": "Bearer " + token,
                     "Content-Type": "application/json"})
        urllib.request.urlopen(req, timeout=20); return True
    except Exception as e:
        log("whatsapp: " + str(e)[:120]); return False


def send_webhook(cfg, text):
    url = cfg.get("WEBHOOK_URL")
    if not url: return False
    try:
        if "slack.com" in url or "office.com" in url: body = {"text": text}
        elif "discord" in url: body = {"content": text[:1900]}
        else: body = {"message": text}
        data = json.dumps(body).encode("utf-8")
        req = urllib.request.Request(url, data=data,
            headers={"Content-Type": "application/json"})
        urllib.request.urlopen(req, timeout=15); return True
    except Exception: return False


def notify(cfg, subject, body, plain):
    ok = False
    if send_email(cfg, subject, body): ok = True
    if send_telegram(cfg, plain): ok = True
    if send_slack(cfg, plain): ok = True
    if send_teams(cfg, plain): ok = True
    if send_whatsapp(cfg, plain): ok = True
    if send_webhook(cfg, plain): ok = True
    return ok


def call_worker(mode, query, max_reviews=50):
    try:
        p = subprocess.run([sys.executable, str(WORKER), mode, query,
                            "--max", str(max_reviews), "--lang", "ar"],
                           capture_output=True, timeout=240)
        if p.returncode != 0: return {"error": p.stderr.decode("utf-8","ignore")[:500]}
        out = p.stdout.decode("utf-8", "ignore").strip()
        return json.loads(out) if out else {"error": "empty"}
    except Exception as e: return {"error": str(e)}


def db_connect():
    c = sqlite3.connect(str(DB_PATH), check_same_thread=False, timeout=30)
    c.row_factory = sqlite3.Row
    c.execute("PRAGMA journal_mode=WAL")
    c.execute("PRAGMA busy_timeout=30000")
    c.execute("""CREATE TABLE IF NOT EXISTS notifications(
        pk TEXT NOT NULL, review_hash TEXT NOT NULL, kind TEXT NOT NULL,
        sent_at TEXT NOT NULL, PRIMARY KEY(pk, review_hash, kind))""")
    c.execute("""CREATE TABLE IF NOT EXISTS alert_delivery_policy(workspace_id INTEGER PRIMARY KEY,enabled INTEGER DEFAULT 1,digest_frequency TEXT DEFAULT 'daily',min_severity TEXT DEFAULT 'warning',quiet_start TEXT DEFAULT '22:00',quiet_end TEXT DEFAULT '07:00',channels_json TEXT DEFAULT '[]',dedupe_minutes INTEGER DEFAULT 120,max_retries INTEGER DEFAULT 3,updated_at TEXT)""")
    c.execute("""CREATE TABLE IF NOT EXISTS digest_runs(id INTEGER PRIMARY KEY AUTOINCREMENT,workspace_id INTEGER DEFAULT 1,frequency TEXT,window_start TEXT,window_end TEXT,dedupe_key TEXT,severity_max TEXT,event_count INTEGER,branch_count INTEGER,source_event_ids TEXT,summary TEXT,status TEXT DEFAULT 'built',created_at TEXT)""")
    c.execute("""CREATE TABLE IF NOT EXISTS alert_outbox(id INTEGER PRIMARY KEY AUTOINCREMENT,workspace_id INTEGER DEFAULT 1,digest_run_id INTEGER,dedupe_key TEXT,severity TEXT,channel TEXT,recipient TEXT,subject TEXT,body TEXT,source_event_ids TEXT,payload_json TEXT,status TEXT DEFAULT 'queued',attempts INTEGER DEFAULT 0,max_attempts INTEGER DEFAULT 3,next_attempt_at TEXT,queued_at TEXT,sent_at TEXT,error TEXT)""")
    c.execute("CREATE UNIQUE INDEX IF NOT EXISTS ux_daemon_digest_key ON digest_runs(workspace_id,dedupe_key)")
    c.execute("CREATE UNIQUE INDEX IF NOT EXISTS ux_daemon_outbox_key_channel ON alert_outbox(workspace_id,dedupe_key,channel)")
    return c


def get_targets():
    with db_connect() as c:
        try:
            rows = c.execute("SELECT * FROM monitored WHERE enabled=1").fetchall()
        except sqlite3.OperationalError: return []
    now = datetime.now(); out = []
    for r in rows:
        iv = int(r["interval"] or 6); last = r["last_success"]
        if last:
            try:
                if now - datetime.fromisoformat(last) < timedelta(hours=iv): continue
            except Exception: pass
        out.append(dict(r))
    return out


NEG_KW = ["سيء","قذر","غالي","بطيء","وقح","زفت","أسوأ","كذب","bad","terrible","rude","dirty","scam"]
POS_KW = ["ممتاز","رائع","لذيذ","سريع","نظيف","أفضل","شكرا","great","excellent","perfect"]


def sentiment_lite(text, rating):
    try: r = float(rating or 3)
    except Exception: r = 3
    s = "positive" if r >= 4 else "negative" if r <= 2 else "neutral"
    t = (text or "").lower()
    if any(k in t for k in NEG_KW): s = "negative"
    elif any(k in t for k in POS_KW): s = "positive"
    return s


def save_reviews(pk, info, reviews, ws_id=1):
    new_items = []
    with db_connect() as c:
        c.execute("""INSERT INTO companies (pk,workspace_id,name,address,rating,review_count,
            phone,mobile,email,email_source,contact_checked_at,website,category,maps_url,
            cid,place_id,last_updated,latitude,longitude)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            ON CONFLICT(pk,workspace_id) DO UPDATE SET
                name=COALESCE(NULLIF(excluded.name,''),companies.name),
                address=COALESCE(NULLIF(excluded.address,''),companies.address),
                rating=COALESCE(excluded.rating,companies.rating),
                review_count=COALESCE(excluded.review_count,companies.review_count),
                phone=COALESCE(NULLIF(excluded.phone,''),companies.phone),
                mobile=COALESCE(NULLIF(excluded.mobile,''),companies.mobile),
                email=COALESCE(NULLIF(excluded.email,''),companies.email),
                email_source=COALESCE(NULLIF(excluded.email_source,''),companies.email_source),
                contact_checked_at=COALESCE(NULLIF(excluded.contact_checked_at,''),companies.contact_checked_at),
                website=COALESCE(NULLIF(excluded.website,''),companies.website),
                category=COALESCE(NULLIF(excluded.category,''),companies.category),
                maps_url=COALESCE(NULLIF(excluded.maps_url,''),companies.maps_url),
                cid=COALESCE(NULLIF(excluded.cid,''),companies.cid),
                place_id=COALESCE(NULLIF(excluded.place_id,''),companies.place_id),
                latitude=COALESCE(excluded.latitude,companies.latitude),
                longitude=COALESCE(excluded.longitude,companies.longitude),
                last_updated=excluded.last_updated""",
            (pk, ws_id, info.get("name"), info.get("address"), info.get("rating"),
             info.get("review_count"), info.get("phone"), info.get("mobile"),
             info.get("email"), info.get("email_source"), info.get("contact_checked_at"),
             info.get("website"), info.get("category"), info.get("url"),
             info.get("cid"), info.get("place_id"), datetime.now().isoformat(),
             info.get("latitude"), info.get("longitude")))
        for r in reviews:
            rid = str(r.get("review_id") or "").strip()
            if rid:
                if c.execute("SELECT 1 FROM reviews WHERE pk=? AND review_id=? AND workspace_id=?",
                             (pk, rid, ws_id)).fetchone(): continue
                rh = "rid:{}:{}:{}".format(ws_id, pk, rid)
            else:
                rh = hashlib.md5("{}|{}|{}|{}".format(ws_id, pk,
                    r.get("author",""), r.get("text","")).encode()).hexdigest()
            sent = sentiment_lite(r.get("text"), r.get("rating"))
            try:
                c.execute("""INSERT INTO reviews(pk,workspace_id,review_id,author,rating,text,
                    review_date,fetched_at,sentiment,review_hash) VALUES (?,?,?,?,?,?,?,?,?,?)""",
                    (pk, ws_id, rid or None, r.get("author"), r.get("rating"),
                     r.get("text"), r.get("date"), datetime.now().isoformat(), sent, rh))
                new_items.append({**r, "sentiment": sent, "hash": rh})
            except sqlite3.IntegrityError: pass
    return new_items


SEV_RANK={"info":1,"warning":2,"critical":3}
def severity_rank(v):
    return SEV_RANK.get(str(v or "info").lower(),1)

def parse_hhmm(v,default):
    try:
        h,m=[int(x) for x in str(v or "").split(":",1)]
        if 0<=h<=23 and 0<=m<=59: return h,m
    except Exception:
        pass
    return default

def in_quiet_hours(now,start,end):
    sh,sm=parse_hhmm(start,(22,0)); eh,em=parse_hhmm(end,(7,0))
    cur=now.hour*60+now.minute; s=sh*60+sm; e=eh*60+em
    if s==e: return False
    return (cur>=s or cur<e) if s>e else s<=cur<e

def digest_window(frequency,now):
    h=1 if frequency=="hourly" else 168 if frequency=="weekly" else 24
    return now-timedelta(hours=h),now

def configured_channels(cfg):
    out=[]
    if cfg.get("SMTP_SENDER") and cfg.get("SMTP_PASSWORD") and cfg.get("SMTP_RECIPIENT"): out.append("email")
    if cfg.get("TELEGRAM_BOT_TOKEN") and cfg.get("TELEGRAM_CHAT_ID"): out.append("telegram")
    if cfg.get("SLACK_WEBHOOK_URL"): out.append("slack")
    if cfg.get("TEAMS_WEBHOOK_URL"): out.append("teams")
    if cfg.get("WHATSAPP_TOKEN") and cfg.get("WHATSAPP_PHONE_ID") and cfg.get("WHATSAPP_TO"): out.append("whatsapp")
    if cfg.get("WEBHOOK_URL"): out.append("webhook")
    return out

def send_channel(cfg,ch,subject,body):
    if ch=="email": return send_email(cfg,subject,body.replace("\n","<br>"))
    if ch=="telegram": return send_telegram(cfg,body)
    if ch=="slack": return send_slack(cfg,body)
    if ch=="teams": return send_teams(cfg,body)
    if ch=="whatsapp": return send_whatsapp(cfg,body)
    if ch=="webhook": return send_webhook(cfg,body)
    return False

def build_and_queue_due_digests(cfg,now=None):
    now=now or datetime.now(); queued=0
    with db_connect() as c:
        policies=c.execute("SELECT * FROM alert_delivery_policy WHERE enabled=1").fetchall()
        for pr in policies:
            p=dict(pr); ws=int(p.get("workspace_id") or 1); freq=str(p.get("digest_frequency") or "daily")
            start,end=digest_window(freq,now)
            rows=c.execute("""SELECT e.*,co.name,co.maps_url FROM branch_change_events e
                LEFT JOIN companies co ON co.pk=e.pk AND co.workspace_id=e.workspace_id
                WHERE e.workspace_id=? AND e.created_at>=? AND e.created_at<=? ORDER BY e.id""",
                (ws,start.isoformat(),end.isoformat())).fetchall()
            rows=[dict(r) for r in rows if severity_rank(r["severity"])>=severity_rank(p.get("min_severity"))]
            if not rows: continue
            sev=max((r.get("severity") or "info" for r in rows),key=severity_rank)
            ids=[int(r["id"]) for r in rows]
            key="digest:{}:{}".format(freq,hashlib.sha1((str(ws)+freq+start.replace(microsecond=0).isoformat()+end.replace(microsecond=0).isoformat()+sev).encode()).hexdigest()[:24])
            c.execute("""INSERT OR IGNORE INTO digest_runs(workspace_id,frequency,window_start,window_end,dedupe_key,severity_max,event_count,branch_count,source_event_ids,summary,status,created_at)
                VALUES(?,?,?,?,?,?,?,?,?,?,?,?)""",
                (ws,freq,start.isoformat(),end.isoformat(),key,sev,len(rows),len(set(r.get("pk") for r in rows)),json.dumps(ids),"{} change(s)".format(len(rows)),"built",now.isoformat()))
            dr=c.execute("SELECT id FROM digest_runs WHERE workspace_id=? AND dedupe_key=?",(ws,key)).fetchone(); drid=int(dr[0]) if dr else None
            try: channels=json.loads(p.get("channels_json") or "[]")
            except Exception: channels=[]
            channels=[x for x in channels if x in ("email","telegram","slack","teams","whatsapp","webhook")] or configured_channels(cfg)
            if not channels: continue
            subject="{}: {} important change(s)".format(sev.upper(),len(rows)); lines=[subject]
            for r in rows[:12]:
                line="• {} — {}: {}".format(r.get("name") or r.get("pk"),r.get("event_type"),r.get("detail") or r.get("new_value") or "")
                if r.get("maps_url"): line+=" | Google Maps: "+str(r.get("maps_url"))
                lines.append(line[:900])
            body="\n".join(lines)
            for ch in channels:
                cur=c.execute("""INSERT OR IGNORE INTO alert_outbox(workspace_id,digest_run_id,dedupe_key,severity,channel,subject,body,source_event_ids,status,attempts,max_attempts,queued_at)
                    VALUES(?,?,?,?,?,?,?,?,?,?,?,?)""",
                    (ws,drid,key,sev,ch,subject,body,json.dumps(ids),"queued",0,int(p.get("max_retries") or 3),now.isoformat()))
                queued+=1 if cur.rowcount else 0
    return queued

def process_outbox(cfg,now=None,limit=50):
    now=now or datetime.now(); sent=failed=deferred=0
    with db_connect() as c:
        rows=c.execute("""SELECT o.*,p.quiet_start,p.quiet_end FROM alert_outbox o
            LEFT JOIN alert_delivery_policy p ON p.workspace_id=o.workspace_id
            WHERE o.status IN ('queued','retry_wait') AND (o.next_attempt_at IS NULL OR o.next_attempt_at<=?)
            ORDER BY CASE o.severity WHEN 'critical' THEN 3 WHEN 'warning' THEN 2 ELSE 1 END DESC,o.id LIMIT ?""",
            (now.isoformat(),int(limit))).fetchall()
        for rr in rows:
            r=dict(rr)
            if in_quiet_hours(now,r.get("quiet_start") or "22:00",r.get("quiet_end") or "07:00"):
                deferred+=1; continue
            ok=send_channel(cfg,str(r.get("channel")),str(r.get("subject") or "Reviews Tracker"),str(r.get("body") or ""))
            attempts=int(r.get("attempts") or 0)+1; maxr=max(1,int(r.get("max_attempts") or 3))
            if ok:
                c.execute("UPDATE alert_outbox SET status='sent',attempts=?,sent_at=?,error=NULL,next_attempt_at=NULL WHERE id=?",
                          (attempts,now.isoformat(),int(r["id"]))); sent+=1
            elif attempts>=maxr:
                c.execute("UPDATE alert_outbox SET status='failed',attempts=?,error='delivery_failed',next_attempt_at=NULL WHERE id=?",
                          (attempts,int(r["id"]))); failed+=1
            else:
                nxt=now+timedelta(minutes=5*(2**(attempts-1)))
                c.execute("UPDATE alert_outbox SET status='retry_wait',attempts=?,error='delivery_failed',next_attempt_at=? WHERE id=?",
                          (attempts,nxt.isoformat(),int(r["id"]))); failed+=1
    return sent,failed,deferred


def run_once():
    log("Tick..."); cfg = load_cfg(); targets = get_targets()
    try:
        qn=build_and_queue_due_digests(cfg); sn,fn,dn=process_outbox(cfg); log("Delivery: queued={} sent={} failed={} deferred={}".format(qn,sn,fn,dn))
    except Exception as e:
        log("delivery: "+str(e)[:180])
    log("Targets: {}".format(len(targets)))
    for t in targets:
        pk = t["pk"]; name = t["name"]; ws = t.get("workspace_id", 1) or 1
        log("Checking: " + name)
        res = call_worker("scrape", name, 50)
        if res.get("error"):
            with db_connect() as c:
                c.execute("UPDATE monitored SET last_error=?, attempts=attempts+1 WHERE pk=? AND workspace_id=?",
                          (str(res["error"])[:300], pk, ws))
            continue
        new_items = save_reviews(pk, res.get("info", {}), res.get("reviews", []), ws)
        log("  New: {}".format(len(new_items)))
        negs = [r for r in new_items if r.get("sentiment") == "negative"]
        if negs:
            subject = "🚨 {} مراجعة سلبية جديدة — {}".format(len(negs), name)
            rows = ""
            for r in negs[:10]:
                stars = "*" * int(r.get("rating") or 0)
                rows += "<tr><td>{}</td><td>{}</td><td>{}</td></tr>".format(
                    html.escape(str(r.get("author", ""))[:30]), stars,
                    html.escape(str(r.get("text", ""))[:200]))
            body = "<html><body><h2>{}</h2><table border='1'>{}</table></body></html>".format(
                html.escape(subject), rows)
            plain = subject + "\n\n" + "\n".join(
                "- [{}] {}".format(r.get("rating"), str(r.get("text"))[:140]) for r in negs[:8])
            notify(cfg, subject, body, plain)
        with db_connect() as c:
            c.execute("UPDATE monitored SET last_success=?, last_error=NULL, attempts=0 WHERE pk=? AND workspace_id=?",
                      (datetime.now().isoformat(), pk, ws))
    log("Done.\n")


def main():
    log("=" * 50); log("Daemon v40.0 PID {}".format(os.getpid())); log("=" * 50)
    try: run_once()
    except Exception as e: log("Tick: " + str(e))
    while True:
        try:
            time.sleep(1800); run_once()
        except KeyboardInterrupt: break
        except Exception as e: log("Loop: " + str(e)); time.sleep(300)


if __name__ == "__main__": main()

#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Reviews Tracker v43 — Verified Data, RTL Dashboard & Complete Export."""
import sys, os, io, json, re, html, hashlib, sqlite3, subprocess, shutil, math, time, difflib, threading, zipfile, platform, queue, contextlib
from datetime import datetime, timedelta, date
from pathlib import Path
import urllib.request, urllib.parse
import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go

try:
    from langdetect import detect_langs, DetectorFactory
    DetectorFactory.seed = 42
    LANGDETECT_AVAILABLE = True
except Exception:
    detect_langs = None
    LANGDETECT_AVAILABLE = False

VERSION = "43.0.0-beta.1"
SCHEMA_VERSION = 49
APP_DIR = Path(__file__).parent
DATA_DIR = Path(os.environ.get("REVIEWS_TRACKER_DATA_DIR") or APP_DIR)
DATA_DIR.mkdir(parents=True, exist_ok=True)
DB_PATH = DATA_DIR / "reviews.db"
WORKER = APP_DIR / "worker.py"
DAEMON = APP_DIR / "daemon.py"
API_FILE = APP_DIR / "api.py"
API_KEY_FILE = DATA_DIR / ".api_key"
BACKUP_DIR = DATA_DIR / "backups"
BACKUP_DIR.mkdir(exist_ok=True)
JOB_DIR = DATA_DIR / "jobs"
JOB_DIR.mkdir(exist_ok=True)
SEARCH_SESSION_DIR = JOB_DIR / "sessions"
SEARCH_SESSION_DIR.mkdir(parents=True,exist_ok=True)
SEARCH_MANIFEST_VERSION = 1

def _pid_is_running(pid):
    try:
        pid=int(pid or 0)
        if pid<=0: return False
        if os.name=="nt":
            import ctypes
            PROCESS_QUERY_LIMITED_INFORMATION=0x1000
            handle=ctypes.windll.kernel32.OpenProcess(PROCESS_QUERY_LIMITED_INFORMATION,False,pid)
            if not handle: return False
            code=ctypes.c_ulong()
            ok=ctypes.windll.kernel32.GetExitCodeProcess(handle,ctypes.byref(code))
            ctypes.windll.kernel32.CloseHandle(handle)
            return bool(ok and code.value==259)
        os.kill(pid,0)
        return True
    except PermissionError:
        return True
    except Exception:
        return False

def _terminate_pid(pid):
    try:
        pid=int(pid or 0)
        if pid<=0: return False
        if os.name=="nt":
            r=subprocess.run(["taskkill","/PID",str(pid),"/T","/F"],stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL,timeout=8,check=False)
            return r.returncode==0
        import signal
        os.kill(pid,signal.SIGTERM)
        return True
    except Exception:
        return False

def _manifest_jsonable(value):
    if isinstance(value,dict): return {str(k):_manifest_jsonable(v) for k,v in value.items() if k!="proc"}
    if isinstance(value,(list,tuple)): return [_manifest_jsonable(v) for v in value]
    if isinstance(value,(str,int,float,bool)) or value is None: return value
    return str(value)

def _write_search_manifest(job,status=None):
    if not isinstance(job,dict) or not job.get("local_id"): return None
    path=Path(job.get("manifest") or (SEARCH_SESSION_DIR/(str(job["local_id"])+".manifest.json")))
    payload=_manifest_jsonable(job)
    proc=job.get("proc")
    if proc is not None:
        try: payload["pid"]=int(proc.pid)
        except Exception: pass
    payload["manifest_version"]=SEARCH_MANIFEST_VERSION
    payload["status"]=str(status or payload.get("status") or "running")
    payload["updated_at"]=datetime.now().isoformat(timespec="seconds")
    payload["manifest"]=str(path)
    path.parent.mkdir(parents=True,exist_ok=True)
    tmp=path.with_name(path.name+".tmp")
    tmp.write_text(json.dumps(payload,ensure_ascii=False,indent=2,default=str),encoding="utf-8")
    os.replace(str(tmp),str(path))
    job["manifest"]=str(path); job["status"]=payload["status"]; job["pid"]=payload.get("pid")
    return str(path)

def _load_search_manifest(path):
    try:
        p=Path(path)
        data=json.loads(p.read_text(encoding="utf-8"))
        if not isinstance(data,dict): return None
        if int(data.get("manifest_version") or 0)!=SEARCH_MANIFEST_VERSION: return None
        data["manifest"]=str(p)
        return data
    except Exception:
        return None

def _checkpoint_summary(path):
    try:
        p=Path(path or "")
        if not p.exists(): return {}
        data=json.loads(p.read_text(encoding="utf-8"))
        if not isinstance(data,dict): return {}
        return {"status":data.get("status"),"phase":data.get("phase"),"results":len(data.get("results") or []),
                "completed_jobs":len(data.get("completed_jobs") or []),"completed_details":len(data.get("completed_details") or []),
                "resume_count":int(data.get("resume_count") or 0),"updated_at":data.get("updated_at")}
    except Exception:
        return {}

def _find_resumable_search_sessions(limit=8):
    rows=[]
    try:
        paths=sorted(SEARCH_SESSION_DIR.glob("*.manifest.json"),key=lambda x:x.stat().st_mtime,reverse=True)
    except Exception:
        paths=[]
    for p in paths:
        m=_load_search_manifest(p)
        if not m or not m.get("query"): continue
        status=str(m.get("status") or "")
        running=_pid_is_running(m.get("pid")) if status=="running" else False
        if status=="running" and not running:
            status="ready_to_finalize" if Path(m.get("out") or "").exists() and Path(m.get("out") or "").stat().st_size>0 else "interrupted"
        cp=_checkpoint_summary(m.get("checkpoint"))
        if status in {"completed","error"} and cp.get("status")=="completed":
            continue
        if status not in {"running","ready_to_finalize","interrupted","cancelled","partial"} and not cp:
            continue
        m["effective_status"]=status; m["process_running"]=running; m["checkpoint_summary"]=cp
        rows.append(m)
        if len(rows)>=max(1,int(limit or 8)): break
    return rows

def _restore_detached_search_job():
    if isinstance(st.session_state.get("active_search_job"),dict): return False
    if st.session_state.get("resume_scan_done_v280"): return False
    st.session_state["resume_scan_done_v280"]=True
    for m in _find_resumable_search_sessions(5):
        if m.get("effective_status") in {"running","ready_to_finalize"}:
            job=dict(m); job["proc"]=None; job["detached"]=True; job["finalized"]=False
            st.session_state.active_search_job=job
            return True
    return False

def pyexe():
    v = APP_DIR / ".venv"
    p = v / ("Scripts/python.exe" if os.name == "nt" else "bin/python")
    return p if p.exists() else Path(sys.executable)


def load_cfg():
    cfg = {}
    for p in [APP_DIR / ".streamlit" / "secrets.toml", Path.home() / ".streamlit" / "secrets.toml"]:
        if p.exists():
            try:
                for line in p.read_text(encoding="utf-8").splitlines():
                    line = line.strip()
                    if not line or line.startswith("#") or "=" not in line: continue
                    k, v = line.split("=", 1)
                    cfg[k.strip()] = v.strip().strip('"').strip("'")
            except Exception: pass
    for k in ["SMTP_SERVER","SMTP_PORT","SMTP_SENDER","SMTP_PASSWORD","SMTP_RECIPIENT",
              "TELEGRAM_BOT_TOKEN","TELEGRAM_CHAT_ID","WEBHOOK_URL","SLACK_WEBHOOK_URL",
              "TEAMS_WEBHOOK_URL","WHATSAPP_TOKEN","WHATSAPP_PHONE_ID","WHATSAPP_TO",
              "GOOGLE_MAPS_API_KEY","GOOGLE_PLACES_API_KEY"]:
        if os.environ.get(k): cfg[k] = os.environ[k]
    return cfg


CFG = load_cfg()


# ═══════════════════════════════════════════════════════════════════════
#  Scheduled Insights & Alert Delivery 3.0 — pure policy helpers
# ═══════════════════════════════════════════════════════════════════════
def severity_rank(value):
    return {"info":1,"warning":2,"critical":3}.get(str(value or "info").lower(),1)

def _parse_hhmm(value, default=(22,0)):
    try:
        h,m=[int(x) for x in str(value or "").split(":",1)]
        if 0 <= h <= 23 and 0 <= m <= 59: return h,m
    except Exception:
        pass
    return default

def in_quiet_hours(now, start="22:00", end="07:00"):
    now=now or datetime.now(); sh,sm=_parse_hhmm(start,(22,0)); eh,em=_parse_hhmm(end,(7,0))
    cur=now.hour*60+now.minute; s=sh*60+sm; e=eh*60+em
    if s==e: return False
    return (cur>=s or cur<e) if s>e else (s<=cur<e)

def digest_window_bounds(frequency, now=None):
    now=now or datetime.now(); f=str(frequency or "daily").lower()
    hours=1 if f=="hourly" else 168 if f=="weekly" else 24
    return now-timedelta(hours=hours), now

def digest_dedupe_key(workspace_id, frequency, window_start, window_end, severity="info"):
    raw="{}|{}|{}|{}|{}".format(int(workspace_id),str(frequency),window_start.replace(microsecond=0).isoformat(),window_end.replace(microsecond=0).isoformat(),str(severity))
    return "digest:{}:{}".format(str(frequency),hashlib.sha1(raw.encode("utf-8","ignore")).hexdigest()[:24])

def save_local_secret(name, value):
    """Persist one local secret without exposing it in the UI."""
    name = str(name or "").strip(); value = str(value or "").strip()
    if not name or not value: return False, "empty value"
    try:
        sp = APP_DIR / ".streamlit" / "secrets.toml"
        sp.parent.mkdir(parents=True, exist_ok=True)
        lines = sp.read_text(encoding="utf-8").splitlines() if sp.exists() else []
        out=[]; replaced=False
        for line in lines:
            if re.match(r"^\s*{}\s*=".format(re.escape(name)), line):
                out.append('{} = {}'.format(name, json.dumps(value)))
                replaced=True
            else:
                out.append(line)
        if not replaced: out.append('{} = {}'.format(name, json.dumps(value)))
        sp.write_text("\n".join(out).rstrip()+"\n", encoding="utf-8")
        CFG[name]=value
        return True, str(sp)
    except Exception as e:
        return False, "{}: {}".format(type(e).__name__, e)

def test_google_live_connection():
    """Test keyless Google Search + Maps reachability and optional Places API."""
    out={"search":False,"maps":False,"places":False,"search_detail":"","maps_detail":"","places_detail":""}
    try:
        req=urllib.request.Request("https://www.google.com/search?q=restaurant+riyadh&hl=en",headers={"User-Agent":"Mozilla/5.0 ReviewsTracker/28.0"})
        with urllib.request.urlopen(req,timeout=8) as r:
            out["search"] = 200 <= int(getattr(r,"status",200)) < 400
            out["search_detail"] = "HTTP {}".format(getattr(r,"status",200))
    except Exception as e:
        out["search_detail"] = "{}: {}".format(type(e).__name__,str(e)[:160])
    try:
        req=urllib.request.Request("https://www.google.com/maps?hl=en",headers={"User-Agent":"Mozilla/5.0 ReviewsTracker/28.0"})
        with urllib.request.urlopen(req,timeout=8) as r:
            out["maps"] = 200 <= int(getattr(r,"status",200)) < 400
            out["maps_detail"] = "HTTP {}".format(getattr(r,"status",200))
    except Exception as e:
        out["maps_detail"] = "{}: {}".format(type(e).__name__,str(e)[:160])
    key=CFG.get("GOOGLE_MAPS_API_KEY") or CFG.get("GOOGLE_PLACES_API_KEY")
    if key:
        try:
            body={"textQuery":"restaurant Riyadh","languageCode":"en","pageSize":1}
            req=urllib.request.Request("https://places.googleapis.com/v1/places:searchText",
                data=json.dumps(body).encode("utf-8"),method="POST",headers={
                    "Content-Type":"application/json","X-Goog-Api-Key":key,
                    "X-Goog-FieldMask":"places.id,places.displayName"})
            with urllib.request.urlopen(req,timeout=10) as r:
                data=json.loads(r.read(256000).decode("utf-8","ignore"))
            out["places"] = bool(data.get("places"))
            out["places_detail"] = "{} result(s)".format(len(data.get("places") or []))
        except Exception as e:
            out["places_detail"] = "{}: {}".format(type(e).__name__,str(e)[:180])
    else:
        out["places_detail"] = "Optional only; keyless Google Search + Maps remain active"
    return out

def run_windows_search_preflight(query, lang="ar", timeout_seconds=45):
    """Run the worker field-preflight before starting a real user search."""
    query=str(query or "").strip() or "مطعم الرياض"
    cmd=[str(pyexe()),str(WORKER),"preflight",query,"--max","5","--lang",str(lang or "ar")]
    started=time.perf_counter()
    try:
        proc=subprocess.run(cmd,cwd=str(APP_DIR),stdout=subprocess.PIPE,stderr=subprocess.PIPE,timeout=timeout_seconds,check=False)
        out=proc.stdout.decode("utf-8","ignore").strip(); err=proc.stderr.decode("utf-8","ignore")
        payload=json.loads(out) if out else {"ready":False,"reason":"empty_preflight_output"}
        if not isinstance(payload,dict): payload={"ready":False,"reason":"invalid_preflight_output"}
        payload["returncode"]=proc.returncode; payload["app_elapsed_ms"]=round((time.perf_counter()-started)*1000,1)
        payload["stderr_tail"]="\n".join(err.splitlines()[-12:])
        return payload
    except subprocess.TimeoutExpired:
        return {"ready":False,"hard_block":False,"reason":"preflight_timeout","error":"preflight timeout after {}s".format(timeout_seconds)}
    except Exception as e:
        return {"ready":False,"hard_block":False,"reason":"preflight_error","error":"{}: {}".format(type(e).__name__,str(e)[:300])}


def build_field_diagnostic_bundle(query="مطعم الرياض", lang="ar", timeout_seconds=90):
    """Ask the worker to create a redacted Windows/search diagnostic ZIP."""
    query=str(query or "").strip() or "مطعم الرياض"
    out=JOB_DIR/("ReviewsTracker_Field_Diagnostics_"+datetime.now().strftime("%Y%m%d_%H%M%S")+".zip")
    cmd=[str(pyexe()),str(WORKER),"diagnostics",query,"--lang",str(lang or "ar"),"--output",str(out)]
    try:
        proc=subprocess.run(cmd,cwd=str(APP_DIR),stdout=subprocess.PIPE,stderr=subprocess.PIPE,timeout=timeout_seconds,check=False)
        raw=proc.stdout.decode("utf-8","ignore").strip(); payload=json.loads(raw) if raw else {}
        path=Path((payload or {}).get("path") or out)
        return {"ok":proc.returncode==0 and path.exists(),"path":str(path),"payload":payload,
                "error":"\n".join(proc.stderr.decode("utf-8","ignore").splitlines()[-12:])}
    except Exception as e:
        return {"ok":False,"path":str(out),"error":"{}: {}".format(type(e).__name__,str(e)[:300])}


def run_google_maps_live_probe(query, lang="ar", timeout_seconds=75):
    """Run the real worker once and return browser/provider diagnostics without mutating saved branches."""
    query=str(query or "").strip()
    if not query:
        return {"ok":False,"error":"empty query"}
    cmd=[str(pyexe()),str(WORKER),"search",query,"--max","8","--lang",str(lang or "ar")]
    started=time.perf_counter()
    try:
        proc=subprocess.run(cmd,cwd=str(APP_DIR),stdout=subprocess.PIPE,stderr=subprocess.PIPE,timeout=timeout_seconds,check=False)
        out=proc.stdout.decode("utf-8","ignore").strip(); err=proc.stderr.decode("utf-8","ignore")
        payload=json.loads(out) if out else {}
        meta=payload.get("meta") if isinstance(payload,dict) else {}
        results=payload.get("results") if isinstance(payload,dict) else []
        progress=[]
        for line in err.splitlines():
            if line.startswith("RT_PROGRESS|"):
                try: progress.append(json.loads(line.split("|",1)[1]))
                except Exception: pass
        return {
            "ok":proc.returncode==0 and isinstance(payload,dict),"returncode":proc.returncode,
            "elapsed_ms":round((time.perf_counter()-started)*1000,1),"results":len(results or []),
            "browser_engine":(meta or {}).get("browser_engine"),"async_mode":(meta or {}).get("async_mode"),
            "browser_engines_tried":(meta or {}).get("browser_engines_tried") or [],
            "browser_failover_used":bool((meta or {}).get("browser_failover_used")),
            "browser_failover_engine":(meta or {}).get("browser_failover_engine"),
            "partial_results_preserved":bool((meta or {}).get("partial_results_preserved")),
            "termination_reason":(meta or {}).get("termination_reason"),"no_result_reason":(meta or {}).get("no_result_reason"),
            "providers":(meta or {}).get("providers"),"discovery_concurrency":(meta or {}).get("async_discovery_concurrency"),
            "detail_concurrency":(meta or {}).get("async_detail_concurrency"),"errors":(meta or {}).get("errors") or [],
            "selector_strategy_order":(meta or {}).get("selector_strategy_order") or [],
            "best_selector_strategy":(meta or {}).get("best_selector_strategy") or "",
            "extraction_telemetry":(meta or {}).get("extraction_telemetry") or {},
            "selector_health":(meta or {}).get("selector_health") or [],
            "progress":progress[-12:],"stderr_tail":"\n".join(err.splitlines()[-12:]),
        }
    except subprocess.TimeoutExpired:
        return {"ok":False,"error":"probe timeout after {}s".format(timeout_seconds),"termination_reason":"probe_timeout"}
    except Exception as e:
        return {"ok":False,"error":"{}: {}".format(type(e).__name__,str(e)[:300])}

@st.cache_data(ttl=45, show_spinner=False)
def google_search_suggestions(fragment, lang="ar", limit=8):
    """Return keyless Google Search suggestions for a full or partial business name."""
    q=str(fragment or "").strip()
    if len(q) < 2:
        return []
    try:
        url="https://suggestqueries.google.com/complete/search?"+urllib.parse.urlencode({
            "client":"firefox","hl":str(lang or "ar"),"q":q
        })
        req=urllib.request.Request(url,headers={
            "User-Agent":"Mozilla/5.0 (Windows NT 10.0; Win64; x64) ReviewsTracker/28.0",
            "Accept":"application/json,text/plain,*/*"
        })
        with urllib.request.urlopen(req,timeout=4.0) as r:
            data=json.loads(r.read(256000).decode("utf-8","ignore"))
        raw=data[1] if isinstance(data,list) and len(data)>1 and isinstance(data[1],list) else []
        norm=lambda x: re.sub(r"\s+"," ",str(x or "").strip()).casefold()
        base=norm(q); out=[]
        for item in raw:
            txt=str(item or "").strip()
            if not txt: continue
            nt=norm(txt)
            if base and base not in nt and not any(tok and tok in nt for tok in base.split()):
                continue
            if txt not in out: out.append(txt)
            if len(out)>=max(1,min(int(limit or 8),12)): break
        return out
    except Exception:
        return []


# ═══════════════════════════════════════════════════════════════════════
#  I18N — 4 languages
# ═══════════════════════════════════════════════════════════════════════
I18N = {
"ar": {
    "dashboard":"لوحة التحكم","discovery":"الاكتشاف","reviews":"المراجعات",
    "branches":"الفروع","analytics":"التحليلات","followup":"المتابعة",
    "reports":"التقارير","system":"النظام","quick_search":"بحث سريع",
    "branch_search":"بحث الفروع","map_url":"رابط Google Maps","all_reviews":"جميع المراجعات",
    "negative":"السلبية","unanswered":"بدون رد","all_branches":"جميع الفروع",
    "map":"الخريطة","compare":"المقارنة","performance":"الأداء","executive":"الملخص التنفيذي",
    "alerts":"التنبيهات","tickets":"التذاكر","monitoring":"المراقبة","settings":"الإعدادات",
    "diagnostics":"التشخيص","workspace":"مساحة الفرع","global_search":"بحث… Ctrl+K",
    "period":"الفترة","language":"اللغة","theme":"المظهر","today":"اليوم",
    "d7":"7 أيام","d30":"30 يومًا","d90":"90 يومًا","all":"الكل",
    "avg_rating":"متوسط التقييم","total_reviews":"إجمالي المراجعات","response_rate":"نسبة الرد",
    "best_branch":"أفضل فرع","worst_branch":"أسوأ فرع","health":"الصحة",
    "no_data":"لا توجد بيانات","search":"بحث","analyze":"تحليل","responded":"تم الرد",
    "ignore":"تجاهل","open":"فتح","priority":"الأولوية","workspaces":"مساحات العمل",
    "users":"المستخدمون","sync":"المزامنة","reports_builder":"منشئ التقارير",
    "activity_log":"سجل النشاط","schema_doctor":"طبيب المخطط","ai_settings":"إعدادات الذكاء الاصطناعي",
    "sla":"SLA","forecast":"توقعات","anomalies":"الشذوذ","duplicates":"التكرار",
    "team":"الفريق","competitors":"المنافسون","templates":"القوالب",
    "api":"REST API","whitelabel":"الهوية البصرية","whatsapp":"واتساب","notifications":"الإشعارات",
    "aspects":"الجوانب","wordcloud":"سحابة الكلمات","reviewers":"المُقيّمون",
    "backups":"النسخ الاحتياطية","market_share":"الحصة السوقية","benchmark":"المقارنة المرجعية",
},
"en": {
    "dashboard":"Dashboard","discovery":"Discovery","reviews":"Reviews",
    "branches":"Branches","analytics":"Analytics","followup":"Follow-up",
    "reports":"Reports","system":"System","quick_search":"Quick Search",
    "branch_search":"Branch Search","map_url":"Maps URL","all_reviews":"All Reviews",
    "negative":"Negative","unanswered":"Unanswered","all_branches":"All Branches",
    "map":"Map","compare":"Compare","performance":"Performance","executive":"Executive",
    "alerts":"Alerts","tickets":"Tickets","monitoring":"Monitoring","settings":"Settings",
    "diagnostics":"Diagnostics","workspace":"Workspace","global_search":"Search… Ctrl+K",
    "period":"Period","language":"Language","theme":"Theme","today":"Today",
    "d7":"7 days","d30":"30 days","d90":"90 days","all":"All",
    "avg_rating":"Average Rating","total_reviews":"Total Reviews","response_rate":"Response Rate",
    "best_branch":"Best","worst_branch":"Worst","health":"Health",
    "no_data":"No data","search":"Search","analyze":"Analyze","responded":"Responded",
    "ignore":"Ignore","open":"Open","priority":"Priority","workspaces":"Workspaces",
    "users":"Users","sync":"Sync","reports_builder":"Report Builder",
    "activity_log":"Activity Log","schema_doctor":"Schema Doctor","ai_settings":"AI Settings",
    "sla":"SLA","forecast":"Forecast","anomalies":"Anomalies","duplicates":"Duplicates",
    "team":"Team","competitors":"Competitors","templates":"Templates",
    "api":"REST API","whitelabel":"White-label","whatsapp":"WhatsApp","notifications":"Notifications",
    "aspects":"Aspects","wordcloud":"Word Cloud","reviewers":"Reviewers",
    "backups":"Backups","market_share":"Market Share","benchmark":"Benchmarking",
},
"ur": {
    "dashboard":"ڈیش بورڈ","discovery":"دریافت","reviews":"جائزے",
    "branches":"برانچز","analytics":"تجزیات","followup":"پیروی",
    "reports":"رپورٹس","system":"سسٹم","quick_search":"فوری تلاش",
    "branch_search":"برانچ تلاش","map_url":"گوگل میپ لنک","all_reviews":"تمام جائزے",
    "negative":"منفی","unanswered":"جواب نہیں","all_branches":"تمام برانچز",
    "map":"نقشہ","compare":"موازنہ","performance":"کارکردگی","executive":"ایگزیکٹو",
    "alerts":"انتباہات","tickets":"ٹکٹس","monitoring":"نگرانی","settings":"ترتیبات",
    "diagnostics":"تشخیص","workspace":"ورک اسپیس","global_search":"تلاش… Ctrl+K",
    "period":"مدت","language":"زبان","theme":"تھیم","today":"آج",
    "d7":"7 دن","d30":"30 دن","d90":"90 دن","all":"تمام",
    "avg_rating":"اوسط ریٹنگ","total_reviews":"کل جائزے","response_rate":"جواب کی شرح",
    "best_branch":"بہترین","worst_branch":"بدترین","health":"صحت",
    "no_data":"ڈیٹا نہیں","search":"تلاش","analyze":"تجزیہ","responded":"جواب دیا",
    "ignore":"نظر انداز","open":"کھولیں","priority":"ترجیح","workspaces":"ورک اسپیسز",
    "users":"صارفین","sync":"مطابقت","reports_builder":"رپورٹ بلڈر",
    "activity_log":"سرگرمی لاگ","schema_doctor":"اسکیما ڈاکٹر","ai_settings":"AI ترتیبات",
    "sla":"SLA","forecast":"پیشن گوئی","anomalies":"بے ضابطگیاں","duplicates":"نقل",
    "team":"ٹیم","competitors":"حریف","templates":"ٹیمپلیٹس",
    "api":"REST API","whitelabel":"سفید لیبل","whatsapp":"واٹس ایپ","notifications":"اطلاعات",
    "aspects":"پہلو","wordcloud":"لفظ بادل","reviewers":"جائزہ لینے والے",
    "backups":"بیک اپ","market_share":"مارکیٹ شیئر","benchmark":"بینچ مارکنگ",
},
"fr": {
    "dashboard":"Tableau de bord","discovery":"Découverte","reviews":"Avis",
    "branches":"Succursales","analytics":"Analytique","followup":"Suivi",
    "reports":"Rapports","system":"Système","quick_search":"Recherche rapide",
    "branch_search":"Recherche succursale","map_url":"URL Google Maps","all_reviews":"Tous les avis",
    "negative":"Négatifs","unanswered":"Sans réponse","all_branches":"Toutes les succursales",
    "map":"Carte","compare":"Comparer","performance":"Performance","executive":"Exécutif",
    "alerts":"Alertes","tickets":"Tickets","monitoring":"Surveillance","settings":"Paramètres",
    "diagnostics":"Diagnostic","workspace":"Espace de travail","global_search":"Rechercher… Ctrl+K",
    "period":"Période","language":"Langue","theme":"Thème","today":"Aujourd'hui",
    "d7":"7 jours","d30":"30 jours","d90":"90 jours","all":"Tout",
    "avg_rating":"Note moyenne","total_reviews":"Total avis","response_rate":"Taux de réponse",
    "best_branch":"Meilleure","worst_branch":"Pire","health":"Santé",
    "no_data":"Pas de données","search":"Rechercher","analyze":"Analyser","responded":"Répondu",
    "ignore":"Ignorer","open":"Ouvrir","priority":"Priorité","workspaces":"Espaces",
    "users":"Utilisateurs","sync":"Sync","reports_builder":"Générateur de rapports",
    "activity_log":"Journal","schema_doctor":"Docteur de schéma","ai_settings":"Paramètres IA",
    "sla":"SLA","forecast":"Prévision","anomalies":"Anomalies","duplicates":"Doublons",
    "team":"Équipe","competitors":"Concurrents","templates":"Modèles",
    "api":"API REST","whitelabel":"Marque blanche","whatsapp":"WhatsApp","notifications":"Notifications",
    "aspects":"Aspects","wordcloud":"Nuage de mots","reviewers":"Évaluateurs",
    "backups":"Sauvegardes","market_share":"Part de marché","benchmark":"Analyse comparative",
}}


def t(key):
    lang = st.session_state.get("lang", "ar")
    return I18N.get(lang, I18N["ar"]).get(key, I18N["ar"].get(key, key))


def safe_num(v, d=0.0):
    try:
        if pd.isna(v): return d
        return float(v)
    except Exception: return d


def safe_error_payload_v38(exc, context=""):
    """Return a concise user-safe error plus an opaque diagnostic reference."""
    kind="database" if isinstance(exc, sqlite3.Error) else "runtime"
    raw="{}: {}".format(type(exc).__name__, str(exc or ""))
    seed="{}|{}|{}".format(str(context or ""), type(exc).__name__, str(exc or ""))
    ref="RT-"+hashlib.sha1(seed.encode("utf-8","ignore")).hexdigest()[:10].upper()
    if kind=="database":
        msg="تعذر قراءة بعض بيانات قاعدة البيانات. استخدم فحص النظام/إصلاح المخطط ثم أعد المحاولة."
    else:
        msg="تعذر إكمال هذه العملية. أعد المحاولة، وإن تكرر الخطأ استخدم صفحة حالة النظام."
    return {"kind":kind,"reference":ref,"user_message":msg,"technical":raw,"context":str(context or "")}


def record_local_error_v38(payload, app_dir=None):
    """Persist technical error details locally while keeping the UI message concise."""
    root=Path(app_dir or APP_DIR); root.mkdir(parents=True,exist_ok=True)
    path=root/"app_errors_v38.jsonl"
    try:
        if path.exists() and path.stat().st_size > 2*1024*1024:
            old=root/"app_errors_v38.previous.jsonl"
            try:
                if old.exists(): old.unlink()
                path.replace(old)
            except Exception: pass
        row={"ts":datetime.now().isoformat(timespec="seconds"),"reference":payload.get("reference"),
             "kind":payload.get("kind"),"context":payload.get("context"),"technical":payload.get("technical")}
        with path.open("a",encoding="utf-8") as f: f.write(json.dumps(row,ensure_ascii=False,default=str)+"\n")
    except Exception:
        pass
    return str(path)


def build_local_health_check_v38(db, app_dir=None, db_path=None):
    """Fast local-only health checks; never requires Google/network access."""
    root=Path(app_dir or APP_DIR); dbp=Path(db_path or DB_PATH); rows=[]
    try:
        comp=db.compatibility_report_v38(); integ=str(db.integrity() or "")
        status="ok" if comp.get("status")=="ok" and integ.lower()=="ok" else "warning"
        detail="schema={} · integrity={}".format(comp.get("status"),integ or "unknown")
    except Exception as exc:
        p=safe_error_payload_v38(exc,"local_health.database"); status="error"; detail=p["reference"]
    rows.append({"key":"database","label":"قاعدة البيانات","status":status,"detail":detail})
    for key,name in (("worker","worker.py"),("api","api.py"),("daemon","daemon.py")):
        ok=(root/name).is_file(); rows.append({"key":key,"label":name,"status":"ok" if ok else "error","detail":str(root/name)})
    writable=False
    try:
        probe=root/".rt38_write_probe"; probe.write_text("ok",encoding="utf-8"); probe.unlink(missing_ok=True); writable=True
    except Exception: writable=False
    rows.append({"key":"writable","label":"مجلد البرنامج قابل للكتابة","status":"ok" if writable else "error","detail":str(root)})
    rows.append({"key":"db_file","label":"ملف قاعدة البيانات","status":"ok" if dbp.exists() else "warning","detail":str(dbp)})
    return rows


# ═══════════════════════════════════════════════════════════════════════
#  DATABASE — Self-Healing
# ═══════════════════════════════════════════════════════════════════════

# ═══════════════════════════════════════════════════════════════════════
#  v39.0 Final Release — verified backup / restore / recovery / preflight
# ═══════════════════════════════════════════════════════════════════════
def _sha256_file_v39(path):
    p=Path(path); h=hashlib.sha256()
    with p.open("rb") as f:
        for chunk in iter(lambda:f.read(1024*1024),b""):
            h.update(chunk)
    return h.hexdigest()


def sqlite_integrity_report_v39(path):
    p=Path(path)
    out={"path":str(p),"exists":p.exists(),"ok":False,"integrity":"missing","foreign_key_issues":0,"size":0}
    if not p.exists() or not p.is_file(): return out
    out["size"]=p.stat().st_size
    con=None
    try:
        con=sqlite3.connect("file:{}?mode=ro".format(p.as_posix()),uri=True,timeout=10)
        rows=con.execute("PRAGMA integrity_check").fetchall()
        vals=[str(r[0]) for r in rows]
        out["integrity"]="; ".join(vals[:20])
        try: fk=con.execute("PRAGMA foreign_key_check").fetchall()
        except Exception: fk=[]
        out["foreign_key_issues"]=len(fk)
        out["ok"]=bool(vals) and all(v.lower()=="ok" for v in vals) and len(fk)==0
    except Exception as exc:
        out["integrity"]="{}: {}".format(type(exc).__name__,str(exc)[:240])
        out["ok"]=False
    finally:
        if con is not None:
            try: con.close()
            except Exception: pass
    return out


def _sqlite_online_copy_v39(source_path,dest_path):
    src_p=Path(source_path); dst_p=Path(dest_path); dst_p.parent.mkdir(parents=True,exist_ok=True)
    if not src_p.exists(): raise FileNotFoundError(str(src_p))
    try: dst_p.unlink(missing_ok=True)
    except Exception: pass
    src=dst=None
    try:
        src=sqlite3.connect(str(src_p),timeout=30)
        dst=sqlite3.connect(str(dst_p),timeout=30)
        with dst: src.backup(dst,pages=256,sleep=0.01)
    finally:
        if src is not None:
            try: src.close()
            except Exception: pass
        if dst is not None:
            try: dst.close()
            except Exception: pass
    return dst_p


def create_verified_backup_v39(source_path=None,backup_dir=None,prefix="reviews"):
    source=Path(source_path or DB_PATH); out_dir=Path(backup_dir or BACKUP_DIR); out_dir.mkdir(parents=True,exist_ok=True)
    source_check=sqlite_integrity_report_v39(source)
    if not source_check.get("ok"):
        raise RuntimeError("source database failed integrity check: {}".format(source_check.get("integrity")))
    stamp=datetime.now().strftime("%Y%m%d_%H%M%S_%f")
    final=out_dir/("{}_{}.db".format(str(prefix or "reviews"),stamp)); tmp=final.with_suffix(".db.tmp")
    try:
        _sqlite_online_copy_v39(source,tmp)
        check=sqlite_integrity_report_v39(tmp)
        if not check.get("ok"):
            raise RuntimeError("backup failed integrity check: {}".format(check.get("integrity")))
        os.replace(str(tmp),str(final))
        sha=_sha256_file_v39(final)
        final.with_suffix(final.suffix+".sha256").write_text("{}  {}\n".format(sha,final.name),encoding="utf-8")
        return {"ok":True,"path":str(final),"sha256":sha,"integrity":check.get("integrity"),"size":final.stat().st_size}
    except Exception:
        try: tmp.unlink(missing_ok=True)
        except Exception: pass
        try: final.unlink(missing_ok=True)
        except Exception: pass
        raise


def backup_validate_v39(path):
    p=Path(path); check=sqlite_integrity_report_v39(p)
    check["sha256"]=_sha256_file_v39(p) if p.exists() and check.get("ok") else ""
    return check


def restore_verified_backup_v39(backup_path,target_path=None,backup_dir=None):
    source=Path(backup_path); target=Path(target_path or DB_PATH); out_dir=Path(backup_dir or BACKUP_DIR); out_dir.mkdir(parents=True,exist_ok=True)
    source_check=sqlite_integrity_report_v39(source)
    if source.exists() and source_check.get("ok"):
        source_check["sha256"]=_sha256_file_v39(source)
    if not source_check.get("ok"):
        raise RuntimeError("backup is not a valid SQLite database: {}".format(source_check.get("integrity")))
    rollback=None
    if target.exists() and target.stat().st_size>0:
        rollback=create_verified_backup_v39(target,out_dir,prefix="pre_restore")
    tmp=target.with_name(target.name+".restore_v39.tmp")
    try:
        _sqlite_online_copy_v39(source,tmp)
        tmp_check=sqlite_integrity_report_v39(tmp)
        if not tmp_check.get("ok"):
            raise RuntimeError("restored temporary database failed validation")
        # No persistent DB connection is held by the app; remove stale WAL/SHM before atomic swap.
        for suffix in ("-wal","-shm"):
            try: Path(str(target)+suffix).unlink(missing_ok=True)
            except Exception: pass
        os.replace(str(tmp),str(target))
        final_check=sqlite_integrity_report_v39(target)
        if not final_check.get("ok"):
            raise RuntimeError("restored database failed final validation")
        return {"ok":True,"path":str(target),"sha256":_sha256_file_v39(target),
                "rollback_path":str((rollback or {}).get("path") or ""),"integrity":final_check.get("integrity")}
    except Exception:
        try: tmp.unlink(missing_ok=True)
        except Exception: pass
        raise


def _peek_schema_version_v39(path):
    p=Path(path)
    if not p.exists() or p.stat().st_size<=0: return None
    con=None
    try:
        con=sqlite3.connect("file:{}?mode=ro".format(p.as_posix()),uri=True,timeout=5)
        exists=con.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='schema_version'").fetchone()
        if not exists: return 0
        row=con.execute("SELECT version FROM schema_version WHERE id=1").fetchone()
        return int(row[0]) if row and row[0] is not None else 0
    except Exception:
        return 0
    finally:
        if con is not None:
            try: con.close()
            except Exception: pass


def pre_migration_backup_v39(db_path=None,backup_dir=None,current_schema=None):
    p=Path(db_path or DB_PATH); current=int(current_schema if current_schema is not None else SCHEMA_VERSION)
    if not p.exists() or p.stat().st_size<=0: return None
    old=_peek_schema_version_v39(p)
    if old == current: return None
    return create_verified_backup_v39(p,backup_dir or BACKUP_DIR,prefix="pre_migration_s{}_to_s{}".format(old,current))


def recover_stale_search_jobs_v39(db_path=None,session_dir=None,stale_minutes=120):
    dbp=Path(db_path or DB_PATH); sdir=Path(session_dir or SEARCH_SESSION_DIR)
    active=set(); kept_live=0; recovered=0; manifests_recovered=0
    if sdir.exists():
        for mp in sdir.glob("*.manifest.json"):
            try:
                m=json.loads(mp.read_text(encoding="utf-8"))
                ids={str(m.get("job_id") or ""),str(m.get("local_id") or "")}-{""}
                if str(m.get("status") or "")=="running" and _pid_is_running(m.get("pid")):
                    active.update(ids); kept_live+=1
                elif str(m.get("status") or "")=="running":
                    m["status"]="interrupted"; m["updated_at"]=datetime.now().isoformat(timespec="seconds")
                    tmp=mp.with_name(mp.name+".tmp")
                    tmp.write_text(json.dumps(m,ensure_ascii=False,indent=2,default=str),encoding="utf-8"); os.replace(str(tmp),str(mp)); manifests_recovered+=1
            except Exception: pass
    if not dbp.exists(): return {"recovered":0,"kept_live":kept_live,"manifests_recovered":manifests_recovered}
    cutoff=(datetime.now()-timedelta(minutes=max(1,int(stale_minutes)))).isoformat(); now=datetime.now().isoformat(timespec="seconds")
    con=None
    try:
        con=sqlite3.connect(str(dbp),timeout=15)
        exists=con.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='search_jobs'").fetchone()
        if not exists: return {"recovered":0,"kept_live":kept_live,"manifests_recovered":manifests_recovered}
        rows=con.execute("SELECT job_id FROM search_jobs WHERE status IN ('running','started') AND COALESCE(started_at,'')<?",(cutoff,)).fetchall()
        for (job_id,) in rows:
            if str(job_id or "") in active: continue
            con.execute("UPDATE search_jobs SET status='interrupted',completed_at=?,detail=COALESCE(detail,'') || ? WHERE job_id=?",
                        (now," | v39 recovered stale runtime state",job_id)); recovered+=1
        con.commit()
    finally:
        if con is not None:
            try: con.close()
            except Exception: pass
    return {"recovered":recovered,"kept_live":kept_live,"manifests_recovered":manifests_recovered}


def final_preflight_v39(app_dir=None,db_path=None):
    root=Path(app_dir or APP_DIR); dbp=Path(db_path or DB_PATH); checks=[]
    dbcheck=sqlite_integrity_report_v39(dbp)
    checks.append({"key":"database","status":"ok" if dbcheck.get("ok") else "error","detail":dbcheck.get("integrity")})
    for key,name in (("worker","worker.py"),("api","api.py"),("daemon","daemon.py")):
        fp=root/name; checks.append({"key":key,"status":"ok" if fp.exists() and fp.stat().st_size>0 else "error","detail":str(fp)})
    probe=root/(".v39_write_probe_{}".format(os.getpid()))
    try:
        probe.write_text("ok",encoding="utf-8"); writable=probe.read_text(encoding="utf-8")=="ok"
    except Exception: writable=False
    finally:
        try: probe.unlink(missing_ok=True)
        except Exception: pass
    checks.append({"key":"writable","status":"ok" if writable else "error","detail":str(root)})
    for key,module in (("reportlab","reportlab"),("openpyxl","openpyxl")):
        try: __import__(module); status="ok"; detail="available"
        except Exception as exc: status="error"; detail="{}: {}".format(type(exc).__name__,str(exc)[:120])
        checks.append({"key":key,"status":status,"detail":detail})
    return {"ok":all(x.get("status")=="ok" for x in checks),"checks":checks,"timestamp":datetime.now().isoformat(timespec="seconds")}

class DB:
    def __init__(self):
        self.migration_log = []
        self.pre_migration_backup_v39 = None
        try:
            self.pre_migration_backup_v39 = pre_migration_backup_v39(DB_PATH, BACKUP_DIR, SCHEMA_VERSION)
            if self.pre_migration_backup_v39:
                self.migration_log.append("v39 verified pre-migration backup: " + str(self.pre_migration_backup_v39.get("path") or ""))
        except Exception as exc:
            self.migration_log.append("v39 pre-migration backup warning: {}: {}".format(type(exc).__name__,str(exc)[:160]))
        self._init()
        self._ensure_search_jobs_compat_v41()

    def _ensure_search_jobs_compat_v41(self):
        """Idempotently repair the v40 database mismatch without removing legacy rows."""
        with contextlib.closing(self._c()) as c, c:
            if not self._table_exists(c, "search_jobs"):
                return
            cols = self._cols(c, "search_jobs")
            if "job_id" not in cols:
                c.execute("ALTER TABLE search_jobs ADD COLUMN job_id TEXT")
            c.execute("CREATE UNIQUE INDEX IF NOT EXISTS ux_search_jobs_job_id_v41 "
                      "ON search_jobs(job_id) WHERE job_id IS NOT NULL")

    def _c(self):
        c = sqlite3.connect(str(DB_PATH), check_same_thread=False, timeout=30)
        c.row_factory = sqlite3.Row
        c.execute("PRAGMA journal_mode=WAL")
        c.execute("PRAGMA busy_timeout=30000")
        c.execute("PRAGMA synchronous=NORMAL")
        c.execute("PRAGMA foreign_keys=ON")
        c.execute("PRAGMA temp_store=MEMORY")
        c.execute("PRAGMA cache_size=-32768")
        try: c.execute("PRAGMA mmap_size=268435456")
        except Exception: pass
        return c

    def _table_exists(self, c, name):
        try:
            return c.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",
                             (name,)).fetchone() is not None
        except Exception: return False

    def _cols(self, c, table):
        try: return {r[1] for r in c.execute("PRAGMA table_info({})".format(table))}
        except Exception: return set()

    def _company_row_order_expr(self, c):
        """Return a stable row key compatible with legacy companies tables.

        Older Reviews Tracker databases used ``pk`` without an integer ``id``.
        SQLite keeps ``rowid`` for those ordinary tables, so identity scans and
        migration dedupe must not assume that ``id`` exists.
        """
        if "id" in self._cols(c, "companies"):
            return "id"
        try:
            c.execute("SELECT rowid FROM companies LIMIT 1").fetchone()
            return "rowid"
        except sqlite3.OperationalError:
            return "pk"

    def compatibility_report_v38(self):
        """Non-destructive compatibility report for current and legacy SQLite layouts."""
        required={
            "companies":{"pk","workspace_id","name"},
            "reviews":{"workspace_id","company_pk"},
            "workspaces":{"id"},
            "users":{"id"},
        }
        report={"status":"ok","tables":{},"warnings":[]}
        with self._c() as c:
            for table,need in required.items():
                exists=self._table_exists(c,table); cols=self._cols(c,table) if exists else set()
                missing=sorted(need-cols) if exists else sorted(need)
                row_key=None
                if table=="companies" and exists:
                    row_key=self._company_row_order_expr(c)
                item={"exists":bool(exists),"columns":sorted(cols),"required_missing":missing,"row_key":row_key}
                report["tables"][table]=item
                if not exists:
                    report["warnings"].append("missing table: "+table)
                elif missing:
                    report["warnings"].append("{} missing: {}".format(table,", ".join(missing)))
            try:
                quick=str(c.execute("PRAGMA quick_check").fetchone()[0])
            except Exception as exc:
                quick="error: "+type(exc).__name__
            report["quick_check"]=quick
        if any(not x.get("exists") or x.get("required_missing") for x in report["tables"].values()):
            report["status"]="warning"
        if str(report.get("quick_check") or "").lower()!="ok": report["status"]="warning"
        return report

    def _add(self, c, table, col, defn):
        try:
            if col in self._cols(c, table): return False
            c.execute("ALTER TABLE {} ADD COLUMN {} {}".format(table, col, defn))
            self.migration_log.append("+ {}.{}".format(table, col))
            return True
        except sqlite3.OperationalError as e:
            self.migration_log.append("✗ {}.{}: {}".format(table, col, str(e)[:80]))
            return False

    def _create_tables(self):
        with self._c() as c:
            c.executescript("""
                CREATE TABLE IF NOT EXISTS workspaces(
                    id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT UNIQUE NOT NULL,
                    description TEXT, icon TEXT DEFAULT '🏢', color TEXT DEFAULT '#2563EB',
                    created_at TEXT, active INTEGER DEFAULT 1);
                CREATE TABLE IF NOT EXISTS users(
                    id INTEGER PRIMARY KEY AUTOINCREMENT, username TEXT UNIQUE NOT NULL,
                    display_name TEXT, password_hash TEXT NOT NULL, salt TEXT NOT NULL,
                    role TEXT DEFAULT 'viewer', workspace_id INTEGER,
                    email TEXT, active INTEGER DEFAULT 1, created_at TEXT, last_login TEXT);
                CREATE TABLE IF NOT EXISTS activity_log(
                    id INTEGER PRIMARY KEY AUTOINCREMENT, username TEXT,
                    workspace_id INTEGER, action TEXT, detail TEXT, ts TEXT);
                CREATE TABLE IF NOT EXISTS companies(
                    id INTEGER PRIMARY KEY AUTOINCREMENT, pk TEXT NOT NULL,
                    workspace_id INTEGER DEFAULT 1, name TEXT, address TEXT,
                    rating REAL, review_count INTEGER, phone TEXT, mobile TEXT,
                    email TEXT, email_source TEXT, contact_checked_at TEXT,
                    website TEXT, category TEXT, search_query TEXT, cid TEXT,
                    place_id TEXT, maps_url TEXT, latitude REAL, longitude REAL,
                    branch_group TEXT, favorite INTEGER DEFAULT 0, parent TEXT,
                    last_updated TEXT, is_competitor INTEGER DEFAULT 0,
                    competitor_of TEXT);
                CREATE TABLE IF NOT EXISTS reviews(
                    id INTEGER PRIMARY KEY AUTOINCREMENT, pk TEXT,
                    workspace_id INTEGER DEFAULT 1, review_id TEXT, author TEXT,
                    rating INTEGER, text TEXT, review_date TEXT, language TEXT,
                    sentiment TEXT, sentiment_score REAL, aspects TEXT,
                    review_hash TEXT, fetched_at TEXT, responded INTEGER DEFAULT 0,
                    response_text TEXT, responded_at TEXT, urgency REAL DEFAULT 0,
                    fake_score REAL DEFAULT 0, topic TEXT,
                    workflow_status TEXT DEFAULT 'new', priority TEXT DEFAULT 'normal',
                    resolved_at TEXT, content_fingerprint TEXT,
                    ignored_at TEXT, last_action_at TEXT);
                CREATE TABLE IF NOT EXISTS monitored(
                    pk TEXT, workspace_id INTEGER DEFAULT 1, name TEXT,
                    interval INTEGER DEFAULT 6, last_success TEXT, last_error TEXT,
                    attempts INTEGER DEFAULT 0, enabled INTEGER DEFAULT 1,
                    PRIMARY KEY(pk, workspace_id));
                CREATE TABLE IF NOT EXISTS snapshots(
                    pk TEXT, workspace_id INTEGER DEFAULT 1, day TEXT,
                    rating REAL, total INTEGER, neg INTEGER, pos INTEGER,
                    PRIMARY KEY(pk, workspace_id, day));
                CREATE TABLE IF NOT EXISTS tickets(
                    id INTEGER PRIMARY KEY AUTOINCREMENT, pk TEXT,
                    workspace_id INTEGER DEFAULT 1, review_id INTEGER,
                    title TEXT, status TEXT DEFAULT 'new', priority TEXT DEFAULT 'medium',
                    owner TEXT, notes TEXT, source_kind TEXT, source_id TEXT, action_key TEXT, due_at TEXT,
                    created_at TEXT, updated_at TEXT);
                CREATE TABLE IF NOT EXISTS alerts(
                    id INTEGER PRIMARY KEY AUTOINCREMENT, fingerprint TEXT,
                    workspace_id INTEGER DEFAULT 1, pk TEXT, review_id INTEGER,
                    kind TEXT, severity TEXT, title TEXT, detail TEXT,
                    status TEXT DEFAULT 'new', created_at TEXT, resolved_at TEXT);
                CREATE TABLE IF NOT EXISTS notif_center(
                    id INTEGER PRIMARY KEY AUTOINCREMENT, workspace_id INTEGER DEFAULT 1,
                    kind TEXT, severity TEXT, title TEXT, body TEXT,
                    pk TEXT, review_id INTEGER, read_at TEXT, created_at TEXT);
                CREATE TABLE IF NOT EXISTS integrations(
                    id INTEGER PRIMARY KEY AUTOINCREMENT, workspace_id INTEGER DEFAULT 1,
                    name TEXT, channel TEXT, webhook_url TEXT,
                    active INTEGER DEFAULT 1, last_test TEXT, created_at TEXT);
                CREATE TABLE IF NOT EXISTS search_history(
                    id INTEGER PRIMARY KEY AUTOINCREMENT, workspace_id INTEGER DEFAULT 1,
                    q TEXT, ts TEXT, results INTEGER);
                CREATE TABLE IF NOT EXISTS saved_views(
                    id INTEGER PRIMARY KEY AUTOINCREMENT, workspace_id INTEGER DEFAULT 1,
                    name TEXT, filters_json TEXT, created_at TEXT, updated_at TEXT);
                CREATE TABLE IF NOT EXISTS report_templates(
                    id INTEGER PRIMARY KEY AUTOINCREMENT, workspace_id INTEGER DEFAULT 1,
                    name TEXT, sections_json TEXT, created_at TEXT);
                CREATE TABLE IF NOT EXISTS review_notes(
                    id INTEGER PRIMARY KEY AUTOINCREMENT, review_id INTEGER NOT NULL,
                    workspace_id INTEGER DEFAULT 1, note TEXT NOT NULL, author TEXT,
                    created_at TEXT NOT NULL);
                CREATE TABLE IF NOT EXISTS branch_notes(
                    id INTEGER PRIMARY KEY AUTOINCREMENT, workspace_id INTEGER DEFAULT 1,
                    pk TEXT NOT NULL, note TEXT NOT NULL, author TEXT, created_at TEXT NOT NULL);
                CREATE TABLE IF NOT EXISTS branch_sync_state(
                    workspace_id INTEGER DEFAULT 1, pk TEXT NOT NULL, status TEXT DEFAULT 'never',
                    last_sync_at TEXT, last_success_at TEXT, last_error TEXT, attempts INTEGER DEFAULT 0,
                    new_reviews INTEGER DEFAULT 0, duplicate_reviews INTEGER DEFAULT 0, changed_fields TEXT,
                    previous_rating REAL, current_rating REAL, previous_review_count INTEGER, current_review_count INTEGER,
                    PRIMARY KEY(workspace_id, pk));
                CREATE TABLE IF NOT EXISTS sync_queue(
                    id INTEGER PRIMARY KEY AUTOINCREMENT, workspace_id INTEGER DEFAULT 1, pk TEXT NOT NULL,
                    source TEXT DEFAULT 'manual', priority INTEGER DEFAULT 0, status TEXT DEFAULT 'queued',
                    attempts INTEGER DEFAULT 0, max_attempts INTEGER DEFAULT 3, queued_at TEXT, started_at TEXT,
                    completed_at TEXT, next_attempt_at TEXT, stage TEXT DEFAULT 'queued', progress_pct REAL DEFAULT 0,
                    error TEXT, new_reviews INTEGER DEFAULT 0, duplicate_reviews INTEGER DEFAULT 0, changed_fields TEXT);
                CREATE TABLE IF NOT EXISTS sync_runs(
                    id INTEGER PRIMARY KEY AUTOINCREMENT, queue_id INTEGER, workspace_id INTEGER DEFAULT 1, pk TEXT NOT NULL,
                    started_at TEXT, completed_at TEXT, status TEXT DEFAULT 'running', new_reviews INTEGER DEFAULT 0,
                    duplicate_reviews INTEGER DEFAULT 0, changed_fields TEXT, error TEXT);
                CREATE TABLE IF NOT EXISTS sync_policies(
                    workspace_id INTEGER PRIMARY KEY, enabled INTEGER DEFAULT 1,
                    base_interval_hours REAL DEFAULT 6, min_interval_hours REAL DEFAULT 1, max_interval_hours REAL DEFAULT 24,
                    batch_limit INTEGER DEFAULT 5, archive_days INTEGER DEFAULT 30,
                    rating_drop_threshold REAL DEFAULT 0.2, negative_review_threshold INTEGER DEFAULT 2,
                    review_spike_threshold INTEGER DEFAULT 10, alert_contact_change INTEGER DEFAULT 1, updated_at TEXT);
                CREATE TABLE IF NOT EXISTS branch_change_events(
                    id INTEGER PRIMARY KEY AUTOINCREMENT, workspace_id INTEGER DEFAULT 1, pk TEXT NOT NULL, sync_run_id INTEGER,
                    event_type TEXT NOT NULL, severity TEXT DEFAULT 'info', metric TEXT, old_value TEXT, new_value TEXT,
                    delta REAL, detail TEXT, created_at TEXT NOT NULL);
                CREATE TABLE IF NOT EXISTS sync_queue_archive(
                    id INTEGER PRIMARY KEY AUTOINCREMENT, original_queue_id INTEGER UNIQUE, workspace_id INTEGER DEFAULT 1,
                    pk TEXT NOT NULL, source TEXT, priority INTEGER, status TEXT, attempts INTEGER, max_attempts INTEGER,
                    queued_at TEXT, started_at TEXT, completed_at TEXT, next_attempt_at TEXT, stage TEXT, progress_pct REAL,
                    error TEXT, new_reviews INTEGER, duplicate_reviews INTEGER, changed_fields TEXT, archived_at TEXT);
                CREATE TABLE IF NOT EXISTS alert_delivery_policy(
                    workspace_id INTEGER PRIMARY KEY, enabled INTEGER DEFAULT 1, digest_frequency TEXT DEFAULT 'daily',
                    min_severity TEXT DEFAULT 'warning', quiet_start TEXT DEFAULT '22:00', quiet_end TEXT DEFAULT '07:00',
                    channels_json TEXT DEFAULT '[]', dedupe_minutes INTEGER DEFAULT 120, max_retries INTEGER DEFAULT 3, updated_at TEXT);
                CREATE TABLE IF NOT EXISTS digest_runs(
                    id INTEGER PRIMARY KEY AUTOINCREMENT, workspace_id INTEGER DEFAULT 1, frequency TEXT NOT NULL,
                    window_start TEXT NOT NULL, window_end TEXT NOT NULL, dedupe_key TEXT NOT NULL, severity_max TEXT DEFAULT 'info',
                    event_count INTEGER DEFAULT 0, branch_count INTEGER DEFAULT 0, source_event_ids TEXT, summary TEXT,
                    status TEXT DEFAULT 'built', created_at TEXT);
                CREATE TABLE IF NOT EXISTS alert_outbox(
                    id INTEGER PRIMARY KEY AUTOINCREMENT, workspace_id INTEGER DEFAULT 1, digest_run_id INTEGER,
                    dedupe_key TEXT NOT NULL, severity TEXT DEFAULT 'info', channel TEXT NOT NULL, recipient TEXT, subject TEXT, body TEXT,
                    source_event_ids TEXT, payload_json TEXT, status TEXT DEFAULT 'queued', attempts INTEGER DEFAULT 0,
                    max_attempts INTEGER DEFAULT 3, next_attempt_at TEXT, queued_at TEXT, sent_at TEXT, error TEXT);
                CREATE TABLE IF NOT EXISTS review_tags(
                    review_id INTEGER NOT NULL, tag TEXT NOT NULL,
                    PRIMARY KEY(review_id, tag));
                CREATE TABLE IF NOT EXISTS response_templates(
                    id INTEGER PRIMARY KEY AUTOINCREMENT, workspace_id INTEGER DEFAULT 1,
                    name TEXT, category TEXT, content TEXT NOT NULL,
                    lang TEXT DEFAULT 'ar', uses INTEGER DEFAULT 0,
                    created_at TEXT, updated_at TEXT);
                CREATE TABLE IF NOT EXISTS review_deep_sync_state(
                    workspace_id INTEGER DEFAULT 1, pk TEXT NOT NULL, status TEXT DEFAULT 'never',
                    last_sync_at TEXT, last_success_at TEXT, last_error TEXT, batches INTEGER DEFAULT 0,
                    discovered_new INTEGER DEFAULT 0, duplicates INTEGER DEFAULT 0, known_count INTEGER DEFAULT 0,
                    target_total INTEGER DEFAULT 0, last_batch_limit INTEGER DEFAULT 0, updated_at TEXT,
                    PRIMARY KEY(workspace_id,pk));
                CREATE TABLE IF NOT EXISTS review_response_drafts(
                    id INTEGER PRIMARY KEY AUTOINCREMENT, workspace_id INTEGER DEFAULT 1, pk TEXT NOT NULL,
                    review_id INTEGER NOT NULL, draft_text TEXT NOT NULL, lang TEXT DEFAULT 'ar', tone TEXT DEFAULT 'professional',
                    similarity_score REAL DEFAULT 0, status TEXT DEFAULT 'draft', created_at TEXT, updated_at TEXT);
                CREATE TABLE IF NOT EXISTS webhook_log(
                    id INTEGER PRIMARY KEY AUTOINCREMENT, event_key TEXT,
                    url TEXT, status INTEGER, response TEXT, sent_at TEXT, error TEXT);
                CREATE TABLE IF NOT EXISTS branch_groups(
                    id INTEGER PRIMARY KEY AUTOINCREMENT, workspace_id INTEGER DEFAULT 1,
                    name TEXT, color TEXT DEFAULT '#2563EB', icon TEXT DEFAULT '📁',
                    created_at TEXT);
                CREATE TABLE IF NOT EXISTS branch_group_members(
                    group_id INTEGER NOT NULL, pk TEXT NOT NULL,
                    PRIMARY KEY(group_id, pk));
                CREATE TABLE IF NOT EXISTS team_members(
                    id INTEGER PRIMARY KEY AUTOINCREMENT, workspace_id INTEGER DEFAULT 1,
                    name TEXT, role TEXT DEFAULT 'agent', email TEXT,
                    active INTEGER DEFAULT 1, created_at TEXT);
                CREATE TABLE IF NOT EXISTS review_assignments(
                    id INTEGER PRIMARY KEY AUTOINCREMENT, review_id INTEGER NOT NULL,
                    assignee TEXT NOT NULL, status TEXT DEFAULT 'assigned',
                    note TEXT, assigned_at TEXT, completed_at TEXT);
                CREATE TABLE IF NOT EXISTS competitors(
                    id INTEGER PRIMARY KEY AUTOINCREMENT, workspace_id INTEGER DEFAULT 1,
                    name TEXT, pk TEXT, category TEXT, notes TEXT, added_at TEXT,
                    primary_pk TEXT, market_data TEXT);
                CREATE TABLE IF NOT EXISTS scheduled_reports(
                    id INTEGER PRIMARY KEY AUTOINCREMENT, workspace_id INTEGER DEFAULT 1,
                    name TEXT, frequency TEXT DEFAULT 'weekly', recipients TEXT,
                    include_branches TEXT, last_sent TEXT, next_send TEXT,
                    active INTEGER DEFAULT 1, created_at TEXT);
                CREATE TABLE IF NOT EXISTS anomalies(
                    id INTEGER PRIMARY KEY AUTOINCREMENT, fingerprint TEXT,
                    workspace_id INTEGER DEFAULT 1, pk TEXT, kind TEXT,
                    severity TEXT, title TEXT, detail TEXT,
                    created_at TEXT, dismissed INTEGER DEFAULT 0);
                CREATE TABLE IF NOT EXISTS search_jobs(
                    job_id TEXT PRIMARY KEY, workspace_id INTEGER DEFAULT 1, query TEXT,
                    status TEXT DEFAULT 'running', params_json TEXT, started_at TEXT, completed_at TEXT,
                    elapsed_ms REAL, results INTEGER DEFAULT 0, discovered INTEGER DEFAULT 0,
                    tiles INTEGER DEFAULT 0, errors INTEGER DEFAULT 0, detail TEXT);
                CREATE TABLE IF NOT EXISTS performance_log(
                    id INTEGER PRIMARY KEY AUTOINCREMENT, workspace_id INTEGER DEFAULT 1,
                    ts TEXT, metric TEXT, ms REAL, detail TEXT);
                CREATE TABLE IF NOT EXISTS user_preferences(
                    pref_key TEXT PRIMARY KEY, value_json TEXT, updated_at TEXT);
                CREATE TABLE IF NOT EXISTS audit_log(
                    id INTEGER PRIMARY KEY AUTOINCREMENT, entity_type TEXT,
                    entity_id TEXT, action TEXT, payload TEXT, created_at TEXT);
                CREATE TABLE IF NOT EXISTS branch_aliases(
                    id INTEGER PRIMARY KEY AUTOINCREMENT, workspace_id INTEGER DEFAULT 1,
                    alias_type TEXT NOT NULL, alias_value TEXT NOT NULL, canonical_pk TEXT NOT NULL,
                    confidence REAL DEFAULT 1, source TEXT, created_at TEXT, updated_at TEXT,
                    UNIQUE(workspace_id,alias_type,alias_value));
                CREATE TABLE IF NOT EXISTS branch_merge_audit(
                    id INTEGER PRIMARY KEY AUTOINCREMENT, workspace_id INTEGER DEFAULT 1,
                    source_pk TEXT NOT NULL, target_pk TEXT NOT NULL, reason TEXT, status TEXT DEFAULT 'merged',
                    moved_json TEXT, created_at TEXT);
                CREATE TABLE IF NOT EXISTS schema_version(
                    id INTEGER PRIMARY KEY CHECK(id=1), version INTEGER NOT NULL,
                    updated_at TEXT NOT NULL);
                CREATE TABLE IF NOT EXISTS migration_log(
                    id INTEGER PRIMARY KEY AUTOINCREMENT, from_version INTEGER,
                    to_version INTEGER, action TEXT, detail TEXT, created_at TEXT);
                CREATE TABLE IF NOT EXISTS ai_jobs(
                    id INTEGER PRIMARY KEY AUTOINCREMENT, workspace_id INTEGER DEFAULT 1,
                    kind TEXT, target TEXT, status TEXT DEFAULT 'pending',
                    result TEXT, created_at TEXT, completed_at TEXT);
                CREATE TABLE IF NOT EXISTS brand_settings(
                    id INTEGER PRIMARY KEY CHECK(id=1),
                    brand_name TEXT DEFAULT 'Reviews Tracker',
                    brand_tagline TEXT, logo_url TEXT, primary_color TEXT DEFAULT '#2563EB',
                    accent_color TEXT DEFAULT '#10B981',
                    custom_domain TEXT, footer_text TEXT, updated_at TEXT);
            """)

    def _heal_schema(self):
        WS_TABLES = [
            "companies","reviews","monitored","snapshots","tickets","alerts",
            "notif_center","integrations","search_history","saved_views",
            "report_templates","response_templates","branch_groups","team_members",
            "competitors","scheduled_reports","anomalies","activity_log","ai_jobs",
            "search_jobs","performance_log","branch_notes","review_notes","branch_sync_state",
            "sync_queue","sync_runs","sync_policies","branch_change_events","sync_queue_archive","alert_delivery_policy","digest_runs","alert_outbox",
            "branch_aliases","branch_merge_audit","review_deep_sync_state","review_response_drafts"
        ]
        REQUIRED = {
            "companies": [("workspace_id","INTEGER DEFAULT 1"),("name","TEXT"),
                          ("address","TEXT"),("rating","REAL"),("review_count","INTEGER"),
                          ("phone","TEXT"),("mobile","TEXT"),("email","TEXT"),
                          ("email_source","TEXT"),("contact_checked_at","TEXT"),
                          ("website","TEXT"),("category","TEXT"),("search_query","TEXT"),
                          ("cid","TEXT"),("place_id","TEXT"),("maps_url","TEXT"),
                          ("latitude","REAL"),("longitude","REAL"),("branch_group","TEXT"),
                          ("favorite","INTEGER DEFAULT 0"),("parent","TEXT"),
                          ("last_updated","TEXT"),("is_competitor","INTEGER DEFAULT 0"),
                          ("competitor_of","TEXT"),("source_fetched_at","TEXT"),
                          ("confidence_score","REAL DEFAULT 0")],
            "branch_aliases": [("workspace_id","INTEGER DEFAULT 1"),("alias_type","TEXT"),("alias_value","TEXT"),
                               ("canonical_pk","TEXT"),("confidence","REAL DEFAULT 1"),("source","TEXT"),
                               ("created_at","TEXT"),("updated_at","TEXT")],
            "branch_merge_audit": [("workspace_id","INTEGER DEFAULT 1"),("source_pk","TEXT"),("target_pk","TEXT"),
                                    ("reason","TEXT"),("status","TEXT DEFAULT 'merged'"),("moved_json","TEXT"),
                                    ("created_at","TEXT")],
            "reviews": [("pk","TEXT"),("workspace_id","INTEGER DEFAULT 1"),
                        ("review_id","TEXT"),("author","TEXT"),("rating","INTEGER"),
                        ("text","TEXT"),("review_date","TEXT"),("language","TEXT"),
                        ("sentiment","TEXT"),("sentiment_score","REAL"),("aspects","TEXT"),
                        ("review_hash","TEXT"),("fetched_at","TEXT"),
                        ("urgency","REAL DEFAULT 0"),("fake_score","REAL DEFAULT 0"),
                        ("topic","TEXT"),("workflow_status","TEXT DEFAULT 'new'"),
                        ("response_text","TEXT"),("responded","INTEGER DEFAULT 0"),
                        ("responded_at","TEXT"),("ignored_at","TEXT"),
                        ("last_action_at","TEXT"),("analysis_method","TEXT DEFAULT 'rule-based'"),
                        ("analysis_confidence","REAL DEFAULT 0"),("aspect_primary","TEXT"),
                        ("priority","TEXT DEFAULT 'normal'"),("resolved_at","TEXT"),
                        ("content_fingerprint","TEXT"),("sync_run_id","INTEGER")],
            "review_deep_sync_state": [("workspace_id","INTEGER DEFAULT 1"),("pk","TEXT"),("status","TEXT DEFAULT 'never'"),
                                       ("last_sync_at","TEXT"),("last_success_at","TEXT"),("last_error","TEXT"),("batches","INTEGER DEFAULT 0"),
                                       ("discovered_new","INTEGER DEFAULT 0"),("duplicates","INTEGER DEFAULT 0"),("known_count","INTEGER DEFAULT 0"),
                                       ("target_total","INTEGER DEFAULT 0"),("last_batch_limit","INTEGER DEFAULT 0"),("updated_at","TEXT")],
            "review_response_drafts": [("workspace_id","INTEGER DEFAULT 1"),("pk","TEXT"),("review_id","INTEGER"),
                                       ("draft_text","TEXT"),("lang","TEXT DEFAULT 'ar'"),("tone","TEXT DEFAULT 'professional'"),
                                       ("similarity_score","REAL DEFAULT 0"),("status","TEXT DEFAULT 'draft'"),("created_at","TEXT"),("updated_at","TEXT")],
            "monitored": [("pk","TEXT"),("workspace_id","INTEGER DEFAULT 1"),
                          ("name","TEXT"),("interval","INTEGER DEFAULT 6"),
                          ("last_success","TEXT"),("last_error","TEXT"),
                          ("attempts","INTEGER DEFAULT 0"),("enabled","INTEGER DEFAULT 1")],
            "snapshots": [("pk","TEXT"),("workspace_id","INTEGER DEFAULT 1"),
                          ("day","TEXT"),("rating","REAL"),("total","INTEGER"),
                          ("neg","INTEGER"),("pos","INTEGER")],
            "tickets": [("pk","TEXT"),("workspace_id","INTEGER DEFAULT 1"),
                        ("review_id","INTEGER"),("title","TEXT"),("status","TEXT DEFAULT 'new'"),
                        ("priority","TEXT DEFAULT 'medium'"),("owner","TEXT"),("notes","TEXT"),
                        ("source_kind","TEXT"),("source_id","TEXT"),("action_key","TEXT"),("due_at","TEXT"),
                        ("created_at","TEXT"),("updated_at","TEXT")],
            "alerts": [("fingerprint","TEXT"),("workspace_id","INTEGER DEFAULT 1"),
                       ("pk","TEXT"),("review_id","INTEGER"),("kind","TEXT"),
                       ("severity","TEXT"),("title","TEXT"),("detail","TEXT"),
                       ("status","TEXT DEFAULT 'new'"),("created_at","TEXT"),("resolved_at","TEXT")],
            "notif_center": [("workspace_id","INTEGER DEFAULT 1"),("kind","TEXT"),
                             ("severity","TEXT"),("title","TEXT"),("body","TEXT"),
                             ("pk","TEXT"),("review_id","INTEGER"),("read_at","TEXT"),("created_at","TEXT")],
            "search_history": [("workspace_id","INTEGER DEFAULT 1"),("q","TEXT"),
                               ("ts","TEXT"),("results","INTEGER")],
            "search_jobs": [("workspace_id","INTEGER DEFAULT 1"),("query","TEXT"),
                            ("status","TEXT DEFAULT 'running'"),("params_json","TEXT"),
                            ("started_at","TEXT"),("completed_at","TEXT"),("elapsed_ms","REAL"),
                            ("results","INTEGER DEFAULT 0"),("discovered","INTEGER DEFAULT 0"),
                            ("tiles","INTEGER DEFAULT 0"),("errors","INTEGER DEFAULT 0"),("detail","TEXT")],
            "performance_log": [("workspace_id","INTEGER DEFAULT 1"),("ts","TEXT"),
                                ("metric","TEXT"),("ms","REAL"),("detail","TEXT")],
            "branch_notes": [("workspace_id","INTEGER DEFAULT 1"),("pk","TEXT"),
                             ("note","TEXT"),("author","TEXT"),("created_at","TEXT")],
            "branch_sync_state": [("workspace_id","INTEGER DEFAULT 1"),("pk","TEXT"),
                                  ("status","TEXT DEFAULT 'never'"),("last_sync_at","TEXT"),
                                  ("last_success_at","TEXT"),("last_error","TEXT"),
                                  ("attempts","INTEGER DEFAULT 0"),("new_reviews","INTEGER DEFAULT 0"),
                                  ("duplicate_reviews","INTEGER DEFAULT 0"),("changed_fields","TEXT"),
                                  ("previous_rating","REAL"),("current_rating","REAL"),
                                  ("previous_review_count","INTEGER"),("current_review_count","INTEGER"),
                                  ("next_due_at","TEXT"),("schedule_hours","REAL DEFAULT 6"),
                                  ("priority_score","REAL DEFAULT 0"),("priority_reason","TEXT"),
                                  ("new_negative_reviews","INTEGER DEFAULT 0")],
            "sync_queue": [("workspace_id","INTEGER DEFAULT 1"),("pk","TEXT"),("source","TEXT DEFAULT 'manual'"),
                           ("priority","INTEGER DEFAULT 0"),("status","TEXT DEFAULT 'queued'"),("attempts","INTEGER DEFAULT 0"),
                           ("max_attempts","INTEGER DEFAULT 3"),("queued_at","TEXT"),("started_at","TEXT"),("completed_at","TEXT"),
                           ("next_attempt_at","TEXT"),("stage","TEXT DEFAULT 'queued'"),("progress_pct","REAL DEFAULT 0"),
                           ("error","TEXT"),("new_reviews","INTEGER DEFAULT 0"),("duplicate_reviews","INTEGER DEFAULT 0"),
                           ("changed_fields","TEXT")],
            "sync_runs": [("queue_id","INTEGER"),("workspace_id","INTEGER DEFAULT 1"),("pk","TEXT"),("started_at","TEXT"),
                          ("completed_at","TEXT"),("status","TEXT DEFAULT 'running'"),("new_reviews","INTEGER DEFAULT 0"),
                          ("duplicate_reviews","INTEGER DEFAULT 0"),("changed_fields","TEXT"),("error","TEXT"),
                          ("before_snapshot","TEXT"),("after_snapshot","TEXT"),("new_negative_reviews","INTEGER DEFAULT 0")],
            "sync_policies": [("workspace_id","INTEGER DEFAULT 1"),("enabled","INTEGER DEFAULT 1"),
                              ("base_interval_hours","REAL DEFAULT 6"),("min_interval_hours","REAL DEFAULT 1"),
                              ("max_interval_hours","REAL DEFAULT 24"),("batch_limit","INTEGER DEFAULT 5"),
                              ("archive_days","INTEGER DEFAULT 30"),("rating_drop_threshold","REAL DEFAULT 0.2"),
                              ("negative_review_threshold","INTEGER DEFAULT 2"),("review_spike_threshold","INTEGER DEFAULT 10"),
                              ("alert_contact_change","INTEGER DEFAULT 1"),("updated_at","TEXT")],
            "branch_change_events": [("workspace_id","INTEGER DEFAULT 1"),("pk","TEXT"),("sync_run_id","INTEGER"),
                                      ("event_type","TEXT"),("severity","TEXT DEFAULT 'info'"),("metric","TEXT"),
                                      ("old_value","TEXT"),("new_value","TEXT"),("delta","REAL"),("detail","TEXT"),("created_at","TEXT")],
            "sync_queue_archive": [("original_queue_id","INTEGER"),("workspace_id","INTEGER DEFAULT 1"),("pk","TEXT"),
                                   ("source","TEXT"),("priority","INTEGER"),("status","TEXT"),("attempts","INTEGER"),
                                   ("max_attempts","INTEGER"),("queued_at","TEXT"),("started_at","TEXT"),("completed_at","TEXT"),
                                   ("next_attempt_at","TEXT"),("stage","TEXT"),("progress_pct","REAL"),("error","TEXT"),
                                   ("new_reviews","INTEGER"),("duplicate_reviews","INTEGER"),("changed_fields","TEXT"),("archived_at","TEXT")],
            "alert_delivery_policy": [("workspace_id","INTEGER DEFAULT 1"),("enabled","INTEGER DEFAULT 1"),("digest_frequency","TEXT DEFAULT 'daily'"),
                                      ("min_severity","TEXT DEFAULT 'warning'"),("quiet_start","TEXT DEFAULT '22:00'"),("quiet_end","TEXT DEFAULT '07:00'"),
                                      ("channels_json","TEXT DEFAULT '[]'"),("dedupe_minutes","INTEGER DEFAULT 120"),("max_retries","INTEGER DEFAULT 3"),("updated_at","TEXT")],
            "digest_runs": [("workspace_id","INTEGER DEFAULT 1"),("frequency","TEXT"),("window_start","TEXT"),("window_end","TEXT"),
                            ("dedupe_key","TEXT"),("severity_max","TEXT DEFAULT 'info'"),("event_count","INTEGER DEFAULT 0"),
                            ("branch_count","INTEGER DEFAULT 0"),("source_event_ids","TEXT"),("summary","TEXT"),("status","TEXT DEFAULT 'built'"),("created_at","TEXT")],
            "alert_outbox": [("workspace_id","INTEGER DEFAULT 1"),("digest_run_id","INTEGER"),("dedupe_key","TEXT"),("severity","TEXT DEFAULT 'info'"),
                             ("channel","TEXT"),("recipient","TEXT"),("subject","TEXT"),("body","TEXT"),("source_event_ids","TEXT"),("payload_json","TEXT"),
                             ("status","TEXT DEFAULT 'queued'"),("attempts","INTEGER DEFAULT 0"),("max_attempts","INTEGER DEFAULT 3"),
                             ("next_attempt_at","TEXT"),("queued_at","TEXT"),("sent_at","TEXT"),("error","TEXT")],
            "competitors": [("workspace_id","INTEGER DEFAULT 1"),("name","TEXT"),("pk","TEXT"),
                            ("category","TEXT"),("notes","TEXT"),("added_at","TEXT"),
                            ("primary_pk","TEXT"),("market_data","TEXT")],
            "brand_settings": [("brand_name","TEXT DEFAULT 'Reviews Tracker'"),
                               ("brand_tagline","TEXT"),("logo_url","TEXT"),
                               ("primary_color","TEXT DEFAULT '#2563EB'"),
                               ("accent_color","TEXT DEFAULT '#10B981'"),
                               ("custom_domain","TEXT"),("footer_text","TEXT"),
                               ("updated_at","TEXT")],
            "ai_jobs": [("workspace_id","INTEGER DEFAULT 1"),("kind","TEXT"),
                        ("target","TEXT"),("status","TEXT DEFAULT 'pending'"),
                        ("result","TEXT"),("created_at","TEXT"),("completed_at","TEXT")],
        }
        with self._c() as c:
            for tbl, cols in REQUIRED.items():
                if not self._table_exists(c, tbl): continue
                for col, defn in cols:
                    self._add(c, tbl, col, defn)
            for tbl in WS_TABLES:
                if not self._table_exists(c, tbl): continue
                self._add(c, tbl, "workspace_id", "INTEGER DEFAULT 1")
                try:
                    c.execute("UPDATE {} SET workspace_id=1 WHERE workspace_id IS NULL".format(tbl))
                except sqlite3.OperationalError: pass
            now = datetime.now().isoformat()
            try:
                c.execute("""INSERT OR IGNORE INTO workspaces(id,name,description,icon,color,created_at)
                    VALUES(1,'الافتراضية','مساحة العمل الافتراضية','🏢','#2563EB',?)""", (now,))
                c.execute("""INSERT OR IGNORE INTO brand_settings(id,brand_name,brand_tagline,
                    primary_color,accent_color,updated_at)
                    VALUES(1,'Reviews Tracker','Reputation Platform','#2563EB','#10B981',?)""", (now,))
            except Exception: pass
            # Old builds could contain duplicate (pk, workspace_id) rows and lacked the
            # unique constraint required by ON CONFLICT. Keep the newest row safely.
            try:
                _row_key=self._company_row_order_expr(c)
                if _row_key in {"id","rowid"}:
                    c.execute("DELETE FROM companies WHERE {0} NOT IN (SELECT MAX({0}) FROM companies GROUP BY pk, workspace_id)".format(_row_key))
            except Exception as e:
                self.migration_log.append("company_dedupe: " + str(e)[:100])

    def _create_indexes(self):
        INDEXES = [
            "CREATE UNIQUE INDEX IF NOT EXISTS ux_comp_pk_ws ON companies(pk,workspace_id)",
            "CREATE INDEX IF NOT EXISTS ix_comp_ws ON companies(workspace_id)",
            "CREATE INDEX IF NOT EXISTS ix_comp_name ON companies(workspace_id,name)",
            "CREATE INDEX IF NOT EXISTS ix_comp_place ON companies(workspace_id,place_id)",
            "CREATE INDEX IF NOT EXISTS ix_comp_cid ON companies(workspace_id,cid)",
            "CREATE INDEX IF NOT EXISTS ix_comp_iscmp ON companies(is_competitor)",
            "CREATE INDEX IF NOT EXISTS ix_rev_ws ON reviews(workspace_id)",
            "CREATE INDEX IF NOT EXISTS ix_rev_pk ON reviews(pk)",
            "CREATE INDEX IF NOT EXISTS ix_rev_pk_ws ON reviews(workspace_id,pk,id)",
            "CREATE INDEX IF NOT EXISTS ix_rev_sent ON reviews(sentiment)",
            "CREATE INDEX IF NOT EXISTS ix_rev_fetched ON reviews(fetched_at)",
            "CREATE INDEX IF NOT EXISTS ix_rev_ws_fetched ON reviews(workspace_id,fetched_at DESC)",
            "CREATE INDEX IF NOT EXISTS ix_rev_ws_sent_fetched ON reviews(workspace_id,sentiment,fetched_at DESC)",
            "CREATE INDEX IF NOT EXISTS ix_alerts_ws ON alerts(workspace_id)",
            "CREATE INDEX IF NOT EXISTS ix_alerts_status ON alerts(status, severity)",
            "CREATE INDEX IF NOT EXISTS ix_alerts_ws_status_created ON alerts(workspace_id,status,created_at DESC)",
            "CREATE INDEX IF NOT EXISTS ix_tickets_ws ON tickets(workspace_id)",
            "CREATE INDEX IF NOT EXISTS ix_tickets_ws_status_updated ON tickets(workspace_id,status,updated_at DESC)",
            "CREATE UNIQUE INDEX IF NOT EXISTS ux_tickets_ws_action_key ON tickets(workspace_id,action_key) WHERE action_key IS NOT NULL AND action_key<>''",
            "CREATE INDEX IF NOT EXISTS ix_tickets_ws_due ON tickets(workspace_id,due_at,status)",
            "CREATE UNIQUE INDEX IF NOT EXISTS ux_response_draft_ws_review ON review_response_drafts(workspace_id,review_id)",
            "CREATE INDEX IF NOT EXISTS ix_response_draft_ws_pk ON review_response_drafts(workspace_id,pk,updated_at DESC)",
            "CREATE INDEX IF NOT EXISTS ix_deep_review_sync_ws_updated ON review_deep_sync_state(workspace_id,updated_at DESC)",
            "CREATE INDEX IF NOT EXISTS ix_search_ws_ts ON search_history(workspace_id,ts DESC)",
            "CREATE INDEX IF NOT EXISTS ix_rev_ws_status_fetched ON reviews(workspace_id,workflow_status,fetched_at DESC)",
            "CREATE INDEX IF NOT EXISTS ix_rev_ws_topic_fetched ON reviews(workspace_id,topic,fetched_at DESC)",
            "CREATE INDEX IF NOT EXISTS ix_rev_ws_rating_fetched ON reviews(workspace_id,rating,fetched_at DESC)",
            "CREATE INDEX IF NOT EXISTS ix_rev_ws_priority_fetched ON reviews(workspace_id,priority,fetched_at DESC)",
            "CREATE INDEX IF NOT EXISTS ix_rev_ws_pk_status_rating ON reviews(workspace_id,pk,workflow_status,rating)",
            "CREATE INDEX IF NOT EXISTS ix_rev_ws_pk_fetched ON reviews(workspace_id,pk,fetched_at DESC)",
            "CREATE INDEX IF NOT EXISTS ix_rev_ws_responded_fetched ON reviews(workspace_id,responded,fetched_at DESC)",
            "CREATE INDEX IF NOT EXISTS ix_rev_ws_priority_status_fetched ON reviews(workspace_id,priority,workflow_status,fetched_at DESC)",
            "CREATE INDEX IF NOT EXISTS ix_rev_ws_topic_status ON reviews(workspace_id,topic,workflow_status)",
            "CREATE INDEX IF NOT EXISTS ix_rev_content_fingerprint ON reviews(workspace_id,pk,content_fingerprint)",
            "CREATE INDEX IF NOT EXISTS ix_comp_ws_updated ON companies(workspace_id,last_updated DESC)",
            "CREATE INDEX IF NOT EXISTS ix_comp_ws_rating_reviews ON companies(workspace_id,rating DESC,review_count DESC)",
            "CREATE INDEX IF NOT EXISTS ix_search_jobs_ws_started ON search_jobs(workspace_id,started_at DESC)",
            "CREATE INDEX IF NOT EXISTS ix_perf_ws_ts ON performance_log(workspace_id,ts DESC)",
            "CREATE INDEX IF NOT EXISTS ix_branch_notes_ws_pk ON branch_notes(workspace_id,pk,created_at DESC)",
            "CREATE INDEX IF NOT EXISTS ix_sync_ws_status ON branch_sync_state(workspace_id,status,last_sync_at DESC)",
            "CREATE INDEX IF NOT EXISTS ix_sync_queue_ws_status ON sync_queue(workspace_id,status,priority DESC,id)",
            "CREATE INDEX IF NOT EXISTS ix_sync_queue_due ON sync_queue(status,next_attempt_at,priority DESC,id)",
            "CREATE INDEX IF NOT EXISTS ix_sync_runs_ws_pk ON sync_runs(workspace_id,pk,id DESC)",
            "CREATE INDEX IF NOT EXISTS ix_rev_sync_run ON reviews(workspace_id,sync_run_id,id DESC)",
            "CREATE INDEX IF NOT EXISTS ix_sync_state_due ON branch_sync_state(workspace_id,next_due_at,priority_score DESC)",
            "CREATE INDEX IF NOT EXISTS ix_change_ws_created ON branch_change_events(workspace_id,created_at DESC)",
            "CREATE INDEX IF NOT EXISTS ix_change_ws_pk_created ON branch_change_events(workspace_id,pk,created_at DESC)",
            "CREATE INDEX IF NOT EXISTS ix_change_ws_sev ON branch_change_events(workspace_id,severity,created_at DESC)",
            "CREATE UNIQUE INDEX IF NOT EXISTS ux_sync_archive_original ON sync_queue_archive(original_queue_id)",
            "CREATE INDEX IF NOT EXISTS ix_sync_archive_ws_completed ON sync_queue_archive(workspace_id,completed_at DESC)",
            "CREATE UNIQUE INDEX IF NOT EXISTS ux_digest_ws_key ON digest_runs(workspace_id,dedupe_key)",
            "CREATE INDEX IF NOT EXISTS ix_digest_ws_created ON digest_runs(workspace_id,created_at DESC)",
            "CREATE UNIQUE INDEX IF NOT EXISTS ux_outbox_ws_key_channel ON alert_outbox(workspace_id,dedupe_key,channel)",
            "CREATE INDEX IF NOT EXISTS ix_outbox_ws_status_due ON alert_outbox(workspace_id,status,next_attempt_at,id)",
            "CREATE INDEX IF NOT EXISTS ix_outbox_ws_queued ON alert_outbox(workspace_id,queued_at DESC)",
            "CREATE INDEX IF NOT EXISTS ix_mon_ws ON monitored(workspace_id)",
            "CREATE INDEX IF NOT EXISTS ix_snap_ws ON snapshots(workspace_id, day)",
            "CREATE INDEX IF NOT EXISTS ix_notif_ws ON notif_center(workspace_id)",
            "CREATE INDEX IF NOT EXISTS ix_ai_jobs_ws ON ai_jobs(workspace_id, status)",
            "CREATE UNIQUE INDEX IF NOT EXISTS ux_branch_alias_ws_value ON branch_aliases(workspace_id,alias_type,alias_value)",
            "CREATE INDEX IF NOT EXISTS ix_branch_alias_ws_canonical ON branch_aliases(workspace_id,canonical_pk)",
            "CREATE INDEX IF NOT EXISTS ix_branch_merge_ws_created ON branch_merge_audit(workspace_id,created_at DESC)",
        ]
        with self._c() as c:
            for stmt in INDEXES:
                try: c.execute(stmt)
                except sqlite3.OperationalError as e:
                    self.migration_log.append("index: " + str(e)[:100])

    def _seed(self):
        now = datetime.now().isoformat()
        with self._c() as c:
            try:
                row = c.execute("SELECT COUNT(*) as n FROM users").fetchone()
                if not row or row["n"] == 0:
                    salt = hashlib.sha256(os.urandom(16)).hexdigest()[:16]
                    pwd = hashlib.pbkdf2_hmac("sha256", "admin".encode(), salt.encode(), 100000).hex()
                    c.execute("""INSERT INTO users(username,display_name,password_hash,salt,
                        role,workspace_id,active,created_at)
                        VALUES('admin','مدير النظام',?,?,'admin',1,1,?)""",
                        (pwd, salt, now))
                    self.migration_log.append("+ default admin (admin/admin)")
            except Exception as e:
                self.migration_log.append("admin: " + str(e)[:80])
            try:
                old = c.execute("SELECT version FROM schema_version WHERE id=1").fetchone()
                old_v = old["version"] if old else 0
                c.execute("""INSERT INTO schema_version(id,version,updated_at) VALUES(1,?,?)
                    ON CONFLICT(id) DO UPDATE SET version=excluded.version, updated_at=excluded.updated_at""",
                    (SCHEMA_VERSION, now))
                if old_v != SCHEMA_VERSION:
                    self.migration_log.append("schema {} → {}".format(old_v, SCHEMA_VERSION))
                try:
                    c.execute("""INSERT INTO migration_log(from_version,to_version,action,detail,created_at)
                        VALUES(?,?,?,?,?)""",
                        (old_v, SCHEMA_VERSION, "auto_heal",
                         " | ".join(self.migration_log[-30:]), now))
                except Exception: pass
            except Exception as e:
                self.migration_log.append("version: " + str(e)[:80])

    def _init(self):
        try:
            self._create_tables()
            self._heal_schema()
            self._create_indexes()
            self._seed()
        except Exception as e:
            self.migration_log.append("FATAL: " + str(e)[:200])

    def schema_report(self):
        report = {"tables": {}, "version": None, "warnings": list(self.migration_log)}
        try:
            with self._c() as c:
                try:
                    v = c.execute("SELECT version FROM schema_version WHERE id=1").fetchone()
                    report["version"] = v["version"] if v else None
                except Exception: pass
                for row in c.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"):
                    tname = row[0]
                    if tname.startswith("sqlite_"): continue
                    try:
                        cols = list(self._cols(c, tname))
                        count = c.execute("SELECT COUNT(*) FROM {}".format(tname)).fetchone()[0]
                        report["tables"][tname] = {"columns": cols, "rows": count,
                                                    "has_ws": "workspace_id" in cols}
                    except Exception as e:
                        report["tables"][tname] = {"error": str(e)[:80]}
        except Exception: pass
        return report

    # ─── Auth ─────────────────────────────────────────────────────────
    @staticmethod
    def hash_password(password, salt):
        return hashlib.pbkdf2_hmac("sha256", password.encode(), salt.encode(), 100000).hex()

    def authenticate(self, username, password):
        try:
            with self._c() as c:
                r = c.execute("SELECT * FROM users WHERE username=? AND active=1", (username,)).fetchone()
            if not r: return None
            if self.hash_password(password, r["salt"]) != r["password_hash"]: return None
            with self._c() as c:
                c.execute("UPDATE users SET last_login=? WHERE id=?",
                          (datetime.now().isoformat(), r["id"]))
            return dict(r)
        except Exception: return None

    def add_user(self, username, password, display_name, role, workspace_id, email=""):
        salt = hashlib.sha256(os.urandom(16)).hexdigest()[:16]
        pwd = self.hash_password(password, salt)
        try:
            with self._c() as c:
                c.execute("""INSERT INTO users(username,display_name,password_hash,salt,
                    role,workspace_id,email,active,created_at) VALUES(?,?,?,?,?,?,?,1,?)""",
                    (username, display_name, pwd, salt, role, workspace_id, email,
                     datetime.now().isoformat()))
            return True
        except sqlite3.IntegrityError: return False

    def list_users(self):
        try:
            with self._c() as c:
                return pd.read_sql_query("""SELECT u.id,u.username,u.display_name,u.role,
                    u.workspace_id,u.email,u.active,u.last_login,w.name as workspace_name
                    FROM users u LEFT JOIN workspaces w ON w.id=u.workspace_id ORDER BY u.id""", c)
        except Exception: return pd.DataFrame()

    def delete_user(self, uid):
        try:
            with self._c() as c: c.execute("UPDATE users SET active=0 WHERE id=?", (uid,))
        except Exception: pass

    def change_password(self, uid, new_password):
        salt = hashlib.sha256(os.urandom(16)).hexdigest()[:16]
        pwd = self.hash_password(new_password, salt)
        try:
            with self._c() as c:
                c.execute("UPDATE users SET password_hash=?, salt=? WHERE id=?", (pwd, salt, uid))
        except Exception: pass

    # ─── Workspaces ───────────────────────────────────────────────────
    def add_workspace(self, name, description="", icon="🏢", color="#2563EB"):
        try:
            with self._c() as c:
                c.execute("""INSERT INTO workspaces(name,description,icon,color,created_at,active)
                    VALUES(?,?,?,?,?,1)""",
                    (name, description, icon, color, datetime.now().isoformat()))
            return True
        except sqlite3.IntegrityError: return False

    def list_workspaces(self):
        try:
            with self._c() as c:
                return pd.read_sql_query("""SELECT w.*,
                    (SELECT COUNT(*) FROM companies WHERE workspace_id=w.id) as branch_count,
                    (SELECT COUNT(*) FROM reviews WHERE workspace_id=w.id) as review_count,
                    (SELECT COUNT(*) FROM users WHERE workspace_id=w.id) as user_count
                    FROM workspaces w WHERE active=1 ORDER BY id""", c)
        except Exception: return pd.DataFrame()

    def delete_workspace(self, wid):
        if wid == 1: return False
        try:
            with self._c() as c: c.execute("UPDATE workspaces SET active=0 WHERE id=?", (wid,))
            return True
        except Exception: return False

    # ─── Activity / Migration log ─────────────────────────────────────
    def log_activity(self, username, workspace_id, action, detail=""):
        try:
            with self._c() as c:
                c.execute("""INSERT INTO activity_log(username,workspace_id,action,detail,ts)
                    VALUES(?,?,?,?,?)""",
                    (username, workspace_id, action, str(detail)[:500],
                     datetime.now().isoformat()))
        except Exception: pass

    def list_activity(self, workspace_id=None, limit=200):
        try:
            with self._c() as c:
                q = "SELECT * FROM activity_log WHERE 1=1"; p = []
                if workspace_id: q += " AND workspace_id=?"; p.append(workspace_id)
                q += " ORDER BY id DESC LIMIT ?"; p.append(limit)
                return pd.read_sql_query(q, c, params=p)
        except Exception: return pd.DataFrame()

    def list_migrations(self, limit=50):
        try:
            with self._c() as c:
                return pd.read_sql_query("SELECT * FROM migration_log ORDER BY id DESC LIMIT ?",
                                          c, params=(limit,))
        except Exception: return pd.DataFrame()

    # ─── Brand Settings (v20.3) ───────────────────────────────────────
    def get_brand(self):
        try:
            with self._c() as c:
                r = c.execute("SELECT * FROM brand_settings WHERE id=1").fetchone()
            return dict(r) if r else {}
        except Exception: return {}

    def save_brand(self, data):
        try:
            with self._c() as c:
                c.execute("""UPDATE brand_settings SET brand_name=?, brand_tagline=?,
                    logo_url=?, primary_color=?, accent_color=?,
                    custom_domain=?, footer_text=?, updated_at=? WHERE id=1""",
                    (data.get("brand_name"), data.get("brand_tagline"),
                     data.get("logo_url"), data.get("primary_color"),
                     data.get("accent_color"), data.get("custom_domain"),
                     data.get("footer_text"), datetime.now().isoformat()))
            return True
        except Exception: return False

    # ─── Companies / Reviews ──────────────────────────────────────────
    def _company_pk(self, info, q=""):
        """Stable branch identity: never collapse distinct branches that share a brand name."""
        place_id = str(info.get("place_id") or "").strip()
        cid = str(info.get("cid") or "").strip()
        if place_id: return "pid:" + place_id[:220]
        if cid: return "cid:" + cid[:220]
        url = str(info.get("url") or info.get("maps_url") or "").strip()
        m = re.search(r"[?&]cid=(\d+)", url)
        if m: return "cid:" + m.group(1)
        m = re.search(r"!1s([^!/?&]+)", url)
        if m: return "ftid:" + m.group(1)[:220]
        name = re.sub(r"\s+", " ", str(info.get("name") or q or "").strip().lower())
        try:
            lat, lng = float(info.get("latitude")), float(info.get("longitude"))
            raw = "{}|{:.5f}|{:.5f}".format(name, lat, lng)
            return "geo:" + hashlib.sha256(raw.encode("utf-8")).hexdigest()[:32]
        except Exception: pass
        address = re.sub(r"\s+", " ", str(info.get("address") or "").strip().lower())
        if address:
            return "addr:" + hashlib.sha256((name+"|"+address).encode("utf-8")).hexdigest()[:32]
        if url:
            return "url:" + hashlib.sha256(url.encode("utf-8")).hexdigest()[:32]
        return "name:" + hashlib.sha256(name.encode("utf-8")).hexdigest()[:32]

    @staticmethod
    def _identity_norm(value):
        text=re.sub(r"\s+"," ",str(value or "").strip().lower())
        text=re.sub(r"[إأآٱ]","ا",text).replace("ى","ي").replace("ة","ه")
        text=re.sub(r"[ًٌٍَُِّْـ]","",text)
        return text

    @staticmethod
    def _geo_distance_km(lat1,lon1,lat2,lon2):
        try:
            r=6371.0088
            p1,p2=math.radians(float(lat1)),math.radians(float(lat2))
            dp=math.radians(float(lat2)-float(lat1)); dl=math.radians(float(lon2)-float(lon1))
            a=math.sin(dp/2)**2+math.cos(p1)*math.cos(p2)*math.sin(dl/2)**2
            return 2*r*math.asin(math.sqrt(a))
        except Exception:
            return None

    def _identity_aliases(self, info, q="", include_weak=True):
        info=dict(info or {}); out=[]
        def add(kind,value,confidence=1.0):
            v=str(value or "").strip()
            if v and (kind,v) not in [(x[0],x[1]) for x in out]: out.append((kind,v,float(confidence)))
        place_id=str(info.get("place_id") or "").strip()
        cid=str(info.get("cid") or "").strip()
        url=str(info.get("url") or info.get("maps_url") or "").strip()
        if place_id: add("place_id",place_id,1.0)
        if cid: add("cid",cid,1.0)
        if url:
            m=re.search(r"[?&]cid=(\d+)",url)
            if m: add("cid",m.group(1),1.0)
            m=re.search(r"!1s([^!/?&]+)",url)
            if m: add("ftid",m.group(1)[:220],.98)
            add("maps_url",url,.96)
        if include_weak:
            name=self._identity_norm(info.get("name") or q)
            address=self._identity_norm(info.get("address"))
            try:
                lat=float(info.get("latitude")); lng=float(info.get("longitude"))
                raw="{}|{:.4f}|{:.4f}".format(name,lat,lng)
                add("geo",hashlib.sha256(raw.encode("utf-8")).hexdigest()[:40],.82)
            except Exception: pass
            if name and address:
                add("address",hashlib.sha256((name+"|"+address).encode("utf-8")).hexdigest()[:40],.78)
        return out

    def _register_branch_aliases(self, c, ws_id, canonical_pk, info, q="", source="resolver", include_pk=True):
        now=datetime.now().isoformat(); conflicts=[]; added=0
        aliases=self._identity_aliases(info,q,include_weak=True)
        if include_pk and canonical_pk: aliases.insert(0,("pk",str(canonical_pk),1.0))
        for kind,value,confidence in aliases:
            try:
                row=c.execute("SELECT canonical_pk FROM branch_aliases WHERE workspace_id=? AND alias_type=? AND alias_value=? LIMIT 1",
                              (int(ws_id),kind,value)).fetchone()
                if row and str(row["canonical_pk"])!=str(canonical_pk):
                    conflicts.append({"alias_type":kind,"alias_value":value,"existing_pk":row["canonical_pk"],"incoming_pk":canonical_pk})
                    continue
                if row:
                    c.execute("UPDATE branch_aliases SET confidence=MAX(COALESCE(confidence,0),?),source=?,updated_at=? WHERE workspace_id=? AND alias_type=? AND alias_value=?",
                              (float(confidence),source,now,int(ws_id),kind,value))
                else:
                    c.execute("INSERT INTO branch_aliases(workspace_id,alias_type,alias_value,canonical_pk,confidence,source,created_at,updated_at) VALUES(?,?,?,?,?,?,?,?)",
                              (int(ws_id),kind,value,str(canonical_pk),float(confidence),source,now,now)); added+=1
            except Exception as exc:
                conflicts.append({"alias_type":kind,"alias_value":value,"error":str(exc)[:160]})
        return {"added":added,"conflicts":conflicts}

    def _strong_identity_conflict(self, a, b):
        a=dict(a or {}); b=dict(b or {})
        ap=str(a.get("place_id") or "").strip(); bp=str(b.get("place_id") or "").strip()
        ac=str(a.get("cid") or "").strip(); bc=str(b.get("cid") or "").strip()
        if ap and bp and ap!=bp: return True
        if ac and bc and ac!=bc: return True
        return False

    def _resolve_canonical_pk(self, info, ws_id=1, q="", register=True):
        info=dict(info or {}); ws_id=int(ws_id)
        derived=self._company_pk(info,q)
        aliases=self._identity_aliases(info,q,include_weak=True)
        with self._c() as c:
            # 1) Strong alias registry/exact company identities always win.
            strong=[x for x in aliases if x[0] in {"place_id","cid","ftid","maps_url"}]
            for kind,value,_ in strong:
                row=c.execute("SELECT canonical_pk FROM branch_aliases WHERE workspace_id=? AND alias_type=? AND alias_value=? LIMIT 1",
                              (ws_id,kind,value)).fetchone()
                if row:
                    canonical=str(row["canonical_pk"])
                    if register: self._register_branch_aliases(c,ws_id,canonical,info,q,"strong-alias")
                    return canonical
            if info.get("place_id"):
                row=c.execute("SELECT pk,* FROM companies WHERE workspace_id=? AND place_id=? LIMIT 1",(ws_id,str(info.get("place_id")))).fetchone()
                if row:
                    canonical=str(row["pk"])
                    if register: self._register_branch_aliases(c,ws_id,canonical,info,q,"place-id")
                    return canonical
            if info.get("cid"):
                row=c.execute("SELECT pk,* FROM companies WHERE workspace_id=? AND cid=? LIMIT 1",(ws_id,str(info.get("cid")))).fetchone()
                if row:
                    canonical=str(row["pk"])
                    if register: self._register_branch_aliases(c,ws_id,canonical,info,q,"cid")
                    return canonical
            maps_url=str(info.get("url") or info.get("maps_url") or "").strip()
            if maps_url:
                row=c.execute("SELECT pk,* FROM companies WHERE workspace_id=? AND maps_url=? LIMIT 1",(ws_id,maps_url)).fetchone()
                if row:
                    canonical=str(row["pk"])
                    if register: self._register_branch_aliases(c,ws_id,canonical,info,q,"maps-url")
                    return canonical
            # 2) Weak promotion: same normalized name plus near coordinates or same normalized address.
            incoming_name=self._identity_norm(info.get("name") or q); incoming_addr=self._identity_norm(info.get("address"))
            try: ilat=float(info.get("latitude")); ilng=float(info.get("longitude"))
            except Exception: ilat=ilng=None
            rows=c.execute("SELECT * FROM companies WHERE workspace_id=?",(ws_id,)).fetchall()
            best=None; best_score=-1
            for row in rows:
                r=dict(row)
                if incoming_name and self._identity_norm(r.get("name"))!=incoming_name: continue
                if self._strong_identity_conflict(info,r): continue
                score=0
                raddr=self._identity_norm(r.get("address"))
                if incoming_addr and raddr and incoming_addr==raddr: score+=60
                if ilat is not None and ilng is not None and r.get("latitude") is not None and r.get("longitude") is not None:
                    d=self._geo_distance_km(ilat,ilng,r.get("latitude"),r.get("longitude"))
                    if d is not None and d<=.12: score+=max(20,55-int(d*200))
                # Promote only with tangible location evidence; never name-only.
                if score>best_score and score>=55:
                    best=r; best_score=score
            canonical=str(best["pk"]) if best else str(derived)
            if register: self._register_branch_aliases(c,ws_id,canonical,info,q,"weak-promotion" if best else "new")
            return canonical

    def branch_identity_health(self, ws_id=1):
        ws_id=int(ws_id)
        with self._c() as c:
            rows=[dict(r) for r in c.execute("SELECT pk,place_id,cid,maps_url FROM companies WHERE workspace_id=?",(ws_id,)).fetchall()]
            aliases=c.execute("SELECT COUNT(*) FROM branch_aliases WHERE workspace_id=?",(ws_id,)).fetchone()[0]
            conflicts=0
            for col in ("place_id","cid","maps_url"):
                conflicts += c.execute("SELECT COUNT(*) FROM (SELECT {} FROM companies WHERE workspace_id=? AND COALESCE({},'')<>'' GROUP BY {} HAVING COUNT(*)>1)".format(col,col,col),(ws_id,)).fetchone()[0]
        strong=sum(1 for r in rows if r.get("place_id") or r.get("cid") or r.get("maps_url"))
        weak=len(rows)-strong
        candidates=self.find_identity_duplicates(ws_id,limit=500)
        return {"branches":len(rows),"strong":strong,"weak":weak,"aliases":int(aliases),"identity_conflicts":int(conflicts),
                "duplicate_candidates":len(candidates),"auto_merge_candidates":sum(1 for x in candidates if x.get("auto_merge"))}

    def find_identity_duplicates(self, ws_id=1, limit=500):
        ws_id=int(ws_id); out=[]; seen=set()
        with self._c() as c:
            _order=self._company_row_order_expr(c)
            rows=[dict(r) for r in c.execute("SELECT * FROM companies WHERE workspace_id=? ORDER BY {}".format(_order),(ws_id,)).fetchall()]
        def add(a,b,reason,score,auto_merge,conflict=False):
            key=tuple(sorted((str(a.get("pk")),str(b.get("pk")))))
            if key in seen or key[0]==key[1]: return
            seen.add(key)
            a_strong=bool(a.get("place_id") or a.get("cid") or a.get("maps_url")); b_strong=bool(b.get("place_id") or b.get("cid") or b.get("maps_url"))
            if a_strong != b_strong:
                source_pk,target_pk=(a.get("pk"),b.get("pk")) if not a_strong else (b.get("pk"),a.get("pk"))
            else:
                a_new=str(a.get("last_updated") or ""); b_new=str(b.get("last_updated") or "")
                source_pk,target_pk=(a.get("pk"),b.get("pk")) if b_new>=a_new else (b.get("pk"),a.get("pk"))
            out.append({"pk_a":a.get("pk"),"name_a":a.get("name"),"pk_b":b.get("pk"),"name_b":b.get("name"),
                        "source_pk":source_pk,"target_pk":target_pk,"reason":reason,"score":round(float(score),1),
                        "auto_merge":bool(auto_merge),"strong_conflict":bool(conflict)})
        # Strong duplicate values are safe unless another strong field explicitly conflicts.
        for i,a in enumerate(rows):
            for b in rows[i+1:]:
                same_place=bool(a.get("place_id") and b.get("place_id") and str(a.get("place_id"))==str(b.get("place_id")))
                same_cid=bool(a.get("cid") and b.get("cid") and str(a.get("cid"))==str(b.get("cid")))
                if same_place or same_cid:
                    conflict=self._strong_identity_conflict(a,b)
                    add(a,b,"same_place_id" if same_place else "same_cid",100 if same_place else 98,not conflict,conflict); continue
                an=self._identity_norm(a.get("name")); bn=self._identity_norm(b.get("name"))
                if not an or an!=bn: continue
                conflict=self._strong_identity_conflict(a,b)
                same_addr=bool(self._identity_norm(a.get("address")) and self._identity_norm(a.get("address"))==self._identity_norm(b.get("address")))
                d=None
                if a.get("latitude") is not None and a.get("longitude") is not None and b.get("latitude") is not None and b.get("longitude") is not None:
                    d=self._geo_distance_km(a.get("latitude"),a.get("longitude"),b.get("latitude"),b.get("longitude"))
                near=d is not None and d<=.12
                if conflict and (same_addr or near):
                    add(a,b,"strong_id_conflict",95,False,True)
                elif same_addr or near:
                    a_strong=bool(a.get("place_id") or a.get("cid") or a.get("maps_url")); b_strong=bool(b.get("place_id") or b.get("cid") or b.get("maps_url"))
                    auto=bool(a_strong ^ b_strong) and not conflict
                    add(a,b,"same_address" if same_addr else "same_name_near_geo",88 if same_addr else 84,auto,False)
                if len(out)>=int(limit): break
            if len(out)>=int(limit): break
        return sorted(out,key=lambda x:(not x.get("strong_conflict"),not x.get("auto_merge"),-float(x.get("score") or 0)))[:int(limit)]

    def _merge_relation_rows(self, c, table, ws_id, source_pk, target_pk):
        cols_info=list(c.execute("PRAGMA table_info({})".format(table)).fetchall())
        cols=[r[1] for r in cols_info]
        if "pk" not in cols or "workspace_id" not in cols: return 0
        count=c.execute("SELECT COUNT(*) FROM {} WHERE workspace_id=? AND pk=?".format(table),(ws_id,source_pk)).fetchone()[0]
        if not count: return 0
        try:
            c.execute("UPDATE {} SET pk=? WHERE workspace_id=? AND pk=?".format(table),(target_pk,ws_id,source_pk))
            return int(count)
        except sqlite3.IntegrityError:
            # Composite keys may collide (e.g. snapshot day or monitored PK). Clone safely and remove source rows.
            rows=[dict(r) for r in c.execute("SELECT * FROM {} WHERE workspace_id=? AND pk=?".format(table),(ws_id,source_pk)).fetchall()]
            auto_ids={r[1] for r in cols_info if int(r[5] or 0)>0 and str(r[2] or "").upper()=="INTEGER" and r[1]!="pk"}
            use_cols=[x for x in cols if x not in auto_ids]
            marks=",".join("?" for _ in use_cols)
            for row in rows:
                row["pk"]=target_pk
                vals=[row.get(x) for x in use_cols]
                c.execute("INSERT OR IGNORE INTO {}({}) VALUES({})".format(table,",".join(use_cols),marks),vals)
            c.execute("DELETE FROM {} WHERE workspace_id=? AND pk=?".format(table),(ws_id,source_pk))
            return int(count)

    def merge_branch_records(self, ws_id, source_pk, target_pk, reason="manual", dry_run=False):
        ws_id=int(ws_id); source_pk=str(source_pk); target_pk=str(target_pk)
        if not source_pk or not target_pk or source_pk==target_pk: return {"ok":False,"error":"invalid_source_or_target"}
        with self._c() as c:
            src=c.execute("SELECT * FROM companies WHERE workspace_id=? AND pk=?",(ws_id,source_pk)).fetchone()
            dst=c.execute("SELECT * FROM companies WHERE workspace_id=? AND pk=?",(ws_id,target_pk)).fetchone()
            if not src or not dst: return {"ok":False,"error":"branch_not_found"}
            src=dict(src); dst=dict(dst)
            if self._strong_identity_conflict(src,dst): return {"ok":False,"error":"strong_identity_conflict"}
            tables=[]
            for r in c.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"):
                name=r[0]
                if name.startswith("sqlite_") or name in {"companies","branch_aliases","branch_merge_audit"}: continue
                cs={x[1] for x in c.execute("PRAGMA table_info({})".format(name)).fetchall()}
                if "pk" in cs and "workspace_id" in cs:
                    n=c.execute("SELECT COUNT(*) FROM {} WHERE workspace_id=? AND pk=?".format(name),(ws_id,source_pk)).fetchone()[0]
                    if n: tables.append((name,int(n)))
            if dry_run: return {"ok":True,"dry_run":True,"source_pk":source_pk,"target_pk":target_pk,"relations":dict(tables)}
            moved={}
            try:
                # Choose fresher source values only when target is empty or source is newer.
                src_newer=str(src.get("last_updated") or "")>str(dst.get("last_updated") or "")
                merge_fields=["name","address","rating","review_count","phone","mobile","email","email_source","contact_checked_at","website","category","search_query","cid","place_id","maps_url","branch_group","latitude","longitude","source_fetched_at","confidence_score","last_updated"]
                updates={}
                for f in merge_fields:
                    sv=src.get(f); dv=dst.get(f)
                    empty=dv is None or dv==""
                    if sv not in (None,"") and (empty or (src_newer and f in {"address","rating","review_count","phone","mobile","email","website","category","latitude","longitude","source_fetched_at","confidence_score","last_updated"})):
                        updates[f]=sv
                if updates:
                    c.execute("UPDATE companies SET {} WHERE workspace_id=? AND pk=?".format(",".join("{}=?".format(k) for k in updates)),list(updates.values())+[ws_id,target_pk])
                for table,_ in tables:
                    moved[table]=self._merge_relation_rows(c,table,ws_id,source_pk,target_pk)
                c.execute("UPDATE branch_aliases SET canonical_pk=?,updated_at=? WHERE workspace_id=? AND canonical_pk=?",(target_pk,datetime.now().isoformat(),ws_id,source_pk))
                self._register_branch_aliases(c,ws_id,target_pk,src,source="merge-source")
                now=datetime.now().isoformat()
                # Explicit aliases preserve every historic key.
                for oldpk in (source_pk,target_pk):
                    row=c.execute("SELECT canonical_pk FROM branch_aliases WHERE workspace_id=? AND alias_type='pk' AND alias_value=?",(ws_id,oldpk)).fetchone()
                    if not row:
                        c.execute("INSERT INTO branch_aliases(workspace_id,alias_type,alias_value,canonical_pk,confidence,source,created_at,updated_at) VALUES(?,?,?,?,?,?,?,?)",(ws_id,"pk",oldpk,target_pk,1.0,"merge",now,now))
                    elif str(row["canonical_pk"])!=target_pk:
                        c.execute("UPDATE branch_aliases SET canonical_pk=?,source='merge',updated_at=? WHERE workspace_id=? AND alias_type='pk' AND alias_value=?",(target_pk,now,ws_id,oldpk))
                c.execute("DELETE FROM companies WHERE workspace_id=? AND pk=?",(ws_id,source_pk))
                c.execute("INSERT INTO branch_merge_audit(workspace_id,source_pk,target_pk,reason,status,moved_json,created_at) VALUES(?,?,?,?,?,?,?)",(ws_id,source_pk,target_pk,str(reason or "manual"),"merged",json.dumps(moved,ensure_ascii=False),now))
                return {"ok":True,"source_pk":source_pk,"target_pk":target_pk,"moved":moved,"updated_fields":sorted(updates)}
            except Exception as exc:
                raise

    def save_company(self, info, q="", ws_id=1, branch_group="", is_competitor=0, competitor_of=None):
        pk = self._resolve_canonical_pk(info, ws_id, q, register=True)
        now = datetime.now().isoformat()
        try:
            with self._c() as c:
                identity = None
                if info.get("place_id"):
                    identity = c.execute("SELECT pk FROM companies WHERE workspace_id=? AND place_id=? LIMIT 1",
                                         (ws_id, str(info.get("place_id")))).fetchone()
                if identity is None and info.get("cid"):
                    identity = c.execute("SELECT pk FROM companies WHERE workspace_id=? AND cid=? LIMIT 1",
                                         (ws_id, str(info.get("cid")))).fetchone()
                if identity is None and info.get("url"):
                    identity = c.execute("SELECT pk FROM companies WHERE workspace_id=? AND maps_url=? LIMIT 1",
                                         (ws_id, str(info.get("url")))).fetchone()
                if identity is not None:
                    pk = identity["pk"]
                c.execute("""INSERT INTO companies(pk,workspace_id,name,address,rating,review_count,
                    phone,mobile,email,email_source,contact_checked_at,website,category,search_query,
                    cid,place_id,maps_url,branch_group,latitude,longitude,is_competitor,competitor_of,
                    source_fetched_at,confidence_score,last_updated)
                    VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
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
                    search_query=COALESCE(NULLIF(excluded.search_query,''),companies.search_query),
                    cid=COALESCE(NULLIF(excluded.cid,''),companies.cid),
                    place_id=COALESCE(NULLIF(excluded.place_id,''),companies.place_id),
                    maps_url=COALESCE(NULLIF(excluded.maps_url,''),companies.maps_url),
                    latitude=COALESCE(excluded.latitude,companies.latitude),
                    longitude=COALESCE(excluded.longitude,companies.longitude),
                    is_competitor=excluded.is_competitor,
                    competitor_of=COALESCE(excluded.competitor_of,companies.competitor_of),
                    branch_group=COALESCE(NULLIF(excluded.branch_group,''),companies.branch_group),
                    source_fetched_at=COALESCE(NULLIF(excluded.source_fetched_at,''),companies.source_fetched_at),
                    confidence_score=MAX(COALESCE(excluded.confidence_score,0),COALESCE(companies.confidence_score,0)),
                    last_updated=excluded.last_updated""",
                    (pk, ws_id, info.get("name"), info.get("address"), info.get("rating"),
                     info.get("review_count"), info.get("phone"), info.get("mobile"),
                     info.get("email"), info.get("email_source"), info.get("contact_checked_at"),
                     info.get("website"), info.get("category"), q,
                     info.get("cid"), info.get("place_id"), info.get("url") or info.get("maps_url"),
                     branch_group, info.get("latitude"), info.get("longitude"),
                     is_competitor, competitor_of, info.get("source_fetched_at") or now,
                     safe_num(info.get("confidence_score"), 0), now))
                self._register_branch_aliases(c,ws_id,pk,info,q,"save-company")
                # v40: start the factual rating timeline as soon as a branch is saved.
                # The same-day row is later enriched/updated by a full review sync.
                if info.get("rating") not in (None,"") or info.get("review_count") not in (None,""):
                    c.execute("""INSERT INTO snapshots(pk,workspace_id,day,rating,total,neg,pos)
                        VALUES(?,?,?,?,?,?,?)
                        ON CONFLICT(pk,workspace_id,day) DO UPDATE SET
                        rating=CASE WHEN excluded.rating IS NOT NULL AND excluded.rating>0 THEN excluded.rating ELSE snapshots.rating END,
                        total=MAX(COALESCE(snapshots.total,0),COALESCE(excluded.total,0))""",
                        (pk,ws_id,date.today().isoformat(),safe_num(info.get("rating"),0),int(safe_num(info.get("review_count"),0)),0,0))
        except Exception as e:
            self.migration_log.append("save_company: " + str(e)[:160])
            return None
        return pk

    def save_companies(self, items, q="", ws_id=1):
        saved = 0; keys = []
        for item in items or []:
            pk = self.save_company(item, q, ws_id)
            if pk:
                saved += 1; keys.append(pk)
        return saved, keys

    def list_companies_with_stats(self, ws_id=1):
        """Fast branch list using one pre-aggregated review scan; avoids a large join/group on every rerun."""
        try:
            with self._c() as c:
                return pd.read_sql_query("""WITH rs AS (
                    SELECT workspace_id,pk,COUNT(*) AS stored_reviews,
                           SUM(CASE WHEN sentiment='negative' THEN 1 ELSE 0 END) AS negative_reviews,
                           SUM(CASE WHEN responded=1 THEN 1 ELSE 0 END) AS responded_reviews
                    FROM reviews WHERE workspace_id=? GROUP BY workspace_id,pk
                )
                SELECT c.*,COALESCE(rs.stored_reviews,0) AS stored_reviews,
                       COALESCE(rs.negative_reviews,0) AS negative_reviews,
                       COALESCE(rs.responded_reviews,0) AS responded_reviews
                FROM companies c
                LEFT JOIN rs ON rs.pk=c.pk AND rs.workspace_id=c.workspace_id
                WHERE c.workspace_id=? AND COALESCE(c.is_competitor,0)=0
                ORDER BY c.favorite DESC,c.rating DESC,c.review_count DESC,c.name""",
                    c,params=(ws_id,ws_id))
        except Exception:
            return self.list_companies(ws_id)

    def list_companies(self, ws_id=1, include_competitors=False):
        try:
            with self._c() as c:
                q = "SELECT * FROM companies WHERE workspace_id=?"
                if not include_competitors: q += " AND COALESCE(is_competitor,0)=0"
                q += " ORDER BY favorite DESC, last_updated DESC"
                return pd.read_sql_query(q, c, params=(ws_id,))
        except Exception: return pd.DataFrame()

    def list_competitors(self, ws_id=1):
        try:
            with self._c() as c:
                return pd.read_sql_query(
                    "SELECT * FROM companies WHERE workspace_id=? AND COALESCE(is_competitor,0)=1 ORDER BY rating DESC",
                    c, params=(ws_id,))
        except Exception: return pd.DataFrame()

    def get_company(self, pk, ws_id=1):
        try:
            with self._c() as c:
                r = c.execute("SELECT * FROM companies WHERE pk=? AND workspace_id=?",
                              (pk, ws_id)).fetchone()
            return dict(r) if r else None
        except Exception: return None

    def _review_fingerprint(self, pk, ws_id, row):
        author = re.sub(r"\s+", " ", str(row.get("author") or "").strip().lower())
        text = re.sub(r"[^\w\u0600-\u06ff]+", " ", str(row.get("text") or "").strip().lower(), flags=re.UNICODE)
        text = re.sub(r"\s+", " ", text).strip()
        rating = str(row.get("rating") or "")
        date = re.sub(r"\s+", " ", str(row.get("date") or row.get("review_date") or "").strip().lower())
        raw = "{}|{}|{}|{}|{}|{}".format(ws_id, pk, author, rating, date, text)
        return hashlib.sha256(raw.encode("utf-8")).hexdigest()

    def save_reviews(self, pk, df, ws_id=1, sync_run_id=None):
        if df is None or df.empty: return 0, 0
        n = dup = 0; ts = datetime.now().isoformat(); rows = []
        try:
            with self._c() as c:
                existing_ids = {str(r[0]) for r in c.execute(
                    "SELECT review_id FROM reviews WHERE pk=? AND workspace_id=? AND review_id IS NOT NULL",
                    (pk, ws_id)).fetchall() if r[0]}
                existing_hash = {str(r[0]) for r in c.execute(
                    "SELECT review_hash FROM reviews WHERE pk=? AND workspace_id=? AND review_hash IS NOT NULL",
                    (pk, ws_id)).fetchall() if r[0]}
                existing_fp = {str(r[0]) for r in c.execute(
                    "SELECT content_fingerprint FROM reviews WHERE pk=? AND workspace_id=? AND content_fingerprint IS NOT NULL",
                    (pk, ws_id)).fetchall() if r[0]}
                records = df.to_dict("records")
                for r in records:
                    rid = str(r.get("review_id") or "").strip()
                    fp = self._review_fingerprint(pk, ws_id, r)
                    if fp in existing_fp:
                        dup += 1; continue
                    if rid:
                        if rid in existing_ids: dup += 1; continue
                        rh = "rid:{}:{}:{}".format(ws_id, pk, rid)
                    else:
                        rh = hashlib.sha256("{}|{}|{}|{}".format(
                            ws_id, pk, r.get("author",""), r.get("text","")).encode()).hexdigest()
                        if rh in existing_hash: dup += 1; continue
                    owner_resp = r.get("owner_response") or None
                    rows.append((pk, ws_id, rid or None, r.get("author"), r.get("rating"),
                                 r.get("text"), r.get("date") or r.get("review_date"), r.get("language") or "unknown", ts, r.get("sentiment"),
                                 r.get("sentiment_score"), r.get("urgency", 0), r.get("fake_score", 0),
                                 r.get("topic", "other"), json.dumps(r.get("aspects") or {}, ensure_ascii=False),
                                 rh, owner_resp, 1 if owner_resp else 0, ts if owner_resp else None,
                                 "responded" if owner_resp else "new", r.get("analysis_method","rule-based-v3"),
                                 r.get("analysis_confidence", r.get("sentiment_score",0)), r.get("aspect_primary"),
                                 r.get("priority","normal"), None, fp, sync_run_id))
                    if rid: existing_ids.add(rid)
                    existing_hash.add(rh)
                    existing_fp.add(fp)
                if rows:
                    c.executemany("""INSERT INTO reviews(pk,workspace_id,review_id,author,rating,text,
                        review_date,language,fetched_at,sentiment,sentiment_score,urgency,fake_score,topic,
                        aspects,review_hash,response_text,responded,responded_at,workflow_status,
                        analysis_method,analysis_confidence,aspect_primary,priority,resolved_at,content_fingerprint,sync_run_id)
                        VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""", rows)
                    n = len(rows)
        except Exception as e:
            self.migration_log.append("save_reviews: " + str(e)[:140])
        return n, dup

    def all_reviews(self, ws_id=1, since=None, until=None, pk=None):
        try:
            q = """SELECT r.*, c.name company_name, c.category company_category,
                c.address company_address, c.rating company_rating, c.is_competitor
                FROM reviews r LEFT JOIN companies c ON c.pk=r.pk AND c.workspace_id=r.workspace_id
                WHERE r.workspace_id=?"""; p = [ws_id]
            if since: q += " AND r.fetched_at>=?"; p.append(since.isoformat())
            if until: q += " AND r.fetched_at<=?"; p.append(until.isoformat())
            if pk: q += " AND r.pk=?"; p.append(pk)
            q += " ORDER BY r.id DESC"
            with self._c() as c:
                return pd.read_sql_query(q, c, params=p)
        except Exception: return pd.DataFrame()

    def get_reviews(self, pk, ws_id=1, since=None):
        try:
            q = "SELECT * FROM reviews WHERE pk=? AND workspace_id=?"; p = [pk, ws_id]
            if since: q += " AND fetched_at>=?"; p.append(since.isoformat())
            q += " ORDER BY id DESC"
            with self._c() as c:
                return pd.read_sql_query(q, c, params=p)
        except Exception: return pd.DataFrame()

    def get_deep_review_sync_state(self, pk, ws_id=1):
        try:
            with self._c() as c:
                r=c.execute("SELECT * FROM review_deep_sync_state WHERE workspace_id=? AND pk=?",(int(ws_id),str(pk))).fetchone()
            return dict(r) if r else {"workspace_id":int(ws_id),"pk":str(pk),"status":"never","batches":0,"discovered_new":0,"duplicates":0,"known_count":0}
        except Exception:
            return {"workspace_id":int(ws_id),"pk":str(pk),"status":"never","batches":0,"discovered_new":0,"duplicates":0,"known_count":0}

    def update_deep_review_sync_state(self, pk, ws_id=1, status="ok", new_reviews=0, duplicates=0, known_count=0, target_total=0, batch_limit=0, error=None):
        now=datetime.now().isoformat()
        try:
            with self._c() as c:
                c.execute("""INSERT INTO review_deep_sync_state(workspace_id,pk,status,last_sync_at,last_success_at,last_error,batches,discovered_new,duplicates,known_count,target_total,last_batch_limit,updated_at)
                    VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)
                    ON CONFLICT(workspace_id,pk) DO UPDATE SET status=excluded.status,last_sync_at=excluded.last_sync_at,
                    last_success_at=CASE WHEN excluded.status='ok' THEN excluded.last_success_at ELSE review_deep_sync_state.last_success_at END,
                    last_error=excluded.last_error,batches=review_deep_sync_state.batches+1,
                    discovered_new=review_deep_sync_state.discovered_new+excluded.discovered_new,
                    duplicates=review_deep_sync_state.duplicates+excluded.duplicates,known_count=excluded.known_count,
                    target_total=excluded.target_total,last_batch_limit=excluded.last_batch_limit,updated_at=excluded.updated_at""",
                    (int(ws_id),str(pk),str(status),now,now if status=="ok" else None,str(error or "")[:1000] or None,1,int(new_reviews or 0),int(duplicates or 0),int(known_count or 0),int(target_total or 0),int(batch_limit or 0),now))
            return True
        except Exception as e:
            self.migration_log.append("deep_review_sync_state: "+str(e)[:160]); return False

    def save_review_response_draft(self, review_id, pk, draft_text, ws_id=1, lang="ar", tone="professional", similarity_score=0.0, status="draft"):
        text=str(draft_text or "").strip()
        if not text: return None
        now=datetime.now().isoformat()
        try:
            with self._c() as c:
                c.execute("""INSERT INTO review_response_drafts(workspace_id,pk,review_id,draft_text,lang,tone,similarity_score,status,created_at,updated_at)
                    VALUES(?,?,?,?,?,?,?,?,?,?)
                    ON CONFLICT(workspace_id,review_id) DO UPDATE SET pk=excluded.pk,draft_text=excluded.draft_text,lang=excluded.lang,
                    tone=excluded.tone,similarity_score=excluded.similarity_score,status=excluded.status,updated_at=excluded.updated_at""",
                    (int(ws_id),str(pk),int(review_id),text,str(lang or "ar"),str(tone or "professional"),float(similarity_score or 0),str(status or "draft"),now,now))
                r=c.execute("SELECT id FROM review_response_drafts WHERE workspace_id=? AND review_id=?",(int(ws_id),int(review_id))).fetchone()
            return int(r[0]) if r else None
        except Exception as e:
            self.migration_log.append("save_response_draft: "+str(e)[:160]); return None

    def list_review_response_drafts(self, ws_id=1, pk=None, review_id=None, limit=200):
        try:
            q="SELECT * FROM review_response_drafts WHERE workspace_id=?"; params=[int(ws_id)]
            if pk not in (None,""): q+=" AND pk=?"; params.append(str(pk))
            if review_id not in (None,""): q+=" AND review_id=?"; params.append(int(review_id))
            q+=" ORDER BY updated_at DESC,id DESC LIMIT ?"; params.append(max(1,min(int(limit or 200),1000)))
            with self._c() as c: return pd.read_sql_query(q,c,params=params)
        except Exception: return pd.DataFrame()

    def review_action(self, rid, status, response_text=None):
        now = datetime.now().isoformat()
        try:
            with self._c() as c:
                if status == "responded":
                    c.execute("""UPDATE reviews SET responded=1, workflow_status='responded',
                        responded_at=?, response_text=COALESCE(?,response_text),
                        last_action_at=? WHERE id=?""", (now, response_text, now, rid))
                elif status == "ignored":
                    c.execute("UPDATE reviews SET workflow_status='ignored', ignored_at=?, last_action_at=? WHERE id=?",
                              (now, now, rid))
                elif status == "resolved":
                    c.execute("UPDATE reviews SET workflow_status='resolved', resolved_at=?, last_action_at=? WHERE id=?",
                              (now, now, rid))
                elif status == "needs_response":
                    c.execute("UPDATE reviews SET workflow_status='needs_response', responded=0, resolved_at=NULL, last_action_at=? WHERE id=?",
                              (now, rid))
                else:
                    c.execute("UPDATE reviews SET workflow_status=?, resolved_at=NULL, last_action_at=? WHERE id=?",
                              (status, now, rid))
        except Exception: pass

    def update_review_analysis(self, rid, result):
        try:
            aspects = result.get("aspects") or {}
            if not isinstance(aspects, str):
                aspects = json.dumps(aspects, ensure_ascii=False)
            with self._c() as c:
                c.execute("""UPDATE reviews SET language=?, sentiment=?, sentiment_score=?, topic=?, aspect_primary=?,
                    aspects=?, urgency=?, fake_score=?, analysis_method=?, analysis_confidence=?, priority=?, last_action_at=?
                    WHERE id=?""",
                    (result.get("language") or "unknown", result.get("sentiment") or "neutral",
                     safe_num(result.get("sentiment_score"), 0), result.get("topic") or "other",
                     result.get("aspect_primary") or result.get("topic") or "other", aspects,
                     safe_num(result.get("urgency"), 0), safe_num(result.get("fake_score"), 0),
                     result.get("analysis_method") or "rule-based-v3", safe_num(result.get("analysis_confidence"), 0),
                     result.get("priority") or "normal", datetime.now().isoformat(), rid))
            return True
        except Exception:
            return False

    def update_review_ai(self, rid, sentiment, sentiment_score, topic, urgency, fake_score, language=None, aspects=None, method="ai", priority=None):
        return self.update_review_analysis(rid, {
            "language": language or "unknown", "sentiment": sentiment, "sentiment_score": sentiment_score,
            "topic": topic, "aspect_primary": topic, "aspects": aspects or {}, "urgency": urgency,
            "fake_score": fake_score, "analysis_method": method, "analysis_confidence": sentiment_score,
            "priority": priority or ("critical" if safe_num(urgency)>=80 else "high" if safe_num(urgency)>=60 else "normal")
        })

    def add_note(self, rid, note, ws_id=1, author=None):
        if not str(note or "").strip(): return False
        try:
            with self._c() as c:
                c.execute("INSERT INTO review_notes(review_id,workspace_id,note,author,created_at) VALUES(?,?,?,?,?)",
                          (rid, ws_id, str(note).strip(), author, datetime.now().isoformat()))
            return True
        except Exception: return False

    def notes(self, rid, ws_id=1):
        try:
            with self._c() as c:
                return pd.read_sql_query("SELECT * FROM review_notes WHERE review_id=? AND workspace_id=? ORDER BY id DESC",
                                          c, params=(rid,ws_id))
        except Exception: return pd.DataFrame()

    def set_review_priority(self, rid, priority):
        priority = priority if priority in {"low","normal","high","critical"} else "normal"
        try:
            with self._c() as c:
                c.execute("UPDATE reviews SET priority=?, last_action_at=? WHERE id=?",
                          (priority, datetime.now().isoformat(), int(rid)))
            return True
        except Exception: return False

    def bulk_review_action(self, review_ids, status):
        ids=[int(x) for x in review_ids if str(x).isdigit()]
        if not ids: return 0
        n=0
        for rid in ids:
            self.review_action(rid,status); n+=1
        return n

    def add_branch_note(self, pk, note, ws_id=1, author=None):
        if not pk or not str(note or "").strip(): return False
        try:
            with self._c() as c:
                c.execute("INSERT INTO branch_notes(workspace_id,pk,note,author,created_at) VALUES(?,?,?,?,?)",
                          (ws_id, str(pk), str(note).strip(), author, datetime.now().isoformat()))
            return True
        except Exception:
            return False

    def branch_notes(self, pk, ws_id=1, limit=300):
        try:
            with self._c() as c:
                return pd.read_sql_query("""SELECT id,pk,note,author,created_at FROM branch_notes
                    WHERE workspace_id=? AND pk=? ORDER BY id DESC LIMIT ?""",
                    c, params=(ws_id, str(pk), int(limit)))
        except Exception:
            return pd.DataFrame()

    def delete_branch_note(self, note_id, pk, ws_id=1):
        try:
            with self._c() as c:
                cur = c.execute("DELETE FROM branch_notes WHERE id=? AND workspace_id=? AND pk=?",
                                (int(note_id), ws_id, str(pk)))
            return bool(cur.rowcount)
        except Exception:
            return False

    def set_tags(self, rid, tags):
        clean = sorted({x.strip() for x in tags if x and x.strip()})
        try:
            with self._c() as c:
                c.execute("DELETE FROM review_tags WHERE review_id=?", (rid,))
                for x in clean:
                    try: c.execute("INSERT OR IGNORE INTO review_tags(review_id,tag) VALUES(?,?)",
                                   (rid, x))
                    except Exception: pass
        except Exception: pass

    def tags(self, rid):
        try:
            with self._c() as c:
                rows = c.execute("SELECT tag FROM review_tags WHERE review_id=?", (rid,)).fetchall()
            return [r[0] for r in rows]
        except Exception: return []

    def save_snapshot(self, pk, rating, total, neg, pos, ws_id=1):
        try:
            with self._c() as c:
                c.execute("""INSERT INTO snapshots(pk,workspace_id,day,rating,total,neg,pos)
                    VALUES(?,?,?,?,?,?,?)
                    ON CONFLICT(pk,workspace_id,day) DO UPDATE SET
                    rating=excluded.rating, total=excluded.total,
                    neg=excluded.neg, pos=excluded.pos""",
                    (pk, ws_id, date.today().isoformat(), rating or 0, total, neg, pos))
        except Exception: pass

    def snapshots(self, ws_id=1, pk=None, days=365):
        try:
            q = "SELECT * FROM snapshots WHERE workspace_id=? AND day>=?"
            p = [ws_id, (date.today()-timedelta(days=days)).isoformat()]
            if pk: q += " AND pk=?"; p.append(pk)
            q += " ORDER BY day"
            with self._c() as c:
                return pd.read_sql_query(q, c, params=p)
        except Exception: return pd.DataFrame()

    def add_monitored(self, pk, name, interval=6, ws_id=1):
        try:
            with self._c() as c:
                c.execute("""INSERT INTO monitored(pk,workspace_id,name,interval,enabled)
                    VALUES(?,?,?,?,1)
                    ON CONFLICT(pk,workspace_id) DO UPDATE SET
                    name=excluded.name, interval=excluded.interval, enabled=1""",
                    (pk, ws_id, name, interval))
        except Exception: pass

    def list_monitored(self, ws_id=1):
        try:
            with self._c() as c:
                return pd.read_sql_query(
                    "SELECT * FROM monitored WHERE workspace_id=? ORDER BY enabled DESC,name",
                    c, params=(ws_id,))
        except Exception: return pd.DataFrame()

    def record_monitor_result(self, pk, ws_id=1, success=True, error=None):
        now=datetime.now().isoformat()
        try:
            with self._c() as c:
                if success:
                    c.execute("UPDATE monitored SET last_success=?, last_error=NULL, attempts=0 WHERE pk=? AND workspace_id=?",(now,str(pk),ws_id))
                else:
                    c.execute("UPDATE monitored SET last_error=?, attempts=COALESCE(attempts,0)+1 WHERE pk=? AND workspace_id=?",(str(error or "sync_failed")[:600],str(pk),ws_id))
            return True
        except Exception:
            return False

    def update_branch_sync(self, pk, ws_id=1, status="ok", new_reviews=0, duplicate_reviews=0, changed_fields=None,
                           previous_rating=None, current_rating=None, previous_review_count=None, current_review_count=None,
                           new_negative_reviews=0, error=None):
        now=datetime.now().isoformat(); changes=changed_fields or []
        if not isinstance(changes,str): changes=json.dumps(changes,ensure_ascii=False)
        try:
            with self._c() as c:
                c.execute("""INSERT INTO branch_sync_state(workspace_id,pk,status,last_sync_at,last_success_at,last_error,attempts,
                    new_reviews,duplicate_reviews,changed_fields,previous_rating,current_rating,previous_review_count,current_review_count,new_negative_reviews)
                    VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                    ON CONFLICT(workspace_id,pk) DO UPDATE SET
                    status=excluded.status,last_sync_at=excluded.last_sync_at,
                    last_success_at=CASE WHEN excluded.status='ok' THEN excluded.last_sync_at ELSE branch_sync_state.last_success_at END,
                    last_error=excluded.last_error,attempts=CASE WHEN excluded.status='ok' THEN 0 ELSE COALESCE(branch_sync_state.attempts,0)+1 END,
                    new_reviews=excluded.new_reviews,duplicate_reviews=excluded.duplicate_reviews,changed_fields=excluded.changed_fields,
                    previous_rating=excluded.previous_rating,current_rating=excluded.current_rating,
                    previous_review_count=excluded.previous_review_count,current_review_count=excluded.current_review_count,
                    new_negative_reviews=excluded.new_negative_reviews""",
                    (ws_id,str(pk),status,now,now if status=="ok" else None,str(error or "")[:600] if error else None,
                     0 if status=="ok" else 1,int(new_reviews or 0),int(duplicate_reviews or 0),changes,
                     previous_rating,current_rating,previous_review_count,current_review_count,int(new_negative_reviews or 0)))
            return True
        except Exception as e:
            self.migration_log.append("branch_sync_state: "+str(e)[:140]); return False

    def branch_sync_states(self, ws_id=1):
        try:
            with self._c() as c:
                return pd.read_sql_query("""SELECT s.*,c.name,c.address,c.maps_url,c.rating,c.review_count
                    FROM branch_sync_state s LEFT JOIN companies c ON c.pk=s.pk AND c.workspace_id=s.workspace_id
                    WHERE s.workspace_id=? ORDER BY COALESCE(s.last_sync_at,'') DESC""",c,params=(ws_id,))
        except Exception:
            return pd.DataFrame()

    def branch_sync_state(self, pk, ws_id=1):
        try:
            with self._c() as c:
                r=c.execute("SELECT * FROM branch_sync_state WHERE workspace_id=? AND pk=?",(ws_id,str(pk))).fetchone()
            return dict(r) if r else None
        except Exception:
            return None

    def get_sync_policy(self, ws_id=1):
        defaults={"enabled":1,"base_interval_hours":6.0,"min_interval_hours":1.0,"max_interval_hours":24.0,
                  "batch_limit":5,"archive_days":30,"rating_drop_threshold":0.2,
                  "negative_review_threshold":2,"review_spike_threshold":10,"alert_contact_change":1}
        try:
            with self._c() as c:
                r=c.execute("SELECT * FROM sync_policies WHERE workspace_id=?",(int(ws_id),)).fetchone()
                if not r:
                    now=datetime.now().isoformat()
                    c.execute("""INSERT INTO sync_policies(workspace_id,enabled,base_interval_hours,min_interval_hours,max_interval_hours,
                        batch_limit,archive_days,rating_drop_threshold,negative_review_threshold,review_spike_threshold,alert_contact_change,updated_at)
                        VALUES(?,?,?,?,?,?,?,?,?,?,?,?)""",
                        (int(ws_id),1,6.0,1.0,24.0,5,30,0.2,2,10,1,now))
                    return dict(defaults,workspace_id=int(ws_id),updated_at=now)
                out=dict(r)
                for k,v in defaults.items():
                    if out.get(k) is None: out[k]=v
                return out
        except Exception:
            return dict(defaults,workspace_id=int(ws_id))

    def save_sync_policy(self, ws_id=1, values=None):
        base=self.get_sync_policy(ws_id); values=values or {}; base.update({k:v for k,v in values.items() if k in base or k in {
            "enabled","base_interval_hours","min_interval_hours","max_interval_hours","batch_limit","archive_days",
            "rating_drop_threshold","negative_review_threshold","review_spike_threshold","alert_contact_change"}})
        mn=max(0.25,float(base.get("min_interval_hours") or 1)); mx=max(mn,float(base.get("max_interval_hours") or 24))
        bh=min(mx,max(mn,float(base.get("base_interval_hours") or 6)))
        row={"enabled":1 if bool(base.get("enabled",1)) else 0,"base_interval_hours":bh,"min_interval_hours":mn,"max_interval_hours":mx,
             "batch_limit":max(1,min(100,int(base.get("batch_limit") or 5))),"archive_days":max(1,int(base.get("archive_days") or 30)),
             "rating_drop_threshold":max(0.01,float(base.get("rating_drop_threshold") or 0.2)),
             "negative_review_threshold":max(1,int(base.get("negative_review_threshold") or 2)),
             "review_spike_threshold":max(1,int(base.get("review_spike_threshold") or 10)),
             "alert_contact_change":1 if bool(base.get("alert_contact_change",1)) else 0}
        try:
            with self._c() as c:
                c.execute("""INSERT INTO sync_policies(workspace_id,enabled,base_interval_hours,min_interval_hours,max_interval_hours,
                    batch_limit,archive_days,rating_drop_threshold,negative_review_threshold,review_spike_threshold,alert_contact_change,updated_at)
                    VALUES(?,?,?,?,?,?,?,?,?,?,?,?) ON CONFLICT(workspace_id) DO UPDATE SET enabled=excluded.enabled,
                    base_interval_hours=excluded.base_interval_hours,min_interval_hours=excluded.min_interval_hours,max_interval_hours=excluded.max_interval_hours,
                    batch_limit=excluded.batch_limit,archive_days=excluded.archive_days,rating_drop_threshold=excluded.rating_drop_threshold,
                    negative_review_threshold=excluded.negative_review_threshold,review_spike_threshold=excluded.review_spike_threshold,
                    alert_contact_change=excluded.alert_contact_change,updated_at=excluded.updated_at""",
                    (int(ws_id),row["enabled"],row["base_interval_hours"],row["min_interval_hours"],row["max_interval_hours"],
                     row["batch_limit"],row["archive_days"],row["rating_drop_threshold"],row["negative_review_threshold"],
                     row["review_spike_threshold"],row["alert_contact_change"],datetime.now().isoformat()))
            return True
        except Exception as e:
            self.migration_log.append("sync_policy: "+str(e)[:140]); return False

    def scheduler_due_branches(self, ws_id=1, limit=250, now=None):
        policy=self.get_sync_policy(ws_id)
        if not int(policy.get("enabled",1)): return []
        now=now or datetime.now(); since=(now-timedelta(days=7)).isoformat(); rows=[]
        try:
            with self._c() as c:
                data=c.execute("""SELECT m.pk,m.name,m.interval,m.last_success,m.last_error,m.attempts monitor_attempts,
                    c.rating,c.review_count,s.status,s.last_success_at,s.attempts sync_attempts,s.new_reviews,s.changed_fields,
                    s.previous_rating,s.current_rating,s.new_negative_reviews,
                    (SELECT COUNT(*) FROM reviews r WHERE r.workspace_id=m.workspace_id AND r.pk=m.pk AND r.sentiment='negative' AND r.fetched_at>=?) recent_negative
                    FROM monitored m LEFT JOIN companies c ON c.pk=m.pk AND c.workspace_id=m.workspace_id
                    LEFT JOIN branch_sync_state s ON s.pk=m.pk AND s.workspace_id=m.workspace_id
                    WHERE m.workspace_id=? AND COALESCE(m.enabled,1)=1""",(since,int(ws_id))).fetchall()
            for rr in data:
                r=dict(rr); base=float(r.get("interval") or policy.get("base_interval_hours") or 6)
                mn=float(policy.get("min_interval_hours") or 1); mx=float(policy.get("max_interval_hours") or 24); base=min(mx,max(mn,base))
                prev=safe_num(r.get("previous_rating"),0); cur=safe_num(r.get("current_rating"),safe_num(r.get("rating"),0))
                drop=max(0.0,prev-cur) if prev and cur else 0.0; recent_neg=int(r.get("recent_negative") or 0)
                last_new=int(r.get("new_reviews") or 0); attempts=max(int(r.get("monitor_attempts") or 0),int(r.get("sync_attempts") or 0))
                quiet=last_new==0 and str(r.get("changed_fields") or "[]") in ("","[]","null","None")
                reason=[]; schedule=base
                if attempts>0:
                    schedule=mn; reason.append("retry/error")
                if drop >= float(policy.get("rating_drop_threshold") or 0.2):
                    schedule=min(schedule,max(mn,base*0.5)); reason.append("rating drop {:.2f}".format(drop))
                if recent_neg >= int(policy.get("negative_review_threshold") or 2):
                    schedule=min(schedule,max(mn,base*0.5)); reason.append("{} recent negative".format(recent_neg))
                if last_new >= int(policy.get("review_spike_threshold") or 10):
                    schedule=min(schedule,max(mn,base*0.65)); reason.append("review spike {}".format(last_new))
                if quiet and not reason:
                    schedule=min(mx,max(base,base*1.5)); reason.append("quiet branch")
                last_txt=str(r.get("last_success_at") or r.get("last_success") or "").strip(); last_dt=None
                if last_txt:
                    try: last_dt=datetime.fromisoformat(last_txt)
                    except Exception: last_dt=None
                next_due=(last_dt+timedelta(hours=schedule)) if last_dt else now
                due=now>=next_due
                age_hours=((now-last_dt).total_seconds()/3600.0) if last_dt else schedule*3
                overdue=max(0.0,age_hours/max(schedule,0.25))
                score=10.0+min(35.0,overdue*15.0)+min(25.0,(drop/max(float(policy.get("rating_drop_threshold") or 0.2),0.01))*12.5)
                score+=min(20.0,recent_neg*5.0)+min(10.0,attempts*5.0)+min(10.0,last_new*0.7)
                score=max(0.0,min(100.0,score)); reason_txt=", ".join(reason) if reason else "normal cadence"
                r.update({"schedule_hours":round(schedule,2),"priority_score":round(score,1),"priority_reason":reason_txt,
                          "next_due_at":next_due.isoformat(),"due":bool(due),"recent_negative":recent_neg,"rating_drop":round(drop,3)})
                try:
                    with self._c() as c:
                        c.execute("""INSERT INTO branch_sync_state(workspace_id,pk,status,next_due_at,schedule_hours,priority_score,priority_reason)
                            VALUES(?,?,COALESCE(?, 'never'),?,?,?,?) ON CONFLICT(workspace_id,pk) DO UPDATE SET
                            next_due_at=excluded.next_due_at,schedule_hours=excluded.schedule_hours,priority_score=excluded.priority_score,
                            priority_reason=excluded.priority_reason""",
                            (int(ws_id),str(r.get("pk")),r.get("status"),r["next_due_at"],r["schedule_hours"],r["priority_score"],reason_txt))
                except Exception: pass
                if due: rows.append(r)
            rows.sort(key=lambda x:(-float(x.get("priority_score") or 0),str(x.get("next_due_at") or "")))
            return rows[:max(1,int(limit or 250))]
        except Exception as e:
            self.migration_log.append("scheduler: "+str(e)[:140]); return []

    def enqueue_scheduled_due(self, ws_id=1, limit=None, now=None):
        policy=self.get_sync_policy(ws_id); lim=max(1,int(limit or policy.get("batch_limit") or 5)); added=[]
        for r in self.scheduler_due_branches(ws_id,limit=max(lim*4,lim),now=now):
            qid=self.enqueue_sync(str(r.get("pk") or ""),ws_id,source="scheduler",priority=int(round(float(r.get("priority_score") or 0))))
            if qid and qid not in added: added.append(qid)
            if len(added)>=lim: break
        return added

    def scheduler_status(self, ws_id=1):
        policy=self.get_sync_policy(ws_id); due=self.scheduler_due_branches(ws_id,limit=10000,now=datetime.now())
        states=self.branch_sync_states(ws_id); high=sum(1 for r in due if float(r.get("priority_score") or 0)>=60)
        return {"enabled":bool(policy.get("enabled",1)),"due":len(due),"high_priority":high,
                "tracked":0 if states is None or states.empty else len(states),"batch_limit":int(policy.get("batch_limit") or 5)}

    def record_change_intelligence(self, pk, ws_id=1, sync_run_id=None, before=None, after=None, new_negative_reviews=0):
        before=before or {}; after=after or {}; policy=self.get_sync_policy(ws_id); events=[]; now=datetime.now().isoformat()
        def add(event_type,severity,metric,old,new,delta,detail):
            item={"event_type":event_type,"severity":severity,"metric":metric,"old_value":old,"new_value":new,
                  "delta":delta,"detail":detail,"created_at":now}; events.append(item)
            try:
                with self._c() as c:
                    c.execute("""INSERT INTO branch_change_events(workspace_id,pk,sync_run_id,event_type,severity,metric,old_value,new_value,delta,detail,created_at)
                        VALUES(?,?,?,?,?,?,?,?,?,?,?)""",(int(ws_id),str(pk),sync_run_id,event_type,severity,metric,
                        None if old is None else str(old),None if new is None else str(new),delta,detail,now))
            except Exception: pass
        br=safe_num(before.get("rating"),0); ar=safe_num(after.get("rating"),0)
        if br and ar and abs(ar-br)>1e-9:
            delta=round(ar-br,3); thr=float(policy.get("rating_drop_threshold") or 0.2)
            sev="critical" if delta<=-(thr*2) else "warning" if delta<=-thr else "info"
            add("rating_delta",sev,"rating",br,ar,delta,"Rating {:+.2f}".format(delta))
        try: bc=int(before.get("review_count") or 0); ac=int(after.get("review_count") or 0)
        except Exception: bc=ac=0
        if ac!=bc:
            delta=ac-bc; sev="warning" if delta>=int(policy.get("review_spike_threshold") or 10) else "info"
            add("review_count_delta",sev,"review_count",bc,ac,float(delta),"Google review count {:+d}".format(delta))
        contact_changes=[]
        for fld in ("phone","mobile","website","address"):
            ov=str(before.get(fld) or "").strip(); nv=str(after.get(fld) or "").strip()
            if ov!=nv and (ov or nv): contact_changes.append((fld,ov,nv))
        if contact_changes:
            detail="; ".join("{}: {} → {}".format(f,o,n) for f,o,n in contact_changes)[:1000]
            add("contact_delta","warning" if int(policy.get("alert_contact_change") or 0) else "info","contact",None,None,None,detail)
        neg=max(0,int(new_negative_reviews or 0))
        if neg:
            thr=max(1,int(policy.get("negative_review_threshold") or 2)); sev="critical" if neg>=thr*2 else "warning" if neg>=thr else "info"
            add("new_negative_reviews",sev,"negative_reviews",0,neg,float(neg),"{} new negative reviews".format(neg))
        for ev in events:
            if ev["severity"] in ("warning","critical"):
                detail=str(ev.get("detail") or "")[:900]; fp=hashlib.sha1((str(ws_id)+str(pk)+ev["event_type"]+detail+date.today().isoformat()).encode("utf-8","ignore")).hexdigest()
                self.save_alert(fp,pk,None,"change_intelligence",ev["severity"],"Important branch change / تغيير مهم في الفرع",detail,ws_id)
        return events

    def change_events(self, ws_id=1, since_hours=None, pk=None, severity=None, limit=1000):
        try:
            q="""SELECT e.*,c.name,c.maps_url FROM branch_change_events e LEFT JOIN companies c ON c.pk=e.pk AND c.workspace_id=e.workspace_id WHERE e.workspace_id=?"""; p=[int(ws_id)]
            if since_hours is not None: q+=" AND e.created_at>=?"; p.append((datetime.now()-timedelta(hours=float(since_hours))).isoformat())
            if pk: q+=" AND e.pk=?"; p.append(str(pk))
            if severity: q+=" AND e.severity=?"; p.append(str(severity))
            q+=" ORDER BY e.id DESC LIMIT ?"; p.append(max(1,int(limit)))
            with self._c() as c: return pd.read_sql_query(q,c,params=p)
        except Exception: return pd.DataFrame()

    def archive_sync_jobs(self, ws_id=None, archive_days=30, batch=1000):
        cutoff=(datetime.now()-timedelta(days=max(1,int(archive_days or 30)))).isoformat(); moved=0
        try:
            with self._c() as c:
                q="SELECT * FROM sync_queue WHERE status IN ('done','failed') AND completed_at IS NOT NULL AND completed_at<?"; p=[cutoff]
                if ws_id is not None: q+=" AND workspace_id=?"; p.append(int(ws_id))
                q+=" ORDER BY completed_at ASC LIMIT ?"; p.append(max(1,int(batch)))
                rows=c.execute(q,p).fetchall()
                for rr in rows:
                    r=dict(rr)
                    c.execute("""INSERT OR IGNORE INTO sync_queue_archive(original_queue_id,workspace_id,pk,source,priority,status,attempts,max_attempts,
                        queued_at,started_at,completed_at,next_attempt_at,stage,progress_pct,error,new_reviews,duplicate_reviews,changed_fields,archived_at)
                        VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",(r.get("id"),r.get("workspace_id"),r.get("pk"),r.get("source"),r.get("priority"),
                        r.get("status"),r.get("attempts"),r.get("max_attempts"),r.get("queued_at"),r.get("started_at"),r.get("completed_at"),
                        r.get("next_attempt_at"),r.get("stage"),r.get("progress_pct"),r.get("error"),r.get("new_reviews"),r.get("duplicate_reviews"),
                        r.get("changed_fields"),datetime.now().isoformat()))
                    c.execute("DELETE FROM sync_queue WHERE id=?",(int(r.get("id")),)); moved+=1
            return moved
        except Exception as e:
            self.migration_log.append("archive_sync: "+str(e)[:140]); return moved

    def get_delivery_policy(self, ws_id=1):
        defaults={"workspace_id":int(ws_id),"enabled":1,"digest_frequency":"daily","min_severity":"warning",
                  "quiet_start":"22:00","quiet_end":"07:00","channels_json":"[]","dedupe_minutes":120,"max_retries":3}
        try:
            with self._c() as c:
                r=c.execute("SELECT * FROM alert_delivery_policy WHERE workspace_id=?",(int(ws_id),)).fetchone()
            if not r: return defaults
            out=dict(r)
            for k,v in defaults.items():
                if out.get(k) is None: out[k]=v
            return out
        except Exception:
            return defaults

    def save_delivery_policy(self, ws_id=1, values=None):
        base=self.get_delivery_policy(ws_id); values=values or {}; base.update(values)
        freq=str(base.get("digest_frequency") or "daily").lower(); freq=freq if freq in ("hourly","daily","weekly") else "daily"
        sev=str(base.get("min_severity") or "warning").lower(); sev=sev if sev in ("info","warning","critical") else "warning"
        channels=base.get("channels_json",[])
        if isinstance(channels,str):
            try: channels=json.loads(channels)
            except Exception: channels=[]
        channels=[str(x).lower() for x in (channels or []) if str(x).lower() in ("email","telegram","slack","teams","whatsapp","webhook")]
        row=(int(ws_id),1 if bool(base.get("enabled",1)) else 0,freq,sev,str(base.get("quiet_start") or "22:00"),str(base.get("quiet_end") or "07:00"),
             json.dumps(sorted(set(channels))),max(0,int(base.get("dedupe_minutes") or 120)),max(1,min(10,int(base.get("max_retries") or 3))),datetime.now().isoformat())
        try:
            with self._c() as c:
                c.execute("""INSERT INTO alert_delivery_policy(workspace_id,enabled,digest_frequency,min_severity,quiet_start,quiet_end,channels_json,dedupe_minutes,max_retries,updated_at)
                    VALUES(?,?,?,?,?,?,?,?,?,?) ON CONFLICT(workspace_id) DO UPDATE SET enabled=excluded.enabled,digest_frequency=excluded.digest_frequency,
                    min_severity=excluded.min_severity,quiet_start=excluded.quiet_start,quiet_end=excluded.quiet_end,channels_json=excluded.channels_json,
                    dedupe_minutes=excluded.dedupe_minutes,max_retries=excluded.max_retries,updated_at=excluded.updated_at""",row)
            return True
        except Exception as e:
            self.migration_log.append("delivery_policy: "+str(e)[:140]); return False

    def build_change_digest(self, ws_id=1, frequency=None, now=None, persist=True):
        policy=self.get_delivery_policy(ws_id); frequency=str(frequency or policy.get("digest_frequency") or "daily").lower(); now=now or datetime.now()
        start,end=digest_window_bounds(frequency,now); min_rank=severity_rank(policy.get("min_severity") or "warning")
        try:
            q="""SELECT e.*,c.name,c.maps_url FROM branch_change_events e LEFT JOIN companies c ON c.pk=e.pk AND c.workspace_id=e.workspace_id
                 WHERE e.workspace_id=? AND e.created_at>=? AND e.created_at<=? ORDER BY e.id ASC"""
            with self._c() as c: rows=[dict(r) for r in c.execute(q,(int(ws_id),start.isoformat(),end.isoformat())).fetchall()]
        except Exception: rows=[]
        rows=[r for r in rows if severity_rank(r.get("severity"))>=min_rank]
        if not rows: return {"frequency":frequency,"window_start":start,"window_end":end,"events":[],"event_count":0,"branch_count":0,"severity_max":"info","summary":"","dedupe_key":digest_dedupe_key(ws_id,frequency,start,end,"info")}
        sev=max((str(r.get("severity") or "info") for r in rows),key=severity_rank); branches=len(set(str(r.get("pk") or "") for r in rows if r.get("pk")))
        counts={}
        for r in rows: counts[r.get("event_type") or "change"]=counts.get(r.get("event_type") or "change",0)+1
        top=sorted(counts.items(),key=lambda x:(-x[1],x[0]))[:5]
        summary=("{} change(s) across {} branch(es). ".format(len(rows),branches)+", ".join("{} ×{}".format(k,v) for k,v in top)).strip()
        key=digest_dedupe_key(ws_id,frequency,start,end,sev); ids=[int(r["id"]) for r in rows if r.get("id") is not None]; run_id=None
        if persist:
            try:
                with self._c() as c:
                    c.execute("""INSERT OR IGNORE INTO digest_runs(workspace_id,frequency,window_start,window_end,dedupe_key,severity_max,event_count,branch_count,source_event_ids,summary,status,created_at)
                        VALUES(?,?,?,?,?,?,?,?,?,?,?,?)""",(int(ws_id),frequency,start.isoformat(),end.isoformat(),key,sev,len(rows),branches,json.dumps(ids),summary,"built",datetime.now().isoformat()))
                    rr=c.execute("SELECT id FROM digest_runs WHERE workspace_id=? AND dedupe_key=?",(int(ws_id),key)).fetchone(); run_id=int(rr[0]) if rr else None
            except Exception: pass
        return {"id":run_id,"frequency":frequency,"window_start":start,"window_end":end,"events":rows,"event_count":len(rows),"branch_count":branches,"severity_max":sev,"summary":summary,"dedupe_key":key,"source_event_ids":ids}

    def queue_digest_outbox(self, ws_id=1, digest=None, channels=None):
        digest=digest or self.build_change_digest(ws_id,persist=True); policy=self.get_delivery_policy(ws_id)
        if not digest.get("event_count"): return 0
        if channels is None:
            try: channels=json.loads(policy.get("channels_json") or "[]")
            except Exception: channels=[]
        channels=[str(x).lower() for x in (channels or []) if str(x).lower() in ("email","telegram","slack","teams","whatsapp","webhook")]
        if not channels: channels=["telegram","slack","teams","whatsapp","email","webhook"]
        title="{}: {} important change(s)".format(str(digest.get("severity_max") or "info").upper(),int(digest.get("event_count") or 0))
        lines=[digest.get("summary") or title]
        for ev in (digest.get("events") or [])[:12]:
            line="• {} — {}: {}".format(ev.get("name") or ev.get("pk") or "Branch",ev.get("event_type") or "change",ev.get("detail") or ev.get("new_value") or "")
            if ev.get("maps_url"): line += " | Google Maps: "+str(ev.get("maps_url"))
            lines.append(line[:900])
        body="\n".join(lines); added=0; now=datetime.now().isoformat(); maxr=max(1,int(policy.get("max_retries") or 3))
        with self._c() as c:
            for channel in channels:
                cur=c.execute("""INSERT OR IGNORE INTO alert_outbox(workspace_id,digest_run_id,dedupe_key,severity,channel,subject,body,source_event_ids,status,attempts,max_attempts,queued_at)
                    VALUES(?,?,?,?,?,?,?,?,?,?,?,?)""",(int(ws_id),digest.get("id"),digest.get("dedupe_key"),digest.get("severity_max"),channel,title,body,json.dumps(digest.get("source_event_ids") or []),"queued",0,maxr,now))
                if cur.rowcount: added+=1
        return added

    def list_outbox(self, ws_id=1, status=None, limit=500):
        try:
            q="SELECT * FROM alert_outbox WHERE workspace_id=?"; p=[int(ws_id)]
            if status and status!="all": q+=" AND status=?"; p.append(str(status))
            q+=" ORDER BY id DESC LIMIT ?"; p.append(max(1,int(limit)))
            with self._c() as c: return pd.read_sql_query(q,c,params=p)
        except Exception: return pd.DataFrame()

    def list_digest_runs(self, ws_id=1, limit=200):
        try:
            with self._c() as c: return pd.read_sql_query("SELECT * FROM digest_runs WHERE workspace_id=? ORDER BY id DESC LIMIT ?",c,params=[int(ws_id),max(1,int(limit))])
        except Exception: return pd.DataFrame()

    def mark_outbox_result(self, outbox_id, ok, error="", retry_after_minutes=5):
        try:
            with self._c() as c:
                r=c.execute("SELECT attempts,max_attempts FROM alert_outbox WHERE id=?",(int(outbox_id),)).fetchone()
                if not r: return False
                attempts=int(r[0] or 0)+1; maxr=max(1,int(r[1] or 3)); now=datetime.now()
                if ok:
                    c.execute("UPDATE alert_outbox SET status='sent',attempts=?,sent_at=?,error=NULL,next_attempt_at=NULL WHERE id=?",(attempts,now.isoformat(),int(outbox_id)))
                elif attempts>=maxr:
                    c.execute("UPDATE alert_outbox SET status='failed',attempts=?,error=?,next_attempt_at=NULL WHERE id=?",(attempts,str(error)[:900],int(outbox_id)))
                else:
                    nxt=now+timedelta(minutes=max(1,int(retry_after_minutes))*max(1,2**(attempts-1)))
                    c.execute("UPDATE alert_outbox SET status='retry_wait',attempts=?,error=?,next_attempt_at=? WHERE id=?",(attempts,str(error)[:900],nxt.isoformat(),int(outbox_id)))
            return True
        except Exception: return False

    def delivery_status(self, ws_id=1):
        try:
            with self._c() as c:
                rows=c.execute("SELECT status,COUNT(*) n FROM alert_outbox WHERE workspace_id=? GROUP BY status",(int(ws_id),)).fetchall()
            d={str(r[0]):int(r[1]) for r in rows}; return {"queued":d.get("queued",0),"retry_wait":d.get("retry_wait",0),"sent":d.get("sent",0),"failed":d.get("failed",0)}
        except Exception: return {"queued":0,"retry_wait":0,"sent":0,"failed":0}

    def enqueue_sync(self, pk, ws_id=1, source="manual", priority=0, max_attempts=3):
        pk=str(pk or "").strip()
        if not pk: return None
        now=datetime.now().isoformat()
        try:
            with self._c() as c:
                row=c.execute("""SELECT id FROM sync_queue WHERE workspace_id=? AND pk=?
                    AND status IN ('queued','running','retry_wait') ORDER BY id DESC LIMIT 1""",(ws_id,pk)).fetchone()
                if row: return int(row[0])
                cur=c.execute("""INSERT INTO sync_queue(workspace_id,pk,source,priority,status,attempts,max_attempts,
                    queued_at,stage,progress_pct) VALUES(?,?,?,?, 'queued',0,?,?, 'queued',0)""",
                    (ws_id,pk,str(source or 'manual'),int(priority or 0),max(1,int(max_attempts or 3)),now))
                return int(cur.lastrowid)
        except Exception as e:
            self.migration_log.append("enqueue_sync: "+str(e)[:140]); return None

    def enqueue_monitored(self, ws_id=1, force=False, source="monitor"):
        mons=self.list_monitored(ws_id); added=[]; now=datetime.now()
        if mons is None or mons.empty: return added
        for _,m in mons.iterrows():
            if not bool(m.get("enabled")): continue
            due=bool(force)
            if not due:
                last=str(m.get("last_success") or "").strip(); interval=max(1,int(m.get("interval") or 6))
                if not last: due=True
                else:
                    try: due=(now-datetime.fromisoformat(last)) >= timedelta(hours=interval)
                    except Exception: due=True
            if due:
                qid=self.enqueue_sync(str(m.get("pk") or ""),ws_id,source=source,priority=10 if force else 0)
                if qid and qid not in added: added.append(qid)
        return added

    def sync_queue_rows(self, ws_id=1, status=None, limit=250):
        try:
            q="""SELECT q.*,c.name,c.address,c.rating,c.review_count FROM sync_queue q
                 LEFT JOIN companies c ON c.pk=q.pk AND c.workspace_id=q.workspace_id
                 WHERE q.workspace_id=?"""; p=[ws_id]
            if status:
                q += " AND q.status=?"; p.append(status)
            q += " ORDER BY CASE q.status WHEN 'running' THEN 0 WHEN 'queued' THEN 1 WHEN 'retry_wait' THEN 2 WHEN 'failed' THEN 3 ELSE 4 END, q.priority DESC, q.id DESC LIMIT ?"
            p.append(int(limit))
            with self._c() as c: return pd.read_sql_query(q,c,params=p)
        except Exception: return pd.DataFrame()

    def sync_queue_counts(self, ws_id=1):
        out={"queued":0,"running":0,"retry_wait":0,"done":0,"failed":0}
        try:
            with self._c() as c:
                for r in c.execute("SELECT status,COUNT(*) n FROM sync_queue WHERE workspace_id=? GROUP BY status",(ws_id,)):
                    out[str(r[0])]=int(r[1])
        except Exception: pass
        return out

    def claim_next_sync(self, ws_id=None):
        now=datetime.now().isoformat()
        try:
            with self._c() as c:
                c.execute("BEGIN IMMEDIATE")
                q="""SELECT * FROM sync_queue WHERE status IN ('queued','retry_wait')
                     AND (status='queued' OR next_attempt_at IS NULL OR next_attempt_at<=?)"""; p=[now]
                if ws_id is not None: q += " AND workspace_id=?"; p.append(int(ws_id))
                q += " ORDER BY priority DESC,id ASC LIMIT 1"
                row=c.execute(q,p).fetchone()
                if not row: return None
                qid=int(row["id"])
                c.execute("""UPDATE sync_queue SET status='running',started_at=?,completed_at=NULL,next_attempt_at=NULL,
                    stage='starting',progress_pct=3,error=NULL,attempts=COALESCE(attempts,0)+1 WHERE id=?""",(now,qid))
                out=c.execute("SELECT * FROM sync_queue WHERE id=?",(qid,)).fetchone()
                return dict(out) if out else None
        except Exception as e:
            self.migration_log.append("claim_sync: "+str(e)[:140]); return None

    def update_sync_queue_progress(self, queue_id, event):
        if not queue_id: return False
        ev=str((event or {}).get("event") or "working"); pct=None; stage=ev
        if ev=="browser": pct=6; stage="browser"
        elif ev in ("provider","geocode_provider"): pct=12; stage="connecting"
        elif ev in ("reviews","review"):
            cur=max(0,int((event or {}).get("current") or 0)); total=max(1,int((event or {}).get("total") or 1))
            pct=min(92,20+(cur/total)*70); stage="reviews"
        elif ev in ("details_start","details"): pct=80; stage="details"
        elif ev in ("finalizing",): pct=95; stage="finalizing"
        elif ev=="done": pct=97; stage="saving"
        if pct is None: pct=15
        try:
            with self._c() as c:
                c.execute("UPDATE sync_queue SET stage=?,progress_pct=MAX(COALESCE(progress_pct,0),?) WHERE id=?",
                          (stage,float(pct),int(queue_id)))
            return True
        except Exception: return False

    def finish_sync_queue(self, queue_id, status="ok", new_reviews=0, duplicate_reviews=0, changed_fields=None, error=None):
        now=datetime.now(); changes=changed_fields or []
        if not isinstance(changes,str): changes=json.dumps(changes,ensure_ascii=False)
        try:
            with self._c() as c:
                row=c.execute("SELECT attempts,max_attempts FROM sync_queue WHERE id=?",(int(queue_id),)).fetchone()
                if not row: return False
                attempts=int(row["attempts"] or 0); max_attempts=max(1,int(row["max_attempts"] or 3))
                if status=="ok":
                    c.execute("""UPDATE sync_queue SET status='done',stage='done',progress_pct=100,completed_at=?,next_attempt_at=NULL,
                        error=NULL,new_reviews=?,duplicate_reviews=?,changed_fields=? WHERE id=?""",
                        (now.isoformat(),int(new_reviews or 0),int(duplicate_reviews or 0),changes,int(queue_id)))
                elif status=="cancelled":
                    c.execute("UPDATE sync_queue SET status='failed',stage='cancelled',completed_at=?,error=? WHERE id=?",
                              (now.isoformat(),str(error or 'cancelled')[:600],int(queue_id)))
                elif attempts < max_attempts:
                    delay=min(30,2**max(1,attempts)); nxt=(now+timedelta(minutes=delay)).isoformat()
                    c.execute("""UPDATE sync_queue SET status='retry_wait',stage='retry_wait',progress_pct=0,completed_at=NULL,
                        next_attempt_at=?,error=?,new_reviews=?,duplicate_reviews=?,changed_fields=? WHERE id=?""",
                        (nxt,str(error or 'sync_failed')[:600],int(new_reviews or 0),int(duplicate_reviews or 0),changes,int(queue_id)))
                else:
                    c.execute("""UPDATE sync_queue SET status='failed',stage='failed',completed_at=?,next_attempt_at=NULL,error=?,
                        new_reviews=?,duplicate_reviews=?,changed_fields=? WHERE id=?""",
                        (now.isoformat(),str(error or 'sync_failed')[:600],int(new_reviews or 0),int(duplicate_reviews or 0),changes,int(queue_id)))
            return True
        except Exception as e:
            self.migration_log.append("finish_sync: "+str(e)[:140]); return False

    def retry_sync(self, queue_id):
        try:
            with self._c() as c:
                c.execute("""UPDATE sync_queue SET status='queued',stage='queued',progress_pct=0,next_attempt_at=NULL,
                    completed_at=NULL,error=NULL WHERE id=? AND status IN ('failed','retry_wait')""",(int(queue_id),))
                return c.total_changes>0
        except Exception: return False

    def create_sync_run(self, pk, ws_id=1, queue_id=None, before_snapshot=None):
        try:
            snap=json.dumps(before_snapshot or {},ensure_ascii=False,default=str)
            with self._c() as c:
                cur=c.execute("""INSERT INTO sync_runs(queue_id,workspace_id,pk,started_at,status,new_reviews,duplicate_reviews,before_snapshot,new_negative_reviews)
                    VALUES(?,?,?,?, 'running',0,0,?,0)""",(queue_id,ws_id,str(pk),datetime.now().isoformat(),snap))
                return int(cur.lastrowid)
        except Exception as e:
            self.migration_log.append("create_sync_run: "+str(e)[:140]); return None

    def finish_sync_run(self, run_id, status="ok", new_reviews=0, duplicate_reviews=0, changed_fields=None, error=None, after_snapshot=None, new_negative_reviews=0):
        if not run_id: return False
        changes=changed_fields or []
        if not isinstance(changes,str): changes=json.dumps(changes,ensure_ascii=False)
        try:
            with self._c() as c:
                c.execute("""UPDATE sync_runs SET completed_at=?,status=?,new_reviews=?,duplicate_reviews=?,changed_fields=?,error=?,after_snapshot=?,new_negative_reviews=? WHERE id=?""",
                          (datetime.now().isoformat(),status,int(new_reviews or 0),int(duplicate_reviews or 0),changes,
                           str(error or '')[:600] if error else None,json.dumps(after_snapshot or {},ensure_ascii=False,default=str),
                           int(new_negative_reviews or 0),int(run_id)))
            return True
        except Exception: return False

    def sync_runs(self, ws_id=1, pk=None, limit=200):
        try:
            q="""SELECT s.*,c.name FROM sync_runs s LEFT JOIN companies c ON c.pk=s.pk AND c.workspace_id=s.workspace_id
                 WHERE s.workspace_id=?"""; p=[ws_id]
            if pk: q += " AND s.pk=?"; p.append(str(pk))
            q += " ORDER BY s.id DESC LIMIT ?"; p.append(int(limit))
            with self._c() as c: return pd.read_sql_query(q,c,params=p)
        except Exception: return pd.DataFrame()

    def review_delta(self, ws_id=1, mode="latest", pk=None, limit=2000):
        try:
            q="""SELECT r.*,c.name company_name,s.completed_at sync_completed_at,s.id sync_run
                 FROM reviews r JOIN sync_runs s ON s.id=r.sync_run_id
                 LEFT JOIN companies c ON c.pk=r.pk AND c.workspace_id=r.workspace_id
                 WHERE r.workspace_id=? AND s.status='ok'"""; p=[ws_id]
            if pk: q += " AND r.pk=?"; p.append(str(pk))
            if mode=="latest":
                q += " AND s.id=(SELECT MAX(s2.id) FROM sync_runs s2 WHERE s2.workspace_id=s.workspace_id AND s2.pk=s.pk AND s2.status='ok')"
            elif mode=="24h":
                q += " AND s.completed_at>=?"; p.append((datetime.now()-timedelta(hours=24)).isoformat())
            elif mode=="7d":
                q += " AND s.completed_at>=?"; p.append((datetime.now()-timedelta(days=7)).isoformat())
            q += " ORDER BY r.id DESC LIMIT ?"; p.append(int(limit))
            with self._c() as c: return pd.read_sql_query(q,c,params=p)
        except Exception: return pd.DataFrame()

    def add_history(self, q, n, ws_id=1):
        try:
            with self._c() as c:
                c.execute("INSERT INTO search_history(workspace_id,q,ts,results) VALUES(?,?,?,?)",
                          (ws_id, q, datetime.now().isoformat(), n))
        except Exception: pass

    def recent_searches(self, ws_id=1, limit=8):
        try:
            with self._c() as c:
                return pd.read_sql_query(
                    "SELECT * FROM search_history WHERE workspace_id=? ORDER BY id DESC LIMIT ?",
                    c, params=(ws_id, limit))
        except Exception: return pd.DataFrame()

    def save_alert(self, fp, pk, rid, kind, severity, title, detail, ws_id=1):
        try:
            with self._c() as c:
                c.execute("""INSERT OR IGNORE INTO alerts(fingerprint,workspace_id,pk,review_id,
                    kind,severity,title,detail,status,created_at)
                    VALUES(?,?,?,?,?,?,?,?,'new',?)""",
                    (fp, ws_id, pk, rid, kind, severity, title, detail,
                     datetime.now().isoformat()))
        except Exception: pass

    def list_alerts(self, ws_id=1, status="new", limit=300):
        try:
            with self._c() as c:
                q = """SELECT a.*, c.name company_name FROM alerts a
                    LEFT JOIN companies c ON c.pk=a.pk AND c.workspace_id=a.workspace_id
                    WHERE a.workspace_id=?"""; p = [ws_id]
                if status != "all": q += " AND a.status=?"; p.append(status)
                q += " ORDER BY a.id DESC LIMIT ?"; p.append(limit)
                return pd.read_sql_query(q, c, params=p)
        except Exception: return pd.DataFrame()

    def resolve_alert(self, aid):
        try:
            with self._c() as c:
                c.execute("UPDATE alerts SET status='resolved', resolved_at=? WHERE id=?",
                          (datetime.now().isoformat(), aid))
        except Exception: pass

    def list_tickets(self, ws_id=1):
        try:
            with self._c() as c:
                return pd.read_sql_query(
                    "SELECT * FROM tickets WHERE workspace_id=? ORDER BY id DESC",
                    c, params=(ws_id,))
        except Exception: return pd.DataFrame()

    def create_ticket(self, pk, review_id, title, priority="medium", ws_id=1,
                      source_kind=None, source_id=None, action_key=None, due_at=None, owner=None, notes=None):
        """Create one ticket per logical action when action_key is supplied; legacy calls remain compatible."""
        now = datetime.now().isoformat()
        try:
            with self._c() as c:
                if action_key:
                    hit=c.execute("SELECT id FROM tickets WHERE workspace_id=? AND action_key=? LIMIT 1",
                                  (ws_id,str(action_key))).fetchone()
                    if hit: return int(hit[0])
                cur=c.execute("""INSERT INTO tickets(pk,workspace_id,review_id,title,priority,owner,notes,
                    source_kind,source_id,action_key,due_at,created_at,updated_at)
                    VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                    (pk, ws_id, review_id, title, priority, owner, notes, source_kind,
                     str(source_id) if source_id is not None else None, action_key, due_at, now, now))
                return int(cur.lastrowid)
        except Exception as e:
            self.migration_log.append("create_ticket: "+str(e)[:140])
            return None

    def actionable_reviews(self, ws_id=1, limit=1500):
        """Reviews that need attention, optimized for the Action Center."""
        try:
            with self._c() as c:
                return pd.read_sql_query("""SELECT r.*,c.name company_name,c.maps_url,c.address company_address
                    FROM reviews r LEFT JOIN companies c ON c.pk=r.pk AND c.workspace_id=r.workspace_id
                    WHERE r.workspace_id=? AND COALESCE(r.workflow_status,'new') NOT IN ('resolved','ignored')
                      AND (COALESCE(r.responded,0)=0 OR COALESCE(r.sentiment,'')='negative' OR COALESCE(r.urgency,0)>=60)
                    ORDER BY CASE COALESCE(r.priority,'normal') WHEN 'critical' THEN 4 WHEN 'high' THEN 3 WHEN 'normal' THEN 2 ELSE 1 END DESC,
                             COALESCE(r.urgency,0) DESC, r.id DESC LIMIT ?""",c,params=(ws_id,max(1,int(limit))))
        except Exception:
            return pd.DataFrame()

    def update_ticket(self, tid, **fields):
        if not fields: return
        fields["updated_at"] = datetime.now().isoformat()
        cols = ",".join("{}=?".format(k) for k in fields)
        try:
            with self._c() as c:
                c.execute("UPDATE tickets SET {} WHERE id=?".format(cols),
                          list(fields.values()) + [tid])
        except Exception: pass

    def save_report_template(self, name, sections, ws_id=1):
        try:
            now = datetime.now().isoformat()
            payload = json.dumps(sections, ensure_ascii=False)
            with self._c() as c:
                row = c.execute("SELECT id FROM report_templates WHERE workspace_id=? AND name=? ORDER BY id DESC LIMIT 1",
                                (ws_id, name)).fetchone()
                if row:
                    c.execute("UPDATE report_templates SET sections_json=?, created_at=? WHERE id=?",
                              (payload, now, row["id"]))
                else:
                    c.execute("""INSERT INTO report_templates(workspace_id,name,sections_json,created_at)
                        VALUES(?,?,?,?)""", (ws_id, name, payload, now))
            return True
        except Exception:
            return False

    def list_report_templates(self, ws_id=1):
        try:
            with self._c() as c:
                return pd.read_sql_query(
                    "SELECT id,name,sections_json,created_at FROM report_templates WHERE workspace_id=? ORDER BY id DESC",
                    c, params=(ws_id,))
        except Exception:
            return pd.DataFrame()

    def delete_report_template(self, template_id, ws_id=1):
        try:
            with self._c() as c:
                c.execute("DELETE FROM report_templates WHERE id=? AND workspace_id=?", (template_id, ws_id))
            return True
        except Exception:
            return False

    def push_notif(self, kind, severity, title, body, pk=None, review_id=None, ws_id=1):
        try:
            with self._c() as c:
                c.execute("""INSERT INTO notif_center(workspace_id,kind,severity,title,body,
                    pk,review_id,created_at) VALUES(?,?,?,?,?,?,?,?)""",
                    (ws_id, kind, severity, title, body, pk, review_id,
                     datetime.now().isoformat()))
        except Exception: pass

    def list_notifs(self, ws_id=1, status="unread", limit=100):
        try:
            with self._c() as c:
                q = "SELECT * FROM notif_center WHERE workspace_id=?"
                if status == "unread": q += " AND read_at IS NULL"
                q += " ORDER BY id DESC LIMIT ?"
                return pd.read_sql_query(q, c, params=(ws_id, limit))
        except Exception: return pd.DataFrame()

    def mark_notif_read(self, nid):
        try:
            with self._c() as c:
                c.execute("UPDATE notif_center SET read_at=? WHERE id=?",
                          (datetime.now().isoformat(), nid))
        except Exception: pass

    def count_unread_notifs(self, ws_id=1):
        try:
            with self._c() as c:
                r = c.execute("SELECT COUNT(*) FROM notif_center WHERE workspace_id=? AND read_at IS NULL",
                              (ws_id,)).fetchone()
                return int(r[0]) if r else 0
        except Exception: return 0

    def save_view(self, name, filters, ws_id=1):
        now = datetime.now().isoformat()
        payload = json.dumps(filters, ensure_ascii=False)
        try:
            with self._c() as c:
                cur = c.execute("""UPDATE saved_views SET filters_json=?, updated_at=?
                    WHERE name=? AND workspace_id=?""", (payload, now, name, ws_id))
                if cur.rowcount == 0:
                    c.execute("""INSERT INTO saved_views(workspace_id,name,filters_json,
                        created_at,updated_at) VALUES(?,?,?,?,?)""",
                        (ws_id, name, payload, now, now))
        except Exception: pass

    def saved_views(self, ws_id=1):
        try:
            with self._c() as c:
                return pd.read_sql_query(
                    "SELECT * FROM saved_views WHERE workspace_id=? ORDER BY id DESC",
                    c, params=(ws_id,))
        except Exception: return pd.DataFrame()

    def delete_view(self, vid):
        try:
            with self._c() as c: c.execute("DELETE FROM saved_views WHERE id=?", (vid,))
        except Exception: pass

    def get_pref(self, key, default=None):
        try:
            with self._c() as c:
                r = c.execute("SELECT value_json FROM user_preferences WHERE pref_key=?", (key,)).fetchone()
            return json.loads(r[0]) if r and r[0] else default
        except Exception:
            return default

    def set_pref(self, key, value):
        try:
            with self._c() as c:
                c.execute("""INSERT INTO user_preferences(pref_key,value_json,updated_at) VALUES(?,?,?)
                    ON CONFLICT(pref_key) DO UPDATE SET value_json=excluded.value_json,updated_at=excluded.updated_at""",
                    (key, json.dumps(value, ensure_ascii=False), datetime.now().isoformat()))
            return True
        except Exception:
            return False

    def _review_filter_sql(self, ws_id=1, since=None, status=None, sentiment=None, pk=None, text_query=None,
                           topic=None, min_rating=None, max_rating=None, responded=None, priority=None):
        q = " FROM reviews r WHERE r.workspace_id=?"; p=[ws_id]
        if since: q += " AND r.fetched_at>=?"; p.append(since.isoformat())
        if status: q += " AND r.workflow_status=?"; p.append(status)
        if sentiment: q += " AND r.sentiment=?"; p.append(sentiment)
        if pk: q += " AND r.pk=?"; p.append(pk)
        if topic: q += " AND r.topic=?"; p.append(topic)
        if min_rating is not None: q += " AND COALESCE(r.rating,0)>=?"; p.append(int(min_rating))
        if max_rating is not None: q += " AND COALESCE(r.rating,0)<=?"; p.append(int(max_rating))
        if responded is not None: q += " AND COALESCE(r.responded,0)=?"; p.append(1 if responded else 0)
        if priority: q += " AND COALESCE(r.priority,'normal')=?"; p.append(priority)
        if text_query:
            q += " AND (r.text LIKE ? OR r.author LIKE ? OR r.topic LIKE ?)"; like="%{}%".format(text_query); p += [like,like,like]
        return q,p

    def reviews_count(self, ws_id=1, since=None, status=None, sentiment=None, pk=None, text_query=None,
                      topic=None, min_rating=None, max_rating=None, responded=None, priority=None):
        try:
            tail,p=self._review_filter_sql(ws_id,since,status,sentiment,pk,text_query,topic,min_rating,max_rating,responded,priority)
            with self._c() as c: return int(c.execute("SELECT COUNT(*)"+tail,p).fetchone()[0])
        except Exception: return 0

    def reviews_page(self, ws_id=1, since=None, status=None, sentiment=None, pk=None,
                     text_query=None, limit=50, offset=0, topic=None, min_rating=None,
                     max_rating=None, responded=None, priority=None, sort="newest"):
        try:
            tail,p=self._review_filter_sql(ws_id,since,status,sentiment,pk,text_query,topic,min_rating,max_rating,responded,priority)
            q = """SELECT r.*,c.name company_name,c.rating company_rating,c.address company_address
                FROM reviews r LEFT JOIN companies c ON c.pk=r.pk AND c.workspace_id=r.workspace_id
                WHERE r.id IN (SELECT r.id""" + tail + ")"
            order={"oldest":"COALESCE(r.review_date,r.fetched_at,'') ASC, r.id ASC","rating_low":"r.rating ASC, r.id DESC","rating_high":"r.rating DESC, r.id DESC",
                   "priority":"CASE COALESCE(r.priority,'normal') WHEN 'critical' THEN 4 WHEN 'high' THEN 3 WHEN 'normal' THEN 2 ELSE 1 END DESC, r.id DESC",
                   "author_asc":"LOWER(COALESCE(r.author,'')) ASC, r.id DESC","author_desc":"LOWER(COALESCE(r.author,'')) DESC, r.id DESC",
                   "unanswered_first":"COALESCE(r.responded,0) ASC, r.id DESC","answered_first":"COALESCE(r.responded,0) DESC, r.id DESC",
                   "newest":"COALESCE(r.review_date,r.fetched_at,'') DESC, r.id DESC"}.get(sort,"r.id DESC")
            q += " ORDER BY " + order + " LIMIT ? OFFSET ?"; p += [int(limit),int(offset)]
            with self._c() as c: return pd.read_sql_query(q,c,params=p)
        except Exception: return pd.DataFrame()

    def review_topics(self, ws_id=1):
        try:
            with self._c() as c:
                rows=c.execute("SELECT DISTINCT topic FROM reviews WHERE workspace_id=? AND topic IS NOT NULL AND TRIM(topic)<>'' ORDER BY topic",(ws_id,)).fetchall()
            return [str(r[0]) for r in rows if r[0]]
        except Exception: return []

    def log_search_job(self, job_id, query, params, meta, ws_id=1, status="completed"):
        try:
            with self._c() as c:
                c.execute("""INSERT INTO search_jobs(job_id,workspace_id,query,status,params_json,started_at,
                    completed_at,elapsed_ms,results,discovered,tiles,errors,detail)
                    VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)
                    ON CONFLICT(job_id) DO UPDATE SET status=excluded.status,completed_at=excluded.completed_at,
                    elapsed_ms=excluded.elapsed_ms,results=excluded.results,discovered=excluded.discovered,
                    tiles=excluded.tiles,errors=excluded.errors,detail=excluded.detail""",
                    (job_id,ws_id,query,status,json.dumps(params,ensure_ascii=False),
                     meta.get("started_at") or datetime.now().isoformat(), datetime.now().isoformat(),
                     safe_num(meta.get("elapsed_ms"),0), int(meta.get("total_unique") or 0),
                     int(meta.get("total_discovered") or meta.get("total_unique") or 0),
                     int(meta.get("searched_tiles") or 0), len(meta.get("errors") or []),
                     json.dumps(meta,ensure_ascii=False)[:12000]))
            return True
        except Exception: return False

    def list_search_jobs(self, ws_id=1, limit=100):
        try:
            with self._c() as c:
                return pd.read_sql_query("SELECT * FROM search_jobs WHERE workspace_id=? ORDER BY started_at DESC LIMIT ?",
                                         c,params=(ws_id,int(limit)))
        except Exception: return pd.DataFrame()

    def log_performance(self, ws_id, metric, ms, detail=""):
        try:
            with self._c() as c:
                c.execute("INSERT INTO performance_log(workspace_id,ts,metric,ms,detail) VALUES(?,?,?,?,?)",
                          (ws_id,datetime.now().isoformat(),metric,float(ms),str(detail)[:500]))
        except Exception: pass

    def integrity(self):
        try:
            with self._c() as c:
                return c.execute("PRAGMA integrity_check").fetchone()[0]
        except Exception as e: return str(e)

    def optimize(self):
        started = time.perf_counter()
        try:
            with self._c() as c:
                c.execute("PRAGMA optimize")
                c.execute("ANALYZE")
            c = sqlite3.connect(str(DB_PATH), timeout=60, isolation_level=None)
            try:
                c.execute("VACUUM")
            finally:
                c.close()
            elapsed = (time.perf_counter() - started) * 1000
            return True, "Optimize + ANALYZE + VACUUM completed in {:.0f} ms".format(elapsed)
        except Exception as e:
            return False, str(e)

    # ─── AI Jobs (v19.0) ──────────────────────────────────────────────
    def add_ai_job(self, kind, target, ws_id=1):
        try:
            with self._c() as c:
                cur = c.execute("""INSERT INTO ai_jobs(workspace_id,kind,target,status,created_at)
                    VALUES(?,?,?,'pending',?)""",
                    (ws_id, kind, target, datetime.now().isoformat()))
                return cur.lastrowid
        except Exception: return None

    def update_ai_job(self, jid, status, result=""):
        try:
            with self._c() as c:
                c.execute("""UPDATE ai_jobs SET status=?, result=?, completed_at=?
                    WHERE id=?""",
                    (status, result, datetime.now().isoformat(), jid))
        except Exception: pass

    def list_ai_jobs(self, ws_id=1, limit=50):
        try:
            with self._c() as c:
                return pd.read_sql_query(
                    "SELECT * FROM ai_jobs WHERE workspace_id=? ORDER BY id DESC LIMIT ?",
                    c, params=(ws_id, limit))
        except Exception: return pd.DataFrame()


@st.cache_resource(show_spinner=False)
def get_db():
    return DB()


def invalidate_cache():
    try: st.cache_data.clear()
    except Exception: pass


# ═══════════════════════════════════════════════════════════════════════
#  AI Helpers (v19.0)
# ═══════════════════════════════════════════════════════════════════════
@st.cache_resource(show_spinner=False)
def get_ai_pipeline():
    try:
        from transformers import pipeline
        import torch
        device = 0 if torch.cuda.is_available() else -1
    except Exception as e:
        return {"available": False, "error": str(e)[:200],
                "ar": None, "en": None, "device": "none"}
    ar = en = None
    try:
        ar = pipeline("sentiment-analysis",
                      model="iMeshal/arabic-sentiment-classifier-marbert",
                      device=device, truncation=True, max_length=256, batch_size=32)
    except Exception as e:
        print("Arabic model load failed:", str(e)[:100])
    try:
        en = pipeline("sentiment-analysis",
                      model="cardiffnlp/twitter-xlm-roberta-base-sentiment",
                      device=device, truncation=True, max_length=256, batch_size=32)
    except Exception as e:
        print("English model load failed:", str(e)[:100])
    return {"available": bool(ar or en), "error": None, "ar": ar, "en": en,
            "device": "GPU" if device == 0 else "CPU"}


def ai_status():
    p = get_ai_pipeline()
    return {"available": p.get("available", False),
            "device": p.get("device", "none"),
            "arabic": p.get("ar") is not None,
            "english": p.get("en") is not None,
            "error": p.get("error")}


def normalize_ai_label(label, kind):
    x = str(label).lower()
    if "pos" in x: return "positive"
    if "neg" in x: return "negative"
    if x in ("label_0","0"): return "negative"
    if x in ("label_1","1"): return "positive" if kind == "ar" else "neutral"
    if x in ("label_2","2"): return "positive"
    return "neutral"


def run_ai_on_reviews(df):
    """Language-aware AI enhancement. Each text is sent to only one suitable model."""
    if df is None or df.empty: return 0
    p = get_ai_pipeline()
    if not p.get("available"): return 0
    base = analyze_simple(df)
    updated = 0
    for pos, (_, row) in enumerate(base.iterrows()):
        txt = str(row.get("text") or "")[:1500].strip()
        if not txt: continue
        lang = str(row.get("language") or "unknown")
        kind = "ar" if lang in ("ar","ur","fa") else "en"
        pipe = p.get(kind)
        if pipe is None: continue
        try:
            pred = pipe(txt, truncation=True)
            if isinstance(pred, list): pred = pred[0] if pred else {}
            ai_conf = safe_num(pred.get("score"), 0)
            if ai_conf < 0.55: continue
            ai_sent = normalize_ai_label(pred.get("label", ""), kind)
            rule_sent = str(row.get("sentiment") or "neutral")
            agreement = (ai_sent == rule_sent)
            final_conf = min(.995, ai_conf * .82 + safe_num(row.get("analysis_confidence"), .5) * .18 + (.04 if agreement else -.10))
            result = dict(row)
            result.update({
                "sentiment": ai_sent,
                "sentiment_score": round(max(.50, final_conf), 3),
                "analysis_confidence": round(max(.50, final_conf), 3),
                "analysis_method": "ai-{}+rule-v3".format(kind),
            })
            db.update_review_analysis(int(row.get("id")), result)
            updated += 1
        except Exception as e:
            print("AI review error:", str(e)[:120])
    return updated


def reanalyze_rule_based_reviews(df):
    """Re-run Analysis Pipeline 3.0 and persist all transparent rule results."""
    if df is None or df.empty: return 0
    out = analyze_simple(df)
    updated = 0
    for _, row in out.iterrows():
        rid = row.get("id")
        if pd.isna(rid): continue
        if db.update_review_analysis(int(rid), dict(row)): updated += 1
    return updated


# ═══════════════════════════════════════════════════════════════════════
#  Notification senders (v19.1)
# ═══════════════════════════════════════════════════════════════════════
def send_whatsapp(text):
    token = CFG.get("WHATSAPP_TOKEN"); phone_id = CFG.get("WHATSAPP_PHONE_ID")
    to = CFG.get("WHATSAPP_TO")
    if not (token and phone_id and to):
        return False, "whatsapp_not_configured"
    try:
        url = "https://graph.facebook.com/v18.0/{}/messages".format(phone_id)
        body = {"messaging_product": "whatsapp", "to": to,
                "type": "text", "text": {"body": text[:4000]}}
        data = json.dumps(body).encode("utf-8")
        req = urllib.request.Request(url, data=data,
            headers={"Authorization": "Bearer " + token,
                     "Content-Type": "application/json"})
        urllib.request.urlopen(req, timeout=20)
        return True, "ok"
    except Exception as e:
        return False, str(e)[:200]


def send_slack(text):
    url = CFG.get("SLACK_WEBHOOK_URL")
    if not url: return False, "slack_not_configured"
    try:
        data = json.dumps({"text": text[:3000]}).encode("utf-8")
        req = urllib.request.Request(url, data=data,
            headers={"Content-Type": "application/json"})
        urllib.request.urlopen(req, timeout=15)
        return True, "ok"
    except Exception as e:
        return False, str(e)[:200]


def send_teams(text):
    url = CFG.get("TEAMS_WEBHOOK_URL")
    if not url: return False, "teams_not_configured"
    try:
        data = json.dumps({"text": text[:3000]}).encode("utf-8")
        req = urllib.request.Request(url, data=data,
            headers={"Content-Type": "application/json"})
        urllib.request.urlopen(req, timeout=15)
        return True, "ok"
    except Exception as e:
        return False, str(e)[:200]


def send_telegram(text):
    token = CFG.get("TELEGRAM_BOT_TOKEN"); chat = CFG.get("TELEGRAM_CHAT_ID")
    if not (token and chat): return False, "telegram_not_configured"
    try:
        url = "https://api.telegram.org/bot{}/sendMessage".format(token)
        data = urllib.parse.urlencode({"chat_id": chat, "text": text[:4000],
                                        "parse_mode": "HTML"}).encode()
        urllib.request.urlopen(url, data=data, timeout=15)
        return True, "ok"
    except Exception as e:
        return False, str(e)[:200]


def send_email_alert(subject, text):
    import smtplib
    from email.mime.text import MIMEText
    sender=CFG.get("SMTP_SENDER"); password=CFG.get("SMTP_PASSWORD"); recipient=CFG.get("SMTP_RECIPIENT")
    if not (sender and password and recipient): return False,"email_not_configured"
    try:
        msg=MIMEText(str(text or ""),"plain","utf-8"); msg["Subject"]=str(subject or "Reviews Tracker"); msg["From"]=sender; msg["To"]=recipient
        with smtplib.SMTP(CFG.get("SMTP_SERVER","smtp.gmail.com"),int(CFG.get("SMTP_PORT",587)),timeout=20) as s:
            s.starttls(); s.login(sender,password); s.send_message(msg)
        return True,"ok"
    except Exception as e: return False,str(e)[:200]

def send_webhook_alert(text):
    url=CFG.get("WEBHOOK_URL")
    if not url: return False,"webhook_not_configured"
    try:
        data=json.dumps({"message":str(text or "")[:5000]}).encode("utf-8")
        req=urllib.request.Request(url,data=data,headers={"Content-Type":"application/json"})
        urllib.request.urlopen(req,timeout=15); return True,"ok"
    except Exception as e: return False,str(e)[:200]

def dispatch_delivery_channel(channel, subject, body):
    ch=str(channel or "").lower()
    if ch=="telegram": return send_telegram(body)
    if ch=="slack": return send_slack(body)
    if ch=="teams": return send_teams(body)
    if ch=="whatsapp": return send_whatsapp(body)
    if ch=="email": return send_email_alert(subject,body)
    if ch=="webhook": return send_webhook_alert(body)
    return False,"unsupported_channel"

def process_delivery_outbox(db, ws_id=1, limit=20, now=None):
    now=now or datetime.now(); policy=db.get_delivery_policy(ws_id)
    if in_quiet_hours(now,policy.get("quiet_start"),policy.get("quiet_end")): return {"sent":0,"failed":0,"deferred":True}
    df=db.list_outbox(ws_id,"all",max(100,int(limit)*4)); sent=failed=0
    if df is None or df.empty: return {"sent":0,"failed":0,"deferred":False}
    todo=df[df["status"].isin(["queued","retry_wait"])].copy()
    if "next_attempt_at" in todo:
        todo=todo[(todo["next_attempt_at"].isna()) | (todo["next_attempt_at"].astype(str)=="") | (pd.to_datetime(todo["next_attempt_at"],errors="coerce")<=pd.Timestamp(now))]
    for _,r in todo.head(max(1,int(limit))).iterrows():
        ok,msg=dispatch_delivery_channel(r.get("channel"),r.get("subject"),r.get("body")); db.mark_outbox_result(int(r.get("id")),ok,msg)
        if ok: sent+=1
        else: failed+=1
    return {"sent":sent,"failed":failed,"deferred":False}


# ═══════════════════════════════════════════════════════════════════════
#  Release Candidate runtime hygiene
# ═══════════════════════════════════════════════════════════════════════
def cleanup_job_artifacts(max_age_hours=72):
    removed=0; now=time.time()
    try:
        for p in JOB_DIR.glob("*"):
            try:
                if p.is_file() and now-p.stat().st_mtime > float(max_age_hours)*3600:
                    p.unlink(); removed+=1
            except Exception: pass
    except Exception: pass
    return removed

def runtime_health_snapshot(db, ws_id=1):
    out={"job_files":0,"wal_mb":0.0,"shm_mb":0.0,"db_mb":0.0,"branches":0,"reviews":0,"open_alerts":0,"stale_jobs":0}
    try:
        out["job_files"]=len([p for p in JOB_DIR.glob("*") if p.is_file()])
        out["db_mb"]=round(DB_PATH.stat().st_size/1024/1024,2) if DB_PATH.exists() else 0.0
        for suffix,key in [("-wal","wal_mb"),("-shm","shm_mb")]:
            p=Path(str(DB_PATH)+suffix); out[key]=round(p.stat().st_size/1024/1024,2) if p.exists() else 0.0
        with db._c() as c:
            out["branches"]=int(c.execute("SELECT COUNT(*) FROM companies WHERE workspace_id=?",(ws_id,)).fetchone()[0])
            out["reviews"]=int(c.execute("SELECT COUNT(*) FROM reviews WHERE workspace_id=?",(ws_id,)).fetchone()[0])
            out["open_alerts"]=int(c.execute("SELECT COUNT(*) FROM alerts WHERE workspace_id=? AND COALESCE(status,'new')<>'resolved'",(ws_id,)).fetchone()[0])
            cutoff=(datetime.now()-timedelta(hours=2)).isoformat()
            out["stale_jobs"]=int(c.execute("SELECT COUNT(*) FROM search_jobs WHERE workspace_id=? AND status IN ('running','started') AND started_at<?",(ws_id,cutoff)).fetchone()[0])
    except Exception as e:
        out["error"]="{}: {}".format(type(e).__name__,str(e)[:160])
    return out

try:
    cleanup_job_artifacts(72)
except Exception:
    pass

# ═══════════════════════════════════════════════════════════════════════
#  Worker / Scrape helpers
# ═══════════════════════════════════════════════════════════════════════
def call_worker(mode, query, max_val=100, city="", area="", activity="", lat=None, lng=None,
                radius_km=None, deep=False, exact_name=False, max_tiles=36, details=False,
                discover_max=None, progress_cb=None, known_reviews_file=None):
    started=time.perf_counter(); progress_events=[]; err_lines=[]
    try:
        cmd=[str(pyexe()),str(WORKER),mode,query,"--max",str(max_val),"--lang",st.session_state.get("lang","ar")]
        if city: cmd += ["--city",str(city)]
        if area: cmd += ["--area",str(area)]
        if activity: cmd += ["--activity",str(activity)]
        if lat is not None: cmd += ["--lat",str(lat)]
        if lng is not None: cmd += ["--lng",str(lng)]
        if radius_km: cmd += ["--radius",str(radius_km)]
        if deep: cmd += ["--deep"]
        if exact_name: cmd += ["--exact-name"]
        if max_tiles: cmd += ["--max-tiles",str(int(max_tiles))]
        if details and mode=="search": cmd += ["--details"]
        if discover_max and mode=="search": cmd += ["--discover-max",str(int(discover_max))]
        if known_reviews_file and mode in ("scrape","scrape-url"): cmd += ["--known-reviews-file",str(known_reviews_file)]
        timeout=2400 if (deep and details) else 1500 if deep else 600
        proc=subprocess.Popen(cmd,stdout=subprocess.PIPE,stderr=subprocess.PIPE,text=True,encoding="utf-8",errors="ignore",bufsize=1)
        qev=queue.Queue(); stdout_lines=[]
        def read_stdout():
            try:
                for line in iter(proc.stdout.readline,""): stdout_lines.append(line)
            except Exception: pass
        def read_stderr():
            try:
                for line in iter(proc.stderr.readline,""):
                    line=line.strip()
                    if not line: continue
                    if line.startswith("RT_PROGRESS|"):
                        try:qev.put(("progress",json.loads(line.split("|",1)[1])))
                        except Exception: pass
                    else:qev.put(("error",line))
            except Exception: pass
        th_out=threading.Thread(target=read_stdout,daemon=True); th_err=threading.Thread(target=read_stderr,daemon=True); th_out.start(); th_err.start()
        deadline=time.monotonic()+timeout
        while proc.poll() is None:
            if time.monotonic()>deadline:
                proc.kill(); th_out.join(timeout=1); th_err.join(timeout=1)
                return {"error":"Search timeout after {} seconds".format(timeout),"progress":progress_events}
            try:
                while True:
                    kind,payload=qev.get_nowait()
                    if kind=="progress":
                        progress_events.append(payload)
                        if progress_cb:
                            try: progress_cb(payload)
                            except Exception: pass
                    else: err_lines.append(str(payload))
            except queue.Empty: pass
            time.sleep(.06)
        th_out.join(timeout=2); th_err.join(timeout=2)
        try:
            while True:
                kind,payload=qev.get_nowait()
                if kind=="progress":
                    progress_events.append(payload)
                    if progress_cb:
                        try: progress_cb(payload)
                        except Exception: pass
                else: err_lines.append(str(payload))
        except queue.Empty: pass
        out="".join(stdout_lines).strip()
        if proc.returncode!=0: return {"error":"\n".join(err_lines[-20:])[:2000] or "worker_failed","progress":progress_events}
        payload=json.loads(out) if out else {"error":"empty"}; payload["progress"]=progress_events[-200:]
        if isinstance(payload.get("meta"),dict): payload["meta"].setdefault("app_elapsed_ms",round((time.perf_counter()-started)*1000,1))
        return payload
    except Exception as e:
        return {"error":"{}: {}".format(type(e).__name__,e),"progress":progress_events}


def start_search_job(query, max_val=100, city="", area="", activity="", radius_km=None,
                     deep=False, exact_name=False, max_tiles=36, details=False, discover_max=None, auto_mode=False, refresh_stale_only=False,
                     resume_file=None, resumed_from=None):
    # Phase 25.0: verify the real Windows/browser/network path before a user search.
    _pf_cache=st.session_state.get("search_preflight_cache") if hasattr(st,"session_state") else None
    _pf=None; _now=time.time()
    if isinstance(_pf_cache,dict) and (_now-float(_pf_cache.get("ts") or 0))<300 and isinstance(_pf_cache.get("result"),dict):
        _pf=_pf_cache.get("result")
    if not isinstance(_pf,dict):
        _pf=run_windows_search_preflight(query,st.session_state.get("lang","ar"),45)
        st.session_state["search_preflight_cache"]={"ts":_now,"result":_pf}
    st.session_state["search_preflight_result"]=_pf
    _reason=str(_pf.get("reason") or "")
    if _pf.get("hard_block") or _pf.get("external_block") or (not _pf.get("ready") and _reason in {"playwright_unavailable","browser_launch_error","environment_blocked","network_error","captcha","unusual_traffic","rate_limit"}):
        _rel=(_pf.get("reliability") or {})
        st.session_state["search_job_error"]=("فشل فحص ما قبل البحث: {} · Reliability {} {}" if RTL else "Search preflight failed: {} · Reliability {} {}").format(
            _reason or "not_ready",_rel.get("score","—"),_rel.get("grade","—"))
        st.session_state.pop("active_search_job",None)
        return None
    # Stop an older unfinished job in this session before starting another one.
    old=st.session_state.get("active_search_job")
    try:
        if isinstance(old,dict):
            if old.get("proc") and old["proc"].poll() is None: old["proc"].terminate()
            elif old.get("pid") and _pid_is_running(old.get("pid")): _terminate_pid(old.get("pid"))
            _write_search_manifest(old,"interrupted")
    except Exception: pass
    local_id=datetime.now().strftime("%Y%m%d%H%M%S")+"_"+hashlib.sha1((query+str(time.time())).encode()).hexdigest()[:6]
    out_path=JOB_DIR/("{}.json".format(local_id)); log_path=JOB_DIR/("{}.log".format(local_id))
    checkpoint_path=Path(resume_file) if resume_file else (SEARCH_SESSION_DIR/("{}.checkpoint.json".format(local_id)))
    manifest_path=SEARCH_SESSION_DIR/("{}.manifest.json".format(local_id))
    cmd=[str(pyexe()),str(WORKER),"search",query,"--max",str(int(max_val)),"--lang",st.session_state.get("lang","ar")]
    if city: cmd += ["--city",str(city)]
    if area: cmd += ["--area",str(area)]
    if activity: cmd += ["--activity",str(activity)]
    if radius_km: cmd += ["--radius",str(radius_km)]
    if deep: cmd += ["--deep"]
    if exact_name: cmd += ["--exact-name"]
    if max_tiles: cmd += ["--max-tiles",str(int(max_tiles))]
    if details: cmd += ["--details"]
    if discover_max: cmd += ["--discover-max",str(int(discover_max))]
    if auto_mode: cmd += ["--auto"]
    if refresh_stale_only: cmd += ["--refresh-stale-only"]
    cmd += ["--resume-file",str(checkpoint_path)]
    # Clear stale results so live discoveries from the new query are never mixed with the previous job.
    st.session_state.search_results=[]; st.session_state.search_meta={}; st.session_state.search_live_results=[]
    out_f=open(out_path,"w",encoding="utf-8",errors="ignore"); log_f=open(log_path,"w",encoding="utf-8",errors="ignore")
    child_env=os.environ.copy()
    if CFG.get("GOOGLE_MAPS_API_KEY"): child_env["GOOGLE_MAPS_API_KEY"]=str(CFG.get("GOOGLE_MAPS_API_KEY"))
    if CFG.get("GOOGLE_PLACES_API_KEY"): child_env["GOOGLE_PLACES_API_KEY"]=str(CFG.get("GOOGLE_PLACES_API_KEY"))
    try:
        proc=subprocess.Popen(cmd,stdout=out_f,stderr=log_f,text=True,encoding="utf-8",errors="ignore",cwd=str(APP_DIR),env=child_env)
    finally:
        out_f.close(); log_f.close()
    job={"local_id":local_id,"proc":proc,"pid":getattr(proc,"pid",None),"query":query,"out":str(out_path),"log":str(log_path),
         "checkpoint":str(checkpoint_path),"manifest":str(manifest_path),"resumed_from":resumed_from,"status":"running",
         "started_at":datetime.now().isoformat(),"finalized":False,"params":{"city":city,"area":area,"activity":activity,"radius":radius_km,"deep":deep,"exact":exact_name,"tiles":max_tiles,"details":details,"display":max_val,"discover":discover_max,"auto":bool(auto_mode),"refresh_stale_only":bool(refresh_stale_only)}}
    _write_search_manifest(job,"running")
    st.session_state.active_search_job=job
    return job

def _resume_search_manifest(manifest):
    if not isinstance(manifest,dict) or not manifest.get("query") or not manifest.get("checkpoint"):
        return None
    p=manifest.get("params") or {}
    return start_search_job(manifest.get("query"),int(p.get("display") or 700),city=p.get("city") or "",area=p.get("area") or "",
                            activity=p.get("activity") or "",radius_km=p.get("radius"),deep=bool(p.get("deep",True)),
                            exact_name=bool(p.get("exact",False)),max_tiles=int(p.get("tiles") or 64),details=bool(p.get("details",True)),
                            discover_max=int(p.get("discover") or 2000),auto_mode=bool(p.get("auto",True)),
                            refresh_stale_only=bool(p.get("refresh_stale_only",False)),resume_file=manifest.get("checkpoint"),
                            resumed_from=manifest.get("local_id"))

def _job_progress(job):
    events=[]
    try:
        lp=Path(job.get("log",""))
        if lp.exists():
            for line in lp.read_text(encoding="utf-8",errors="ignore").splitlines()[-2500:]:
                if line.startswith("RT_PROGRESS|"):
                    try: events.append(json.loads(line.split("|",1)[1]))
                    except Exception: pass
    except Exception: pass
    return events


def _finish_search_job(job):
    try:
        out=Path(job.get("out","")).read_text(encoding="utf-8",errors="ignore").strip()
        if not out: return {"error":"empty_worker_output"}
        return json.loads(out)
    except Exception as e: return {"error":"{}: {}".format(type(e).__name__,e)}


def cancel_active_search():
    job=st.session_state.get("active_search_job")
    if not isinstance(job,dict): return False
    stopped=False
    try:
        proc=job.get("proc")
        if proc and proc.poll() is None:
            proc.terminate(); stopped=True
        elif job.get("pid") and _pid_is_running(job.get("pid")):
            stopped=_terminate_pid(job.get("pid"))
    except Exception:
        stopped=False
    if stopped or not _pid_is_running(job.get("pid")):
        job["cancelled"]=True; job["status"]="cancelled"; job["completed_at"]=datetime.now().isoformat()
        _write_search_manifest(job,"cancelled")
        st.session_state.active_search_job=job
        return True
    return False


def render_search_job_panel():
    job=st.session_state.get("active_search_job")
    if not isinstance(job,dict): return
    proc=job.get("proc")
    try:
        running=bool(proc and proc.poll() is None) if proc is not None else _pid_is_running(job.get("pid"))
    except Exception:
        running=_pid_is_running(job.get("pid"))
    events=_job_progress(job); last=events[-1] if events else {}
    event=last.get("event","")

    tile_events=[e for e in events if e.get("event") in ("tile_start","tile_done","tile_error","tile_retry")]
    async_events=[e for e in events if e.get("event")=="async_discovery"]
    detail_events=[e for e in events if e.get("event") in ("details_start","details")]
    early_events=[e for e in events if e.get("event")=="early_detail"]
    resume_events=[e for e in events if e.get("event") in ("resume_loaded","resume_plan")]
    if async_events:
        tiles_done=max([int(e.get("current") or 0) for e in async_events] or [0])
        tile_plan=max([int(e.get("total") or 0) for e in async_events] or [1])
    else:
        tiles_done=max([int(e.get("current") or 0) for e in tile_events if e.get("event") in ("tile_done","tile_error")] or [0])
        tile_plan=max([int(e.get("planned") or 0) for e in tile_events] or [int(job.get("params",{}).get("tiles") or 1)])
    discovered=max([int(e.get("found") or 0) for e in events] or [0])
    detail_done=max([int(e.get("current") or 0) for e in detail_events if e.get("event")=="details"] or [0])
    detail_total=max([int(e.get("total") or 0) for e in detail_events] or [0])
    early_reviews=max([int(e.get("reviews") or 0) for e in early_events] or [0])
    errors=sum(1 for e in events if e.get("event")=="tile_error")
    retries=sum(1 for e in events if e.get("event")=="tile_retry")
    try:
        elapsed=max(0,int((datetime.now()-datetime.fromisoformat(job.get("started_at"))).total_seconds()))
    except Exception:
        elapsed=0

    # RC watchdog: terminate a process that has stopped emitting progress for a long time.
    idle_seconds=0
    try:
        lp=Path(job.get("log","")); idle_seconds=max(0,int(time.time()-lp.stat().st_mtime)) if lp.exists() else elapsed
    except Exception:
        idle_seconds=elapsed
    if running and elapsed>90 and idle_seconds>180:
        try:
            if proc is not None and proc.poll() is None: proc.terminate()
            else: _terminate_pid(job.get("pid"))
        except Exception: pass
        job["cancelled"]=True; job["watchdog"]=True; job["status"]="interrupted"; job["completed_at"]=datetime.now().isoformat()
        _write_search_manifest(job,"interrupted")
        st.session_state.search_job_error=("توقف محرك البحث عن إرسال تقدم لأكثر من 3 دقائق وتم إنهاؤه بأمان. يمكن استئناف الجلسة من آخر Checkpoint." if RTL else "Search worker stopped reporting progress for over 3 minutes and was terminated safely. The session can resume from its latest checkpoint.")
        st.session_state.active_search_job=job
        running=False

    provider_events=[e for e in events if e.get("event")=="provider"]
    live_events=[e for e in events if e.get("event")=="result" and isinstance(e.get("record"),dict)]
    live_rows=[]; live_aliases=[]
    def _live_alias_set(rec):
        aliases=set()
        for k in ["place_id","cid"]:
            if rec.get(k): aliases.add(k+":"+str(rec.get(k)).strip().lower())
        u=str(rec.get("maps_url") or rec.get("url") or "").strip().lower()
        if u:
            u=u.split("&")[0].rstrip("/"); aliases.add("url:"+u)
        n=re.sub(r"\s+"," ",str(rec.get("name") or "").strip().lower())
        a=re.sub(r"\s+"," ",str(rec.get("address") or "").strip().lower())
        if n and a: aliases.add("na:"+n+"|"+a)
        return aliases
    for e in live_events:
        rec=e.get("record") or {}; aliases=_live_alias_set(rec); hit=None
        for i,old_aliases in enumerate(live_aliases):
            if aliases and old_aliases and aliases.intersection(old_aliases): hit=i; break
        if hit is None:
            live_rows.append(dict(rec)); live_aliases.append(set(aliases))
        else:
            merged=dict(live_rows[hit]); merged.update({k:v for k,v in rec.items() if v not in (None,"")}); live_rows[hit]=merged; live_aliases[hit].update(aliases)
    live_results=live_rows
    if live_results:
        st.session_state.search_live_results=enrich_google_quick_analysis(live_results)
    providers=[]
    for pe in provider_events:
        name=pe.get("provider")
        if name and name not in providers: providers.append(name)

    if event in ("details_start","details"):
        phase="جلب التفاصيل تلقائيًا" if RTL else "Loading details automatically"
        pct=min(98,75+int((detail_done/max(1,detail_total))*23))
        progress_text=(("تفاصيل الفروع: {} / {}" if RTL else "Branch details: {} / {}").format(detail_done,detail_total))
    elif event=="done":
        phase="اكتمل البحث" if RTL else "Search completed"
        pct=100; progress_text=phase
    else:
        phase="بحث Google + Google Maps التلقائي" if RTL else "Automatic Google + Google Maps discovery"
        pct=min(75,max(5,int((tiles_done/max(1,tile_plan))*75)))
        progress_text=(("المناطق: {} / {} · الفروع المكتشفة: {}" if RTL else "Coverage tiles: {} / {} · discovered branches: {}").format(tiles_done,tile_plan,discovered))

    st.markdown('<div class="rt-job-phase">⏳ {}</div>'.format(html.escape(phase)),unsafe_allow_html=True)
    st.progress(pct,text=progress_text)
    r1,r2,r3,r4,r5=st.columns(5)
    r1.metric("المناطق" if RTL else "Tiles","{} / {}".format(tiles_done,tile_plan))
    r2.metric("المكتشفة" if RTL else "Discovered",discovered)
    r3.metric("التفاصيل" if RTL else "Details","{} / {}".format(detail_done,detail_total) if detail_total else "—")
    r4.metric("الوقت" if RTL else "Elapsed","{}s".format(elapsed))
    r5.metric("الأخطاء" if RTL else "Errors",errors)
    if retries:
        st.caption(("إعادات المحاولة التلقائية: {}" if RTL else "Automatic tile retries: {}").format(retries))
    if resume_events:
        _rr=max([int(e.get("restored_results") or e.get("found") or 0) for e in resume_events] or [0])
        _sj=max([int(e.get("skipped_completed_jobs") or e.get("completed_jobs") or 0) for e in resume_events] or [0])
        st.caption(("↩️ جلسة مستأنفة: استعيد {} نتيجة وتم تجاوز {} مهمة مكتملة." if RTL else "↩️ Resumed session: restored {} results and skipped {} completed jobs.").format(_rr,_sj))
    if early_reviews:
        st.caption(("⚡ جُلبت {} عينة مراجعة مبكرًا من أقوى الفروع بينما يستمر البحث." if RTL else "⚡ {} review samples were fetched early from the strongest branches while discovery continues.").format(early_reviews))
    if providers:
        pretty=[]
        for pvd in providers:
            pretty.append("Google Search Live" if pvd=="google_search_browser" else ("Google Places API" if pvd=="google_places_api" else ("Google Maps Live" if pvd=="google_maps_browser" else str(pvd))))
        st.caption(("مصادر البحث الحية: " if RTL else "Live providers: ")+" + ".join(pretty))
    if running and live_results:
        st.markdown("#### ⚡ "+("نتائج Google الحية + تحليل سريع" if RTL else "Live Google results + instant analysis"))
        enriched_live=enrich_google_quick_analysis(live_results)
        live_df=pd.DataFrame(enriched_live)
        cols=[c for c in ["name","result_score","data_completeness","provider_count","rating","review_count","category","quick_sentiment","quick_topic","quick_negative_pct","address"] if c in live_df.columns]
        st.dataframe(live_df[cols].head(20),use_container_width=True,hide_index=True,height=min(430,105+34*min(20,len(live_df))))
        _, qsum=quick_google_review_analysis(enriched_live)
        if qsum.get("samples"):
            qa1,qa2,qa3,qa4=st.columns(4)
            qa1.metric("عينات Google" if RTL else "Google samples",qsum.get("samples",0))
            qa2.metric("إيجابي" if RTL else "Positive","{:.1f}%".format(qsum.get("positive_pct",0)))
            qa3.metric("سلبي" if RTL else "Negative","{:.1f}%".format(qsum.get("negative_pct",0)))
            qa4.metric("أبرز موضوع" if RTL else "Top topic",qsum.get("top_topic","—"))
        st.caption(("تم اكتشاف {} نتيجة حتى الآن؛ تظهر نتائج Google وتحليل عينات المراجعات فورًا بينما يستمر البحث الأوسع عند الحاجة." if RTL else "{} results discovered so far; Google results and review-sample analysis appear immediately while wider discovery continues when needed.").format(len(live_df)))
    j1,j2,j3=st.columns([2,1,1])
    j1.caption(("بدأ: {} · البحث: {}" if RTL else "Started: {} · query: {}").format(str(job.get("started_at",""))[:19],job.get("query","")))
    if running:
        j2.info("يعمل الآن" if RTL else "Running")
        if j3.button("⏹️ "+("إيقاف البحث" if RTL else "Stop search"),use_container_width=True,key="stop_search_job"):
            cancel_active_search(); st.rerun()
        return
    if job.get("cancelled"):
        if job.get("watchdog"):
            st.error("أوقف Watchdog مهمة بحث معلقة. النتائج الجزئية محفوظة في Checkpoint ويمكن استئنافها." if RTL else "The watchdog stopped a stalled search job. Partial results are checkpointed and can be resumed.")
        else:
            st.warning("تم إيقاف البحث. النتائج الجزئية محفوظة ويمكن استكمالها من آخر نقطة." if RTL else "Search was stopped. Partial results are checkpointed and can resume from the last point.")
        c1,c2=st.columns(2)
        if c1.button("↩️ "+("استئناف البحث" if RTL else "Resume search"),type="primary",use_container_width=True,key="resume_cancelled_job"):
            manifest=_load_search_manifest(job.get("manifest")) or job
            st.session_state.pop("active_search_job",None)
            _resume_search_manifest(manifest); st.rerun()
        if c2.button("مسح حالة المهمة" if RTL else "Clear job",use_container_width=True,key="clear_cancelled_job"):
            st.session_state.pop("active_search_job",None); st.rerun()
        return
    if not job.get("finalized"):
        res=_finish_search_job(job)
        job["finalized"]=True; job["completed_at"]=datetime.now().isoformat(); st.session_state.active_search_job=job
        if res.get("error"):
            st.session_state.search_job_error=res.get("error")
            job["status"]="error"; _write_search_manifest(job,"error")
            st.session_state.pop("active_search_job",None)
        else:
            results=enrich_google_quick_analysis(res.get("results") or []); meta=res.get("meta") or {}
            st.session_state.search_results=results
            st.session_state.search_live_results=results
            st.session_state.search_meta=meta
            st.session_state.result_selected_indices=[]
            st.session_state.pop("results_editor_v217",None)
            st.session_state.pop("map_selected_indices",None)
            for _k in [k for k in list(st.session_state.keys()) if str(k).startswith("card_select_")]: st.session_state.pop(_k,None)
            st.session_state.search_query_used=job.get("query","")
            st.session_state.search_params=job.get("params",{})
            st.session_state.last_search_job={"query":job.get("query",""),"completed_at":job.get("completed_at"),"meta":meta}
            db.add_history(job.get("query",""),len(results),st.session_state.workspace_id)
            db.log_search_job(meta.get("job_id") or job.get("local_id"),job.get("query",""),job.get("params",{}),meta,st.session_state.workspace_id)
            _term=str(meta.get("termination_reason") or "")
            _mstatus="partial" if _term in {"deadline_reached","google_blocked","network_error","environment_blocked"} else "completed"
            job["status"]=_mstatus; job["termination_reason"]=_term; _write_search_manifest(job,_mstatus)
            st.session_state.pop("active_search_job",None)
        st.rerun()

def reset_search_workflow(clear_saved_defaults=False):
    cancel_active_search()
    for key in [
        "active_search_job","last_search_job","search_results","search_live_results","search_meta","search_query_used","search_params",
        "search_job_error","discovery_form_q","search_preset","search_city_v215","search_activity_v215",
        "adv_area","adv_radius","adv_display","adv_discover","adv_tiles","adv_details","adv_exact",
        "result_filter_v21","rf_rating","rf_sort","rf_phone","rf_email","rf_website","rf_distance",
        "rf_confidence","rf_reviews","result_view_mode","detail_pick_v21","result_selected_indices",
        "map_selected_indices","map_color_mode_v218","map_focus_v218","card_select_all_visible","results_editor_v217","v34_sort"
    ]:
        st.session_state.pop(key,None)
    if clear_saved_defaults:
        db.set_pref("search_defaults_{}".format(st.session_state.workspace_id),{})
    return True


def is_maps_url(s):
    s = (s or "").lower()
    return "google.com/maps" in s or "maps.app.goo.gl" in s or "goo.gl/maps" in s


_ARABIC_RE = re.compile(r"[\u0600-\u06FF]")
_URDU_HINT_RE = re.compile(r"[ٹڈڑںھہےکگپچژ]")
_AR_DIACRITICS_RE = re.compile(r"[\u064B-\u065F\u0670\u06D6-\u06ED]")


def normalize_review_text(text):
    s = str(text or "").strip().lower().replace("ـ", "")
    s = _AR_DIACRITICS_RE.sub("", s)
    s = re.sub(r"https?://\S+|www\.\S+", " ", s)
    s = re.sub(r"[^\w\u0600-\u06FF]+", " ", s, flags=re.UNICODE)
    return re.sub(r"\s+", " ", s).strip()


def detect_review_language(text):
    raw = str(text or "").strip()
    if not raw: return "unknown", 0.0, "heuristic"
    letters = [c for c in raw if c.isalpha()]
    ar_count = len(_ARABIC_RE.findall(raw))
    ratio = ar_count / max(1, len(letters))
    if ratio >= .45:
        return ("ur" if _URDU_HINT_RE.search(raw) else "ar"), min(.99, .78 + ratio*.2), "script"
    if LANGDETECT_AVAILABLE and len(raw) >= 8:
        try:
            preds = detect_langs(raw[:1200])
            if preds:
                code = str(preds[0].lang).lower(); prob = float(preds[0].prob)
                if code.startswith("ar"): code="ar"
                elif code.startswith("en"): code="en"
                elif code.startswith("ur"): code="ur"
                elif code.startswith("fa"): code="fa"
                return code, round(prob,3), "langdetect"
        except Exception:
            pass
    latin = sum(1 for c in raw if ("a" <= c.lower() <= "z"))
    if latin >= max(3, ar_count*2): return "en", .72, "script"
    return "other", .45, "heuristic"


_SENTIMENT_LEXICONS = {
    "positive": [
        "ممتاز","رائع","جميل","سريع","نظيف","محترم","لذيذ","شكرا","شكراً","احترافي","مميز","مريح","انصح","أنصح","افضل","أفضل",
        "great","excellent","amazing","good","friendly","clean","fast","professional","helpful","perfect","love","recommend","delicious"
    ],
    "negative": [
        "سيء","سيئ","قذر","غالي","بطيء","وقح","زفت","أسوأ","كذب","تأخير","ازدحام","مشكلة","خربان","رديء","نصب","احتيال","شكوى","تجاهل",
        "bad","terrible","awful","rude","dirty","slow","expensive","worst","problem","broken","scam","fraud","complaint","delay","ignored"
    ],
}
_NEGATORS = ["ليس","مو","مش","ما","لم","لن","بدون","غير","not","no","never","isnt","isn't","wasnt","wasn't"]
_INTENSIFIERS = ["جدا","جداً","مره","مرة","للغاية","كثير","very","extremely","really","too"]
_URGENT_TERMS = ["خطر","تسمم","اصابة","إصابة","حريق","تهديد","احتيال","نصب","سرقة","بلاغ","وزارة","police","danger","poison","injury","fire","threat","fraud","scam","stolen","unsafe"]

_TOPIC_LEXICON = {
    "service": ["خدمة","تعامل","استقبال","خدمة العملاء","service","customer service"],
    "staff": ["موظف","موظفين","عامل","مدير","كاشير","طاقم","staff","employee","manager","cashier","team"],
    "wait_time": ["سريع","بطيء","تأخير","انتظار","طابور","موعد","wait","slow","delay","queue","fast","time"],
    "cleanliness": ["نظيف","نظافة","قذر","وسخ","حمام","clean","dirty","hygiene","toilet"],
    "price": ["غالي","سعر","اسعار","أسعار","مبالغ","رخيص","price","expensive","cheap","cost","value"],
    "quality": ["جودة","طعم","لذيذ","رديء","جيد","quality","taste","delicious","poor quality"],
    "delivery": ["توصيل","مندوب","شحن","delivery","driver","shipping","courier"],
    "parking": ["موقف","مواقف","باركنج","parking","park"],
    "product": ["منتج","طلب","صنف","وجبة","قطعة","product","order","item","meal"],
    "location": ["موقع","فرع","بعيد","قريب","دخول","location","branch","far","near","access"],
    "availability": ["غير متوفر","نفذ","مخلص","متوفر","stock","available","unavailable","sold out"],
}


def _keyword_score(text, words):
    score=0
    for w in words:
        if w and w in text:
            score += 2 if " " in w else 1
    return score


def _sentiment_rule(text, rating):
    tokens=text.split(); pos=_keyword_score(text,_SENTIMENT_LEXICONS["positive"]); neg=_keyword_score(text,_SENTIMENT_LEXICONS["negative"])
    # Basic local negation: flip lexicon evidence when a negator appears immediately before it.
    for i,tok in enumerate(tokens[:-1]):
        if tok in _NEGATORS:
            nxt=tokens[i+1]
            if any(nxt in x or x in nxt for x in _SENTIMENT_LEXICONS["positive"]): pos=max(0,pos-1); neg+=1
            if any(nxt in x or x in nxt for x in _SENTIMENT_LEXICONS["negative"]): neg=max(0,neg-1); pos+=1
    intensity=1 + .12*sum(1 for k in _INTENSIFIERS if k in text)
    lexical=(pos-neg)*intensity
    rating_component=2.2 if rating>=4 else -2.2 if rating<=2 else 0
    combined=rating_component+lexical
    if combined>0.45: sent="positive"
    elif combined<-0.45: sent="negative"
    else: sent="neutral"
    evidence=pos+neg+(1 if rating in (1,2,4,5) else 0)
    rating_agrees=(rating>=4 and sent=="positive") or (rating<=2 and sent=="negative") or (rating==3 and sent=="neutral")
    conf=.48 + min(.28,evidence*.055) + (.10 if rating_agrees else 0) + (.05 if len(text)>=35 else 0)
    if evidence==0 and rating==3: conf=.54
    return sent, max(.40,min(.97,conf)), pos, neg


def analyze_simple(df):
    """Analysis Pipeline 3.0: language → sentiment → topics/aspects → urgency → confidence."""
    if df is None or df.empty: return df
    rows=[]
    for r in df.to_dict("records"):
        raw=str(r.get("text") or ""); txt=normalize_review_text(raw); rating=max(0,min(5,safe_num(r.get("rating"),3)))
        language,lang_conf,lang_method=detect_review_language(raw)
        sentiment,sent_conf,pos,neg=_sentiment_rule(txt,rating)
        topic_hits=[]
        aspects={}
        for topic,kws in _TOPIC_LEXICON.items():
            n=_keyword_score(txt,kws)
            if n:
                aspects[topic]=n; topic_hits.append((n,topic))
        topic_hits.sort(key=lambda x:(x[0],x[1]),reverse=True)
        topic=topic_hits[0][1] if topic_hits else "other"
        aspect_primary=topic
        urgent_hits=sum(1 for k in _URGENT_TERMS if k in txt)
        urgency=(5-rating)*13 + (22 if sentiment=="negative" else 0) + min(28,urgent_hits*14)
        if sentiment=="negative" and topic in ("service","staff","quality","delivery"): urgency += 8
        if len(raw)>=220 and sentiment=="negative": urgency += 4
        urgency=max(0,min(100,urgency))
        priority="critical" if urgency>=80 else "high" if urgency>=60 else "normal" if urgency>=30 else "low"
        topic_conf=.40 if topic=="other" else min(.95,.58 + .08*topic_hits[0][0] + (.05 if len(topic_hits)>1 else 0))
        analysis_conf=min(.98,max(.42,.62*sent_conf + .18*lang_conf + .20*topic_conf))
        method="rule-based-v3+{}".format(lang_method)
        r.update({
            "language":language,"sentiment":sentiment,"sentiment_score":round(sent_conf,3),
            "topic":topic,"aspect_primary":aspect_primary,"aspects":aspects,"urgency":round(urgency,1),
            "fake_score":safe_num(r.get("fake_score"),0),"analysis_method":method,
            "analysis_confidence":round(analysis_conf,3),"priority":priority,
            "language_confidence":round(lang_conf,3),"sentiment_evidence":{"positive":pos,"negative":neg}
        })
        rows.append(r)
    return pd.DataFrame(rows)


def quick_google_review_analysis(records):
    """Analyze review samples returned directly by Google Places without persisting them."""
    review_rows=[]
    for rec in records or []:
        branch_name=str(rec.get("name") or "")
        place_id=rec.get("place_id")
        for rv in rec.get("preview_reviews") or []:
            row=dict(rv)
            row["company_name"]=branch_name
            row["place_id"]=place_id
            review_rows.append(row)
    if not review_rows:
        return pd.DataFrame(), {"samples":0,"positive_pct":0.0,"negative_pct":0.0,"top_topic":"—","urgent":0}
    analyzed=analyze_simple(pd.DataFrame(review_rows))
    if analyzed is None or analyzed.empty:
        return pd.DataFrame(), {"samples":0,"positive_pct":0.0,"negative_pct":0.0,"top_topic":"—","urgent":0}
    total=len(analyzed)
    pos=float((analyzed.get("sentiment")=="positive").mean()*100) if "sentiment" in analyzed else 0.0
    neg=float((analyzed.get("sentiment")=="negative").mean()*100) if "sentiment" in analyzed else 0.0
    topics=analyzed.get("topic",pd.Series(dtype=str)).fillna("other").value_counts()
    top_topic=str(topics.index[0]) if not topics.empty else "—"
    urgent=int((pd.to_numeric(analyzed.get("urgency",pd.Series(dtype=float)),errors="coerce").fillna(0)>=60).sum())
    return analyzed, {"samples":total,"positive_pct":round(pos,1),"negative_pct":round(neg,1),"top_topic":top_topic,"urgent":urgent}


def enrich_google_quick_analysis(records):
    """Attach compact Google sample-review analytics to each branch record."""
    out=[]
    for rec in records or []:
        item=dict(rec)
        analyzed, summary=quick_google_review_analysis([item])
        item["quick_review_samples"]=summary.get("samples",0)
        item["quick_positive_pct"]=summary.get("positive_pct",0.0)
        item["quick_negative_pct"]=summary.get("negative_pct",0.0)
        item["quick_topic"]=summary.get("top_topic","—")
        item["quick_urgent"]=summary.get("urgent",0)
        if not analyzed.empty and "sentiment" in analyzed:
            vc=analyzed["sentiment"].fillna("neutral").value_counts()
            item["quick_sentiment"]=str(vc.index[0]) if not vc.empty else "—"
        else:
            item["quick_sentiment"]="—"
        out.append(item)
    return out


def scrape_and_store(db, query, max_reviews=100, is_url=False, ws_id=1, username="",
                     is_competitor=0, competitor_of=None, target_pk=None, sync_run_id=None, queue_id=None, progress_cb=None, known_reviews_file=None):
    mode = "scrape-url" if is_url else "scrape"
    before = db.get_company(target_pk,ws_id) if target_pk else None
    if target_pk and not sync_run_id:
        sync_run_id=db.create_sync_run(target_pk,ws_id,queue_id=queue_id,before_snapshot=before or {})
    res = call_worker(mode, query, max_reviews, progress_cb=progress_cb, known_reviews_file=known_reviews_file)
    if res.get("error"):
        if target_pk:
            db.update_branch_sync(target_pk,ws_id,status="error",error=res.get("error"))
            db.record_monitor_result(target_pk,ws_id,False,res.get("error"))
            if sync_run_id: db.finish_sync_run(sync_run_id,"error",error=res.get("error"))
        return None, None, 0, 0, res.get("error")
    info = res.get("info") or {}
    # A monitored branch is synchronized against its saved identity. If Google omits an
    # identifier in a later response, keep the previous identity so the branch cannot split.
    if before:
        for src,dst in [("place_id","place_id"),("cid","cid"),("maps_url","url"),("name","name"),("address","address")]:
            if not info.get(dst) and before.get(src): info[dst]=before.get(src)
    raw = res.get("reviews") or []
    df = analyze_simple(pd.DataFrame(raw))
    pk = db.save_company(info, query, ws_id, is_competitor=is_competitor, competitor_of=competitor_of)
    if not pk:
        if target_pk: db.update_branch_sync(target_pk,ws_id,status="error",error="save_company failed")
        if sync_run_id: db.finish_sync_run(sync_run_id,"error",error="save_company failed")
        return None, info, 0, 0, "save_company failed"
    new, dup = db.save_reviews(pk, df, ws_id, sync_run_id=sync_run_id)
    stored = db.get_reviews(pk, ws_id)
    neg = int((stored.get("sentiment") == "negative").sum()) if not stored.empty else 0
    pos = int((stored.get("sentiment") == "positive").sum()) if not stored.empty else 0
    rating = info.get("rating") or (before or {}).get("rating") or 0
    db.save_snapshot(pk, rating, int(safe_num(info.get("review_count"),len(stored))) or len(stored), neg, pos, ws_id)
    db.add_history(query, new, ws_id)
    if target_pk or before:
        old=before or {}; after=db.get_company(pk,ws_id) or info; changed=[]
        for field in ["rating","review_count","phone","mobile","website","address"]:
            ov=old.get(field); nv=after.get(field)
            if ov not in (None,"") and nv not in (None,"") and str(ov)!=str(nv): changed.append({"field":field,"from":ov,"to":nv})
        new_negative=0
        if sync_run_id:
            try:
                with db._c() as _c:
                    _r=_c.execute("SELECT COUNT(*) FROM reviews WHERE workspace_id=? AND pk=? AND sync_run_id=? AND sentiment='negative'",(ws_id,str(pk),int(sync_run_id))).fetchone()
                    new_negative=int(_r[0] if _r else 0)
            except Exception: new_negative=0
        db.update_branch_sync(pk,ws_id,status="ok",new_reviews=new,duplicate_reviews=dup,changed_fields=changed,
                              previous_rating=old.get("rating"),current_rating=after.get("rating"),
                              previous_review_count=old.get("review_count"),current_review_count=after.get("review_count"),
                              new_negative_reviews=new_negative)
        db.record_monitor_result(pk,ws_id,True)
        if sync_run_id: db.finish_sync_run(sync_run_id,"ok",new_reviews=new,duplicate_reviews=dup,changed_fields=changed,after_snapshot=after,new_negative_reviews=new_negative)
        db.record_change_intelligence(pk,ws_id,sync_run_id,before=old,after=after,new_negative_reviews=new_negative)
        if changed:
            detail="; ".join("{}: {} → {}".format(x["field"],x["from"],x["to"]) for x in changed)[:1000]
            fp=hashlib.sha1((str(pk)+detail+date.today().isoformat()).encode("utf-8","ignore")).hexdigest()
            severity="warning" if any(x["field"]=="rating" and safe_num(x["to"])<safe_num(x["from"]) for x in changed) else "info"
            db.save_alert(fp,pk,None,"branch_change",severity,"تغير بيانات الفرع" if RTL else "Branch data changed",detail,ws_id)
    if username: db.log_activity(username, ws_id, "scrape",
                                  "{}: {}".format("competitor" if is_competitor else "branch", query[:60]))
    invalidate_cache()
    return pk, info, new, dup, None


def deep_sync_reviews_for_branch(db, pk, branch, ws_id=1, username="", batch_size=250, progress_cb=None):
    pk=str(pk or ""); branch=dict(branch or {})
    if not pk: return {"ok":False,"error":"missing branch id","new":0,"duplicates":0}
    stored=db.get_reviews(pk,ws_id); keys=[]
    if isinstance(stored,pd.DataFrame) and not stored.empty:
        for row in stored.to_dict("records"): keys.append(review_identity_key(row))
    known_path=JOB_DIR/("deep_reviews_known_{}_{}.json".format(hashlib.sha1(pk.encode("utf-8")).hexdigest()[:10],int(time.time())))
    known_path.write_text(json.dumps({"known_review_keys":sorted(set(keys))},ensure_ascii=False),encoding="utf-8")
    target=branch.get("maps_url") or branch.get("url") or branch.get("name") or pk
    try:
        out_pk,info,new,dup,err=scrape_and_store(db,target,int(batch_size),bool(branch.get("maps_url") or branch.get("url")),ws_id,username,target_pk=pk,progress_cb=progress_cb,known_reviews_file=str(known_path))
        now_count=len(db.get_reviews(out_pk or pk,ws_id))
        db.update_deep_review_sync_state(out_pk or pk,ws_id,status="error" if err else "ok",new_reviews=new,duplicates=dup,known_count=now_count,target_total=int(safe_num((info or {}).get("review_count") or branch.get("review_count"),0)),batch_limit=int(batch_size),error=err)
        return {"ok":not bool(err),"pk":out_pk or pk,"new":int(new or 0),"duplicates":int(dup or 0),"known_before":len(keys),"known_after":now_count,"error":err}
    finally:
        try: known_path.unlink(missing_ok=True)
        except Exception: pass

def current_health_weights():
    # Health 3.0 keeps the familiar 35/25/20/10/10 model, but normalizes only
    # across components that actually have data. Missing data no longer scores 0.
    default={"rating":35,"sentiment":25,"response":20,"trend":10,"critical":10}
    try:
        wid=int(st.session_state.get("workspace_id",1))
        saved=db.get_pref("health_weights_{}".format(wid),default)
        if isinstance(saved,dict):
            out={k:max(0,float(saved.get(k,default[k]))) for k in default}
            return out if sum(out.values())>0 else default
    except Exception:
        pass
    return default


def health_grade_v3(score):
    s=safe_num(score,0)
    if s>=85: return "ممتاز" if RTL else "Excellent"
    if s>=70: return "جيد" if RTL else "Good"
    if s>=55: return "متوسط" if RTL else "Fair"
    if s>=40: return "يحتاج انتباه" if RTL else "Needs attention"
    return "حرج" if RTL else "Critical"


def health_score_v3(df, info=None, trend_delta=None, weights=None):
    """Explainable Health Score 3.0.

    Only available components participate in the weighted score. Google rating may
    be used even when no stored reviews exist; in that case data quality is low and
    is reported separately instead of forcing the Health score to zero.
    """
    info=info or {}
    weights=weights or {"rating":35,"sentiment":25,"response":20,"trend":10,"critical":10}
    frame=df if isinstance(df,pd.DataFrame) else pd.DataFrame()
    total=len(frame)
    comp={}; available={}; raw={}

    ratings=pd.to_numeric(frame.get("rating"),errors="coerce").dropna() if (not frame.empty and "rating" in frame) else pd.Series(dtype=float)
    branch_rating=pd.to_numeric(pd.Series([info.get("rating")]),errors="coerce").dropna()
    avg=float(branch_rating.iloc[0]) if not branch_rating.empty else (float(ratings.mean()) if not ratings.empty else None)
    rating_source="branch_aggregate" if not branch_rating.empty else "sample_reviews" if not ratings.empty else "unavailable"
    if avg is not None:
        comp["rating"]=max(0,min(100,(avg-1.0)/4.0*100.0))
        available["rating"]=True; raw["avg_rating"]=avg
        raw["rating_source"]=rating_source
        if not ratings.empty: raw["sample_rating_avg"]=round(float(ratings.mean()),3)

    if not frame.empty and "sentiment" in frame:
        sent=frame["sentiment"].fillna("").astype(str).str.lower()
        valid=sent.isin(["positive","negative","neutral"])
        if valid.any():
            ss=sent[valid]; neg=float((ss=="negative").mean()*100); pos=float((ss=="positive").mean()*100)
            comp["sentiment"]=max(0,min(100,100-neg*1.10+pos*0.05))
            available["sentiment"]=True; raw.update({"negative_pct":neg,"positive_pct":pos,"sentiment_samples":int(valid.sum())})

    if not frame.empty and "responded" in frame:
        rr=pd.to_numeric(frame["responded"],errors="coerce")
        valid=rr.notna()
        if valid.any():
            response=float((rr[valid].fillna(0)==1).mean()*100)
            comp["response"]=max(0,min(100,response)); available["response"]=True; raw["response_pct"]=response

    if trend_delta is not None:
        try:
            td=float(trend_delta)
            if math.isfinite(td):
                comp["trend"]=max(0,min(100,50+td*40)); available["trend"]=True; raw["trend_delta"]=td
        except Exception:
            pass

    if not frame.empty and ("urgency" in frame or "priority" in frame):
        urgency=pd.to_numeric(frame.get("urgency",pd.Series([0]*len(frame))),errors="coerce").fillna(0)
        priority=frame.get("priority",pd.Series([""]*len(frame))).fillna("").astype(str).str.lower()
        critical_mask=(urgency>=80)|(priority=="critical")
        high_mask=((urgency>=60)|(priority=="high")) & ~critical_mask
        critical_pct=float(critical_mask.mean()*100); high_pct=float(high_mask.mean()*100)
        comp["critical"]=max(0,min(100,100-critical_pct*1.5-high_pct*.6))
        available["critical"]=True; raw.update({"critical_pct":critical_pct,"high_priority_pct":high_pct})

    active=[k for k in weights if k in comp and weights.get(k,0)>0]
    if not active:
        return 0.0,{"reason":"لا توجد بيانات كافية" if RTL else "No usable data","components":{},"weights":weights,
                   "effective_weights":{},"available":{},"data_quality":0.0,"grade":health_grade_v3(0),"raw":raw}
    denom=sum(float(weights.get(k,0)) for k in active) or 1.0
    effective={k:round(float(weights.get(k,0))/denom*100,1) for k in active}
    score=sum(comp[k]*float(weights.get(k,0)) for k in active)/denom

    # Data quality is informational; it does not silently change the Health score.
    volume_quality=min(1.0, math.log1p(max(total,0))/math.log1p(30)) if total>0 else 0.0
    coverage_quality=len(active)/5.0
    data_quality=round(max(0,min(100,(volume_quality*.55+coverage_quality*.45)*100)),1)

    reasons=[]
    if "rating" in comp and comp["rating"]<62: reasons.append(("التقييم منخفض {:.2f}" if RTL else "low rating {:.2f}").format(avg))
    if "sentiment" in comp and raw.get("negative_pct",0)>=25: reasons.append(("السلبية مرتفعة {:.0f}%" if RTL else "high negative share {:.0f}%").format(raw.get("negative_pct",0)))
    if "response" in comp and comp["response"]<50: reasons.append(("الردود منخفضة {:.0f}%" if RTL else "low response {:.0f}%").format(raw.get("response_pct",0)))
    if "trend" in comp and comp["trend"]<45: reasons.append(("اتجاه هابط {:+.2f}" if RTL else "declining trend {:+.2f}").format(raw.get("trend_delta",0)))
    if "critical" in comp and comp["critical"]<70: reasons.append(("مراجعات حرجة/عالية كثيرة" if RTL else "many critical/high-priority reviews"))
    if data_quality<40: reasons.append(("ثقة البيانات محدودة" if RTL else "limited data confidence"))
    reason="، ".join(reasons[:4]) if reasons else ("المؤشرات المتاحة مستقرة" if RTL else "available indicators are stable")
    score=round(max(0,min(100,score)),1)
    return score,{"reason":reason,"components":{k:round(v,1) for k,v in comp.items()},"weights":weights,
                  "effective_weights":effective,"available":available,"data_quality":data_quality,
                  "grade":health_grade_v3(score),"raw":raw}


def health_score_v2(df, trend_delta=0.0, weights=None):
    # Backward-compatible alias for reports/extensions that still call v2.
    return health_score_v3(df,{},trend_delta,weights)


def kpis(df, info=None, trend_delta=None):
    if info is None: info={}
    frame=df if isinstance(df,pd.DataFrame) else pd.DataFrame()
    total=len(frame)
    pos=int((frame.get("sentiment",pd.Series(dtype=str))=="positive").sum()) if total else 0
    neg=int((frame.get("sentiment",pd.Series(dtype=str))=="negative").sum()) if total else 0
    ratings=pd.to_numeric(frame.get("rating"),errors="coerce").dropna() if total and "rating" in frame else pd.Series(dtype=float)
    branch_rating=pd.to_numeric(pd.Series([info.get("rating")]),errors="coerce").dropna()
    avg=float(branch_rating.iloc[0]) if not branch_rating.empty else (float(ratings.mean()) if not ratings.empty else None)
    resp=int((pd.to_numeric(frame.get("responded",pd.Series(dtype=float)),errors="coerce").fillna(0)==1).sum()) if total else 0
    pr=pos/total*100 if total else 0; nr=neg/total*100 if total else 0
    health,detail=health_score_v3(frame,info,trend_delta,current_health_weights())
    return {"total":total,"pos":pos,"neg":neg,"pos_rate":pr,"neg_rate":nr,"avg":avg,
            "health":health,"health_grade":detail.get("grade"),"health_data_quality":detail.get("data_quality",0),
            "response_rate":resp/total*100 if total else 0,"health_reason":detail.get("reason",""),
            "health_components":detail.get("components",{}),"health_effective_weights":detail.get("effective_weights",{}),
            "health_available":detail.get("available",{}),"health_raw":detail.get("raw",{})}


def previous_period_reviews(db, ws_id):
    since, now = period_bounds()
    if since is None: return pd.DataFrame()
    span=max(timedelta(days=1), now-since)
    return db.all_reviews(ws_id, since-span, since)


def executive_insights(current, previous=None):
    if current is None or current.empty or len(current)<3:
        return []
    cur=kpis(current); prev=kpis(previous) if isinstance(previous,pd.DataFrame) and not previous.empty else None
    insights=[]
    if prev:
        dr=safe_num(cur.get("avg"))-safe_num(prev.get("avg")); dn=cur.get("neg_rate",0)-prev.get("neg_rate",0)
        if abs(dr)>=.1: insights.append({"severity":"good" if dr>0 else "warning","title":("تحسن التقييم" if dr>0 else "تراجع التقييم") if RTL else ("Rating improved" if dr>0 else "Rating declined"),"detail":"{:+.2f}".format(dr)})
        if abs(dn)>=5: insights.append({"severity":"warning" if dn>0 else "good","title":("تغير المراجعات السلبية" if RTL else "Negative review shift"),"detail":"{:+.1f}%".format(dn)})
    if "topic" in current:
        neg=current[current.get("sentiment")=="negative"]
        if not neg.empty:
            top=neg["topic"].fillna("other").value_counts().head(3)
            for topic,count in top.items():
                insights.append({"severity":"warning","title":("مشكلة متكررة: " if RTL else "Recurring issue: ")+str(topic),"detail":("{} مراجعة سلبية" if RTL else "{} negative reviews").format(count)})
    if cur.get("response_rate",0)<50:
        insights.append({"severity":"warning","title":"نسبة الرد تحتاج تحسين" if RTL else "Response rate needs attention","detail":"{:.0f}%".format(cur.get("response_rate",0))})
    return insights[:8]


def branch_health_table(companies_df, reviews_df):
    if companies_df is None or companies_df.empty: return pd.DataFrame()
    rows=[]
    for c in companies_df.to_dict("records"):
        sub=reviews_df[reviews_df["pk"]==c.get("pk")] if isinstance(reviews_df,pd.DataFrame) and not reviews_df.empty and "pk" in reviews_df else pd.DataFrame()
        m=kpis(sub,c)
        rows.append({"pk":c.get("pk"),"name":c.get("name"),"rating":c.get("rating"),"reviews":len(sub),
                     "negative_pct":round(m.get("neg_rate",0),1),"response_pct":round(m.get("response_rate",0),1),
                     "health":m.get("health",0),"health_grade":m.get("health_grade"),
                     "data_quality":m.get("health_data_quality",0),"reason":m.get("health_reason","")})
    return pd.DataFrame(rows).sort_values(["health","rating"],ascending=[False,False])


def branch_period_comparison(companies_df, current_df, previous_df):
    """Per-branch current-vs-previous-period deltas for Analytics Center 3.0."""
    if companies_df is None or companies_df.empty: return pd.DataFrame()
    rows=[]
    for c in companies_df.to_dict("records"):
        pk=c.get("pk")
        cur=current_df[current_df["pk"]==pk] if isinstance(current_df,pd.DataFrame) and not current_df.empty and "pk" in current_df else pd.DataFrame()
        prev=previous_df[previous_df["pk"]==pk] if isinstance(previous_df,pd.DataFrame) and not previous_df.empty and "pk" in previous_df else pd.DataFrame()
        pm=kpis(prev,c) if not prev.empty else None
        rating_delta=(safe_num(kpis(cur,c).get("avg"))-safe_num(pm.get("avg"))) if pm else None
        cm=kpis(cur,c,trend_delta=rating_delta)
        rows.append({
            "pk":pk,"name":c.get("name"),"current_reviews":len(cur),"previous_reviews":len(prev),
            "review_delta":len(cur)-len(prev),"rating":round(safe_num(cm.get("avg"),safe_num(c.get("rating"))),2),
            "rating_delta":round(rating_delta,2) if rating_delta is not None else None,
            "negative_pct":round(cm.get("neg_rate",0),1),
            "negative_delta":round(cm.get("neg_rate",0)-pm.get("neg_rate",0),1) if pm else None,
            "response_pct":round(cm.get("response_rate",0),1),
            "response_delta":round(cm.get("response_rate",0)-pm.get("response_rate",0),1) if pm else None,
            "health":cm.get("health",0),
        })
    return pd.DataFrame(rows)


def topic_branch_matrix(reviews_df, top_topics=10, top_branches=15):
    if reviews_df is None or reviews_df.empty or "topic" not in reviews_df: return pd.DataFrame()
    d=reviews_df.copy(); d["topic"]=d["topic"].fillna("other")
    if "company_name" not in d:
        d["company_name"]=d.get("pk",pd.Series(index=d.index,dtype=str)).astype(str)
    tops=d["topic"].value_counts().head(int(top_topics)).index.tolist()
    branches=d["company_name"].fillna("—").value_counts().head(int(top_branches)).index.tolist()
    d=d[d["topic"].isin(tops) & d["company_name"].isin(branches)]
    if d.empty: return pd.DataFrame()
    return pd.crosstab(d["company_name"],d["topic"])


def detect_anomalies_v3(reviews_df):
    if reviews_df is None or reviews_df.empty or len(reviews_df)<10: return pd.DataFrame()
    d=reviews_df.copy(); d["day"]=pd.to_datetime(d.get("fetched_at"),errors="coerce").dt.date
    d["rating_num"]=pd.to_numeric(d.get("rating"),errors="coerce")
    daily=d.dropna(subset=["day"]).groupby("day").agg(
        rating=("rating_num","mean"), volume=("id","count"),
        negative_pct=("sentiment",lambda x:(x=="negative").mean()*100),
        urgent=("urgency",lambda x:(pd.to_numeric(x,errors="coerce").fillna(0)>=60).sum())
    ).reset_index()
    rows=[]
    for metric in ["rating","volume","negative_pct","urgent"]:
        series=pd.to_numeric(daily[metric],errors="coerce")
        roll=series.rolling(7,min_periods=3); mean=roll.mean(); std=roll.std().replace(0,np.nan); z=(series-mean)/std
        for i in daily.index[abs(z)>=2].tolist():
            zv=float(z.loc[i]); severity="high" if abs(zv)>=3 else "medium"
            rows.append({"day":daily.loc[i,"day"],"metric":metric,"value":round(float(series.loc[i]),2),"z":round(zv,2),"severity":severity})
    return pd.DataFrame(rows).sort_values(["severity","day"],ascending=[True,False]) if rows else pd.DataFrame()


def _sanitize_export_dataframe_v41(df):
    """Neutralize formula injection in spreadsheet-compatible text fields."""
    out=df.copy()
    for col in out.columns:
        if pd.api.types.is_object_dtype(out[col]) or pd.api.types.is_string_dtype(out[col]):
            out[col]=out[col].map(
                lambda v: "'"+v if isinstance(v,str) and v.lstrip().startswith(("=","+","-","@")) else v)
    return out

def to_csv(df):
    return _sanitize_export_dataframe_v41(df).to_csv(index=False).encode("utf-8-sig")


def to_excel(df):
    buf = io.BytesIO()
    with pd.ExcelWriter(buf, engine="openpyxl") as w:
        _sanitize_export_dataframe_v41(df).to_excel(w, index=False, sheet_name="Data")
    return buf.getvalue()



# ═══════════════════════════════════════════════════════════════════════
#  v40.0 Historical Rating Trends & Complete Export
# ═══════════════════════════════════════════════════════════════════════
def aggregate_rating_timeline_v40(snapshots_df, frequency="monthly"):
    """Aggregate only real stored snapshots; never synthesize missing periods."""
    if snapshots_df is None:
        return pd.DataFrame(columns=["pk","day","period","rating","total","neg","pos","rating_delta","review_delta"])
    try:
        d=snapshots_df.copy() if isinstance(snapshots_df,pd.DataFrame) else pd.DataFrame(snapshots_df)
    except Exception:
        d=pd.DataFrame()
    cols=["pk","day","period","rating","total","neg","pos","rating_delta","review_delta"]
    if d.empty or "day" not in d.columns:
        return pd.DataFrame(columns=cols)
    d=d.copy()
    d["day"]=pd.to_datetime(d["day"],errors="coerce")
    d=d.dropna(subset=["day"]).sort_values("day")
    if d.empty:
        return pd.DataFrame(columns=cols)
    for c in ["rating","total","neg","pos"]:
        if c not in d.columns: d[c]=0
        d[c]=pd.to_numeric(d[c],errors="coerce")
    freq=str(frequency or "monthly").lower()
    if freq in {"daily","day","available"}:
        d["period"]=d["day"].dt.strftime("%Y-%m-%d")
    elif freq in {"quarterly","quarter","q"}:
        d["period"]=d["day"].dt.to_period("Q").astype(str)
    elif freq in {"yearly","annual","year","y"}:
        d["period"]=d["day"].dt.strftime("%Y")
    else:
        d["period"]=d["day"].dt.to_period("M").astype(str)
    group_cols=["period"]
    if "pk" in d.columns and d["pk"].notna().any(): group_cols=["pk","period"]
    # Latest actual snapshot in each visible period. Missing periods stay missing by design.
    out=d.sort_values("day").groupby(group_cols,dropna=False,as_index=False).tail(1).sort_values(((["pk"] if "pk" in d.columns else [])+["day"]))
    delta_group="pk" if "pk" in out.columns else None
    if delta_group:
        out["rating_delta"]=out.groupby(delta_group,dropna=False)["rating"].diff()
        out["review_delta"]=out.groupby(delta_group,dropna=False)["total"].diff()
    else:
        out["rating_delta"]=out["rating"].diff(); out["review_delta"]=out["total"].diff()
    if "neg" in out.columns and "total" in out.columns:
        denom=out["total"].replace(0,pd.NA)
        out["negative_pct"]=(out["neg"]/denom*100).round(1)
    if "pos" in out.columns and "total" in out.columns:
        denom=out["total"].replace(0,pd.NA)
        out["positive_pct"]=(out["pos"]/denom*100).round(1)
    wanted=[c for c in ["pk","workspace_id","day","period","rating","total","neg","pos","rating_delta","review_delta","negative_pct","positive_pct"] if c in out.columns]
    return out[wanted].reset_index(drop=True)


def filter_rating_snapshots_range_v40(snapshots_df, range_key="all"):
    if snapshots_df is None:
        return pd.DataFrame()
    d=snapshots_df.copy() if isinstance(snapshots_df,pd.DataFrame) else pd.DataFrame(snapshots_df)
    if d.empty or "day" not in d.columns or str(range_key or "all").lower()=="all": return d
    d["day"]=pd.to_datetime(d["day"],errors="coerce")
    d=d.dropna(subset=["day"])
    if d.empty: return d
    months={"3m":3,"6m":6,"12m":12,"24m":24,"36m":36}.get(str(range_key).lower())
    if not months: return d
    end=d["day"].max(); cutoff=end-pd.DateOffset(months=months)
    return d[d["day"]>=cutoff].copy()


def rating_trend_summary_v40(timeline_df):
    empty={"points":0,"current_rating":None,"previous_rating":None,"delta_previous":0.0,"first_rating":None,"delta_all":0.0,
           "high_rating":None,"low_rating":None,"best_change":0.0,"worst_change":0.0,"current_total":0,"review_delta_previous":0,
           "delta_3m":None,"delta_6m":None,"delta_12m":None,"direction":"insufficient"}
    if timeline_df is None: return empty
    d=timeline_df.copy() if isinstance(timeline_df,pd.DataFrame) else pd.DataFrame(timeline_df)
    if d.empty or "rating" not in d.columns: return empty
    if "day" in d.columns:
        d["day"]=pd.to_datetime(d["day"],errors="coerce"); d=d.sort_values("day")
    r=pd.to_numeric(d["rating"],errors="coerce")
    valid=d[r.notna()].copy(); valid["rating"]=r[r.notna()].astype(float)
    if valid.empty: return empty
    cur=float(valid.iloc[-1]["rating"]); first=float(valid.iloc[0]["rating"]); prev=float(valid.iloc[-2]["rating"]) if len(valid)>=2 else None
    deltas=pd.to_numeric(valid.get("rating_delta",valid["rating"].diff()),errors="coerce").dropna()
    total_series=pd.to_numeric(valid.get("total",pd.Series([0]*len(valid),index=valid.index)),errors="coerce").fillna(0)
    review_delta_series=pd.to_numeric(valid.get("review_delta",total_series.diff()),errors="coerce")
    def period_delta(days):
        if "day" not in valid.columns or pd.isna(valid.iloc[-1].get("day")): return None
        target=valid.iloc[-1]["day"]-pd.Timedelta(days=days)
        candidates=valid[valid["day"]<=target]
        if candidates.empty: return None
        return round(cur-float(candidates.iloc[-1]["rating"]),3)
    dp=0.0 if prev is None else cur-prev
    direction="improved" if dp>0.005 else "declined" if dp<-0.005 else "stable"
    return {"points":int(len(valid)),"current_rating":round(cur,3),"previous_rating":None if prev is None else round(prev,3),"delta_previous":round(dp,3),
            "first_rating":round(first,3),"delta_all":round(cur-first,3),"high_rating":round(float(valid["rating"].max()),3),"low_rating":round(float(valid["rating"].min()),3),
            "best_change":round(float(deltas.max()),3) if not deltas.empty else 0.0,"worst_change":round(float(deltas.min()),3) if not deltas.empty else 0.0,
            "current_total":int(total_series.iloc[-1]) if len(total_series) else 0,"review_delta_previous":int(review_delta_series.iloc[-1]) if len(review_delta_series) and pd.notna(review_delta_series.iloc[-1]) else 0,
            "delta_3m":period_delta(92),"delta_6m":period_delta(183),"delta_12m":period_delta(366),"direction":direction}


def complete_export_datasets_v40(db, ws_id, branches_df, pks=None, include_reviews=True):
    branches=branches_df.copy() if isinstance(branches_df,pd.DataFrame) else pd.DataFrame(branches_df or [])
    selected={str(x) for x in (pks or []) if str(x)}
    if selected and not branches.empty and "pk" in branches.columns:
        branches=branches[branches["pk"].astype(str).isin(selected)].copy()
    has_pk=(not branches.empty and "pk" in branches.columns)
    if branches.empty:
        selected=set()
    elif has_pk:
        selected={str(x) for x in branches["pk"].tolist() if str(x or "").strip() and str(x).lower()!="nan"}
    try: history=db.snapshots(ws_id,None,36500)
    except Exception: history=pd.DataFrame()
    history=history.copy() if isinstance(history,pd.DataFrame) else pd.DataFrame(history or [])
    if not has_pk:
        history=pd.DataFrame(columns=history.columns)
    elif not history.empty and "pk" in history.columns:
        history=history[history["pk"].astype(str).isin(selected)].copy()
    name_map={str(r.get("pk")):str(r.get("name") or "") for _,r in branches.iterrows()} if not branches.empty and "pk" in branches else {}
    if not history.empty and "pk" in history.columns:
        history["branch_name"]=history["pk"].astype(str).map(name_map).fillna("")
        history=history.sort_values([c for c in ["pk","day"] if c in history.columns])
    reviews=pd.DataFrame()
    if include_reviews:
        try: reviews=db.all_reviews(ws_id)
        except Exception: reviews=pd.DataFrame()
        reviews=reviews.copy() if isinstance(reviews,pd.DataFrame) else pd.DataFrame(reviews or [])
        if not has_pk:
            reviews=pd.DataFrame(columns=reviews.columns)
        elif not reviews.empty and "pk" in reviews.columns:
            reviews=reviews[reviews["pk"].astype(str).isin(selected)].copy()
        if not reviews.empty and "pk" in reviews.columns:
            if "company_name" not in reviews.columns: reviews["company_name"]=reviews["pk"].astype(str).map(name_map)
            else: reviews["company_name"]=reviews["company_name"].fillna(reviews["pk"].astype(str).map(name_map))
    contact_cols=[c for c in ["pk","name","address","category","latitude","longitude","maps_url","phone","mobile","whatsapp","email","email_source","website","place_id","cid","contact_checked_at","contact_confidence","contact_source","confidence_score","last_updated","source_fetched_at"] if c in branches.columns]
    contacts=branches[contact_cols].copy() if contact_cols else pd.DataFrame()
    if not contacts.empty:
        present=[]
        for _,row in contacts.iterrows():
            present.append(sum(pd.notna(row.get(c)) and str(row.get(c)).strip().lower() not in {"", "nan", "none"}
                               for c in ["phone","mobile","email","website"] if c in contacts.columns))
        contacts["contact_completeness"]=present
        contacts["contact_completeness_pct"]=[round(x/4*100,1) for x in present]
    perf=[]
    for _,br in branches.iterrows():
        pk=str(br.get("pk") or "")
        sub=history[history["pk"].astype(str)==pk].copy() if not history.empty and "pk" in history else pd.DataFrame()
        tl=aggregate_rating_timeline_v40(sub,"monthly")
        sm=rating_trend_summary_v40(tl)
        rev=reviews[reviews["pk"].astype(str)==pk] if include_reviews and not reviews.empty and "pk" in reviews else pd.DataFrame()
        responded=int(pd.to_numeric(rev.get("responded",pd.Series(dtype=float)),errors="coerce").fillna(0).eq(1).sum()) if not rev.empty else 0
        neg=int((rev.get("sentiment",pd.Series(dtype=str)).astype(str)=="negative").sum()) if not rev.empty else 0
        perf.append({"pk":pk,"name":br.get("name"),"current_rating":br.get("rating") if pd.notna(br.get("rating")) else sm.get("current_rating"),
                     "rating_change":sm.get("delta_all"),"previous_period_change":sm.get("delta_previous"),"high_rating":sm.get("high_rating"),"low_rating":sm.get("low_rating"),
                     "history_points":sm.get("points",0),"current_review_count":br.get("review_count"),"snapshot_review_count":sm.get("current_total",0),
                     "stored_reviews":len(rev),"negative_reviews":neg,"responded_reviews":responded,"response_rate_pct":round(responded/len(rev)*100,1) if len(rev) else 0.0})
    performance=pd.DataFrame(perf)
    summary=pd.DataFrame([{"generated_at":datetime.now().strftime("%Y-%m-%d %H:%M:%S"),"branch_count":len(branches),"history_points":len(history),"review_rows":len(reviews),"include_reviews":bool(include_reviews),"schema":49,"version":VERSION}])
    return {"summary":summary,"branches":branches.reset_index(drop=True),"rating_history":history.reset_index(drop=True),"reviews":reviews.reset_index(drop=True),"contacts":contacts.reset_index(drop=True),"performance":performance.reset_index(drop=True)}


def build_complete_export_excel_v40(datasets):
    buf=io.BytesIO()
    sheets=[("Summary","summary"),("Branches","branches"),("Rating History","rating_history"),("Reviews","reviews"),("Contacts","contacts"),("Performance","performance")]
    with pd.ExcelWriter(buf,engine="openpyxl") as w:
        for sheet,key in sheets:
            df=datasets.get(key,pd.DataFrame())
            if not isinstance(df,pd.DataFrame): df=pd.DataFrame(df or [])
            _sanitize_export_dataframe_v41(df).to_excel(w,index=False,sheet_name=sheet)
        for ws in w.book.worksheets:
            ws.freeze_panes="A2"; ws.auto_filter.ref=ws.dimensions
            for col_cells in ws.columns:
                vals=list(col_cells)[:500]
                width=min(60,max(10,max([len(str(c.value or "")) for c in vals] or [8])+2))
                ws.column_dimensions[col_cells[0].column_letter].width=width
            headers={c.value:c.column for c in ws[1] if c.value}
            for name in ["website","maps_url"]:
                if name in headers:
                    idx=headers[name]
                    for row in range(2,ws.max_row+1):
                        cell=ws.cell(row,idx); value=str(cell.value or "")
                        if value.startswith("http://") or value.startswith("https://"):
                            cell.hyperlink=value; cell.style="Hyperlink"
    return buf.getvalue()

def report_period_label():
    key = st.session_state.get("period_key", "d30")
    if key == "all": return "الكل" if RTL else "All"
    if key == "today": return "اليوم" if RTL else "Today"
    if key == "d7": return "7 أيام" if RTL else "7 days"
    if key == "d30": return "30 يومًا" if RTL else "30 days"
    if key == "d90": return "90 يومًا" if RTL else "90 days"
    if key == "custom":
        n = int(st.session_state.get("period_custom_days", 30) or 30)
        return ("{} يومًا" if RTL else "{} days").format(n)
    return key


def _filter_period(df, date_col):
    if df is None or df.empty or date_col not in df.columns:
        return df.copy() if isinstance(df, pd.DataFrame) else pd.DataFrame()
    since, _ = period_bounds()
    if since is None:
        return df.copy()
    out = df.copy()
    vals = pd.to_datetime(out[date_col], errors="coerce")
    return out[vals >= pd.Timestamp(since)].copy()


def report_datasets(db, ws_id, companies_df, reviews_df):
    alerts_df=db.list_alerts(ws_id,status="all",limit=5000); tickets_df=db.list_tickets(ws_id)
    alerts_df=_filter_period(alerts_df,"created_at"); tickets_df=_filter_period(tickets_df,"created_at")
    prev=previous_period_reviews(db,ws_id)
    branches=companies_df.copy() if isinstance(companies_df,pd.DataFrame) else pd.DataFrame()
    reviews=reviews_df.copy() if isinstance(reviews_df,pd.DataFrame) else pd.DataFrame()
    out={"branches":branches,"reviews":reviews,"previous_reviews":prev,"alerts":alerts_df,"tickets":tickets_df,
         "branch_health":branch_health_table(branches,reviews),"insights":executive_insights(reviews,prev)}
    try:
        _pks=branches["pk"].dropna().astype(str).tolist() if not branches.empty and "pk" in branches else []
        _complete=complete_export_datasets_v40(db,ws_id,branches,_pks,include_reviews=True)
        out["rating_history"]=_complete.get("rating_history",pd.DataFrame())
        out["contacts"]=_complete.get("contacts",pd.DataFrame())
        out["historical_performance"]=_complete.get("performance",pd.DataFrame())
    except Exception:
        out["rating_history"]=pd.DataFrame(); out["contacts"]=pd.DataFrame(); out["historical_performance"]=pd.DataFrame()
    out["comparison"]=report_comparison_frame(out)
    out["top_branches"],out["attention_branches"]=report_rankings(out)
    return out


def report_comparison_frame(datasets):
    reviews_df=datasets.get("reviews",pd.DataFrame()); prev=datasets.get("previous_reviews",pd.DataFrame())
    cur=kpis(reviews_df); old=kpis(prev) if isinstance(prev,pd.DataFrame) and not prev.empty else {}
    L=lambda ar,en: ar if RTL else en
    rows=[
        (L("المراجعات","Reviews"),int(cur.get("total",0)),int(old.get("total",0))),
        (L("متوسط التقييم","Average rating"),round(safe_num(cur.get("avg")),2),round(safe_num(old.get("avg")),2) if old else "—"),
        (L("السلبية %","Negative %"),round(safe_num(cur.get("neg_rate")),1),round(safe_num(old.get("neg_rate")),1) if old else "—"),
        (L("نسبة الرد %","Response %"),round(safe_num(cur.get("response_rate")),1),round(safe_num(old.get("response_rate")),1) if old else "—"),
        (L("Health 3.0","Health 3.0"),round(safe_num(cur.get("health")),1),round(safe_num(old.get("health")),1) if old else "—"),
    ]
    out=pd.DataFrame(rows,columns=[L("المؤشر","Metric"),L("الحالي","Current"),L("السابق","Previous")])
    def delta(row):
        try: return round(float(row.iloc[1])-float(row.iloc[2]),2)
        except Exception: return "—"
    out[L("الفرق","Delta")]=out.apply(delta,axis=1)
    return out


def report_rankings(datasets):
    bh=datasets.get("branch_health",pd.DataFrame())
    if not isinstance(bh,pd.DataFrame) or bh.empty:
        return pd.DataFrame(),pd.DataFrame()
    top=bh.sort_values("health",ascending=False).head(10).copy() if "health" in bh else bh.head(10).copy()
    attention=bh.sort_values("health",ascending=True).head(10).copy() if "health" in bh else bh.tail(10).copy()
    return top,attention


def report_summary_frame(datasets):
    reviews_df=datasets.get("reviews",pd.DataFrame()); prev=datasets.get("previous_reviews",pd.DataFrame())
    branches_df=datasets.get("branches",pd.DataFrame()); alerts_df=datasets.get("alerts",pd.DataFrame()); tickets_df=datasets.get("tickets",pd.DataFrame())
    m=kpis(reviews_df); pm=kpis(prev) if isinstance(prev,pd.DataFrame) and not prev.empty else None
    avg_google=pd.to_numeric(branches_df.get("rating",pd.Series(dtype=float)),errors="coerce").dropna()
    total_google=int(pd.to_numeric(branches_df.get("review_count",pd.Series(dtype=float)),errors="coerce").fillna(0).sum()) if not branches_df.empty else 0
    freshness="—"
    if not branches_df.empty and "last_updated" in branches_df:
        x=pd.to_datetime(branches_df["last_updated"],errors="coerce").dropna()
        if not x.empty: freshness=str(x.max())[:19]
    L=lambda ar,en: ar if RTL else en
    rows=[
        (L("مساحة العمل","Workspace"),str(ws_name)),(L("الفترة","Period"),report_period_label()),
        (L("وقت الإنشاء","Generated at"),datetime.now().strftime("%Y-%m-%d %H:%M:%S")),(L("حداثة البيانات","Data freshness"),freshness),
        (L("عدد الفروع","Branches"),len(branches_df)),(L("المراجعات المخزنة","Stored reviews"),len(reviews_df)),
        (L("إجمالي تقييمات Google","Google review count"),total_google),(L("متوسط تقييم المراجعات","Average stored rating"),round(safe_num(m.get("avg")),2)),
        (L("متوسط تقييم Google","Average Google rating"),round(float(avg_google.mean()),2) if not avg_google.empty else 0),
        (L("تغير التقييم عن الفترة السابقة","Rating delta vs previous"),round(safe_num(m.get("avg"))-safe_num(pm.get("avg")),2) if pm else "—"),
        (L("المراجعات الإيجابية","Positive reviews"),int(m.get("pos",0))),(L("المراجعات السلبية","Negative reviews"),int(m.get("neg",0))),
        (L("نسبة الرد %","Response rate %"),round(float(m.get("response_rate",0)),1)),(L("مؤشر الصحة 3.0","Health score 3.0"),round(float(m.get("health",0)),1)),
        (L("سبب مؤشر الصحة","Health explanation"),m.get("health_reason","")),(L("التنبيهات","Alerts"),len(alerts_df)),(L("التذاكر","Tickets"),len(tickets_df)),
    ]
    return pd.DataFrame(rows,columns=[L("البند","Metric"),L("القيمة","Value")])


def build_report_excel(sections, datasets):
    buf=io.BytesIO(); summary=report_summary_frame(datasets)
    with pd.ExcelWriter(buf,engine="openpyxl") as w:
        summary.to_excel(w,index=False,sheet_name="Summary")
        if "branches" in sections: datasets["branches"].to_excel(w,index=False,sheet_name="Branches")
        if not datasets.get("branch_health",pd.DataFrame()).empty: datasets["branch_health"].to_excel(w,index=False,sheet_name="Branch Health")
        if "reviews" in sections: datasets["reviews"].to_excel(w,index=False,sheet_name="Reviews")
        if isinstance(datasets.get("previous_reviews"),pd.DataFrame) and not datasets["previous_reviews"].empty: datasets["previous_reviews"].to_excel(w,index=False,sheet_name="Previous Period")
        comp=datasets.get("comparison",pd.DataFrame())
        if isinstance(comp,pd.DataFrame) and not comp.empty: comp.to_excel(w,index=False,sheet_name="Period Comparison")
        top=datasets.get("top_branches",pd.DataFrame()); att=datasets.get("attention_branches",pd.DataFrame())
        if isinstance(top,pd.DataFrame) and not top.empty: top.to_excel(w,index=False,sheet_name="Top Branches")
        if isinstance(att,pd.DataFrame) and not att.empty: att.to_excel(w,index=False,sheet_name="Needs Attention")
        if "alerts" in sections: datasets["alerts"].to_excel(w,index=False,sheet_name="Alerts")
        if "tickets" in sections: datasets["tickets"].to_excel(w,index=False,sheet_name="Tickets")
        rh=datasets.get("rating_history",pd.DataFrame())
        if isinstance(rh,pd.DataFrame) and not rh.empty: rh.to_excel(w,index=False,sheet_name="Rating History")
        ct=datasets.get("contacts",pd.DataFrame())
        if isinstance(ct,pd.DataFrame) and not ct.empty: ct.to_excel(w,index=False,sheet_name="Contacts")
        hp=datasets.get("historical_performance",pd.DataFrame())
        if isinstance(hp,pd.DataFrame) and not hp.empty: hp.to_excel(w,index=False,sheet_name="Historical Performance")
        ins=pd.DataFrame(datasets.get("insights") or [])
        if not ins.empty: ins.to_excel(w,index=False,sheet_name="Insights")
        for ws in w.book.worksheets:
            ws.freeze_panes="A2"; ws.auto_filter.ref=ws.dimensions
            for col_cells in ws.columns:
                width=min(50,max(10,max(len(str(c.value or "")) for c in col_cells[:250])+2)); ws.column_dimensions[col_cells[0].column_letter].width=width
    return buf.getvalue()


def build_report_csv_zip(sections, datasets):
    buf=io.BytesIO()
    with zipfile.ZipFile(buf,"w",compression=zipfile.ZIP_DEFLATED) as zf:
        zf.writestr("summary.csv",report_summary_frame(datasets).to_csv(index=False).encode("utf-8-sig"))
        for sec in ["branches","reviews","alerts","tickets"]:
            if sec in sections: zf.writestr(sec+".csv",datasets.get(sec,pd.DataFrame()).to_csv(index=False).encode("utf-8-sig"))
        bh=datasets.get("branch_health",pd.DataFrame())
        if isinstance(bh,pd.DataFrame) and not bh.empty: zf.writestr("branch_health.csv",bh.to_csv(index=False).encode("utf-8-sig"))
        prev=datasets.get("previous_reviews",pd.DataFrame())
        if isinstance(prev,pd.DataFrame) and not prev.empty: zf.writestr("previous_period.csv",prev.to_csv(index=False).encode("utf-8-sig"))
        comp=datasets.get("comparison",pd.DataFrame())
        if isinstance(comp,pd.DataFrame) and not comp.empty: zf.writestr("period_comparison.csv",comp.to_csv(index=False).encode("utf-8-sig"))
        top=datasets.get("top_branches",pd.DataFrame()); att=datasets.get("attention_branches",pd.DataFrame())
        if isinstance(top,pd.DataFrame) and not top.empty: zf.writestr("top_branches.csv",top.to_csv(index=False).encode("utf-8-sig"))
        if isinstance(att,pd.DataFrame) and not att.empty: zf.writestr("needs_attention.csv",att.to_csv(index=False).encode("utf-8-sig"))
        rh=datasets.get("rating_history",pd.DataFrame())
        if isinstance(rh,pd.DataFrame) and not rh.empty: zf.writestr("rating_history.csv",rh.to_csv(index=False).encode("utf-8-sig"))
        ct=datasets.get("contacts",pd.DataFrame())
        if isinstance(ct,pd.DataFrame) and not ct.empty: zf.writestr("contacts.csv",ct.to_csv(index=False).encode("utf-8-sig"))
        hp=datasets.get("historical_performance",pd.DataFrame())
        if isinstance(hp,pd.DataFrame) and not hp.empty: zf.writestr("historical_performance.csv",hp.to_csv(index=False).encode("utf-8-sig"))
        ins=pd.DataFrame(datasets.get("insights") or [])
        if not ins.empty: zf.writestr("insights.csv",ins.to_csv(index=False).encode("utf-8-sig"))
    return buf.getvalue()


def _report_font():
    try:
        from reportlab.pdfbase import pdfmetrics
        from reportlab.pdfbase.ttfonts import TTFont
        candidates = [
            r"C:\Windows\Fonts\arial.ttf", r"C:\Windows\Fonts\tahoma.ttf",
            "/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf",
            "/usr/share/fonts/truetype/liberation2/LiberationSans-Regular.ttf",
        ]
        for fp in candidates:
            if os.path.exists(fp):
                try:
                    pdfmetrics.registerFont(TTFont("RTFont", fp))
                    return "RTFont", True
                except Exception:
                    continue
    except Exception:
        pass
    return "Helvetica", False


# Lightweight built-in Arabic shaping fallback.  The bootstrap also installs
# arabic-reshaper/python-bidi, but this keeps exported PDFs readable even when
# those optional helpers are temporarily unavailable.
_AR_FORMS = {
    "\u0621": ("\uFE80", None, None, None),
    "\u0622": ("\uFE81", "\uFE82", None, None), "\u0623": ("\uFE83", "\uFE84", None, None),
    "\u0624": ("\uFE85", "\uFE86", None, None), "\u0625": ("\uFE87", "\uFE88", None, None),
    "\u0626": ("\uFE89", "\uFE8A", "\uFE8B", "\uFE8C"), "\u0627": ("\uFE8D", "\uFE8E", None, None),
    "\u0628": ("\uFE8F", "\uFE90", "\uFE91", "\uFE92"), "\u0629": ("\uFE93", "\uFE94", None, None),
    "\u062A": ("\uFE95", "\uFE96", "\uFE97", "\uFE98"), "\u062B": ("\uFE99", "\uFE9A", "\uFE9B", "\uFE9C"),
    "\u062C": ("\uFE9D", "\uFE9E", "\uFE9F", "\uFEA0"), "\u062D": ("\uFEA1", "\uFEA2", "\uFEA3", "\uFEA4"),
    "\u062E": ("\uFEA5", "\uFEA6", "\uFEA7", "\uFEA8"), "\u062F": ("\uFEA9", "\uFEAA", None, None),
    "\u0630": ("\uFEAB", "\uFEAC", None, None), "\u0631": ("\uFEAD", "\uFEAE", None, None),
    "\u0632": ("\uFEAF", "\uFEB0", None, None), "\u0633": ("\uFEB1", "\uFEB2", "\uFEB3", "\uFEB4"),
    "\u0634": ("\uFEB5", "\uFEB6", "\uFEB7", "\uFEB8"), "\u0635": ("\uFEB9", "\uFEBA", "\uFEBB", "\uFEBC"),
    "\u0636": ("\uFEBD", "\uFEBE", "\uFEBF", "\uFEC0"), "\u0637": ("\uFEC1", "\uFEC2", "\uFEC3", "\uFEC4"),
    "\u0638": ("\uFEC5", "\uFEC6", "\uFEC7", "\uFEC8"), "\u0639": ("\uFEC9", "\uFECA", "\uFECB", "\uFECC"),
    "\u063A": ("\uFECD", "\uFECE", "\uFECF", "\uFED0"), "\u0641": ("\uFED1", "\uFED2", "\uFED3", "\uFED4"),
    "\u0642": ("\uFED5", "\uFED6", "\uFED7", "\uFED8"), "\u0643": ("\uFED9", "\uFEDA", "\uFEDB", "\uFEDC"),
    "\u0644": ("\uFEDD", "\uFEDE", "\uFEDF", "\uFEE0"), "\u0645": ("\uFEE1", "\uFEE2", "\uFEE3", "\uFEE4"),
    "\u0646": ("\uFEE5", "\uFEE6", "\uFEE7", "\uFEE8"), "\u0647": ("\uFEE9", "\uFEEA", "\uFEEB", "\uFEEC"),
    "\u0648": ("\uFEED", "\uFEEE", None, None), "\u0649": ("\uFEEF", "\uFEF0", None, None),
    "\u064A": ("\uFEF1", "\uFEF2", "\uFEF3", "\uFEF4"),
}
_AR_MARK_RE = re.compile(r"[\u064B-\u065F\u0670\u06D6-\u06ED]")
_AR_RUN_RE = re.compile(r"[\u0600-\u06FF\uFE70-\uFEFF]+")


def _fallback_shape_arabic(s):
    chars = list(s)
    shaped = []
    def prev_letter(i):
        j = i - 1
        while j >= 0 and _AR_MARK_RE.match(chars[j]): j -= 1
        return chars[j] if j >= 0 else None
    def next_letter(i):
        j = i + 1
        while j < len(chars) and _AR_MARK_RE.match(chars[j]): j += 1
        return chars[j] if j < len(chars) else None
    for i, ch in enumerate(chars):
        forms = _AR_FORMS.get(ch)
        if not forms:
            shaped.append(ch); continue
        prev = prev_letter(i); nxt = next_letter(i)
        pf = _AR_FORMS.get(prev); nf = _AR_FORMS.get(nxt)
        join_prev = bool(pf and pf[2] and forms[1])
        join_next = bool(forms[2] and nf and nf[1])
        if join_prev and join_next and forms[3]: out = forms[3]
        elif join_prev and forms[1]: out = forms[1]
        elif join_next and forms[2]: out = forms[2]
        else: out = forms[0]
        shaped.append(out)
    return "".join(shaped)


def _fallback_bidi_display(s):
    shaped = _fallback_shape_arabic(s)
    # Split Arabic-shaped runs from LTR material.  For an RTL base line, reverse
    # run order while reversing only Arabic runs internally; Latin/numbers stay LTR.
    parts = re.split(r"([\u0600-\u06FF\uFE70-\uFEFF]+)", shaped)
    out = []
    for part in reversed(parts):
        if not part: continue
        if re.fullmatch(r"[\u0600-\u06FF\uFE70-\uFEFF]+", part):
            out.append(part[::-1])
        else:
            out.append(part)
    return "".join(out).strip()


def _pdf_text(value, bidi_ok=True):
    s = str(value if value is not None else "")
    if bidi_ok and re.search(r"[\u0600-\u06FF]", s):
        try:
            import arabic_reshaper
            from bidi.algorithm import get_display
            s = get_display(arabic_reshaper.reshape(s))
        except Exception:
            s = _fallback_bidi_display(s)
    return html.escape(s)




def _rating_history_drawing_v40(history_df, width=500, height=180):
    from reportlab.graphics.shapes import Drawing
    from reportlab.graphics.charts.linecharts import HorizontalLineChart
    from reportlab.lib import colors
    d=history_df.copy() if isinstance(history_df,pd.DataFrame) else pd.DataFrame(history_df or [])
    if d.empty or "rating" not in d.columns: return None
    if "period" not in d.columns: d=aggregate_rating_timeline_v40(d,"monthly")
    vals=pd.to_numeric(d.get("rating"),errors="coerce")
    mask=vals.notna(); d=d[mask].copy(); vals=vals[mask].astype(float).tolist()
    if not vals: return None
    labels=d.get("period",d.get("day",pd.Series(range(len(d))))).astype(str).tolist()
    drawing=Drawing(width,height)
    lc=HorizontalLineChart(); lc.x=42; lc.y=34; lc.width=width-65; lc.height=height-55; lc.data=[vals]
    low=max(1.0,min(vals)-0.25); high=min(5.0,max(vals)+0.25)
    if high-low<0.4: low=max(1.0,low-0.2); high=min(5.0,high+0.2)
    lc.valueAxis.valueMin=round(low,1); lc.valueAxis.valueMax=round(high,1); lc.valueAxis.valueStep=0.1 if high-low<=0.8 else 0.25
    # Keep a readable subset of labels when history is long; no data points are removed.
    step=max(1,int(len(labels)/10)); lc.categoryAxis.categoryNames=[lab if (i%step==0 or i==len(labels)-1) else "" for i,lab in enumerate(labels)]
    lc.categoryAxis.labels.fontSize=6; lc.valueAxis.labels.fontSize=7
    lc.lines[0].strokeWidth=2; lc.lines[0].strokeColor=colors.HexColor("#2563EB")
    lc.joinedLines=1
    drawing.add(lc)
    return drawing


def build_complete_export_pdf_v40(datasets,title=None,max_reviews_per_branch=120):
    from reportlab.lib import colors
    from reportlab.lib.pagesizes import A4, landscape
    from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
    from reportlab.lib.enums import TA_RIGHT, TA_LEFT
    from reportlab.lib.units import mm
    from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle, PageBreak
    branches=datasets.get("branches",pd.DataFrame()); history=datasets.get("rating_history",pd.DataFrame()); reviews=datasets.get("reviews",pd.DataFrame()); perf=datasets.get("performance",pd.DataFrame())
    branches=branches if isinstance(branches,pd.DataFrame) else pd.DataFrame(branches or [])
    history=history if isinstance(history,pd.DataFrame) else pd.DataFrame(history or [])
    reviews=reviews if isinstance(reviews,pd.DataFrame) else pd.DataFrame(reviews or [])
    perf=perf if isinstance(perf,pd.DataFrame) else pd.DataFrame(perf or [])
    buf=io.BytesIO(); font_name,bidi_ok=_report_font(); styles=getSampleStyleSheet(); rtl=bool(globals().get("RTL",True))
    body=ParagraphStyle("V40Body",parent=styles["BodyText"],fontName=font_name,fontSize=8,leading=10,alignment=TA_RIGHT if rtl else TA_LEFT)
    head=ParagraphStyle("V40Head",parent=styles["Heading2"],fontName=font_name,fontSize=13,leading=16,alignment=TA_RIGHT if rtl else TA_LEFT,textColor=colors.HexColor("#0F172A"))
    title_style=ParagraphStyle("V40Title",parent=styles["Title"],fontName=font_name,fontSize=18,leading=22,alignment=TA_RIGHT if rtl else TA_LEFT)
    doc=SimpleDocTemplate(buf,pagesize=landscape(A4),rightMargin=10*mm,leftMargin=10*mm,topMargin=10*mm,bottomMargin=10*mm,title=str(title or "Reviews Tracker v40 Complete Export"))
    story=[Paragraph(_pdf_text(title or ("التقرير الشامل — Reviews Tracker" if rtl else "Reviews Tracker Complete Report"),bidi_ok),title_style),
           Paragraph(_pdf_text(("وقت الإنشاء: " if rtl else "Generated: ")+datetime.now().strftime("%Y-%m-%d %H:%M:%S"),bidi_ok),body),
           Paragraph(_pdf_text((("الفروع: {} · نقاط التاريخ: {} · التعليقات: {}" if rtl else "Branches: {} · history points: {} · reviews: {}").format(len(branches),len(history),len(reviews))),bidi_ok),body),Spacer(1,8)]
    if branches.empty:
        story.append(Paragraph(_pdf_text("لا توجد بيانات" if rtl else "No data",bidi_ok),body))
    for bi,(_,br) in enumerate(branches.iterrows()):
        if bi>0: story.append(PageBreak())
        pk=str(br.get("pk") or ""); name=str(br.get("name") or pk or "—")
        story.append(Paragraph(_pdf_text(name,bidi_ok),head))
        info=[
            ("التقييم" if rtl else "Rating",br.get("rating")),("عدد المراجعات" if rtl else "Review count",br.get("review_count")),
            ("العنوان" if rtl else "Address",br.get("address")),("النشاط" if rtl else "Category",br.get("category")),
            ("الهاتف" if rtl else "Phone",br.get("phone")),("الجوال" if rtl else "Mobile",br.get("mobile")),
            ("WhatsApp",br.get("whatsapp")),("البريد" if rtl else "Email",br.get("email")),
            ("الموقع الإلكتروني" if rtl else "Website",br.get("website")),("Google Maps",br.get("maps_url")),
            ("Latitude",br.get("latitude")),("Longitude",br.get("longitude")),("Place ID",br.get("place_id")),("CID",br.get("cid")),
            ("آخر تحديث" if rtl else "Last update",br.get("last_updated") or br.get("source_fetched_at"))]
        data=[[Paragraph(_pdf_text(k,bidi_ok),body),Paragraph(_pdf_text(v if v not in (None,"") else "—",bidi_ok),body)] for k,v in info]
        it=Table(data,colWidths=[45*mm,210*mm]); it.setStyle(TableStyle([("GRID",(0,0),(-1,-1),.25,colors.HexColor("#CBD5E1")),("BACKGROUND",(0,0),(0,-1),colors.HexColor("#EFF6FF")),("FONTNAME",(0,0),(-1,-1),font_name),("VALIGN",(0,0),(-1,-1),"TOP")]))
        story.append(it); story.append(Spacer(1,7))
        sub=history[history["pk"].astype(str)==pk].copy() if not history.empty and "pk" in history else pd.DataFrame()
        timeline=aggregate_rating_timeline_v40(sub,"monthly")
        story.append(Paragraph(_pdf_text("تطور التقييم عبر الزمن" if rtl else "Rating history over time",bidi_ok),head))
        if len(timeline)>=1:
            drawing=_rating_history_drawing_v40(timeline)
            if drawing is not None: story.append(drawing)
            sm=rating_trend_summary_v40(timeline)
            story.append(Paragraph(_pdf_text((("الحالي {} · التغير {} · الأعلى {} · الأدنى {}" if rtl else "Current {} · change {} · high {} · low {}").format(sm.get("current_rating"),sm.get("delta_all"),sm.get("high_rating"),sm.get("low_rating"))),bidi_ok),body))
            hist_cols=[c for c in ["period","day","rating","total","rating_delta","review_delta"] if c in timeline]
            def _fmt_hist_cell_v40(col,val):
                if pd.isna(val): return "—"
                if col in {"rating","rating_delta"}:
                    try:
                        num=float(val); return (("{:+.2f}" if col=="rating_delta" else "{:.2f}").format(num))
                    except Exception: return str(val)
                if col in {"total","review_delta"}:
                    try:
                        num=int(round(float(val))); return (("{:+d}" if col=="review_delta" else "{:d}").format(num))
                    except Exception: return str(val)
                if col=="day":
                    try: return pd.to_datetime(val).strftime("%Y-%m-%d")
                    except Exception: return str(val)
                return str(val)
            hd=[[Paragraph(_pdf_text(c,bidi_ok),body) for c in hist_cols]]
            for _,_hr in timeline[hist_cols].tail(36).iterrows():
                hd.append([Paragraph(_pdf_text(_fmt_hist_cell_v40(c,_hr.get(c)),bidi_ok),body) for c in hist_cols])
            ht=Table(hd,repeatRows=1); ht.setStyle(TableStyle([("BACKGROUND",(0,0),(-1,0),colors.HexColor("#DBEAFE")),("GRID",(0,0),(-1,-1),.25,colors.HexColor("#CBD5E1")),("FONTNAME",(0,0),(-1,-1),font_name),("VALIGN",(0,0),(-1,-1),"TOP")]))
            story.append(Spacer(1,5)); story.append(ht)
        else:
            story.append(Paragraph(_pdf_text("لا توجد لقطة تاريخية محفوظة بعد." if rtl else "No historical snapshot is stored yet.",bidi_ok),body))
        rv=reviews[reviews["pk"].astype(str)==pk].copy() if not reviews.empty and "pk" in reviews else pd.DataFrame()
        story.append(Spacer(1,8)); story.append(Paragraph(_pdf_text("التعليقات" if rtl else "Reviews",bidi_ok),head))
        if rv.empty:
            story.append(Paragraph(_pdf_text("لا توجد تعليقات مخزنة." if rtl else "No stored reviews.",bidi_ok),body))
        else:
            cols=[c for c in ["author","rating","review_date","sentiment","topic","text","responded","response_text"] if c in rv.columns]
            out=rv[cols].head(max(1,int(max_reviews_per_branch))).copy()
            for c in out.columns: out[c]=out[c].fillna("").astype(str).map(lambda x:x[:500])
            rd=[[Paragraph(_pdf_text(c,bidi_ok),body) for c in cols]]
            for row in out.values.tolist(): rd.append([Paragraph(_pdf_text(x,bidi_ok),body) for x in row])
            page_w=landscape(A4)[0]-20*mm; widths=[page_w/max(1,len(cols))]*len(cols)
            rt=Table(rd,colWidths=widths,repeatRows=1); rt.setStyle(TableStyle([("BACKGROUND",(0,0),(-1,0),colors.HexColor("#E0F2FE")),("GRID",(0,0),(-1,-1),.2,colors.HexColor("#CBD5E1")),("FONTNAME",(0,0),(-1,-1),font_name),("VALIGN",(0,0),(-1,-1),"TOP")]))
            story.append(rt)
            if len(rv)>len(out): story.append(Paragraph(_pdf_text((("يعرض PDF أول {} تعليق لهذا الفرع؛ Excel يحتوي جميع التعليقات." if rtl else "PDF shows the first {} reviews for this branch; Excel contains all reviews.").format(len(out))),bidi_ok),body))
    def footer(canvas,doc_obj):
        canvas.saveState(); canvas.setFont(font_name,7); canvas.setFillColor(colors.HexColor("#64748B")); canvas.drawCentredString(landscape(A4)[0]/2,5*mm,"Reviews Tracker 40.0 · Page {}".format(doc_obj.page)); canvas.restoreState()
    doc.build(story,onFirstPage=footer,onLaterPages=footer)
    return buf.getvalue()

def build_report_pdf(sections, datasets, title=None):
    from reportlab.lib import colors
    from reportlab.lib.pagesizes import A4, landscape
    from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
    from reportlab.lib.enums import TA_RIGHT, TA_LEFT
    from reportlab.lib.units import mm
    from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle, PageBreak

    buf = io.BytesIO()
    rtl=bool(globals().get("RTL",True))
    font_name, bidi_ok = _report_font()
    doc = SimpleDocTemplate(buf, pagesize=landscape(A4), rightMargin=12*mm, leftMargin=12*mm,
                            topMargin=12*mm, bottomMargin=12*mm,
                            title=title or "Reviews Tracker Report")
    styles = getSampleStyleSheet()
    title_style = ParagraphStyle("RTTitle", parent=styles["Title"], fontName=font_name,
                                 fontSize=18, leading=22, alignment=TA_RIGHT if RTL else TA_LEFT,
                                 textColor=colors.HexColor("#0F172A"))
    h_style = ParagraphStyle("RTH", parent=styles["Heading2"], fontName=font_name,
                             fontSize=13, leading=16, alignment=TA_RIGHT if rtl else TA_LEFT)
    body_style = ParagraphStyle("RTBody", parent=styles["BodyText"], fontName=font_name,
                                fontSize=8.5, leading=11, alignment=TA_RIGHT if rtl else TA_LEFT)
    story = []
    report_title = title or ("تقرير Reviews Tracker" if RTL else "Reviews Tracker Report")
    story.append(Paragraph(_pdf_text(report_title, bidi_ok), title_style))
    story.append(Paragraph(_pdf_text(("الفترة: " if RTL else "Period: ") + report_period_label(), bidi_ok), body_style))
    story.append(Paragraph(_pdf_text(("وقت الإنشاء: " if RTL else "Generated: ") + datetime.now().strftime("%Y-%m-%d %H:%M:%S"), bidi_ok), body_style))
    story.append(Spacer(1, 8))

    summary = report_summary_frame(datasets)
    sum_data = [[Paragraph(_pdf_text(x, bidi_ok), body_style) for x in summary.columns.tolist()]]
    for row in summary.astype(str).values.tolist():
        sum_data.append([Paragraph(_pdf_text(x, bidi_ok), body_style) for x in row])
    tbl = Table(sum_data, colWidths=[70*mm, 80*mm], repeatRows=1)
    tbl.setStyle(TableStyle([
        ("BACKGROUND", (0,0), (-1,0), colors.HexColor("#E2E8F0")),
        ("TEXTCOLOR", (0,0), (-1,0), colors.HexColor("#0F172A")),
        ("FONTNAME", (0,0), (-1,-1), font_name),
        ("GRID", (0,0), (-1,-1), .4, colors.HexColor("#CBD5E1")),
        ("VALIGN", (0,0), (-1,-1), "TOP"),
        ("ROWBACKGROUNDS", (0,1), (-1,-1), [colors.white, colors.HexColor("#F8FAFC")]),
        ("LEFTPADDING", (0,0), (-1,-1), 5), ("RIGHTPADDING", (0,0), (-1,-1), 5),
        ("TOPPADDING", (0,0), (-1,-1), 4), ("BOTTOMPADDING", (0,0), (-1,-1), 4),
    ]))
    story.append(tbl)
    comp=datasets.get("comparison",pd.DataFrame())
    if isinstance(comp,pd.DataFrame) and not comp.empty:
        story.append(Spacer(1,8)); story.append(Paragraph(_pdf_text("مقارنة الفترة الحالية بالسابقة" if RTL else "Current vs previous period",bidi_ok),h_style))
        cdata=[[Paragraph(_pdf_text(c,bidi_ok),body_style) for c in comp.columns.tolist()]]
        for row in comp.astype(str).values.tolist(): cdata.append([Paragraph(_pdf_text(x,bidi_ok),body_style) for x in row])
        ct=Table(cdata,repeatRows=1); ct.setStyle(TableStyle([("BACKGROUND",(0,0),(-1,0),colors.HexColor("#E0F2FE")),("GRID",(0,0),(-1,-1),.3,colors.HexColor("#CBD5E1")),("FONTNAME",(0,0),(-1,-1),font_name),("ROWBACKGROUNDS",(0,1),(-1,-1),[colors.white,colors.HexColor("#F8FAFC")])]))
        story.append(ct)
    insights = datasets.get("insights") or []
    if insights:
        story.append(Spacer(1, 8))
        story.append(Paragraph(_pdf_text("أهم الاستنتاجات" if RTL else "Key Insights", bidi_ok), h_style))
        for item in insights[:8]:
            story.append(Paragraph(_pdf_text("• {} — {}".format(item.get("title",""), item.get("detail","")), bidi_ok), body_style))
    bh = datasets.get("branch_health", pd.DataFrame())
    if isinstance(bh, pd.DataFrame) and not bh.empty:
        story.append(Spacer(1, 8))
        story.append(Paragraph(_pdf_text("ترتيب الفروع حسب Health 3.0" if RTL else "Branch ranking by Health 3.0", bidi_ok), h_style))
        rank_cols=[c for c in ["name","rating","reviews","negative_pct","response_pct","health"] if c in bh.columns]
        rank=bh[rank_cols].head(12).copy()
        data=[[Paragraph(_pdf_text(c,bidi_ok),body_style) for c in rank_cols]]
        for row in rank.astype(str).values.tolist(): data.append([Paragraph(_pdf_text(x,bidi_ok),body_style) for x in row])
        rt=Table(data,repeatRows=1); rt.setStyle(TableStyle([("BACKGROUND",(0,0),(-1,0),colors.HexColor("#DBEAFE")),("GRID",(0,0),(-1,-1),.3,colors.HexColor("#CBD5E1")),("FONTNAME",(0,0),(-1,-1),font_name),("ROWBACKGROUNDS",(0,1),(-1,-1),[colors.white,colors.HexColor("#F8FAFC")]),("VALIGN",(0,0),(-1,-1),"TOP")]))
        story.append(rt)
        try:
            from reportlab.graphics.shapes import Drawing
            from reportlab.graphics.charts.barcharts import VerticalBarChart
            vals=pd.to_numeric(bh.head(10).get("rating"),errors="coerce").fillna(0).tolist()
            if vals:
                drawing=Drawing(520,180); chart_obj=VerticalBarChart(); chart_obj.x=45; chart_obj.y=35; chart_obj.height=115; chart_obj.width=440; chart_obj.data=[vals]; chart_obj.valueAxis.valueMin=0; chart_obj.valueAxis.valueMax=5; chart_obj.valueAxis.valueStep=1; chart_obj.categoryAxis.categoryNames=[str(i+1) for i in range(len(vals))]; drawing.add(chart_obj); story.append(Spacer(1,5)); story.append(drawing)
        except Exception:
            pass

    rh=datasets.get("rating_history",pd.DataFrame())
    if isinstance(rh,pd.DataFrame) and not rh.empty and "pk" in rh.columns:
        story.append(PageBreak()); story.append(Paragraph(_pdf_text("تطور التقييم" if RTL else "Rating history",bidi_ok),h_style))
        names={str(r.get("pk")):str(r.get("name") or r.get("pk")) for _,r in datasets.get("branches",pd.DataFrame()).iterrows()} if isinstance(datasets.get("branches"),pd.DataFrame) else {}
        for _pk,_grp in list(rh.groupby(rh["pk"].astype(str)))[:8]:
            _tl=aggregate_rating_timeline_v40(_grp,"monthly")
            if _tl.empty: continue
            story.append(Paragraph(_pdf_text(names.get(str(_pk),str(_pk)),bidi_ok),body_style))
            _drawing=_rating_history_drawing_v40(_tl,520,160)
            if _drawing is not None: story.append(_drawing)
            story.append(Spacer(1,5))

    specs = {
        "branches": (["name","rating","review_count","category","address","latitude","longitude","maps_url","phone","mobile","email","website","place_id","cid","last_updated"], "الفروع" if RTL else "Branches"),
        "reviews": (["company_name","author","rating","sentiment","text","review_date"], "المراجعات" if RTL else "Reviews"),
        "alerts": (["company_name","severity","title","detail","status","created_at"], "التنبيهات" if RTL else "Alerts"),
        "tickets": (["pk","title","status","priority","owner","updated_at"], "التذاكر" if RTL else "Tickets"),
    }
    for sec in ["branches","reviews","alerts","tickets"]:
        if sec not in sections: continue
        df = datasets.get(sec, pd.DataFrame())
        story.append(PageBreak())
        story.append(Paragraph(_pdf_text(specs[sec][1], bidi_ok), h_style))
        story.append(Paragraph(_pdf_text(("الإجمالي: " if RTL else "Total: ") + str(len(df)), bidi_ok), body_style))
        story.append(Spacer(1, 5))
        if df.empty:
            story.append(Paragraph(_pdf_text("لا توجد بيانات" if RTL else "No data", bidi_ok), body_style))
            continue
        cols = [c for c in specs[sec][0] if c in df.columns]
        out = df[cols].copy().head(500)
        for c in out.columns:
            out[c] = out[c].fillna("").astype(str).map(lambda x: x[:220])
        data = [[Paragraph(_pdf_text(c, bidi_ok), body_style) for c in cols]]
        for row in out.values.tolist():
            data.append([Paragraph(_pdf_text(x, bidi_ok), body_style) for x in row])
        page_w = landscape(A4)[0] - 24*mm
        widths = [page_w / max(1, len(cols))] * len(cols)
        t = Table(data, colWidths=widths, repeatRows=1)
        t.setStyle(TableStyle([
            ("BACKGROUND", (0,0), (-1,0), colors.HexColor("#DBEAFE")),
            ("FONTNAME", (0,0), (-1,-1), font_name),
            ("GRID", (0,0), (-1,-1), .3, colors.HexColor("#CBD5E1")),
            ("VALIGN", (0,0), (-1,-1), "TOP"),
            ("ROWBACKGROUNDS", (0,1), (-1,-1), [colors.white, colors.HexColor("#F8FAFC")]),
            ("LEFTPADDING", (0,0), (-1,-1), 3), ("RIGHTPADDING", (0,0), (-1,-1), 3),
            ("TOPPADDING", (0,0), (-1,-1), 3), ("BOTTOMPADDING", (0,0), (-1,-1), 3),
        ]))
        story.append(t)
        if len(df) > len(out):
            story.append(Spacer(1, 4))
            story.append(Paragraph(_pdf_text(("PDF يعرض أول 500 صف؛ ملف Excel يحتوي جميع الصفوف." if RTL else
                                             "PDF shows the first 500 rows; Excel contains all rows."), bidi_ok), body_style))
    def _footer(canvas, doc_obj):
        canvas.saveState(); canvas.setFont(font_name, 7); canvas.setFillColor(colors.HexColor("#64748B"));
        footer=("Reviews Tracker {} · {} · Page {}".format(VERSION, datetime.now().strftime("%Y-%m-%d %H:%M"), doc_obj.page)); canvas.drawCentredString(landscape(A4)[0]/2, 6*mm, footer); canvas.restoreState()
    doc.build(story, onFirstPage=_footer, onLaterPages=_footer)
    return buf.getvalue()


def dataframe_to_pdf_v39(df,title="Reviews Tracker Export",max_rows=1000,columns=None):
    """Export a user-facing DataFrame to a real PDF using the existing Arabic-safe PDF pipeline."""
    from reportlab.lib import colors
    from reportlab.lib.pagesizes import A4, landscape
    from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
    from reportlab.lib.enums import TA_RIGHT, TA_LEFT
    from reportlab.lib.units import mm
    from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle
    frame=df.copy() if isinstance(df,pd.DataFrame) else pd.DataFrame(df or [])
    rtl=bool(globals().get("RTL", bool(re.search(r"[\u0600-\u06FF]", str(title or "")))))
    preferred=["name","rating","review_count","distance_km","category","address","phone","mobile","email","website","last_updated"]
    if columns is None:
        chosen=[c for c in preferred if c in frame.columns]
        if not chosen: chosen=list(frame.columns[:10])
    else: chosen=[c for c in columns if c in frame.columns]
    if not chosen: chosen=list(frame.columns[:10])
    out=frame[chosen].head(max(1,int(max_rows))).copy() if chosen else pd.DataFrame()
    for c in out.columns:
        out[c]=out[c].fillna("").astype(str).map(lambda x:x[:260])
    buf=io.BytesIO(); font_name,bidi_ok=_report_font(); styles=getSampleStyleSheet()
    body=ParagraphStyle("V39Body",parent=styles["BodyText"],fontName=font_name,fontSize=7.2,leading=9,
                        alignment=TA_RIGHT if rtl else TA_LEFT)
    h=ParagraphStyle("V39Title",parent=styles["Title"],fontName=font_name,fontSize=16,leading=20,
                     alignment=TA_RIGHT if rtl else TA_LEFT,textColor=colors.HexColor("#0F172A"))
    doc=SimpleDocTemplate(buf,pagesize=landscape(A4),rightMargin=9*mm,leftMargin=9*mm,topMargin=10*mm,bottomMargin=10*mm,
                          title=str(title or "Reviews Tracker Export"))
    story=[Paragraph(_pdf_text(title or "Reviews Tracker Export",bidi_ok),h),
           Paragraph(_pdf_text(("عدد الصفوف: " if rtl else "Rows: ")+str(len(frame)),bidi_ok),body),Spacer(1,6)]
    if out.empty:
        story.append(Paragraph(_pdf_text("لا توجد بيانات" if rtl else "No data",bidi_ok),body))
    else:
        data=[[Paragraph(_pdf_text(c,bidi_ok),body) for c in out.columns]]
        for row in out.values.tolist(): data.append([Paragraph(_pdf_text(x,bidi_ok),body) for x in row])
        page_w=landscape(A4)[0]-18*mm; widths=[page_w/max(1,len(out.columns))]*len(out.columns)
        table=Table(data,colWidths=widths,repeatRows=1)
        table.setStyle(TableStyle([("BACKGROUND",(0,0),(-1,0),colors.HexColor("#DBEAFE")),("GRID",(0,0),(-1,-1),.3,colors.HexColor("#CBD5E1")),
                                   ("FONTNAME",(0,0),(-1,-1),font_name),("VALIGN",(0,0),(-1,-1),"TOP"),("ROWBACKGROUNDS",(0,1),(-1,-1),[colors.white,colors.HexColor("#F8FAFC")]),
                                   ("LEFTPADDING",(0,0),(-1,-1),2.5),("RIGHTPADDING",(0,0),(-1,-1),2.5),("TOPPADDING",(0,0),(-1,-1),2.5),("BOTTOMPADDING",(0,0),(-1,-1),2.5)]))
        story.append(table)
        if len(frame)>len(out): story.append(Spacer(1,4)); story.append(Paragraph(_pdf_text(("يعرض PDF أول {} صف؛ استخدم Excel/CSV لجميع البيانات." if rtl else "PDF shows the first {} rows; use Excel/CSV for all data.").format(len(out)),bidi_ok),body))
    version=str(globals().get("VERSION","40.0.0"))
    def footer(canvas,doc_obj):
        canvas.saveState(); canvas.setFont(font_name,7); canvas.setFillColor(colors.HexColor("#64748B"));
        canvas.drawCentredString(landscape(A4)[0]/2,5*mm,"Reviews Tracker {} · Page {}".format(version,doc_obj.page)); canvas.restoreState()
    doc.build(story,onFirstPage=footer,onLaterPages=footer)
    return buf.getvalue()


@st.cache_data(show_spinner=False,max_entries=16)
def dataframe_to_pdf_cached_v39(df,title="Reviews Tracker Export",max_rows=1000):
    return dataframe_to_pdf_v39(df,title,max_rows=max_rows)




def render_complete_export_controls_v40(db,ws_id,all_branches_df,visible_df=None,current_pk=None,key_prefix="export"):
    """Prepare complete v40 PDF/Excel on demand without removing legacy exports."""
    all_df=all_branches_df.copy() if isinstance(all_branches_df,pd.DataFrame) else pd.DataFrame(all_branches_df or [])
    vis=visible_df.copy() if isinstance(visible_df,pd.DataFrame) else all_df.copy()
    scope_options=["current","visible","all"]
    scope=st.selectbox("نطاق التصدير" if RTL else "Export scope",scope_options,
        format_func=lambda x:{"current":"الفرع الحالي","visible":"النتائج الظاهرة","all":"جميع الفروع"}.get(x,x) if RTL else {"current":"Current branch","visible":"Visible results","all":"All branches"}.get(x,x),
        index=1 if len(vis)>1 else 0,key="v40_complete_export_scope_"+str(key_prefix))
    include_reviews=st.checkbox("تضمين التعليقات كاملة" if RTL else "Include full reviews",value=True,key="v40_include_reviews_"+str(key_prefix))
    if scope=="current":
        if current_pk and not all_df.empty and "pk" in all_df.columns:
            frame=all_df[all_df["pk"].astype(str)==str(current_pk)].copy()
        elif current_pk and not vis.empty and "pk" in vis.columns:
            frame=vis[vis["pk"].astype(str)==str(current_pk)].copy()
        else: frame=pd.DataFrame(columns=all_df.columns if len(all_df.columns) else vis.columns)
    elif scope=="all": frame=all_df.copy()
    else: frame=vis.copy()
    st.caption(("سيشمل التصدير: التقييم، التاريخ، العنوان، الموقع، الاتصال{} والبيانات المتاحة الأخرى." if RTL else "Export includes rating, history, address, location, contact{} and other available fields.").format("، التعليقات" if include_reviews and RTL else ", reviews" if include_reviews else ""))
    bundle_key="v40_complete_export_bundle_"+str(key_prefix)
    if st.button("📦 "+("تجهيز PDF وExcel الشامل" if RTL else "Prepare complete PDF & Excel"),use_container_width=True,key="v40_prepare_complete_"+str(key_prefix),disabled=frame.empty):
        with st.spinner("جاري بناء التصدير الشامل..." if RTL else "Building complete export..."):
            pks=frame["pk"].dropna().astype(str).tolist() if "pk" in frame.columns else []
            ds=complete_export_datasets_v40(db,ws_id,frame,pks,include_reviews=include_reviews)
            st.session_state[bundle_key]={"pdf":build_complete_export_pdf_v40(ds,"تقرير Reviews Tracker الشامل" if RTL else "Reviews Tracker Complete Report"),
                "excel":build_complete_export_excel_v40(ds),"branches":len(ds.get("branches",[])),"reviews":len(ds.get("reviews",[])),"scope":scope,"include_reviews":include_reviews}
    bundle=st.session_state.get(bundle_key)
    if isinstance(bundle,dict):
        c1,c2=st.columns(2)
        stamp=datetime.now().strftime("%Y%m%d_%H%M")
        c1.download_button("📄 "+("PDF شامل" if RTL else "Complete PDF"),bundle["pdf"],file_name="ReviewsTracker_v40_Complete_{}.pdf".format(stamp),mime="application/pdf",use_container_width=True,key="v40_pdf_download_"+str(key_prefix))
        c2.download_button("📊 "+("Excel شامل" if RTL else "Complete Excel"),bundle["excel"],file_name="ReviewsTracker_v40_Complete_{}.xlsx".format(stamp),mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",use_container_width=True,key="v40_excel_download_"+str(key_prefix))
        st.success(("جاهز: {} فرع · {} تعليق" if RTL else "Ready: {} branches · {} reviews").format(bundle.get("branches",0),bundle.get("reviews",0)))

def run_performance_benchmark(db, ws_id):
    tests=[]
    def run(name,func):
        start=time.perf_counter(); ok=True; detail=""
        try:
            value=func(); detail=("{} rows".format(len(value)) if isinstance(value,pd.DataFrame) else str(value)[:160] if value is not None else "")
        except Exception as e: ok=False; detail=str(e)[:180]
        ms=(time.perf_counter()-start)*1000; tests.append({"Test":name,"Status":"OK" if ok else "ERROR","ms":round(ms,2),"Detail":detail}); db.log_performance(ws_id,name,ms,detail)
    def ping_db():
        with db._c() as c:return c.execute("SELECT 1").fetchone()[0]
    def fk_check():
        with db._c() as c:return c.execute("PRAGMA foreign_key_check").fetchall()
    def query_plan():
        with db._c() as c:
            rows=c.execute("EXPLAIN QUERY PLAN SELECT id FROM reviews WHERE workspace_id=? AND workflow_status=? ORDER BY fetched_at DESC LIMIT 100",(ws_id,"new")).fetchall()
            return " | ".join(str(tuple(r)) for r in rows)[:500]
    run("SQLite SELECT 1",ping_db)
    run("Branch stats aggregate",lambda:db.list_companies_with_stats(ws_id))
    run("Load branches",lambda:db.list_companies(ws_id))
    since,_=period_bounds()
    run("Load reviews period",lambda:db.all_reviews(ws_id,since))
    run("Reviews count",lambda:db.reviews_count(ws_id,since))
    run("Reviews page 100",lambda:db.reviews_page(ws_id,since=since,limit=100,offset=0))
    run("Load monitored",lambda:db.list_monitored(ws_id))
    run("Load alerts",lambda:db.list_alerts(ws_id,status="all",limit=500))
    run("Search jobs",lambda:db.list_search_jobs(ws_id,100))
    run("Indexed query plan",query_plan)
    run("DB quick integrity",lambda:db.integrity())
    run("Foreign key check",fk_check)
    df=pd.DataFrame(tests); ok_df=df[df["Status"]=="OK"]
    avg=float(ok_df["ms"].mean()) if not ok_df.empty else 0; worst=float(ok_df["ms"].max()) if not ok_df.empty else 0
    p95=float(ok_df["ms"].quantile(.95)) if not ok_df.empty else 0
    errors=int((df["Status"]!="OK").sum()) if not df.empty else 0
    grade="Excellent" if errors==0 and p95<120 else "Good" if errors==0 and p95<350 else "Fair" if errors==0 and p95<1000 else "Needs attention"
    return df,{"avg_ms":round(avg,2),"worst_ms":round(worst,2),"p95_ms":round(p95,2),"errors":errors,"grade":grade,"timestamp":datetime.now().isoformat(timespec="seconds")}


def backup_create():
    result=create_verified_backup_v39(DB_PATH,BACKUP_DIR,prefix="reviews")
    return Path(result["path"])


def backup_list():
    return sorted(BACKUP_DIR.glob("*.db"),
                  key=lambda p: p.stat().st_mtime, reverse=True)


def api_key():
    if API_KEY_FILE.exists():
        return API_KEY_FILE.read_text(encoding="utf-8").strip()
    return ""


# ═══════════════════════════════════════════════════════════════════════
#  Auto-Sync
# ═══════════════════════════════════════════════════════════════════════
class AutoSyncWorker:
    def __init__(self, db):
        self.db = db
        self._stop = threading.Event()
        self._thread = None
        self._last_run = None
        self._lock = threading.Lock()

    def start(self):
        with self._lock:
            if self._thread and self._thread.is_alive(): return False
            self._stop.clear()
            self._thread = threading.Thread(target=self._run, daemon=True)
            self._thread.start()
            return True

    def stop(self):
        with self._lock: self._stop.set()

    def is_running(self):
        return bool(self._thread and self._thread.is_alive())

    def last_run(self):
        return self._last_run

    def enqueue_workspace(self, ws_id=1, force=False):
        return self.db.enqueue_monitored(ws_id,force=force,source="manual" if force else "monitor")

    def process_once(self, limit=5, ws_id=None):
        done=[]
        for _ in range(max(1,int(limit))):
            if self._stop.is_set() and self.is_running(): break
            job=self.db.claim_next_sync(ws_id)
            if not job: break
            qid=int(job["id"]); wid=int(job.get("workspace_id") or 1); pk=str(job.get("pk") or "")
            branch=self.db.get_company(pk,wid) or {}
            target=str(branch.get("maps_url") or branch.get("name") or pk)
            use_url=bool(branch.get("maps_url"))
            def progress(ev, _qid=qid): self.db.update_sync_queue_progress(_qid,ev)
            try:
                _pk,info,new,dup,err=scrape_and_store(self.db,target,60,use_url,wid,target_pk=pk,queue_id=qid,progress_cb=progress)
                state=self.db.branch_sync_state(pk,wid) or {}; changes=state.get("changed_fields") or "[]"
                if err:
                    self.db.finish_sync_queue(qid,"error",new_reviews=new,duplicate_reviews=dup,changed_fields=changes,error=err)
                else:
                    self.db.finish_sync_queue(qid,"ok",new_reviews=new,duplicate_reviews=dup,changed_fields=changes)
                done.append({"id":qid,"pk":pk,"ok":not bool(err),"new":new,"duplicate":dup,"error":err})
            except Exception as e:
                self.db.finish_sync_queue(qid,"error",error=str(e))
                self.db.update_branch_sync(pk,wid,status="error",error=str(e))
                self.db.record_monitor_result(pk,wid,False,str(e))
                done.append({"id":qid,"pk":pk,"ok":False,"new":0,"duplicate":0,"error":str(e)})
        return done

    def _run(self):
        while not self._stop.is_set():
            try: self._tick()
            except Exception: pass
            self._stop.wait(60)

    def _tick(self):
        self._last_run=datetime.now()
        try:
            workspaces=self.db.list_workspaces()
            if not workspaces.empty:
                for _,ws in workspaces.iterrows():
                    wid=int(ws["id"]); policy=self.db.get_sync_policy(wid)
                    if not int(policy.get("enabled",1)): continue
                    batch=max(1,int(policy.get("batch_limit") or 5))
                    self.db.enqueue_scheduled_due(wid,limit=batch,now=datetime.now())
                    self.process_once(limit=batch,ws_id=wid)
                    self.db.archive_sync_jobs(wid,archive_days=int(policy.get("archive_days") or 30),batch=max(100,batch*20))
        except Exception: pass


# ═══════════════════════════════════════════════════════════════════════
#  UI
# ═══════════════════════════════════════════════════════════════════════
st.set_page_config(page_title="Reviews Tracker {}".format(VERSION), page_icon="◆",
                   layout="wide", initial_sidebar_state="expanded")

defaults = {"lang":"ar","theme":"light","page":"dashboard","period_key":"d30",
            "period_custom_days":30,"selected_branch_pk":None,"result_view":"table",
            "logged_in":False,"username":None,"user":None,"workspace_id":1,
            "command_query":"","selected_company":None,"selected_review":None,
            "use_ai":False}
for k, v in defaults.items():
    if k not in st.session_state: st.session_state[k] = v

# v22.0 is intentionally LIGHT-ONLY.  Force this on every rerun so an old
# session/database preference cannot bring back dark widgets or mixed colors.
st.session_state.theme = "light"
lang = st.session_state.lang
RTL = (lang in ("ar","ur"))
theme = "light"

C = {"bg":"#F4F7FB","panel":"#FFFFFF","panel2":"#F8FAFD","text":"#111827",
     "muted":"#4B5B6D","border":"#C8D4E3","blue":"#1D4ED8","blue2":"#2563EB",
     "green":"#15803D","amber":"#A65B00","red":"#B42318","focus":"#2563EB",
     "hover":"#EAF1FF","soft_blue":"#EFF6FF","soft_green":"#ECFDF3",
     "soft_amber":"#FFF7E6","soft_red":"#FEF3F2"}

db = get_db()
try:
    if not st.session_state.get("v39_runtime_recovery_done"):
        st.session_state["v39_runtime_recovery"] = recover_stale_search_jobs_v39(DB_PATH, SEARCH_SESSION_DIR, 120)
        st.session_state["v39_runtime_recovery_done"] = True
except Exception as _v39_recovery_exc:
    try:
        _v39p=safe_error_payload_v38(_v39_recovery_exc,"startup.v39_recovery"); record_local_error_v38(_v39p)
    except Exception: pass

# ─── Direct open / no login ────────────────────────────────────────────
# This desktop/local edition opens immediately. Authentication classes remain
# available internally for compatibility, but the login screen is bypassed.
def _direct_open_user():
    fallback = {"id": 1, "username": "admin", "display_name": "مدير النظام",
                "role": "admin", "workspace_id": 1, "email": "", "active": 1}
    try:
        udf = db.list_users()
        if udf is not None and not udf.empty:
            active = udf[pd.to_numeric(udf.get("active"), errors="coerce").fillna(1) == 1]
            if not active.empty:
                admins = active[active.get("role", "").astype(str).str.lower() == "admin"]
                row = admins.iloc[0] if not admins.empty else active.iloc[0]
                out = dict(row)
                out["workspace_id"] = int(out.get("workspace_id") or 1)
                return out
    except Exception:
        pass
    return fallback

_AUTO_USER = _direct_open_user()
st.session_state.logged_in = True
st.session_state.username = _AUTO_USER.get("username", "admin")
st.session_state.user = _AUTO_USER
st.session_state.workspace_id = int(_AUTO_USER.get("workspace_id") or 1)
if not st.session_state.get("_last_page_loaded"):
    # New desktop sessions open the approved dashboard. The previously visited
    # page remains available through the explicit "Resume last page" action.
    try:
        st.session_state.rt42_previous_page = db.get_pref(
            "last_page_{}".format(st.session_state.workspace_id), "dashboard")
    except Exception:
        st.session_state.rt42_previous_page = "dashboard"
    st.session_state.page = "dashboard"
    st.session_state._last_page_loaded = True

st.markdown("""<style>
/* v38 general hardening / readability polish */
.rt-v36-contact-grid{gap:8px!important;align-items:stretch}.rt-v36-contact-chip{min-width:0;overflow-wrap:anywhere}
.rt-v30-result-card,.rt-v31-detail-pane,.rt-v31-navigator{box-shadow:0 1px 2px rgba(15,23,42,.04)}
[data-testid="stAlert"]{border-radius:10px} [data-testid="stDataFrame"]{border-radius:10px;overflow:hidden}
@media(max-width:900px){.rt-v36-contact-grid{grid-template-columns:1fr!important}.rt-v30-result-meta{gap:6px!important}}
</style>""",unsafe_allow_html=True)

# ─── Brand settings injection (v20.3) ─────────────────────────────────
BRAND = db.get_brand() or {}
BRAND_NAME = BRAND.get("brand_name") or "Reviews Tracker"
BRAND_TAGLINE = BRAND.get("brand_tagline") or "Reputation Platform"
BRAND_PRIMARY = BRAND.get("primary_color") or C["blue"]
BRAND_ACCENT = BRAND.get("accent_color") or C["green"]
BRAND_LOGO = BRAND.get("logo_url") or ""

# Prefer Windows/system fonts so Arabic remains sharp even when internet fonts are unavailable.
FF = ("Tahoma,'Segoe UI',Arial,sans-serif" if RTL
      else "'Segoe UI',Tahoma,Arial,sans-serif")

# Persistent visual-comfort preferences. Defaults deliberately favor readability.
_ui_pref_key = "ui_visual_{}".format(st.session_state.workspace_id)
_ui_pref = db.get_pref(_ui_pref_key, {}) or {}
try:
    UI_FONT_SCALE = max(.90, min(1.30, float(_ui_pref.get("font_scale", 1.12))))
except Exception:
    UI_FONT_SCALE = 1.12
UI_DENSITY = str(_ui_pref.get("density", "compact"))
UI_HIGH_CONTRAST = bool(_ui_pref.get("high_contrast", True))
BASE_FONT_PX = round(15.4 * UI_FONT_SCALE, 2)
CONTROL_H = 42 if UI_DENSITY == "comfortable" else 38

_contrast_border = C["border"] if UI_HIGH_CONTRAST else "#DCE3EC"
_css = f"""<style>
*{{box-sizing:border-box}}
html,body,.stApp{{font-family:{FF}!important;direction:{'rtl' if RTL else 'ltr'};font-size:{BASE_FONT_PX}px!important;line-height:1.6;color-scheme:light!important}}
body{{letter-spacing:0!important}}
.stApp{{background:{C['bg']};color:{C['text']}}}
[data-testid="stMain"]{{overflow-x:hidden}}
[data-testid="stMainBlockContainer"]{{max-width:none!important;width:100%!important;padding:.55rem .75rem 2rem;overflow-x:hidden;margin-inline:auto}}
.block-container>div{{gap:.22rem}}
[data-testid="stVerticalBlock"]{{gap:.42rem!important}}
[data-testid="stHorizontalBlock"]{{gap:.55rem!important}}

/* Sidebar */
[data-testid="stSidebar"]{{background:#FFFFFF;border-inline-end:1px solid {_contrast_border};min-width:238px!important;max-width:238px!important;box-shadow:0 0 20px rgba(15,23,42,.06)}}
[data-testid="stSidebar"] *{{writing-mode:horizontal-tb!important}}
[data-testid="stSidebar"] [data-testid="stSidebarUserContent"]{{padding-top:.55rem}}
[data-testid="stSidebar"] .stButton>button{{justify-content:flex-start!important;text-align:start!important;width:100%;min-height:{CONTROL_H}px}}
[data-testid="stSidebar"] details{{border:1px solid {_contrast_border};border-radius:11px;background:#FFFFFF;margin:.45rem 0;overflow:hidden}}
[data-testid="stSidebar"] details summary{{font-weight:800!important;padding:.35rem .2rem}}
[data-testid="stSidebar"] details[open] summary{{color:{C['blue']}!important}}
.rt-brand{{padding:5px 3px 13px;border-bottom:1px solid {_contrast_border};margin-bottom:10px}}
.rt-brand h2{{margin:0;color:{C['text']};font-size:22px;font-weight:800;letter-spacing:-.2px}}
.rt-brand small{{color:{C['muted']};font-weight:600}}

/* Page hierarchy */
.rt-breadcrumb{{font-size:.78rem;color:{C['muted']};font-weight:700;margin:2px 2px 7px}}
.rt-page-head{{display:flex;justify-content:space-between;align-items:center;gap:9px;margin:0 0 10px;padding:11px 14px;background:{C['panel']};border:1px solid {_contrast_border};border-radius:15px;box-shadow:0 3px 16px rgba(0,0,0,.055)}}
.rt-page-head h2{{margin:0;color:{C['text']};font-size:1.65rem;line-height:1.25;font-weight:850}}
.rt-page-head p{{margin:3px 0 0;color:{C['muted']};font-size:.9rem;font-weight:650;line-height:1.55;max-width:980px}}
.rt-section-label{{font-size:.82rem;color:{C['muted']};font-weight:850;letter-spacing:0;margin:2px 0 5px}}
hr{{border:none!important;border-top:1px solid {_contrast_border}!important;margin:.65rem 0!important}}
h1,h2,h3,h4,h5,h6{{color:{C['text']}!important;font-family:{FF}!important;letter-spacing:0!important}}
h3{{font-size:1.18rem!important}} h4{{font-size:1.06rem!important}}
p,li,label,input,textarea,button{{font-family:{FF}!important}}
/* Never override Streamlit's Material Symbols font: doing so renders icon names as text. */
[data-testid="stIconMaterial"],.material-symbols-rounded,.material-symbols-outlined{{font-family:"Material Symbols Rounded","Material Symbols Outlined"!important;font-weight:normal!important;font-style:normal!important;letter-spacing:normal!important;text-transform:none!important;white-space:nowrap!important;word-wrap:normal!important;direction:ltr!important}}

/* Cards / KPI */
.rt-card,.rt-panel{{background:{C['panel']};border:1px solid {_contrast_border};border-radius:12px;padding:12px;margin-bottom:8px;box-shadow:0 2px 12px rgba(2,6,23,.055)}}
.rt-kpi{{background:{C['panel']};border:1px solid {_contrast_border};border-radius:12px;padding:10px 12px;min-height:88px;box-shadow:0 2px 10px rgba(2,6,23,.045)}}
.rt-kpi .label{{color:{C['muted']};font-size:.82rem;font-weight:800;line-height:1.4}}
.rt-kpi .value{{font-size:1.72rem;font-weight:850;color:{C['text']};margin-top:7px;line-height:1.2}}
.rt-kpi .sub{{font-size:.76rem;color:{C['muted']};font-weight:650;margin-top:6px;line-height:1.4}}
[data-testid="stMetric"]{{background:{C['panel']};border:1px solid {_contrast_border};border-radius:11px;padding:9px 11px;min-height:78px}}
[data-testid="stMetricLabel"]{{color:{C['muted']}!important;font-weight:800!important}}
[data-testid="stMetricValue"]{{color:{C['text']}!important;font-weight:850!important}}

/* Inputs */
[data-testid="stForm"]{{background:#FFFFFF;border:1px solid {_contrast_border};border-radius:13px;padding:12px;box-shadow:0 3px 14px rgba(15,23,42,.045)}}
/* Native Streamlit/BaseWeb controls are forced light so browser/OS dark preferences cannot leak in. */
[data-baseweb="input"]>div,[data-baseweb="select"]>div,[data-baseweb="base-input"],
[data-baseweb="textarea"]>div,[data-testid="stTextInput"]>div>div,[data-testid="stNumberInput"]>div>div{{background:#FFFFFF!important;border:1px solid {_contrast_border}!important;border-radius:10px!important;color:{C['text']}!important;min-height:{CONTROL_H}px;box-shadow:none!important}}
[data-baseweb="input"]>div:focus-within,[data-baseweb="select"]>div:focus-within,[data-baseweb="base-input"]:focus-within,
[data-baseweb="textarea"]>div:focus-within{{border-color:{C['focus']}!important;box-shadow:0 0 0 3px rgba(37,99,235,.14)!important}}
input,textarea,[data-baseweb="input"] input,[data-baseweb="select"] input,[data-baseweb="base-input"] input,
[data-testid="stTextInput"] input,input[type="text"],input[type="search"]{{background:#FFFFFF!important;color:#111827!important;-webkit-text-fill-color:#111827!important;font-size:1rem!important;font-weight:750!important;caret-color:#1D4ED8!important;opacity:1!important;text-shadow:none!important}}
[data-testid="stTextInput"] input:focus,[data-baseweb="input"] input:focus,input[type="text"]:focus,input[type="search"]:focus{{color:#111827!important;-webkit-text-fill-color:#111827!important;background:#FFFFFF!important;opacity:1!important}}
[data-testid="stTextInput"] input::selection,input[type="text"]::selection,input[type="search"]::selection{{background:#BFDBFE!important;color:#111827!important;-webkit-text-fill-color:#111827!important}}
html[dir="rtl"] [data-testid="stTextInput"] input,html[dir="rtl"] input[type="text"],html[dir="rtl"] input[type="search"]{{direction:rtl!important;text-align:right!important}}
input:-webkit-autofill,input:-webkit-autofill:hover,input:-webkit-autofill:focus{{-webkit-text-fill-color:#111827!important;-webkit-box-shadow:0 0 0 1000px #FFFFFF inset!important;box-shadow:0 0 0 1000px #FFFFFF inset!important}}
input:disabled,textarea:disabled,[aria-disabled="true"]{{background:#F1F5F9!important;color:#64748B!important;-webkit-text-fill-color:#64748B!important;opacity:1!important}}
input::placeholder,textarea::placeholder{{color:#64748B!important;-webkit-text-fill-color:#64748B!important;opacity:1!important}}
[data-baseweb="select"] span{{color:{C['text']}!important}}
[data-baseweb="select"] svg{{fill:{C['text']}!important}}
input,textarea,select{{color-scheme:light!important}}
[data-testid="stNumberInput"] button{{background:#F8FAFD!important;color:{C['text']}!important;border-color:{_contrast_border}!important}}
[data-baseweb="popover"] ul,[role="listbox"]{{background:#FFFFFF!important;color:{C['text']}!important;border:1px solid {_contrast_border}!important}}
[role="option"]{{background:#FFFFFF!important;color:{C['text']}!important}}
[role="option"]:hover,[aria-selected="true"][role="option"]{{background:{C['hover']}!important;color:{C['text']}!important}}
.stTextInput label,.stNumberInput label,.stSelectbox label,.stMultiSelect label,.stCheckbox label,.stRadio label,.stTextArea label{{font-size:.88rem!important;font-weight:800!important;color:{C['text']}!important}}

/* v25.0 compact field rhythm: less dead space without shrinking text. */
.stTextInput,.stNumberInput,.stSelectbox,.stMultiSelect,.stDateInput,.stTextArea,.stRadio,.stCheckbox{{margin-bottom:-.12rem!important}}
[data-testid="stWidgetLabel"]{{margin-bottom:.12rem!important}}
.rt-compact-search{{padding:8px 10px;margin:4px 0 7px;border:1px solid {_contrast_border};border-radius:12px;background:#FFFFFF;box-shadow:0 1px 6px rgba(15,23,42,.035)}}
.rt-compact-search b{{color:{C['text']};font-size:.9rem}}
.rt-result-head{{margin-top:5px!important;margin-bottom:5px!important}}

/* Buttons: strong hierarchy and high-visibility focus */
.stButton>button,.stDownloadButton>button,.stLinkButton>a,[data-testid="stBaseButton-secondary"]{{min-height:{CONTROL_H}px;border-radius:10px!important;border:1px solid #B8C7D9!important;background:#FFFFFF!important;color:{C['text']}!important;font-size:.93rem!important;font-weight:800!important;box-shadow:0 1px 2px rgba(15,23,42,.04)!important;transition:background .12s ease,border-color .12s ease,transform .12s ease,box-shadow .12s ease!important}}
.stButton>button:hover,.stDownloadButton>button:hover,.stLinkButton>a:hover,[data-testid="stBaseButton-secondary"]:hover{{background:{C['hover']}!important;border-color:{C['focus']}!important;color:{C['text']}!important;transform:translateY(-1px);box-shadow:0 3px 8px rgba(37,99,235,.10)!important}}
.stButton>button:focus-visible,.stDownloadButton>button:focus-visible,.stLinkButton>a:focus-visible{{outline:3px solid rgba(37,99,235,.25)!important;outline-offset:2px!important}}
.stButton>button[kind="primary"],.stDownloadButton>button[kind="primary"],[data-testid="stBaseButton-primary"]{{background:#1D4ED8!important;color:#FFFFFF!important;-webkit-text-fill-color:#FFFFFF!important;border-color:#1D4ED8!important;box-shadow:0 3px 9px rgba(29,78,216,.20)!important}}
.stButton>button[kind="primary"]:hover,.stDownloadButton>button[kind="primary"]:hover,[data-testid="stBaseButton-primary"]:hover{{background:#1E40AF!important;border-color:#1E40AF!important;color:#FFFFFF!important;-webkit-text-fill-color:#FFFFFF!important;filter:none!important}}
[data-testid="stBaseButton-primary"] p,[data-testid="stBaseButton-primary"] span,.stButton>button[kind="primary"] p,.stButton>button[kind="primary"] span{{color:#FFFFFF!important;-webkit-text-fill-color:#FFFFFF!important}}
[data-testid="stBaseButton-secondary"] p,[data-testid="stBaseButton-secondary"] span{{color:{C['text']}!important;-webkit-text-fill-color:{C['text']}!important}}
.stButton>button:disabled,.stDownloadButton>button:disabled{{opacity:1!important;background:#E5E7EB!important;color:#94A3B8!important;border-color:#D1D5DB!important;transform:none!important;cursor:not-allowed!important;box-shadow:none!important}}
.rt-danger button{{background:{C['red']}!important;color:#fff!important;border-color:{C['red']}!important}}

/* Expander / tabs / radio */
[data-testid="stExpander"]{{background:{C['panel']};border:1px solid {_contrast_border}!important;border-radius:12px!important}}
[data-testid="stExpander"] summary{{font-weight:800!important;color:{C['text']}!important}}
[data-baseweb="tab-list"]{{gap:5px;border-bottom:1px solid {_contrast_border}}}
[data-baseweb="tab"]{{font-weight:800!important;color:{C['muted']}!important;padding:.65rem .85rem!important}}
[aria-selected="true"][data-baseweb="tab"]{{color:{C['blue']}!important;border-bottom-color:{C['blue']}!important}}
div[role="radiogroup"] label{{background:#FFFFFF;border:1px solid {_contrast_border};border-radius:10px;padding:8px 11px;margin:2px;color:{C['text']}!important}}
div[role="radiogroup"] label:has(input:checked){{background:{C['soft_blue']};border-color:{C['blue']};box-shadow:0 0 0 1px {C['blue']} inset}}

/* Tables */
[data-testid="stDataFrame"],[data-testid="stDataEditor"]{{border:1px solid {_contrast_border};border-radius:12px;overflow:hidden;background:{C['panel']};box-shadow:0 1px 4px rgba(2,6,23,.035)}}
[data-testid="stDataFrame"] *,[data-testid="stDataEditor"] *{{font-size:.89rem!important}}
[data-testid="stDataFrame"] [role="columnheader"],[data-testid="stDataEditor"] [role="columnheader"]{{font-weight:850!important;color:{C['text']}!important}}
[data-testid="stDataFrame"] [role="gridcell"],[data-testid="stDataEditor"] [role="gridcell"]{{line-height:1.45!important}}

/* Messages and badges */
.stCaption{{color:{C['muted']}!important;font-size:.79rem!important;font-weight:600!important}}
[data-testid="stAlert"]{{border-radius:12px!important;border-width:1px!important;font-weight:700!important;color:{C['text']}!important}}
[data-testid="stAlert"] *{{color:inherit!important}}
[data-testid="stAlert"][data-baseweb="notification"]{{background:#FFFFFF!important}}
div[data-testid="stAlert"]:has([data-testid="stAlertContentWarning"]){{background:{C['soft_amber']}!important;border-color:#F3C969!important;color:#6B3A00!important}}
div[data-testid="stAlert"]:has([data-testid="stAlertContentError"]){{background:{C['soft_red']}!important;border-color:#FDA29B!important;color:#7A271A!important}}
div[data-testid="stAlert"]:has([data-testid="stAlertContentSuccess"]){{background:{C['soft_green']}!important;border-color:#86D7A5!important;color:#14532D!important}}
div[data-testid="stAlert"]:has([data-testid="stAlertContentInfo"]){{background:{C['soft_blue']}!important;border-color:#93B4EF!important;color:#1E3A8A!important}}
.rt-success{{border-inline-start:4px solid {C['green']}}}.rt-warning{{border-inline-start:4px solid {C['amber']}}}.rt-danger-card{{border-inline-start:4px solid {C['red']}}}
.rt-review{{background:{C['panel']};border:1px solid {_contrast_border};border-radius:14px;padding:15px 17px;margin:11px 0}}
.rt-review .text{{font-size:.95rem;line-height:1.85;color:{C['text']};margin-top:9px}}
.rt-badge{{display:inline-flex;padding:4px 9px;border-radius:999px;font-size:.73rem;font-weight:850;line-height:1.2}}
.b-green{{background:#DCFCE7;color:#14532D}}.b-red{{background:#FEE2E2;color:#7F1D1D}}.b-amber{{background:#FEF3C7;color:#78350F}}.b-blue{{background:#DBEAFE;color:#1E3A8A}}.b-gray{{background:#E2E8F0;color:#334155}}
.rt-hero{{background:linear-gradient(135deg,#0F2E66 0%,{C['blue']} 100%);border-radius:18px;padding:22px 24px;color:#fff;margin-bottom:17px;box-shadow:0 6px 22px rgba(15,46,102,.18)}}
.rt-hero h1{{margin:0;color:#fff!important;font-size:1.7rem}}.rt-hero p{{margin:6px 0 0;color:#EAF2FF;font-weight:600}}
.rt-step{{display:flex;align-items:center;gap:10px;margin:4px 0 11px;padding:10px 12px;border-radius:10px;background:{C['panel2']};border:1px solid {_contrast_border};font-weight:800;color:{C['text']}}}
.rt-step-num{{display:inline-grid;place-items:center;min-width:27px;height:27px;border-radius:50%;background:{C['blue']};color:#fff;font-size:.78rem;font-weight:900}}
.rt-toolbar{{background:{C['panel']};border:1px solid {_contrast_border};border-radius:13px;padding:12px;margin:8px 0 13px}}
.rt-result-head{{display:flex;align-items:center;justify-content:space-between;gap:12px;flex-wrap:wrap;background:{C['panel']};border:1px solid {_contrast_border};border-radius:13px;padding:12px 14px;margin:9px 0 12px}}
.rt-result-head strong{{font-size:1rem;color:{C['text']}}}
.rt-result-meta{{font-size:.8rem;color:{C['muted']};font-weight:700}}
.rt-action-strip{{background:{C['soft_blue']};border:1px solid #B9D0F6;border-radius:13px;padding:11px 12px;margin:10px 0 13px}}
.rt-search-memory{{background:#F8FAFC;border:1px solid {_contrast_border};border-radius:12px;padding:10px 12px;margin:5px 0 12px;color:{C['muted']};font-size:.82rem;font-weight:700}}
.rt-search-memory b{{color:{C['text']}}}
.rt-job-phase{{display:inline-flex;align-items:center;gap:7px;background:{C['soft_blue']};border:1px solid #B7CCF5;border-radius:999px;padding:5px 10px;color:#1E3A8A;font-size:.78rem;font-weight:850;margin-bottom:8px}}
.rt-searchbar{{background:{C['panel']};border:1px solid {_contrast_border};border-radius:15px;padding:14px 15px;margin:0 0 12px;box-shadow:0 2px 10px rgba(2,6,23,.04)}}
.rt-search-help{{color:{C['muted']};font-size:.8rem;font-weight:650;margin-top:3px}}
.rt-info-item{{background:{C['panel2']};border:1px solid {_contrast_border};border-radius:12px;padding:12px 13px;margin-bottom:10px;min-height:80px}}
.rt-info-k{{font-size:.78rem;color:{C['muted']};font-weight:800;margin-bottom:6px}}
.rt-info-v{{font-size:.98rem;color:{C['text']};font-weight:700;line-height:1.6;word-break:break-word;overflow-wrap:anywhere}}
.rt-branch-hero{{background:linear-gradient(135deg,#EFF6FF 0%,#FFFFFF 58%,#F8FAFD 100%);border:1px solid {_contrast_border};border-radius:16px;padding:17px 19px;margin:0 0 14px;box-shadow:0 3px 14px rgba(15,23,42,.05)}}
.rt-branch-hero h3{{margin:0 0 5px!important;font-size:1.35rem!important;color:{C['text']}!important}}
.rt-branch-hero p{{margin:0;color:{C['muted']};font-weight:650;line-height:1.55}}
.rt-health-box{{background:{C['soft_green']};border:1px solid #86D7A5;border-radius:12px;padding:12px 14px;margin:8px 0 12px;color:#14532D;font-weight:750}}
.rt-fresh-ok{{color:#166534;font-weight:850}}.rt-fresh-warn{{color:#9A5A00;font-weight:850}}.rt-fresh-bad{{color:#B42318;font-weight:850}}
.rt-v30-search-hero{{background:linear-gradient(135deg,#F8FBFF 0%,#FFFFFF 48%,#EEF5FF 100%);border:1px solid #C9D9F2;border-radius:20px;padding:18px 20px 14px;margin:0 0 12px;box-shadow:0 9px 28px rgba(30,64,175,.08)}}
.rt-v30-search-hero h3{{margin:0 0 3px!important;font-size:1.28rem!important;font-weight:900!important;color:#102A56!important}}
.rt-v30-search-hero p{{margin:0;color:#64748B;font-size:.86rem;font-weight:650}}
.rt-v30-search-hero [data-testid="stTextInput"] input{{font-size:1.12rem!important;font-weight:780!important;min-height:50px!important;border-radius:14px!important}}
.rt-v30-search-hero .stButton>button{{min-height:50px!important;border-radius:14px!important;font-size:1rem!important;font-weight:900!important}}
.rt-v30-status{{display:flex;align-items:center;gap:10px;flex-wrap:wrap;padding:7px 2px 9px;color:#64748B;font-size:.80rem;font-weight:750}}
.rt-v30-status b{{color:#172033}}
.rt-v30-result-card{{background:#FFFFFF;border:1px solid #D9E2EF;border-radius:17px;padding:14px 15px 12px;margin:0 0 9px;box-shadow:0 3px 14px rgba(15,23,42,.045);transition:transform .12s ease,border-color .12s ease,box-shadow .12s ease}}
.rt-v30-result-card:hover{{border-color:#9EBBE8;box-shadow:0 8px 22px rgba(37,99,235,.09);transform:translateY(-1px)}}
.rt-v30-result-card.selected{{border-color:#4F7DD1;box-shadow:0 0 0 3px rgba(37,99,235,.09)}}
.rt-v30-result-title{{font-size:1.04rem;font-weight:900;color:#172033;line-height:1.42;margin-bottom:5px}}
.rt-v30-result-address{{font-size:.82rem;color:#64748B;font-weight:650;line-height:1.55;white-space:normal;overflow-wrap:anywhere}}
.rt-v30-result-meta{{display:flex;gap:6px 12px;flex-wrap:wrap;margin-top:9px;font-size:.77rem;color:#475569;font-weight:750}}
.rt-v30-result-meta strong{{color:#172033}}
.rt-v30-group{{display:inline-flex;align-items:center;border-radius:999px;padding:3px 8px;font-size:.70rem;font-weight:900}}
.rt-v30-group.strong{{background:#DCFCE7;color:#166534}}.rt-v30-group.confirmed{{background:#DBEAFE;color:#1D4ED8}}.rt-v30-group.related{{background:#FEF3C7;color:#92400E}}.rt-v30-group.additional{{background:#F1F5F9;color:#475569}}
.rt-v30-toolbar{{background:#FFFFFF;border:1px solid #D9E2EF;border-radius:14px;padding:8px 10px;margin:0 0 10px;box-shadow:0 2px 9px rgba(15,23,42,.035)}}
.rt-v30-inspector{{background:linear-gradient(180deg,#FFFFFF 0%,#FAFCFF 100%);border:1px solid #D5E0EE;border-radius:19px;padding:16px 17px;margin-bottom:10px;box-shadow:0 8px 24px rgba(15,23,42,.065)}}
.rt-v30-inspector h3{{margin:0 0 5px!important;font-size:1.32rem!important;font-weight:900!important}}
.rt-v30-inspector-sub{{color:#64748B;font-size:.83rem;font-weight:650;line-height:1.55}}
.rt-v30-detail-grid{{display:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:8px;margin:11px 0}}
.rt-v30-detail{{background:#F8FAFC;border:1px solid #E2E8F0;border-radius:11px;padding:9px 10px;min-width:0}}
.rt-v30-detail .k{{font-size:.70rem;color:#64748B;font-weight:850;margin-bottom:3px}}.rt-v30-detail .v{{font-size:.85rem;color:#172033;font-weight:760;white-space:normal;overflow-wrap:anywhere}}
.rt-v30-review{{background:#FFFFFF;border:1px solid #E2E8F0;border-radius:12px;padding:10px 11px;margin:7px 0}}.rt-v30-review .head{{font-size:.77rem;font-weight:850;color:#334155}}.rt-v30-review .body{{font-size:.84rem;line-height:1.72;color:#172033;margin-top:5px}}.rt-v30-review .owner{{font-size:.76rem;color:#475569;margin-top:6px;padding-top:6px;border-top:1px dashed #CBD5E1}}
@media(max-width:1100px){{.rt-v30-detail-grid{{grid-template-columns:1fr}}}}
.rt-v31-workspace{{background:linear-gradient(180deg,#F8FBFF 0%,#FFFFFF 100%);border:1px solid #D8E2F0;border-radius:20px;padding:12px 14px;margin:0 0 10px;box-shadow:0 8px 26px rgba(15,23,42,.055)}}
.rt-v31-workspace h3{{margin:0!important;font-size:1.22rem!important;font-weight:900!important;color:#172033!important}}
.rt-v31-workspace p{{margin:4px 0 0;color:#64748B;font-size:.82rem;font-weight:650}}
.rt-v31-navigator{{background:#FFFFFF;border:1px solid #DCE5F1;border-radius:16px;padding:11px 12px;margin-bottom:9px;box-shadow:0 3px 12px rgba(15,23,42,.035)}}
.rt-v31-navigator h4{{margin:0 0 4px!important;font-size:.96rem!important;font-weight:900!important;color:#1E293B!important}}
.rt-v31-detail-pane{{background:#FFFFFF;border:1px solid #D7E2EF;border-radius:18px;padding:14px 15px;margin-bottom:10px;box-shadow:0 6px 22px rgba(15,23,42,.05)}}
.rt-v31-detail-pane h3{{margin:0 0 3px!important;font-size:1.35rem!important;font-weight:900!important;color:#172033!important}}
.rt-v31-detail-sub{{color:#64748B;font-size:.82rem;font-weight:650;line-height:1.55}}
.rt-v31-intel{{background:linear-gradient(135deg,#F0F7FF 0%,#FFFFFF 70%);border:1px solid #CFE0F6;border-radius:15px;padding:12px 13px;margin:8px 0 10px}}
.rt-v31-intel .summary{{font-size:.88rem;font-weight:780;color:#1E3A5F;line-height:1.75}}
.rt-v31-topic{{display:inline-flex;margin:3px 4px;padding:4px 8px;border-radius:999px;background:#EFF6FF;color:#1D4ED8;font-size:.73rem;font-weight:850}}
.rt-v31-urgent{{background:#FFF7ED;border:1px solid #FED7AA;border-radius:12px;padding:9px 10px;margin:6px 0;color:#7C2D12}}
.rt-v31-contact-grid{{display:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:8px;margin-top:8px}}
@media(max-width:1050px){{.rt-v31-contact-grid{{grid-template-columns:1fr}}}}

.rt-footer{{text-align:center;color:{C['muted']};font-size:.75rem;padding:20px}}
.rt-topmenus{{background:#FFFFFF;border:1px solid {_contrast_border};border-radius:13px;padding:8px 9px;margin:0 0 10px;box-shadow:0 2px 9px rgba(15,23,42,.04)}}
[data-testid="stPopover"]>button{{min-height:42px!important;font-weight:850!important;background:#FFFFFF!important;border:1px solid {_contrast_border}!important;color:{C['text']}!important}}
[data-testid="stPopover"]>button:hover{{background:{C['hover']}!important;border-color:{C['focus']}!important}}
.rt-side-note{{font-size:.76rem;color:{C['muted']};font-weight:700;line-height:1.55;padding:6px 2px}}
#MainMenu,footer,header{{visibility:hidden}}

@media(max-width:1280px){{
 [data-testid="stSidebar"]{{min-width:220px!important;max-width:220px!important}}
 [data-testid="stMainBlockContainer"]{{padding-left:1rem;padding-right:1rem}}
 .rt-page-head h2{{font-size:1.45rem}}
 .rt-kpi .value{{font-size:1.5rem}}
}}
@media(max-width:980px){{
 [data-testid="stMainBlockContainer"]{{padding:.8rem .75rem 2rem}}
 .rt-page-head{{padding:14px 15px;border-radius:12px}}
 .rt-page-head h2{{font-size:1.3rem}}
 .rt-page-head p{{font-size:.84rem}}
 .rt-searchbar{{padding:11px 12px}}
 [data-testid="stForm"]{{padding:14px}}
}}
@media(max-width:760px){{
 html,body,.stApp{{font-size:max(15px,{BASE_FONT_PX}px)!important}}
 .rt-kpi{{min-height:92px;padding:12px}}
 .rt-page-head{{margin-bottom:12px}}
}}
@media(max-width:900px){{
 [data-testid="stMainBlockContainer"]{{padding:.8rem .7rem 2rem}}
 .rt-page-head{{padding:14px}}
 .rt-kpi{{min-height:90px}}
}}
</style>"""
st.markdown(_css, unsafe_allow_html=True)
# Approved Reviews Tracker UI. Keep old stylesheet for non-dashboard legacy pages.
for _stylesheet in ("ui_v41.css", "ui_v42.css"):
    _style_path = APP_DIR / _stylesheet
    if _style_path.is_file():
        st.markdown("<style>\n" + _style_path.read_text(encoding="utf-8") + "\n</style>",
                    unsafe_allow_html=True)



# ═══════════════════════════════════════════════════════════════════════
#  DIRECT OPEN
# ═══════════════════════════════════════════════════════════════════════
# Login screen intentionally disabled for this local desktop build.


@st.cache_resource(show_spinner=False)
def get_sync_worker():
    return AutoSyncWorker(db)

sync_worker = get_sync_worker()


# ═══════════════════════════════════════════════════════════════════════
#  HELPERS
# ═══════════════════════════════════════════════════════════════════════
def nav_to(page, company=None, review=None):
    st.session_state.page = page
    try: db.set_pref("last_page_{}".format(st.session_state.workspace_id), page)
    except Exception: pass
    if company is not None:
        st.session_state.selected_company = company; st.session_state.selected_branch_pk = company
    if review is not None: st.session_state.selected_review = review
    st.rerun()


def period_bounds():
    key = st.session_state.get("period_key", "d30")
    now = datetime.now()
    if key == "today":
        return datetime.combine(date.today(), datetime.min.time()), now
    if key == "d7":
        return now - timedelta(days=7), now
    if key == "d30":
        return now - timedelta(days=30), now
    if key == "d90":
        return now - timedelta(days=90), now
    if key == "custom":
        try:
            days = int(st.session_state.get("period_custom_days", 30) or 30)
        except Exception:
            days = 30
        days = max(1, min(3650, days))
        st.session_state.period_custom_days = days
        return now - timedelta(days=days), now
    if key == "all":
        return None, now
    return now - timedelta(days=30), now


@st.cache_data(ttl=30, show_spinner=False)
def cached_companies(ws_id):
    return db.list_companies(ws_id)


@st.cache_data(ttl=30, show_spinner=False)
def cached_reviews(ws_id, since_iso):
    since = datetime.fromisoformat(since_iso) if since_iso else None
    return db.all_reviews(ws_id, since)


def all_data():
    since, _ = period_bounds()
    return cached_companies(st.session_state.workspace_id), \
           cached_reviews(st.session_state.workspace_id, since.isoformat() if since else "")


def badge(text, kind="gray"):
    return '<span class="rt-badge b-%s">%s</span>' % (kind, html.escape(str(text)))


def ui_step(number, title, subtitle=""):
    body = '<div class="rt-step"><span class="rt-step-num">{}</span><span>{}</span></div>'.format(
        html.escape(str(number)), html.escape(str(title)))
    st.markdown(body, unsafe_allow_html=True)
    if subtitle:
        st.caption(subtitle)


def metric_card(label, value, sub=""):
    st.markdown('<div class="rt-kpi"><div class="label">{}</div><div class="value">{}</div></div>'.format(
        html.escape(str(label)), html.escape(str(value))), unsafe_allow_html=True)


def section_title(title, sub=""):
    # Backward-compatible alias so every legacy page uses the same v21 page header.
    page_header(title, sub, st.session_state.get("page"))


def normalize_search_text_v35(value):
    """Normalize Arabic/Latin search text without changing stored data."""
    text=str(value or "").casefold().strip()
    text=re.sub(r"[\u064b-\u065f\u0670\u0640]","",text)
    text=re.sub(r"[إأآٱ]","ا",text).replace("ى","ي").replace("ة","ه")
    text=re.sub(r"[^\w\u0600-\u06ff]+"," ",text,flags=re.UNICODE)
    return re.sub(r"\s+"," ",text).strip()


def build_search_blob_v35(df):
    """Build one normalized search blob per result; callers may cache it as a DataFrame column."""
    if df is None or df.empty:
        return pd.Series(dtype=str)
    cols=[c for c in ["name","address","category","phone","mobile","email","website","result_group"] if c in df.columns]
    if not cols:
        return pd.Series("",index=df.index,dtype=str)
    raw=df[cols].fillna("").astype(str).agg(" ".join,axis=1)
    return raw.map(normalize_search_text_v35)


def search_text_score_v35(query, text):
    """Cheap typo-tolerant score used only when exact/token matching found nothing."""
    q=normalize_search_text_v35(query); t=normalize_search_text_v35(text)
    if not q or not t: return 0.0
    if q==t: return 1.0
    if q in t or t in q: return .97
    qt=[x for x in q.split() if x]; tt=[x for x in t.split() if x]
    prefix=sum(1 for x in qt if any(y.startswith(x) or x.startswith(y) for y in tt))/max(1,len(qt))
    seq=difflib.SequenceMatcher(None,q,t).ratio()
    token_scores=[]
    for x in qt:
        best=0.0
        for y in tt:
            best=max(best,difflib.SequenceMatcher(None,x,y).ratio())
        token_scores.append(best)
    token_avg=sum(token_scores)/max(1,len(token_scores))
    return max(seq,prefix*.90,token_avg*.96)


def search_result_filter_mask(df, query):
    """Arabic-aware partial search with strict-first matching and a Google-like broad fallback."""
    if df is None or df.empty:
        return pd.Series(dtype=bool)

    _norm_fn=globals().get("normalize_search_text_v35")
    if not callable(_norm_fn):
        def _norm_fn(value):
            text=str(value or "").casefold().strip()
            text=re.sub(r"[\u064b-\u065f\u0670\u0640]","",text)
            text=re.sub(r"[إأآٱ]","ا",text).replace("ى","ي").replace("ة","ه")
            text=re.sub(r"[^\w\u0600-\u06ff]+"," ",text,flags=re.UNICODE)
            return re.sub(r"\s+"," ",text).strip()

    q=_norm_fn(query)
    if not q:
        return pd.Series(True,index=df.index,dtype=bool)
    if "_search_blob_v35" in df.columns:
        hay=df["_search_blob_v35"].fillna("").astype(str)
    else:
        cols=[c for c in ["name","address","category","phone","mobile","email","website","result_group"] if c in df.columns]
        if not cols:
            return pd.Series(True,index=df.index,dtype=bool)
        hay=df[cols].fillna("").astype(str).agg(" ".join,axis=1).map(_norm_fn)
    tokens=[x for x in q.split(" ") if x]
    if not tokens:
        return pd.Series(True,index=df.index,dtype=bool)

    strict=pd.Series(True,index=df.index,dtype=bool)
    hit_count=pd.Series(0,index=df.index,dtype="int64")
    for tok in tokens:
        hit=hay.str.contains(re.escape(tok),regex=True,na=False)
        strict &= hit; hit_count += hit.astype("int64")
    if strict.any():
        return strict

    needed=(len(tokens)+1)//2 if len(tokens)>=3 else len(tokens)
    broad=hit_count>=max(1,needed)
    if broad.any():
        return broad

    names=df["name"].fillna("").astype(str) if "name" in df.columns else pd.Series("",index=df.index,dtype=str)
    _score_fn=globals().get("search_text_score_v35")
    if not callable(_score_fn):
        def _score_fn(query_value,text_value):
            import difflib as _df
            qq=_norm_fn(query_value); tt=_norm_fn(text_value)
            if not qq or not tt: return 0.0
            if qq==tt: return 1.0
            if qq in tt or tt in qq: return .97
            qt=[x for x in qq.split() if x]; tw=[x for x in tt.split() if x]
            prefix=sum(1 for x in qt if any(y.startswith(x) or x.startswith(y) for y in tw))/max(1,len(qt))
            seq=_df.SequenceMatcher(None,qq,tt).ratio()
            scores=[]
            for x in qt:
                scores.append(max([_df.SequenceMatcher(None,x,y).ratio() for y in tw] or [0.0]))
            avg=sum(scores)/max(1,len(scores))
            return max(seq,prefix*.90,avg*.96)
    return names.map(lambda x: _score_fn(q,x)>=.78)


def branch_family_key(row):
    """Return a display-only family key. It never changes canonical branch identity."""
    try:
        explicit=str(row.get("branch_group") or "").strip()
    except Exception:
        explicit=""
    if explicit:
        return re.sub(r"\s+"," ",explicit).strip()
    try:
        name=str(row.get("name") or "").strip()
    except Exception:
        name=str(row or "").strip()
    if not name:
        return "—"
    name=re.sub(r"[\u064b-\u065f\u0670\u0640]","",name)
    name=re.sub(r"[إأآٱ]","ا",name).replace("ى","ي")
    # Explicit branch suffixes are safe display grouping cues; identity remains untouched.
    name=re.sub(r"\s*[-–—|,:]*\s*(?:فرع|branch)\b.*$","",name,flags=re.I)
    name=re.sub(r"\s+"," ",name).strip(" -–—|,:")
    return name.casefold() or "—"




def branch_network_key(row):
    """Display-only brand/network grouping key; never changes canonical branch identity."""
    try:
        explicit=str(row.get("branch_group") or "").strip()
    except Exception:
        explicit=""
    if explicit:
        value=explicit
        value=re.sub(r"[\u064b-\u065f\u0670\u0640]","",value)
        value=re.sub(r"[إأآٱ]","ا",value).replace("ى","ي")
        return re.sub(r"\s+"," ",value).strip().casefold() or "—"
    return branch_family_key(row)


def build_branch_network_intelligence(rows, freshness_days=30, now=None):
    """Summarize saved/discovered branch evidence without claiming real-world completeness."""
    if rows is None:
        df=pd.DataFrame()
    elif isinstance(rows,pd.DataFrame):
        df=rows.copy()
    else:
        try: df=pd.DataFrame(list(rows))
        except Exception: df=pd.DataFrame()
    total=int(len(df))
    if total<=0:
        return {"branches":0,"strong_identity":0,"weak_identity":0,"geocoded":0,"contact_complete":0,"fresh":0,
                "identity_pct":0.0,"geo_pct":0.0,"contact_pct":0.0,"fresh_pct":0.0,"evidence_coverage_pct":0.0,
                "coverage_label":"empty","coverage_basis":"saved_discovered_evidence",
                "coverage_claim":"Evidence coverage only; it does not guarantee every real-world branch was found."}
    def _s(col):
        return df[col].fillna("").astype(str).str.strip() if col in df.columns else pd.Series("",index=df.index,dtype=str)
    strong=(_s("place_id")!="") | (_s("cid")!="") | (_s("maps_url")!="")
    lat=pd.to_numeric(df["latitude"],errors="coerce") if "latitude" in df.columns else pd.Series(float("nan"),index=df.index)
    lon=pd.to_numeric(df["longitude"],errors="coerce") if "longitude" in df.columns else pd.Series(float("nan"),index=df.index)
    geo=lat.notna() & lon.notna()
    contact_hits=pd.Series(0,index=df.index,dtype="int64")
    for col in ("phone","mobile","email","website"):
        contact_hits += (_s(col)!="").astype("int64")
    contact_complete=contact_hits>=4
    now_ts=pd.Timestamp(now if now is not None else datetime.now())
    updated=pd.to_datetime(df["last_updated"],errors="coerce") if "last_updated" in df.columns else pd.Series(pd.NaT,index=df.index)
    age_days=(now_ts-updated).dt.total_seconds()/86400.0
    fresh=updated.notna() & age_days.ge(0) & age_days.le(max(1,int(freshness_days or 30)))
    identity_pct=round(100.0*float(strong.sum())/total,1)
    geo_pct=round(100.0*float(geo.sum())/total,1)
    contact_pct=round(100.0*float(contact_hits.sum())/(4.0*total),1)
    fresh_pct=round(100.0*float(fresh.sum())/total,1)
    score=round(identity_pct*.35 + geo_pct*.25 + contact_pct*.20 + fresh_pct*.20,1)
    label="strong" if score>=85 else "good" if score>=70 else "partial" if score>=45 else "low"
    return {"branches":total,"strong_identity":int(strong.sum()),"weak_identity":int(total-strong.sum()),
            "geocoded":int(geo.sum()),"contact_complete":int(contact_complete.sum()),"fresh":int(fresh.sum()),
            "identity_pct":identity_pct,"geo_pct":geo_pct,"contact_pct":contact_pct,"fresh_pct":fresh_pct,
            "evidence_coverage_pct":score,"coverage_label":label,"coverage_basis":"saved_discovered_evidence",
            "coverage_claim":"Evidence coverage only; it does not guarantee every real-world branch was found."}

def build_branch_review_intelligence(reviews):
    """Deterministic review intelligence derived only from supplied review rows."""
    if reviews is None:
        df=pd.DataFrame()
    elif isinstance(reviews,pd.DataFrame):
        df=reviews.copy()
    else:
        try: df=pd.DataFrame(list(reviews))
        except Exception: df=pd.DataFrame()
    if df.empty:
        return {"total":0,"positive":0,"negative":0,"neutral":0,"positive_pct":0.0,"negative_pct":0.0,
                "unanswered":0,"top_topics":[],"issue_topics":[],"positive_topics":[],"urgent_reviews":[],
                "summary_ar":"لا توجد مراجعات كافية للتحليل بعد.","summary_en":"No reviews are available for analysis yet."}
    total=len(df)
    rating=pd.to_numeric(df.get("rating",pd.Series(index=df.index,dtype=float)),errors="coerce")
    raw_sent=df.get("sentiment",pd.Series(index=df.index,dtype=str)).fillna("").astype(str).str.lower().str.strip()
    inferred=pd.Series("neutral",index=df.index,dtype=object)
    inferred.loc[rating>=4]="positive"; inferred.loc[rating<=2]="negative"
    sent=raw_sent.where(raw_sent.isin(["positive","negative","neutral"]),inferred)
    positive=int((sent=="positive").sum()); negative=int((sent=="negative").sum()); neutral=max(0,total-positive-negative)
    responded=pd.to_numeric(df.get("responded",pd.Series(0,index=df.index)),errors="coerce").fillna(0)
    unanswered=int((responded<=0).sum())
    topic=df.get("topic",pd.Series("",index=df.index)).fillna("").astype(str).str.strip()
    def counts(mask=None):
        vals=topic if mask is None else topic[mask]
        vals=vals[(vals!="") & (~vals.str.lower().isin(["other","unknown","none","—"]))]
        vc=vals.value_counts().head(8)
        return [(str(k),int(v)) for k,v in vc.items()]
    urgent_mask=(rating<=2) & (responded<=0)
    urgent_cols=[c for c in ["id","author","rating","text","review_date","date","topic","sentiment","owner_response"] if c in df.columns]
    urgent=df.loc[urgent_mask,urgent_cols].head(8).to_dict("records") if urgent_cols else []
    neg_pct=round(100.0*negative/max(1,total),1); pos_pct=round(100.0*positive/max(1,total),1)
    top=counts(); issues=counts(sent=="negative"); positives=counts(sent=="positive")
    top_name=top[0][0] if top else "—"; issue_name=issues[0][0] if issues else "—"
    summary_ar="{} مراجعة · إيجابي {:.0f}% · سلبي {:.0f}% · بدون رد {} · الموضوع الأبرز {} · أبرز مشكلة {}".format(total,pos_pct,neg_pct,unanswered,top_name,issue_name)
    summary_en="{} reviews · {:.0f}% positive · {:.0f}% negative · {} unanswered · top topic {} · top issue {}".format(total,pos_pct,neg_pct,unanswered,top_name,issue_name)
    return {"total":total,"positive":positive,"negative":negative,"neutral":neutral,"positive_pct":pos_pct,"negative_pct":neg_pct,
            "unanswered":unanswered,"top_topics":top,"issue_topics":issues,"positive_topics":positives,"urgent_reviews":urgent,
            "summary_ar":summary_ar,"summary_en":summary_en}




# Reviews Tracker v34.0 — Advanced Sorting & Result Control
def sort_search_results(df, mode="auto"):
    """Stable, null-safe sorting shared by cards, map and table search views."""
    if df is None:
        return pd.DataFrame()
    out=df.copy()
    if out.empty:
        return out
    mode=str(mode or "auto")

    def _num(col):
        if col not in out.columns:
            return pd.Series([float("nan")]*len(out), index=out.index, dtype="float64")
        return pd.to_numeric(out[col],errors="coerce")

    if "name" not in out.columns: out["name"]=""
    out["__sort_name"]=out["name"].fillna("").astype(str).str.casefold()

    # Result age uses the newest reliable data timestamp available per row.
    date_cols=[c for c in [
        "detail_fetched_at","review_samples_fetched_at","cache_updated_at",
        "source_fetched_at","updated_at","last_seen_at","created_at"
    ] if c in out.columns]
    if date_cols:
        dt=pd.Series(pd.NaT,index=out.index,dtype="datetime64[ns]")
        for c in date_cols:
            parsed=pd.to_datetime(out[c],errors="coerce",utc=True).dt.tz_convert(None)
            dt=dt.fillna(parsed)
        out["__sort_date"]=dt
    else:
        out["__sort_date"]=pd.Series(pd.NaT,index=out.index,dtype="datetime64[ns]")

    out["__sort_rating"]=_num("rating")
    out["__sort_reviews"]=_num("review_count")
    out["__sort_distance"]=_num("distance_km")
    out["__sort_match"]=_num("match_score")
    out["__sort_validation"]=_num("validation_score")
    out["__sort_freshness"]=_num("freshness_score")
    if "result_group_rank" not in out.columns: out["result_group_rank"]=3
    if "result_score" not in out.columns: out["result_score"]=0

    specs={
        "rating_desc":(["__sort_rating","__sort_reviews","__sort_name"],[False,False,True]),
        "rating_asc":(["__sort_rating","__sort_reviews","__sort_name"],[True,False,True]),
        "distance_asc":(["__sort_distance","__sort_rating","__sort_name"],[True,False,True]),
        "distance_desc":(["__sort_distance","__sort_rating","__sort_name"],[False,False,True]),
        "date_desc":(["__sort_date","__sort_rating","__sort_name"],[False,False,True]),
        "date_asc":(["__sort_date","__sort_rating","__sort_name"],[True,False,True]),
        "reviews_desc":(["__sort_reviews","__sort_rating","__sort_name"],[False,False,True]),
        "reviews_asc":(["__sort_reviews","__sort_rating","__sort_name"],[True,False,True]),
        "match_desc":(["__sort_match","__sort_validation","__sort_reviews"],[False,False,False]),
        "validation_desc":(["__sort_validation","__sort_match","__sort_reviews"],[False,False,False]),
        "freshness_desc":(["__sort_freshness","__sort_date","__sort_name"],[False,False,True]),
        "freshness_asc":(["__sort_freshness","__sort_date","__sort_name"],[True,True,True]),
        "name_asc":(["__sort_name"],[True]),
        "name_desc":(["__sort_name"],[False]),
    }
    if mode=="auto":
        cols=[c for c in ["result_group_rank","result_score","validation_score","match_score","review_count"] if c in out.columns]
        asc=[True,False,False,False,False][:len(cols)]
        if cols: out=out.sort_values(cols,ascending=asc,na_position="last",kind="stable")
    elif mode in specs:
        cols,asc=specs[mode]
        out=out.sort_values(cols,ascending=asc,na_position="last",kind="stable")
    return out.drop(columns=[c for c in out.columns if c.startswith("__sort_")],errors="ignore")

def _v30_group_label(group):
    group=str(group or "additional")
    ar={"strong":"الأقرب للاسم","confirmed":"مؤكد","related":"مرتبط","additional":"نتائج إضافية"}
    en={"strong":"Best match","confirmed":"Confirmed","related":"Related","additional":"Additional"}
    return (ar if RTL else en).get(group,group)



def format_rating_v36(value):
    if value in (None,""): return "—"
    try:
        x=float(value)
        if math.isnan(x): return "—"
        return "{:.1f}".format(x)
    except Exception:
        return str(value)


def contact_completeness_v36(rec):
    r=dict(rec or {}); keys=("phone","mobile","email","website")
    count=sum(1 for k in keys if str(r.get(k) or "").strip())
    return {"count":count,"total":4,"pct":round(count/4*100,1),"label":"{}/4".format(count)}


def contact_detail_rows_v36(rec):
    r=dict(rec or {}); fs=r.get("field_sources") if isinstance(r.get("field_sources"),dict) else {}
    source_default=str(r.get("contact_source") or "")
    labels=[("phone","الهاتف" if RTL else "Phone","☎"),("mobile","الجوال" if RTL else "Mobile","📱"),("email","البريد" if RTL else "Email","✉"),("website","الموقع" if RTL else "Website","🌐")]
    out=[]
    for key,label,icon in labels:
        value=str(r.get(key) or "").strip(); source=str(fs.get(key) or source_default or "").strip()
        out.append({"key":key,"label":label,"icon":icon,"value":value,"source":source})
    return out

def render_search_result_card(idx, row, selected=False):
    # v36 explicit rating + contact visibility.
    r=dict(row or {}); group=str(r.get("result_group") or "additional")
    cls="rt-v30-result-card selected" if selected else "rt-v30-result-card"
    name=html.escape(str(r.get("name") or "—")); address=html.escape(str(r.get("address") or "—"))
    rating=format_rating_v36(r.get("rating")); reviews=int(safe_num(r.get("review_count"),0))
    match=safe_num(r.get("match_score"),0)*100; validation=safe_num(r.get("validation_score"),0); freshness=safe_num(r.get("freshness_score"),0)
    category=html.escape(str(r.get("category") or "—")); completeness=contact_completeness_v36(r)
    detail_html=[]
    for d in contact_detail_rows_v36(r):
        val=d["value"] or "—"; source=d["source"]
        src_html=' <small>({})</small>'.format(html.escape(source)) if source else ""
        detail_html.append('<span class="rt-v36-contact-chip"><b>{} {}</b>: {}{}</span>'.format(d["icon"],html.escape(d["label"]),html.escape(val),src_html))
    confidence=safe_num(r.get("contact_confidence"),0)
    body=(
        '<div class="{}">'.format(cls)+
        '<div class="rt-v30-result-title">{} <span class="rt-v30-group {}">{}</span></div>'.format(name,html.escape(group),html.escape(_v30_group_label(group)))+
        '<div class="rt-v30-result-address">{}</div>'.format(address)+
        '<div class="rt-v30-result-meta"><span>⭐ <strong>{}</strong></span><span>📝 <strong>{}</strong></span><span>{}: <strong>{}</strong></span><span>{}: <strong>{:.0f}%</strong></span><span>{}: <strong>{:.0f}%</strong></span><span>{}: <strong>{:.0f}%</strong></span></div>'.format(
            rating,reviews,"النشاط" if RTL else "Category",category,"التطابق" if RTL else "Match",match,"التحقق" if RTL else "Validation",validation,"الحداثة" if RTL else "Freshness",freshness)+
        '<div class="rt-v36-contact-grid">{}</div>'.format("".join(detail_html))+
        '<div class="rt-v30-result-address" style="margin-top:7px">{}: <strong>{}</strong> · {}: <strong>{:.0f}%</strong></div></div>'.format(
            "اكتمال الاتصال" if RTL else "Contact completeness",completeness["label"],"ثقة الاتصال" if RTL else "Contact confidence",confidence)
    )
    st.markdown(body,unsafe_allow_html=True)
    return st.button("فتح مساحة الفرع" if RTL else "Open workspace",key="v30_open_result_{}".format(idx),use_container_width=True,type="primary" if selected else "secondary")



def merge_branch_workspace_record(saved_record, live_record):
    """Overlay fresh live search fields only when both records identify the same branch."""
    saved=dict(saved_record or {}); live=dict(live_record or {})
    if not saved: return live
    if not live: return saved
    conflict=False; matched=False
    for key in ("place_id","cid"):
        a=str(saved.get(key) or "").strip(); b=str(live.get(key) or "").strip()
        if a and b:
            if a!=b: conflict=True
            else: matched=True
    if conflict: return saved
    if str(saved.get("pk") or "") and str(live.get("pk") or "") and str(saved.get("pk"))==str(live.get("pk")): matched=True
    su=str(saved.get("maps_url") or saved.get("url") or "").strip().rstrip("/")
    lu=str(live.get("maps_url") or live.get("url") or "").strip().rstrip("/")
    if su and lu and su==lu: matched=True
    if not matched:
        sn=re.sub(r"\s+"," ",str(saved.get("name") or "").strip()).casefold(); ln=re.sub(r"\s+"," ",str(live.get("name") or "").strip()).casefold()
        sa=re.sub(r"\s+"," ",str(saved.get("address") or "").strip()).casefold(); la=re.sub(r"\s+"," ",str(live.get("address") or "").strip()).casefold()
        matched=bool(sn and ln and sn==ln and ((sa and la and sa==la) or (not sa and not la)))
    if not matched: return saved
    out=dict(saved); canonical_pk=saved.get("pk")
    for k,v in live.items():
        if v not in (None,"",[],{}): out[k]=v
    if canonical_pk: out["pk"]=canonical_pk
    return out

def merge_branch_reviews(stored_reviews, preview_reviews):
    """Merge stored DB reviews with Google preview samples so comments never disappear after a branch is saved."""
    def _records(value):
        if value is None: return []
        if isinstance(value,pd.DataFrame): return value.to_dict("records")
        if isinstance(value,dict): return [dict(value)]
        try: return [dict(x or {}) for x in list(value)]
        except Exception: return []
    def _clean_text(value): return re.sub(r"\s+"," ",str(value or "").strip()).casefold()
    rows=[]; index={}
    for origin,value in (("stored",stored_reviews),("google_preview",preview_reviews)):
        for raw in _records(value):
            r=dict(raw or {})
            rid=str(r.get("review_id") or "").strip()
            review_date=r.get("review_date") or r.get("date") or ""
            owner=r.get("owner_response") or r.get("response_text") or ""
            r["review_date"]=review_date; r["date"]=r.get("date") or review_date
            r["owner_response"]=owner; r["response_text"]=r.get("response_text") or owner
            if owner and not r.get("responded"): r["responded"]=1
            r.setdefault("review_source",origin)
            if rid:
                key="rid:"+rid
            else:
                key="fp:{}|{}|{}|{}".format(_clean_text(r.get("author")),str(r.get("rating") or ""),_clean_text(review_date),_clean_text(r.get("text")))
            if key in index:
                pos=index[key]; old=rows[pos]
                for k,v in r.items():
                    if old.get(k) in (None,"",[],{}) and v not in (None,"",[],{}): old[k]=v
                if origin=="stored": old["review_source"]="stored"
                rows[pos]=old
            else:
                index[key]=len(rows); rows.append(r)
    if not rows: return pd.DataFrame()
    out=pd.DataFrame(rows)
    for col in ["author","text","review_date","date","owner_response","response_text","sentiment","topic","workflow_status","review_source"]:
        if col not in out: out[col]=""
    if "responded" not in out: out["responded"]=0
    return out


def _review_reply_norm(text):
    value=str(text or "").casefold()
    value=re.sub(r"[\u064b-\u065f\u0670]","",value)
    value=re.sub(r"[^\w\u0600-\u06ff]+"," ",value,flags=re.UNICODE)
    return re.sub(r"\s+"," ",value).strip()

def reply_similarity_score(a,b):
    x=_review_reply_norm(a); y=_review_reply_norm(b)
    if not x or not y: return 0.0
    return round(float(difflib.SequenceMatcher(None,x,y).ratio()),4)

def generate_review_reply_draft(review, lang="ar", tone="professional"):
    r=dict(review or {}); author=str(r.get("author") or "").strip(); topic=str(r.get("topic") or "").lower(); sentiment=str(r.get("sentiment") or "").lower()
    try: rating=int(float(r.get("rating") or 0))
    except Exception: rating=0
    lang="en" if str(lang).lower().startswith("en") else "ar"; tone=str(tone or "professional").lower()
    if lang=="ar":
        who=("أستاذ/ة "+author+"، ") if author else ""
        topic_txt={"service":"الخدمة","staff":"تعامل الفريق","price":"الأسعار","quality":"الجودة","cleanliness":"النظافة","wait_time":"وقت الانتظار"}.get(topic,"تجربتك")
        if rating>=4 or sentiment=="positive": body="شكرًا لك على تقييمك الجميل. يسعدنا أن {} نالت رضاك، ونتطلع لخدمتك مرة أخرى قريبًا.".format(topic_txt)
        elif rating<=2 or sentiment=="negative": body="نعتذر لأن {} لم تكن بالمستوى المتوقع. نقدر ملاحظتك وسنتابعها مع الفريق لتحسين التجربة، ونأمل أن تمنحنا فرصة أفضل في زيارتك القادمة.".format(topic_txt)
        else: body="شكرًا لمشاركتنا رأيك. نقدر ملاحظتك حول {} وسنستفيد منها في تحسين تجربتنا باستمرار.".format(topic_txt)
        if tone=="short": body=body.split(". ")[0]+"."
        elif tone=="friendly": body=body.replace("شكرًا لك","سعدنا كثيرًا بتقييمك، شكرًا لك")
        elif tone=="apology" and not body.startswith("نعتذر"): body="نعتذر عن أي جانب لم يلبِ توقعاتك. "+body
        return who+body
    who=("Hi "+author+", ") if author else "Hello, "
    topic_txt={"service":"our service","staff":"our team","price":"our pricing","quality":"the quality","cleanliness":"cleanliness","wait_time":"the wait time"}.get(topic,"your experience")
    if rating>=4 or sentiment=="positive": body="thank you for your kind review. We are glad that {} met your expectations, and we look forward to welcoming you again.".format(topic_txt)
    elif rating<=2 or sentiment=="negative": body="we are sorry that {} did not meet your expectations. We appreciate your feedback and will follow it up with the team so we can improve.".format(topic_txt)
    else: body="thank you for sharing your feedback. We appreciate your comments about {} and will use them to improve the experience.".format(topic_txt)
    if tone=="short": body=body.split(". ")[0]+"."
    elif tone=="friendly": body=body.replace("thank you","thanks so much",1)
    elif tone=="apology" and "sorry" not in body: body="we are sorry for any part of the experience that fell short. "+body
    return who+body

def review_identity_key(review):
    r=dict(review or {}); rid=str(r.get("review_id") or "").strip()
    if rid and not rid.startswith("idx-"): return "rid:"+rid
    author=_review_reply_norm(r.get("author"))
    try: rating="{:g}".format(float(r.get("rating")))
    except Exception: rating=str(r.get("rating") or "")
    dstr=_review_reply_norm(r.get("review_date") or r.get("date")); text=_review_reply_norm(r.get("text"))
    return "fp:"+hashlib.sha256("{}|{}|{}|{}".format(author,rating,dstr,text).encode("utf-8")).hexdigest()

def persist_preview_reviews(db, pk, branch, ws_id=1):
    """Persist review samples available on a live search result when the branch is saved."""
    previews=(branch or {}).get("preview_reviews") or []
    frame=merge_branch_reviews(pd.DataFrame(),previews)
    if frame.empty or not pk: return (0,0)
    return db.save_reviews(str(pk),frame,ws_id)


def build_action_center_feed(reviews, alerts, tickets, sync_states, companies=None):
    """Fuse actionable sources into deterministic, deduplicated work items."""
    def recs(v):
        if v is None: return []
        if isinstance(v,pd.DataFrame): return v.to_dict("records")
        try: return [dict(x or {}) for x in list(v)]
        except Exception: return []
    def num(v,default=0.0):
        try:
            if pd.isna(v): return default
            return float(v)
        except Exception: return default
    ranks={"low":1,"normal":2,"high":3,"critical":4}
    company_map={str(c.get("pk") or ""):c for c in recs(companies)}
    merged={}
    def upsert(key,item,source):
        cur=merged.get(key)
        if cur is None:
            cur={"action_key":key,"sources":[],"priority":"low","priority_reason":"","status":"new",
                 "pk":"","company_name":"","maps_url":"","review_id":None,"ticket_id":None,"alert_id":None,
                 "title":"","detail":"","rating":None,"responded":None,"owner":"","created_at":""}
        if source not in cur["sources"]: cur["sources"].append(source)
        if ranks.get(item.get("priority"),1)>ranks.get(cur.get("priority"),1):
            cur["priority"]=item.get("priority"); cur["priority_reason"]=item.get("priority_reason") or cur.get("priority_reason")
        for k,v in item.items():
            if k in ("priority","priority_reason"): continue
            if v not in (None,"",[],{}) and cur.get(k) in (None,"",[],{}): cur[k]=v
        if item.get("status") not in (None,"","new") or cur.get("status") in (None,"","new"):
            cur["status"]=item.get("status") or cur.get("status")
        pk=str(cur.get("pk") or ""); comp=company_map.get(pk) or {}
        if not cur.get("company_name"): cur["company_name"]=comp.get("name") or pk or "—"
        if not cur.get("maps_url"): cur["maps_url"]=comp.get("maps_url") or ""
        merged[key]=cur
    for r in recs(reviews):
        status=str(r.get("workflow_status") or "new").lower()
        if status in ("resolved","ignored"): continue
        rid=r.get("id"); pk=str(r.get("pk") or "")
        responded=int(num(r.get("responded"),0))>0; rating=num(r.get("rating"),0); urgency=num(r.get("urgency"),0)
        sentiment=str(r.get("sentiment") or "").lower()
        if responded and sentiment!="negative" and urgency<60: continue
        if rating<=1 or urgency>=85: pr="critical"; reason="1-star / very urgent review"
        elif rating<=2 or sentiment=="negative" or urgency>=60: pr="high"; reason="negative or urgent review"
        elif not responded: pr="normal"; reason="review without owner response"
        else: pr="low"; reason="review follow-up"
        key="review:{}:{}".format(pk,rid) if rid is not None else "review:{}:{}".format(pk,str(r.get("review_id") or r.get("text") or "")[:80])
        upsert(key,{"pk":pk,"company_name":r.get("company_name"),"maps_url":r.get("maps_url"),"review_id":rid,
                    "title":"Review needs attention","detail":r.get("text") or "","rating":r.get("rating"),"responded":r.get("responded"),
                    "status":status,"created_at":r.get("review_date") or r.get("fetched_at") or "","priority":pr,"priority_reason":reason},"review")
    for a in recs(alerts):
        if str(a.get("status") or "new").lower()=="resolved": continue
        pk=str(a.get("pk") or ""); rid=a.get("review_id"); sev=str(a.get("severity") or "info").lower()
        pr="critical" if sev=="critical" else "high" if sev in ("warning","high") else "normal"
        key="review:{}:{}".format(pk,rid) if rid not in (None,"") else "alert:{}".format(a.get("id"))
        upsert(key,{"pk":pk,"company_name":a.get("company_name"),"review_id":rid,"alert_id":a.get("id"),"title":a.get("title") or "Alert",
                    "detail":a.get("detail") or "","status":a.get("status") or "new","created_at":a.get("created_at") or "",
                    "priority":pr,"priority_reason":"{} alert".format(sev)},"alert")
    for t in recs(tickets):
        status=str(t.get("status") or "new").lower()
        if status in ("resolved","closed","done"): continue
        pk=str(t.get("pk") or ""); rid=t.get("review_id")
        pr0=str(t.get("priority") or "normal").lower(); pr="critical" if pr0=="critical" else "high" if pr0=="high" else "normal" if pr0 in ("medium","normal") else "low"
        key=str(t.get("action_key") or ("review:{}:{}".format(pk,rid) if rid not in (None,"") else "ticket:{}".format(t.get("id"))))
        upsert(key,{"pk":pk,"review_id":rid,"ticket_id":t.get("id"),"title":t.get("title") or "Ticket","detail":t.get("notes") or "",
                    "status":status,"owner":t.get("owner") or "","created_at":t.get("created_at") or "","due_at":t.get("due_at") or "",
                    "priority":pr,"priority_reason":"ticket priority"},"ticket")
    for x in recs(sync_states):
        status=str(x.get("status") or "").lower(); changed=str(x.get("changed_fields") or "").strip(); neg=int(num(x.get("new_negative_reviews"),0))
        if status not in ("error","failed") and not neg and changed in ("","[]","null","None"): continue
        pk=str(x.get("pk") or ""); pr="critical" if status in ("error","failed") else "high" if neg else "normal"
        detail=x.get("last_error") or ("{} new negative review(s)".format(neg) if neg else changed)
        upsert("sync:"+pk,{"pk":pk,"company_name":x.get("name"),"title":"Branch sync needs attention","detail":detail,
                              "status":"new","created_at":x.get("last_sync_at") or "","priority":pr,
                              "priority_reason":"sync error" if pr=="critical" else "new monitoring change"},"sync")
    out=list(merged.values())
    for x in out: x["source_count"]=len(x.get("sources") or [])
    out.sort(key=lambda x:(-ranks.get(x.get("priority"),1),-int(x.get("source_count") or 0),str(x.get("created_at") or "")))
    return out


def action_center_metrics(feed):
    rows=list(feed or [])
    return {"open":len(rows),"critical":sum(1 for x in rows if x.get("priority")=="critical"),
            "high":sum(1 for x in rows if x.get("priority")=="high"),
            "unanswered":sum(1 for x in rows if x.get("review_id") is not None and not bool(x.get("responded"))),
            "with_ticket":sum(1 for x in rows if x.get("ticket_id") is not None)}

def render_branch_inspector(br):
    """Render the selected branch with location, contacts, identity, freshness and Google review samples."""
    br=dict(br or {})
    if not br:
        st.markdown('<div class="rt-v30-inspector"><h3>{}</h3><div class="rt-v30-inspector-sub">{}</div></div>'.format(
            "تفاصيل الفرع" if RTL else "Branch details","اختر فرعًا من النتائج لعرض جميع بياناته." if RTL else "Choose a result to inspect all branch data."),unsafe_allow_html=True); return
    name=html.escape(str(br.get("name") or "—")); address=html.escape(str(br.get("address") or "—"))
    group=str(br.get("result_group") or "additional")
    st.markdown('<div class="rt-v30-inspector"><h3>{} <span class="rt-v30-group {}">{}</span></h3><div class="rt-v30-inspector-sub">{} · ⭐ {} · {} {}</div></div>'.format(
        name,html.escape(group),html.escape(_v30_group_label(group)),address,html.escape(str(br.get("rating") or "—")),int(safe_num(br.get("review_count"),0)),"مراجعة" if RTL else "reviews"),unsafe_allow_html=True)
    fields=[
        ("العنوان" if RTL else "Address",br.get("address")),("النشاط" if RTL else "Category",br.get("category")),
        ("الهاتف" if RTL else "Phone",br.get("phone")),("الجوال" if RTL else "Mobile",br.get("mobile")),
        ("البريد" if RTL else "Email",br.get("email")),("الموقع" if RTL else "Website",br.get("website")),
        ("WhatsApp",br.get("whatsapp")),
        ("اكتمال الاتصال" if RTL else "Contact completeness",contact_completeness_v36(br).get("label")),
        ("ثقة الاتصال" if RTL else "Contact confidence","{:.0f}%".format(safe_num(br.get("contact_confidence"),0))),
        ("مصدر الاتصال" if RTL else "Contact source",br.get("contact_source") or (", ".join(sorted(set(str(v) for v in (br.get("field_sources") or {}).values()))) if isinstance(br.get("field_sources"),dict) else "")),
        ("Latitude",br.get("latitude")),("Longitude",br.get("longitude")),
        ("Place ID",br.get("place_id")),("CID",br.get("cid")),
        ("التحقق" if RTL else "Validation","{} · {:.0f}%".format(br.get("validation_status") or "—",safe_num(br.get("validation_score"),0))),
        ("الحداثة" if RTL else "Freshness","{} · {:.0f}%".format(br.get("freshness_status") or "—",safe_num(br.get("freshness_score"),0))),
        ("المصادر" if RTL else "Sources",", ".join(map(str,br.get("providers") or [])) or br.get("source")),
        ("آخر تحديث" if RTL else "Last update",br.get("detail_fetched_at") or br.get("source_fetched_at")),
    ]
    items=[]
    for k,v in fields:
        items.append('<div class="rt-v30-detail"><div class="k">{}</div><div class="v">{}</div></div>'.format(html.escape(str(k)),html.escape(str(v if v not in (None,"") else "—"))))
    st.markdown('<div class="rt-v30-detail-grid">{}</div>'.format("".join(items)),unsafe_allow_html=True)
    maps_url=str(br.get("maps_url") or br.get("url") or "").strip(); website=str(br.get("website") or "").strip()
    links=[]
    if maps_url: links.append('<a href="{}" target="_blank">📍 Google Maps</a>'.format(html.escape(maps_url,quote=True)))
    if website: links.append('<a href="{}" target="_blank">🌐 {}</a>'.format(html.escape(website,quote=True),"الموقع الإلكتروني" if RTL else "Website"))
    if links: st.markdown(" · ".join(links),unsafe_allow_html=True)
    st.markdown("#### 📝 "+("تعليقات Google" if RTL else "Google reviews"))
    reviews=br.get("preview_reviews") or []
    if isinstance(reviews,str):
        try: reviews=json.loads(reviews)
        except Exception: reviews=[]
    if not reviews:
        st.caption("لا توجد عينة مراجعات محمّلة لهذا الفرع بعد." if RTL else "No review samples have been loaded for this branch yet.")
    for rv in list(reviews)[:8]:
        rv=rv or {}; author=html.escape(str(rv.get("author") or "—")); date_s=html.escape(str(rv.get("date") or "")); txt=html.escape(str(rv.get("text") or "")); owner=html.escape(str(rv.get("owner_response") or ""))
        stars="★"*max(0,min(5,int(safe_num(rv.get("rating"),0))))
        owner_html='<div class="owner"><b>{}:</b> {}</div>'.format("رد المالك" if RTL else "Owner response",owner) if owner else ""
        st.markdown('<div class="rt-v30-review"><div class="head">{} · {} · {}</div><div class="body">{}</div>{}</div>'.format(author,stars or "—",date_s,txt or "—",owner_html),unsafe_allow_html=True)
    if br.get("quick_sentiment") or br.get("quick_topic"):
        st.caption(("تحليل سريع: {} · الموضوع الأبرز: {} · سلبي {:.0f}%" if RTL else "Quick analysis: {} · top topic: {} · {:.0f}% negative").format(br.get("quick_sentiment") or "—",br.get("quick_topic") or "—",safe_num(br.get("quick_negative_pct"),0)))


PAGE_GROUPS={
    "dashboard":("الرئيسية","Main"),"quick_search":("البحث","Search"),"branch_search":("البحث","Search"),"map_url":("البحث","Search"),
    "branches_all":("الفروع","Branches"),"branch_workspace":("الفروع","Branches"),"branch_360":("الفروع","Branches"),"map":("الفروع","Branches"),"compare":("الفروع","Branches"),"market_share":("الفروع","Branches"),
    "review_all":("المراجعات","Reviews"),"review_negative":("المراجعات","Reviews"),"review_unanswered":("المراجعات","Reviews"),
    "performance":("التحليلات","Analytics"),"executive":("التحليلات","Analytics"),"reports_builder":("التقارير","Reports"),
    "action_center":("المراقبة والتنبيهات","Monitoring & Alerts"),"alerts":("المراقبة والتنبيهات","Monitoring & Alerts"),"monitoring":("المراقبة والتنبيهات","Monitoring & Alerts"),"sync_control":("المراقبة والتنبيهات","Monitoring & Alerts"),"review_delta":("المراقبة والتنبيهات","Monitoring & Alerts"),"change_intelligence":("المراقبة والتنبيهات","Monitoring & Alerts"),"notif_center":("المراقبة والتنبيهات","Monitoring & Alerts"),"tickets":("المراقبة والتنبيهات","Monitoring & Alerts"),
    "settings":("الإعدادات","Settings"),"diagnostics":("أدوات متقدمة","Advanced tools")}

def page_header(title,subtitle="",page_key=None):
    group=PAGE_GROUPS.get(page_key or st.session_state.get("page"),("","")); group_label=group[0] if RTL else group[1]
    clean_title=re.sub(r"^[^\w\u0600-\u06FF]+\s*", "", str(title)).strip()
    home="الرئيسية" if RTL else "Home"
    parts=[home]
    if group_label and group_label not in ("الرئيسية","Main"): parts.append(group_label)
    if clean_title and clean_title != parts[-1]: parts.append(clean_title)
    crumb=" › ".join(parts)
    st.markdown('<div class="rt-breadcrumb">{}</div><div class="rt-page-head"><div><h2>{}</h2><p>{}</p></div></div>'.format(html.escape(crumb),html.escape(str(title)),html.escape(str(subtitle))),unsafe_allow_html=True)

def status_badge(text,kind="gray"):
    return '<span class="rt-badge b-{}">{}</span>'.format(kind,html.escape(str(text)))

def chart(fig, key=None, height=None):
    fig.update_layout(margin=dict(l=10,r=10,t=38,b=10),
                      paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
                      font=dict(family="Tahoma" if RTL else "Segoe UI", color=C["text"]))
    if height: fig.update_layout(height=height)
    st.plotly_chart(fig, use_container_width=True, key=key, config={"displaylogo": False})


def _fit_map_view(geo, lat_col="latitude", lon_col="longitude", focus_id=None, id_col=None, default_zoom=9):
    if geo is None or geo.empty:
        return {"lat": 0.0, "lon": 0.0}, default_zoom
    work=geo.copy()
    work[lat_col]=pd.to_numeric(work[lat_col],errors="coerce")
    work[lon_col]=pd.to_numeric(work[lon_col],errors="coerce")
    work=work.dropna(subset=[lat_col,lon_col])
    if work.empty:
        return {"lat":0.0,"lon":0.0},default_zoom
    if focus_id is not None and id_col and id_col in work.columns:
        hit=work[work[id_col].astype(str)==str(focus_id)]
        if not hit.empty:
            r=hit.iloc[0]
            return {"lat":float(r[lat_col]),"lon":float(r[lon_col])},14
    center={"lat":float(work[lat_col].mean()),"lon":float(work[lon_col].mean())}
    if len(work)==1:
        return center,14
    lat_span=max(float(work[lat_col].max()-work[lat_col].min()),0.00001)
    lon_span=max(float(work[lon_col].max()-work[lon_col].min()),0.00001)
    span=max(lat_span,lon_span)
    if span<=0.005: z=14
    elif span<=0.02: z=12.5
    elif span<=0.08: z=11
    elif span<=0.20: z=9.5
    elif span<=0.50: z=8.5
    elif span<=1.0: z=7.5
    elif span<=2.5: z=6.5
    elif span<=5.0: z=5.5
    elif span<=12.0: z=4.5
    else: z=3.5
    return center,z


def _map_selected_trace(geo, lat_col, lon_col, hover_name, id_col, selected_ids, use_maplibre):
    if not selected_ids or not id_col or id_col not in geo.columns:
        return None
    wanted={str(x) for x in selected_ids}
    sel=geo[geo[id_col].astype(str).isin(wanted)].copy()
    if sel.empty:
        return None
    text=sel.get(hover_name,pd.Series(["—"]*len(sel))).fillna("—").astype(str).tolist()
    custom=sel[[id_col]].astype(str).values.tolist()
    marker=dict(size=23,color="#1D4ED8",opacity=.95)
    kwargs=dict(lat=sel[lat_col],lon=sel[lon_col],mode="markers",text=text,
                customdata=custom,hovertemplate="<b>%{text}</b><extra></extra>",marker=marker,name="Selected")
    try:
        if use_maplibre and hasattr(go,"Scattermap"):
            return go.Scattermap(**kwargs)
        return go.Scattermapbox(**kwargs)
    except Exception:
        return None


def build_map_figure(df, lat_col="latitude", lon_col="longitude", hover_name="name", hover_data=None,
                     zoom=9, title=None, color_field=None, id_col=None, selected_ids=None,
                     fit_all=True, focus_id=None):
    geo = df.copy()
    if geo is None or len(geo) == 0:
        return None
    geo[lat_col] = pd.to_numeric(geo[lat_col], errors="coerce")
    geo[lon_col] = pd.to_numeric(geo[lon_col], errors="coerce")
    geo = geo.dropna(subset=[lat_col, lon_col]).copy()
    if geo.empty:
        return None
    if color_field and color_field in geo.columns:
        geo["_map_color"] = pd.to_numeric(geo[color_field], errors="coerce").fillna(0)
    else:
        geo["_map_color"] = pd.to_numeric(geo.get("rating"), errors="coerce").fillna(0)
    center,auto_zoom=_fit_map_view(geo,lat_col,lon_col,focus_id=focus_id,id_col=id_col,default_zoom=zoom)
    effective_zoom=auto_zoom if fit_all or focus_id is not None else zoom
    custom_data=[id_col] if id_col and id_col in geo.columns else None
    hover_data=hover_data or []
    use_maplibre=False
    fig=None

    if hasattr(px, "scatter_map"):
        try:
            fig=px.scatter_map(
                geo, lat=lat_col, lon=lon_col, hover_name=hover_name, hover_data=hover_data,
                color="_map_color", zoom=effective_zoom, center=center, map_style="open-street-map", title=title,
                custom_data=custom_data, color_continuous_scale=[[0,"#EF4444"],[0.5,"#F59E0B"],[1,"#10B981"]]
            )
            use_maplibre=True
        except Exception:
            fig=None
    if fig is None and hasattr(px, "scatter_mapbox"):
        try:
            fig=px.scatter_mapbox(
                geo, lat=lat_col, lon=lon_col, hover_name=hover_name, hover_data=hover_data,
                color="_map_color", zoom=effective_zoom, center=center, mapbox_style="open-street-map", title=title,
                custom_data=custom_data, color_continuous_scale=[[0, "#EF4444"], [0.5, "#F59E0B"], [1, "#10B981"]]
            )
        except Exception:
            fig=None

    if fig is None:
        hover_text = []
        for _, r in geo.iterrows():
            bits = [str(r.get(hover_name) or "—")]
            for k in hover_data:
                bits.append(f"{k}: {r.get(k) if pd.notna(r.get(k)) else '—'}")
            hover_text.append("<br>".join(bits))
        custom=geo[[id_col]].astype(str).values.tolist() if id_col and id_col in geo.columns else None
        trace_cls=go.Scattermapbox
        fig = go.Figure(trace_cls(
            lat=geo[lat_col], lon=geo[lon_col], mode="markers", customdata=custom,
            text=hover_text, hovertemplate="%{text}<extra></extra>",
            marker=dict(size=14, color=geo["_map_color"], colorscale=[[0, "#EF4444"], [0.5, "#F59E0B"], [1, "#10B981"]],
                        cmin=float(geo["_map_color"].min()), cmax=max(float(geo["_map_color"].max()),float(geo["_map_color"].min())+1), showscale=False, opacity=0.88)
        ))
        fig.update_layout(mapbox=dict(style="open-street-map",zoom=effective_zoom,center=center))

    selected_trace=_map_selected_trace(geo,lat_col,lon_col,hover_name,id_col,selected_ids,use_maplibre)
    if selected_trace is not None:
        fig.add_trace(selected_trace)
    return fig


def chart_select(fig, key=None, height=None):
    fig.update_layout(margin=dict(l=10,r=10,t=38,b=10),paper_bgcolor="rgba(0,0,0,0)",plot_bgcolor="rgba(0,0,0,0)",font=dict(family="Tahoma" if RTL else "Segoe UI",color=C["text"]))
    if height: fig.update_layout(height=height)
    config={"displaylogo":False,"scrollZoom":True,"modeBarButtonsToRemove":["select2d","lasso2d"]}
    try:
        event=st.plotly_chart(fig,use_container_width=True,key=key,config=config,on_select="rerun",selection_mode="points")
    except (TypeError,ValueError):
        st.plotly_chart(fig,use_container_width=True,key=key,config=config)
        return []
    try:
        selection=event.get("selection",{}) if isinstance(event,dict) else getattr(event,"selection",{})
        points=selection.get("points",[]) if isinstance(selection,dict) else getattr(selection,"points",[])
        out=[]
        for p in points or []:
            cd=p.get("customdata") if isinstance(p,dict) else getattr(p,"customdata",None)
            if isinstance(cd,(list,tuple)) and cd: out.append(cd[0])
            elif cd is not None: out.append(cd)
        return out
    except Exception:
        return []


def info_box(label, value):
    shown = "—" if value is None or str(value).strip() == "" else str(value)
    st.markdown(
        '<div class="rt-info-item"><div class="rt-info-k">{}</div><div class="rt-info-v">{}</div></div>'.format(
            html.escape(str(label)), html.escape(shown)
        ),
        unsafe_allow_html=True,
    )


def render_branch_overview(br):
    c1, c2, c3 = st.columns(3)
    with c1:
        info_box("الاسم" if RTL else "Name", br.get("name"))
        info_box("النشاط" if RTL else "Category", br.get("category"))
        info_box("العنوان" if RTL else "Address", br.get("address"))
    with c2:
        info_box("الهاتف" if RTL else "Phone", br.get("phone"))
        info_box("الجوال" if RTL else "Mobile", br.get("mobile"))
        info_box("البريد" if RTL else "Email", br.get("email"))
    with c3:
        info_box("التقييم" if RTL else "Rating", br.get("rating"))
        info_box("تقييمات Google" if RTL else "Google reviews", br.get("review_count"))
        info_box("الثقة" if RTL else "Confidence", (("{:.0f}%".format(safe_num(br.get("confidence_score"), 0))) if br.get("confidence_score") not in (None, "") else "—"))
    l1, l2, l3, l4 = st.columns(4)
    with l1: info_box("Latitude", br.get("latitude"))
    with l2: info_box("Longitude", br.get("longitude"))
    with l3: info_box("CID", br.get("cid"))
    with l4: info_box("Place ID", br.get("place_id"))
    a1, a2 = st.columns(2)
    if br.get("website"):
        a1.link_button("🌐 " + ("الموقع الإلكتروني" if RTL else "Website"), br.get("website"), use_container_width=True)
    if br.get("maps_url") or br.get("url"):
        a2.link_button("📍 Google Maps", br.get("maps_url") or br.get("url"), use_container_width=True)


def permission(role, required):
    hierarchy = {"viewer": 1, "agent": 2, "manager": 3, "admin": 4}
    return hierarchy.get(role, 0) >= hierarchy.get(required, 999)


# ═══════════════════════════════════════════════════════════════════════
#  SIDE QUICK ACTIONS — compact so reports keep maximum horizontal space
# ═══════════════════════════════════════════════════════════════════════
st.markdown("""<style>
.rt-v32-action{border:1px solid #e2e8f0;border-radius:16px;padding:14px 16px;margin:10px 0;background:#fff;box-shadow:0 5px 18px rgba(15,23,42,.045)}
.rt-v32-action .head{display:flex;justify-content:space-between;gap:12px;color:#475569;font-size:.86rem}.rt-v32-action .title{font-weight:800;font-size:1rem;margin-top:5px}.rt-v32-action .detail{color:#334155;margin-top:5px;line-height:1.55}.rt-v32-action .meta{color:#64748b;font-size:.8rem;margin-top:8px}
.rt-v32-action.critical{border-inline-start:5px solid #dc2626}.rt-v32-action.high{border-inline-start:5px solid #ea580c}.rt-v32-action.normal{border-inline-start:5px solid #2563eb}.rt-v32-action.low{border-inline-start:5px solid #94a3b8}
.rt-v32-action-mini{padding:9px 11px;border:1px solid #e2e8f0;border-radius:12px;background:#f8fafc;margin:6px 0}
.rt-v32-review-card{border:1px solid #e2e8f0;border-radius:15px;padding:13px 15px;margin:9px 0;background:#fff}.rt-v32-review-card .head{display:flex;gap:10px;align-items:center;flex-wrap:wrap;color:#64748b}.rt-v32-review-card .head b{color:#0f172a}.rt-v32-review-card .stars{color:#d97706;letter-spacing:1px}.rt-v32-review-card .body{font-size:.98rem;line-height:1.65;color:#1e293b;margin:9px 0}.rt-v32-review-card .tags{display:flex;gap:7px;flex-wrap:wrap}.rt-v32-review-card .tags span{background:#f1f5f9;border-radius:999px;padding:3px 8px;font-size:.78rem;color:#475569}.rt-v32-review-card .owner{margin-top:10px;padding:9px 11px;border-radius:10px;background:#eff6ff;color:#1e3a8a}
.rt-v36-contact-grid{display:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:7px;margin-top:9px}.rt-v36-contact-chip{display:block;background:#f8fafc;border:1px solid #e2e8f0;border-radius:9px;padding:7px 9px;font-size:.83rem;overflow-wrap:anywhere}.rt-v36-contact-chip small{color:#64748b}
</style>""",unsafe_allow_html=True)

user = st.session_state.user
role = user.get("role", "viewer")
ws_id = st.session_state.workspace_id
ws_df = db.list_workspaces()
ws_info = ws_df[ws_df["id"] == ws_id].iloc[0] if not ws_df.empty and (ws_df["id"] == ws_id).any() else None
ws_name = ws_info["name"] if ws_info is not None else "—"

NAV={
    "main":[("dashboard","🏠 "+t("dashboard"))],
    "search":[("branch_search","🔎 "+("بحث ذكي" if RTL else "Smart search")),("map_url","📍 "+t("map_url"))],
    "branches":[("branch_workspace","🏢 "+("مساحة الفروع" if RTL else "Branch workspace")),("branches_all","▦ "+t("all_branches")),("branch_360","🧭 "+("ملف الفرع 360° القديم" if RTL else "Legacy Branch 360°")),("map","🗺️ "+t("map")),("compare","⚖️ "+t("compare")),("market_share","📈 "+t("market_share"))],
    "reviews":[("review_all","📝 "+t("all_reviews")),("review_negative","⚠️ "+t("negative")),("review_unanswered","💬 "+t("unanswered"))],
    "analytics":[("performance","📊 "+("مركز التحليلات" if RTL else "Analytics center")),("executive","💡 "+t("executive"))],
    "reports":[("reports_builder","📄 "+("مركز التقارير" if RTL else "Report center"))],
    "monitoring":[("action_center","⚡ "+("مركز الإجراءات" if RTL else "Action center")),("alerts","🚨 "+t("alerts")),("monitoring","🔄 "+t("monitoring")),("sync_control","⏱️ "+("طابور المزامنة" if RTL else "Sync queue")),("review_delta","🆕 "+("الجديد منذ المزامنة" if RTL else "Review delta")),("change_intelligence","🧠 "+("ذكاء التغييرات" if RTL else "Change intelligence")),("delivery_center","📨 "+("تسليم التنبيهات" if RTL else "Alert delivery")),("notif_center","🔔 "+t("notifications")),("tickets","🎫 "+t("tickets"))],
    "settings":[("settings","⚙️ "+t("settings"))],
    "advanced":[("ai_settings","🤖 "+t("ai_settings")),("ai_analyze","✨ "+("تحليل AI" if RTL else "Analysis Pipeline 3.0")),("diagnostics","🩺 "+("حالة النظام" if RTL else "System status")),("schema_doctor","🗄️ "+t("schema_doctor")),("backups","💾 "+t("backups")),("connections","🔗 "+("الاتصالات" if RTL else "Connections")),("api_page","🔌 "+t("api")),("whitelabel","🎨 "+t("whitelabel")),("workspaces","🏢 "+t("workspaces")),("users","👥 "+t("users")),("team","🎯 "+t("team")),("activity_log","📜 "+t("activity_log")),("templates_lib","📚 "+t("templates"))]
}

with st.sidebar:
    st.markdown(
        '<div class="rt-brand"><h2><span class="rt42-brand-icon">★</span>'
        'Reviews Tracker</h2><small>v{} · {}</small></div>'.format(
            html.escape(str(VERSION)), html.escape(str(ws_name))
        ), unsafe_allow_html=True)
    st.markdown('<div class="rt42-sidebar-label">{}</div>'.format(
        "التنقل الرئيسي" if RTL else "MAIN NAVIGATION"), unsafe_allow_html=True)
    _primary_pages=[
        ("dashboard", "⌂  "+("الرئيسية" if RTL else "Dashboard")),
        ("branch_search", "⌕  "+("البحث" if RTL else "Search")),
        ("branch_workspace", "▣  "+("ساحة الفروع" if RTL else "Branches")),
        ("action_center", "ϟ  "+("مركز الإجراءات" if RTL else "Action center")),
        ("performance", "▥  "+("التحليلات" if RTL else "Analytics")),
        ("reports_builder", "▤  "+("التقارير" if RTL else "Reports")),
        ("settings", "⚙  "+("الإعدادات" if RTL else "Settings")),
        ("alerts", "♧  "+("التنبيهات" if RTL else "Alerts"))
    ]
    for _id,_label in _primary_pages:
        if st.button(_label, key="rt42_nav_"+_id, use_container_width=True,
                     type="primary" if st.session_state.page==_id else "secondary"):
            nav_to(_id)
    _resume_page=st.session_state.get("rt42_previous_page")
    if isinstance(_resume_page,str) and _resume_page not in ("dashboard",st.session_state.page):
        if st.button("↶ متابعة آخر صفحة" if RTL else "Resume last page",
                     key="rt42_resume_last_page",use_container_width=True):
            nav_to(_resume_page)
    with st.expander("خيارات إضافية" if RTL else "More tools", expanded=False):
        for _id,_label in [
            ("review_all","المراجعات" if RTL else "All reviews"),
            ("branches_all","كل الفروع" if RTL else "All branches"),
            ("monitoring","المراقبة" if RTL else "Monitoring"),
            ("sync_control","المزامنة" if RTL else "Synchronization"),
            ("ai_settings","إعدادات الذكاء الاصطناعي" if RTL else "AI settings"),
            ("diagnostics","تشخيص النظام" if RTL else "Diagnostics"),
            ("connections","الاتصالات" if RTL else "Connections")
        ]:
            if st.button(_label,key="rt42_more_"+_id,use_container_width=True):
                nav_to(_id)
    st.markdown('<div class="rt42-sidebar-footer">{}<br>{}</div>'.format(
        html.escape(str(user.get("display_name") or user.get("username") or "Admin")),
        "بيانات محلية · إصدار تطوير" if RTL else "Local records · Development build"
    ), unsafe_allow_html=True)

# Top bar: branch/date filters, search, refresh, language.
companies, period_reviews = all_data()
company_options={"__ALL__":("جميع الفروع" if RTL else "All branches")}
if not companies.empty:
    company_options.update({
        str(_row["pk"]): str(_row["name"]) for _,_row in companies.iterrows()
    })
st.markdown('<div class="rt42-toolbar">{}</div>'.format(
    "التحكم والبحث" if RTL else "FILTERS & SEARCH"), unsafe_allow_html=True)
_tb_branch,_tb_date,_tb_search,_tb_go,_tb_refresh,_tb_language=st.columns(
    [1.55,1.2,2.1,.62,.82,.64],gap="small")
with _tb_branch:
    _branch_values=list(company_options)
    if st.session_state.get("rt42_filter_branch") not in company_options:
        st.session_state.rt42_filter_branch="__ALL__"
    st.selectbox("الفرع" if RTL else "Branch",_branch_values,
        format_func=lambda k:company_options.get(k,k),
        key="rt42_filter_branch")
with _tb_date:
    _period_labels={"today":("اليوم" if RTL else "Today"),
                    "d7":("آخر 7 أيام" if RTL else "7 days"),
                    "d30":("آخر 30 يومًا" if RTL else "30 days"),
                    "d90":("آخر 90 يومًا" if RTL else "90 days"),
                    "custom":("أيام مخصصة" if RTL else "Custom"),
                    "all":("كل الوقت" if RTL else "All time")}
    _period_items=list(_period_labels)
    _period_choice=st.selectbox("الفترة" if RTL else "Period",_period_items,
        index=_period_items.index(st.session_state.period_key)
        if st.session_state.period_key in _period_items else 2,
        format_func=lambda k:_period_labels[k],key="rt42_period_choice")
    if _period_choice!=st.session_state.period_key:
        st.session_state.period_key=_period_choice
        invalidate_cache()
        st.rerun()
if st.session_state.period_key=="custom":
    with st.expander("تحديد عدد الأيام" if RTL else "Custom period days",expanded=False):
        _custom=st.number_input("الأيام" if RTL else "Days",min_value=1,max_value=3650,
             value=int(st.session_state.get("period_custom_days",30)),key="rt42_custom_period")
        if int(_custom)!=int(st.session_state.period_custom_days):
            st.session_state.period_custom_days=int(_custom)
            invalidate_cache()
            st.rerun()
with _tb_search:
    _top_query=st.text_input("بحث عام" if RTL else "Global search",
        placeholder=("بحث عن فرع أو شركة..." if RTL else "Search branches and companies..."),
        key="rt42_global_search")
with _tb_go:
    st.markdown('<div style="height:25px"></div>',unsafe_allow_html=True)
    if st.button("بحث" if RTL else "Go",key="rt42_search_go",use_container_width=True,
                 type="primary"):
        if _top_query.strip():
            st.session_state.discovery_query=_top_query.strip()
            nav_to("branch_search")
        else:
            st.warning("اكتب كلمة للبحث" if RTL else "Enter a search query")
with _tb_refresh:
    st.markdown('<div style="height:25px"></div>',unsafe_allow_html=True)
    if st.button("↻ "+("تحديث" if RTL else "Refresh"),key="rt42_refresh",
                 use_container_width=True):
        invalidate_cache()
        st.rerun()
with _tb_language:
    st.markdown('<div style="height:25px"></div>',unsafe_allow_html=True)
    if st.button("EN" if RTL else "AR",key="rt42_language",
                 use_container_width=True):
        st.session_state.lang="en" if RTL else "ar"
        st.rerun()

# ═══════════════════════════════════════════════════════════════════════
#  PAGES
# ═══════════════════════════════════════════════════════════════════════
page = st.session_state.page

# ---- APPROVED RTL DASHBOARD (v42) ----
if page == "dashboard":
    from dashboard_v42 import render_dashboard_v42
    _period_since,_period_until=period_bounds()
    _history_days=max(1,min(3650,(date.today()-_period_since.date()).days+1)) if _period_since else 3650
    try:
        _snapshots=db.snapshots(ws_id,None,days=_history_days)
    except Exception:
        _snapshots=pd.DataFrame()
    try:
        _open_alerts=db.list_alerts(ws_id,status="new",limit=300)
    except Exception:
        _open_alerts=pd.DataFrame()
    try:
        with db._c() as _conn:
            _alert_count=int(_conn.execute(
                "SELECT COUNT(*) FROM alerts WHERE workspace_id=? AND "
                "LOWER(COALESCE(status,'new')) IN ('new','open','unread','pending')",
                (ws_id,)).fetchone()[0])
    except Exception:
        _alert_count=None
    _model=render_dashboard_v42(
        companies, period_reviews, _snapshots, _open_alerts,
        rtl=RTL,
        selected_pk=st.session_state.get("rt42_filter_branch","__ALL__"),
        navigate=nav_to,
        period_start=_period_since,
        full_alert_count=_alert_count
    )
    if companies.empty:
        st.info("أضف الفروع من صفحة البحث لتظهر مؤشراتك هنا." if RTL
                else "Discover or add branches to populate this dashboard.")
        if st.button("ابدأ البحث" if RTL else "Start searching",
                     type="primary",key="rt42_search_empty"):
            nav_to("branch_search")
    with st.expander("جودة بيانات الفروع" if RTL else "Branch data quality",expanded=False):
        try:
            from quality_v41 import branch_quality_summary
            _quality=branch_quality_summary(companies)
            _cols=st.columns(4)
            for _col,_label,_value in zip(_cols,(
                "الفروع" if RTL else "Branches",
                "هوية Places" if RTL else "Place IDs",
                "روابط الخرائط" if RTL else "Maps URLs",
                "أرقام الاتصال" if RTL else "Phone numbers"
            ),(
                _quality["total_branches"],_quality["place_ids"],
                _quality["maps_urls"],_quality["contact_numbers"]
            )):
                _col.metric(_label,_value)
            if not _quality["missing_rows"].empty:
                st.dataframe(_quality["missing_rows"],use_container_width=True,
                             hide_index=True,height=210)
        except Exception as _quality_exc:
            st.caption(("تعذّر عرض تفاصيل الجودة" if RTL else "Quality summary unavailable")
                       + ": " + type(_quality_exc).__name__)

# ---- DISCOVERY ----
if page in {"quick_search","branch_search","map_url"}:
    page_header("🔎 "+("البحث الذكي 36.0" if RTL else "Smart Search 36.0"),
                "اكتب أي اسم أو جزء منه؛ البرنامج يوسّع البحث تلقائيًا ويجمع النتائج ثم يرتبها ويعرض تفاصيل كل فرع." if RTL else "Type any full or partial name; the app expands the query automatically, fuses discoveries, ranks them and exposes each branch in detail.",page)
    if page=="map_url":
        q=st.text_input("رابط Google Maps" if RTL else "Google Maps URL",key="mq_url"); mx=st.number_input("أقصى مراجعات" if RTL else "Max reviews",20,1000,100,20,key="mmx")
        if st.button("🚀 "+("قراءة وتحليل الفرع" if RTL else "Read & analyze branch"),type="primary",use_container_width=True,key="mgourl") and q.strip():
            if not is_maps_url(q): st.error("الرابط غير صالح" if RTL else "Invalid Maps URL")
            else:
                with st.spinner("جاري قراءة بيانات الفرع والمراجعات..." if RTL else "Reading branch and reviews..."):
                    pk,info,new,dup,err=scrape_and_store(db,q.strip(),int(mx),True,ws_id,user["username"])
                if err: st.error(err)
                else: st.success(("تم حفظ {} مراجعة جديدة، {} مكررة." if RTL else "Saved {} new reviews, {} duplicates.").format(new,dup)); st.session_state.selected_branch_pk=pk
    else:
        incoming=st.session_state.pop("discovery_query","")
        _restore_detached_search_job()
        if incoming: st.session_state["discovery_form_q"]=incoming
        st.session_state.setdefault("discovery_form_q","")
        st.markdown('<div class="rt-v30-search-hero"><h3>{}</h3><p>{}</p></div>'.format(
            "ابحث كما تبحث في Google" if RTL else "Search the way you search Google",
            "اكتب الاسم كاملًا أو جزءًا منه فقط. الاقتراحات وتوسيع الاسم والبحث عن الفروع تتم تلقائيًا في الخلفية." if RTL else "Type a full name or just a fragment. Suggestions, query expansion and branch discovery happen automatically in the background."),unsafe_allow_html=True)
        _search_city=""; _search_area=""; _search_activity=""
        with st.popover("⚙️ "+("خيارات البحث" if RTL else "Search options")):
            _search_city=st.text_input("المدينة — اختياري" if RTL else "City — optional",key="v30_city")
            _search_area=st.text_input("الحي/المنطقة — اختياري" if RTL else "Area — optional",key="v30_area")
            _search_activity=st.text_input("النشاط — اختياري" if RTL else "Activity — optional",key="v30_activity")
            st.caption("اتركها فارغة للبحث الواسع. سيعرض البرنامج ما يجده ثم يرتبه بدل إخفائه مبكرًا." if RTL else "Leave blank for broad discovery. Results are ranked after discovery instead of being hidden early.")
        _sq1,_sq2=st.columns([7.5,1.35])
        q=_sq1.text_input("اسم الشركة أو المحل" if RTL else "Company or store name",key="discovery_form_q",placeholder="مثال: البي، كيو، برجر، صيانة كمبيوتر..." if RTL else "Example: a brand name or any fragment...",label_visibility="collapsed")
        typed_q=q.strip()
        submitted=_sq2.button("🔎 "+("بحث" if RTL else "Search"),type="primary",use_container_width=True,key="one_search_v300")
        if submitted:
            if not typed_q:
                st.warning("اكتب اسمًا أو جزءًا من الاسم أولًا." if RTL else "Type a name or part of a name first.")
            else:
                cancel_active_search()
                st.session_state.search_query_used=typed_q
                st.session_state.search_params={"preset":"v30_google_style","city":_search_city.strip(),"area":_search_area.strip(),"activity":_search_activity.strip(),"radius":None,"display":700,"discover":2500,"tiles":72,"details":True,"exact":False,"auto":True,"typed_query":typed_q}
                start_search_job(typed_q,700,city=_search_city.strip(),area=_search_area.strip(),activity=_search_activity.strip(),radius_km=None,deep=True,exact_name=False,max_tiles=72,details=True,discover_max=2500,auto_mode=True)
                st.rerun()

        if not isinstance(st.session_state.get("active_search_job"),dict):
            _resume_sessions=[m for m in _find_resumable_search_sessions(3) if m.get("effective_status") in {"interrupted","cancelled","partial","error"}]
            if _resume_sessions:
                _rm=_resume_sessions[0]; _cs=_rm.get("checkpoint_summary") or {}
                st.markdown('<div class="rt-v30-status"><b>{}</b><span>{}</span></div>'.format(
                    "جلسة سابقة قابلة للاستئناف" if RTL else "Previous search can resume",
                    html.escape(("{} · {} نتائج محفوظة" if RTL else "{} · {} saved results").format(_rm.get("query",""),int(_cs.get("results") or 0)))),unsafe_allow_html=True)
                if st.button("↩️ "+("استكمال البحث" if RTL else "Resume search"),key="resume_latest_session_v300"):
                    _resume_search_manifest(_rm); st.rerun()
        if isinstance(st.session_state.get("active_search_job"),dict):
            if hasattr(st,"fragment"):
                @st.fragment(run_every=0.8)
                def _search_job_fragment_v21(): render_search_job_panel()
                _search_job_fragment_v21()
            else: render_search_job_panel()
        if st.session_state.get("search_job_error"): st.error("❌ "+str(st.session_state.pop("search_job_error")))

        final_results=st.session_state.get("search_results") or []
        live_results=st.session_state.get("search_live_results") or []
        results=final_results or live_results
        meta=st.session_state.get("search_meta") or ({"live":True,"total_discovered":len(live_results)} if live_results else {})
        if isinstance(st.session_state.get("last_search_job"),dict) and not results and not st.session_state.get("active_search_job"):
            _lm=(st.session_state.get("last_search_job") or {}).get("meta") or {}; _reason=_lm.get("no_result_reason") or _lm.get("termination_reason") or "unknown"
            _msg={"google_blocked":"Google أوقف جلسة المتصفح مؤقتًا.","environment_blocked":"بيئة Windows أو الشبكة منعت الوصول إلى Google.","network_error":"تعذر الوصول إلى Google بسبب الشبكة أو DNS."}.get(_reason,"لم يتم العثور على نتائج قابلة للعرض.") if RTL else {"google_blocked":"Google temporarily blocked the browser session.","environment_blocked":"Windows or the network blocked Google access.","network_error":"Google could not be reached because of network/DNS."}.get(_reason,"No displayable results were found.")
            st.warning(_msg)
        if results:
            enriched=enrich_google_quick_analysis(results)
            rdf=pd.DataFrame(enriched).copy().reset_index(drop=True)
            for _c,_default in [("name",""),("address",""),("category",""),("phone",""),("mobile",""),("email",""),("website",""),("result_group","additional"),("validation_status","weak"),("freshness_status","unknown"),("providers",None),("preview_reviews",None)]:
                if _c not in rdf.columns: rdf[_c]=_default
            for _c in ["result_score","validation_score","match_score","data_completeness","rating","review_count","distance_km","freshness_score"]:
                if _c not in rdf.columns: rdf[_c]=0
            # Live rows from an older/checkpoint payload may not yet contain v30 grouping.
            _rg=rdf["result_group"].fillna("").astype(str)
            _valid_rg=_rg.isin(["strong","confirmed","related","additional"])
            _m=pd.to_numeric(rdf["match_score"],errors="coerce").fillna(0.0)
            _v=rdf["validation_status"].fillna("weak").astype(str)
            _computed=np.select([
                (_m>=.78)&(_v=="confirmed"),
                (_m>=.58)&(_v=="confirmed"),
                (_m>=.38)|(_v=="probable"),
            ],["strong","confirmed","related"],default="additional")
            rdf["result_group"]=_rg.where(_valid_rg,pd.Series(_computed,index=rdf.index))
            rdf["result_group_rank"]=rdf["result_group"].map({"strong":0,"confirmed":1,"related":2,"additional":3}).fillna(3)
            rdf["_search_blob_v35"]=build_search_blob_v35(rdf)
            rdf=rdf.sort_values(["result_group_rank","result_score","validation_score","match_score","review_count"],ascending=[True,False,False,False,False],kind="stable")

            _gcounts=rdf["result_group"].value_counts().to_dict(); _coverage=safe_num(meta.get("coverage_percent"),0); _elapsed=safe_num(meta.get("elapsed_ms",meta.get("app_elapsed_ms",0)),0)
            st.markdown('<div class="rt-v30-status"><b>{}</b><span>{}</span><span>{}</span><span>{}</span><span>{}</span></div>'.format(
                ("{} نتيجة" if RTL else "{} results").format(len(rdf)),
                ("الأقرب: {}" if RTL else "Best: {}").format(_gcounts.get("strong",0)),
                ("مؤكد: {}" if RTL else "Confirmed: {}").format(_gcounts.get("confirmed",0)),
                ("تغطية: {:.0f}%" if RTL else "Coverage: {:.0f}%").format(_coverage),
                ("{:.1f} ث" if RTL else "{:.1f}s").format(_elapsed/1000.0)),unsafe_allow_html=True)

            st.markdown("### "+("🔎 فلترة وترتيب النتائج" if RTL else "🔎 Filter & sort results"))
            _filter_q=st.text_input("بحث داخل النتائج" if RTL else "Filter results",placeholder="اسم، جزء من الاسم، عنوان، نشاط، هاتف، بريد..." if RTL else "Name, partial name, address, category, phone, email...",key="result_filter_v300",label_visibility="collapsed")

            # v35: first-class visible sorting controls that cannot be squeezed out by the view selector.
            st.session_state.setdefault("v35_sort_primary","auto")
            st.session_state.setdefault("v35_sort_direction","forward")
            _primary=st.session_state.get("v35_sort_primary","auto")
            _direction=st.session_state.get("v35_sort_direction","forward")
            _s1,_s2=st.columns(2); _s3,_s4=st.columns(2)
            _quick=[
                (_s1,"rating","⭐ "+("الأعلى تقييمًا" if RTL else "Rating")),
                (_s2,"distance","📍 "+("الأقرب" if RTL else "Nearest")),
                (_s3,"date","🕒 "+("الأحدث" if RTL else "Newest")),
                (_s4,"reviews","📝 "+("الأكثر مراجعات" if RTL else "Most reviews")),
            ]
            for _col,_kind,_label in _quick:
                if _col.button(_label,use_container_width=True,type=("primary" if _primary==_kind else "secondary"),key="v35_sort_btn_"+_kind):
                    if st.session_state.get("v35_sort_primary")==_kind:
                        st.session_state["v35_sort_direction"]="reverse" if st.session_state.get("v35_sort_direction")=="forward" else "forward"
                    else:
                        st.session_state["v35_sort_primary"]=_kind; st.session_state["v35_sort_direction"]="forward"
                    _primary=st.session_state["v35_sort_primary"]; _direction=st.session_state["v35_sort_direction"]

            st.caption("اضغط زر الفرز المحدد مرة أخرى لعكس الاتجاه، أو استخدم زر عكس الاتجاه." if RTL else "Click the selected sort again to reverse it, or use Reverse.")

            _mode_map={
                ("rating","forward"):"rating_desc",("rating","reverse"):"rating_asc",
                ("distance","forward"):"distance_asc",("distance","reverse"):"distance_desc",
                ("date","forward"):"date_desc",("date","reverse"):"date_asc",
                ("reviews","forward"):"reviews_desc",("reviews","reverse"):"reviews_asc",
            }
            _sort_mode=_mode_map.get((_primary,_direction),_primary if _primary in {"match_desc","validation_desc","freshness_desc","freshness_asc","name_asc","name_desc"} else "auto")
            _sort_labels_ar={"auto":"الأفضل تلقائيًا","rating_desc":"⭐ الأعلى تقييمًا ← الأدنى","rating_asc":"⭐ الأدنى تقييمًا ← الأعلى","distance_asc":"📍 الأقرب ← الأبعد","distance_desc":"📍 الأبعد ← الأقرب","date_desc":"🕒 الأحدث ← الأقدم","date_asc":"🕒 الأقدم ← الأحدث","reviews_desc":"📝 الأكثر مراجعات ← الأقل","reviews_asc":"📝 الأقل مراجعات ← الأكثر","match_desc":"🎯 الأعلى تطابقًا","validation_desc":"✅ الأعلى موثوقية","freshness_desc":"🔄 الأحدث تحديثًا","freshness_asc":"🔄 الأقدم تحديثًا","name_asc":"🔤 الاسم أ ← ي","name_desc":"🔤 الاسم ي ← أ"}
            _sort_labels_en={"auto":"Best automatically","rating_desc":"⭐ Highest rating → lowest","rating_asc":"⭐ Lowest rating → highest","distance_asc":"📍 Nearest → farthest","distance_desc":"📍 Farthest → nearest","date_desc":"🕒 Newest → oldest","date_asc":"🕒 Oldest → newest","reviews_desc":"📝 Most reviews → fewest","reviews_asc":"📝 Fewest reviews → most","match_desc":"🎯 Best search match","validation_desc":"✅ Highest validation","freshness_desc":"🔄 Most recently refreshed","freshness_asc":"🔄 Oldest refresh","name_asc":"🔤 Name A → Z","name_desc":"🔤 Name Z → A"}
            _ctl1,_ctl2,_ctl3=st.columns([2.6,1.2,1.2])
            _ctl1.markdown("**{}:** {}".format("الفرز الحالي" if RTL else "Current sort",(_sort_labels_ar if RTL else _sort_labels_en).get(_sort_mode,_sort_mode)))
            if _ctl2.button("↕ "+("عكس الاتجاه" if RTL else "Reverse"),use_container_width=True,key="v35_reverse_btn"):
                if _primary in {"rating","distance","date","reviews"}:
                    st.session_state["v35_sort_direction"]="reverse" if _direction=="forward" else "forward"
                    _direction=st.session_state["v35_sort_direction"]
                    _sort_mode=_mode_map.get((_primary,_direction),_sort_mode)
            with _ctl3.popover("↕ "+("المزيد" if RTL else "More"),use_container_width=True):
                _extra=[("auto","✨ "+("الأفضل تلقائيًا" if RTL else "Best automatically")),("match_desc","🎯 "+("الأعلى تطابقًا" if RTL else "Best match")),("validation_desc","✅ "+("الأعلى موثوقية" if RTL else "Validation")),("freshness_desc","🔄 "+("الأحدث تحديثًا" if RTL else "Freshest")),("freshness_asc","🗂️ "+("الأقدم تحديثًا" if RTL else "Oldest data")),("name_asc","🔤 "+("الاسم أ ← ي" if RTL else "Name A → Z")),("name_desc","🔤 "+("الاسم ي ← أ" if RTL else "Name Z → A"))]
                for _em,_el in _extra:
                    if st.button(_el,use_container_width=True,key="v35_extra_"+_em):
                        st.session_state["v35_sort_primary"]=_em; st.session_state["v35_sort_direction"]="forward"; _primary=_em; _sort_mode=_em

            _view1,_view2=st.columns([2.2,.8])
            _view_mode=_view1.radio("العرض" if RTL else "View",["cards","map","table"],horizontal=True,format_func=lambda x:{"cards":"▤ بطاقات","map":"🗺️ خريطة","table":"▦ جدول"}.get(x,x) if RTL else {"cards":"▤ Cards","map":"🗺️ Map","table":"▦ Table"}.get(x,x),key="result_view_mode_v300",label_visibility="collapsed")
            _group_filter="all"; _min_rating=0.0; _contact_only=False; _page_size=30; _page_no=1
            with _view2.popover("⚙️ "+("فلاتر" if RTL else "Filters"),use_container_width=True):
                _group_filter=st.selectbox("مجموعة النتائج" if RTL else "Result group",["all","strong","confirmed","related","additional"],format_func=lambda x:("الكل" if x=="all" else _v30_group_label(x)),key="v30_group_filter")
                _min_rating=st.select_slider("أدنى تقييم" if RTL else "Minimum rating",options=[0.0,1.0,2.0,3.0,4.0,4.5],value=0.0,key="v30_min_rating")
                _contact_only=st.checkbox("لديه وسيلة تواصل" if RTL else "Has contact details",value=False,key="v30_contact_only")
                _page_size=st.selectbox("بطاقات في الصفحة" if RTL else "Cards per page",[20,30,50,80],index=1,key="v30_page_size")
                _page_no=st.number_input("صفحة البطاقات" if RTL else "Card page",1,999,1,1,key="v30_page_no")
                st.divider()
                if st.button("💾 "+("حفظ النتائج المفلترة" if RTL else "Save filtered results"),use_container_width=True,key="v30_save_filtered_pending"):
                    st.session_state["v30_do_save_filtered"]=True

            mask=search_result_filter_mask(rdf,_filter_q); view=rdf.loc[mask].copy()
            if _group_filter!="all": view=view[view["result_group"]==_group_filter]
            if _min_rating>0: view=view[pd.to_numeric(view["rating"],errors="coerce").fillna(0)>=float(_min_rating)]
            if _contact_only: view=view[(view["phone"].astype(str).str.strip()!="") | (view["mobile"].astype(str).str.strip()!="") | (view["email"].astype(str).str.strip()!="") | (view["website"].astype(str).str.strip()!="")]
            view=sort_search_results(view,_sort_mode)
            if st.session_state.pop("v30_do_save_filtered",False):
                n,_=db.save_companies(view.to_dict("records"),st.session_state.get("search_query_used",""),ws_id); invalidate_cache(); st.success(("تم حفظ {} فرعًا" if RTL else "Saved {} branches").format(n))

            # One secondary actions menu; no always-visible bulk action strip.
            with st.popover("⋯ "+("تصدير وعمليات" if RTL else "Export & actions")):
                st.markdown("**📦 "+("التصدير الشامل v40" if RTL else "v40 complete export")+"**")
                render_complete_export_controls_v40(db,ws_id,companies,view,st.session_state.get("selected_branch_pk"),"search")
                st.divider(); st.caption("التصدير السريع القديم" if RTL else "Legacy quick export")
                st.download_button("📄 "+("تصدير PDF" if RTL else "Export PDF"),dataframe_to_pdf_cached_v39(view,"نتائج البحث" if RTL else "Search results"),file_name="reviews_tracker_search.pdf",mime="application/pdf",use_container_width=True,key="export_pdf_v39",disabled=view.empty)
                st.download_button("📊 "+("تصدير Excel" if RTL else "Export Excel"),to_excel(view),file_name="reviews_tracker_search.xlsx",mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",use_container_width=True,key="export_v300",disabled=view.empty)
                st.download_button("🗂️ "+("تصدير CSV" if RTL else "Export CSV"),to_csv(view),file_name="reviews_tracker_search.csv",mime="text/csv",use_container_width=True,key="export_csv_v39",disabled=view.empty)
                if st.button("💾 "+("حفظ كل المعروض" if RTL else "Save all shown"),use_container_width=True,disabled=view.empty,key="save_view_v300"):
                    n,_=db.save_companies(view.to_dict("records"),st.session_state.get("search_query_used",""),ws_id); invalidate_cache(); st.success(("تم حفظ {} فرعًا" if RTL else "Saved {} branches").format(n))

            st.session_state.setdefault("v30_selected_result_idx",int(view.index[0]) if not view.empty else None)
            selected_idx=st.session_state.get("v30_selected_result_idx")
            if selected_idx not in rdf.index and not view.empty: selected_idx=int(view.index[0]); st.session_state.v30_selected_result_idx=selected_idx
            _left,_right=st.columns([1.65,1.0])
            with _left:
                if view.empty:
                    st.info("لا توجد نتائج بعد الفلترة." if RTL else "No results match the current filters.")
                elif _view_mode=="cards":
                    _pages=max(1,math.ceil(len(view)/int(_page_size))); _p=max(1,min(int(_page_no),_pages)); _start=(_p-1)*int(_page_size); _card_view=view.iloc[_start:_start+int(_page_size)]
                    st.caption(("عرض {}–{} من {} · صفحة {} / {}" if RTL else "Showing {}–{} of {} · page {} / {}").format(_start+1,min(_start+len(_card_view),len(view)),len(view),_p,_pages))
                    for idx,row in _card_view.iterrows():
                        if render_search_result_card(int(idx),row.to_dict(),selected=(int(idx)==selected_idx)):
                            _rec=row.to_dict(); st.session_state.v30_selected_result_idx=int(idx)
                            st.session_state.selected_workspace_record_v31=_rec
                            try:
                                _candidate=db._resolve_canonical_pk(_rec,ws_id,st.session_state.get("search_query_used", ""),register=False)
                                if db.get_company(_candidate,ws_id): st.session_state.selected_branch_pk=str(_candidate)
                                else: st.session_state.selected_branch_pk=None
                            except Exception: st.session_state.selected_branch_pk=None
                            nav_to("branch_workspace"); st.rerun()
                elif _view_mode=="table":
                    _cols=[c for c in ["name","result_group","rating","review_count","category","address","phone","mobile","email","website","whatsapp","contact_confidence","validation_score","freshness_score","distance_km"] if c in view.columns]
                    st.dataframe(view[_cols],use_container_width=True,hide_index=True,height=590)
                    _pick=st.selectbox("فتح تفاصيل فرع" if RTL else "Open branch details",view.index.tolist(),index=(view.index.tolist().index(selected_idx) if selected_idx in view.index else 0),format_func=lambda i:"{} · ⭐ {}".format(rdf.loc[i].get("name","—"),rdf.loc[i].get("rating") or "—"),key="v30_table_pick")
                    if int(_pick)!=selected_idx: st.session_state.v30_selected_result_idx=int(_pick); selected_idx=int(_pick)
                else:
                    geo=view.dropna(subset=["latitude","longitude"]).copy()
                    if geo.empty: st.info("لا توجد إحداثيات لهذه النتائج." if RTL else "No coordinates are available for these results.")
                    else:
                        geo["_source_index"]=geo.index.astype(int)
                        fig=build_map_figure(geo,lat_col="latitude",lon_col="longitude",hover_name="name",hover_data=["rating","review_count","address","phone","mobile","email","validation_score"],zoom=9,title=("خريطة النتائج" if RTL else "Results map"),color_field="validation_score",id_col="_source_index",selected_ids={selected_idx} if selected_idx is not None else set(),fit_all=True,focus_id=None)
                        if fig is not None:
                            clicked=chart_select(fig,"search_map_v300",590)
                            if clicked:
                                try: st.session_state.v30_selected_result_idx=int(clicked[0]); selected_idx=int(clicked[0])
                                except Exception: pass
            with _right:
                br=rdf.loc[int(selected_idx)].to_dict() if selected_idx in rdf.index else {}
                render_branch_inspector(br)
                if br and st.button("🏢 "+("فتح في مساحة الفروع" if RTL else "Open in branch workspace"),use_container_width=True,key="v31_open_selected_search"):
                    st.session_state.selected_workspace_record_v31=dict(br)
                    try:
                        _candidate=db._resolve_canonical_pk(br,ws_id,st.session_state.get("search_query_used", ""),register=False)
                        st.session_state.selected_branch_pk=str(_candidate) if db.get_company(_candidate,ws_id) else None
                    except Exception: st.session_state.selected_branch_pk=None
                    nav_to("branch_workspace"); st.rerun()
        elif typed_q and not st.session_state.get("active_search_job"):
            st.caption("ابدأ البحث لعرض النتائج هنا." if RTL else "Start a search to show results here.")


# ---- REVIEWS ENGINE 3.0 ----
if page in {"review_all","review_negative","review_unanswered"}:
    page_header("📝 "+t("all_reviews"),"محرك مراجعات 3.0: Pagination SQL، فلاتر مركبة، Workflow، أولوية وملاحظات." if RTL else "Reviews Engine 3.0: SQL pagination, compound filters, workflow, priority and notes.",page)
    since,_=period_bounds()
    rv1,rv2=st.columns([2.2,1.4])
    text_q=rv1.text_input("بحث في النص أو الكاتب أو الموضوع" if RTL else "Search text, author or topic",placeholder="اكتب كلمة أو جملة..." if RTL else "Type a word or phrase...",key="rv_text_v220")
    branch_opts=["__ALL__"]+(companies["pk"].astype(str).tolist() if not companies.empty else [])
    branch=rv2.selectbox("الفرع" if RTL else "Branch",branch_opts,format_func=lambda x:("كل الفروع" if RTL else "All branches") if x=="__ALL__" else company_options.get(x,x),key="rv_branch_v220")
    sent_default="negative" if page=="review_negative" else "all"
    status_default="all"
    f1,f2,f3,f4=st.columns(4)
    sentiment=f1.selectbox("المشاعر" if RTL else "Sentiment",["all","positive","neutral","negative"],index=["all","positive","neutral","negative"].index(sent_default),format_func=lambda x:{"all":"الكل","positive":"إيجابي","neutral":"محايد","negative":"سلبي"}.get(x,x) if RTL else x.title(),key="rv_sent_v220")
    statuses=["all","new","needs_response","in_progress","responded","resolved","ignored"]
    status=f2.selectbox("الحالة" if RTL else "Workflow",statuses,index=statuses.index(status_default),format_func=lambda x:{"all":"الكل","new":"جديد","needs_response":"يحتاج رد","in_progress":"قيد المتابعة","responded":"تم الرد","resolved":"تم الحل","ignored":"متجاهل"}.get(x,x) if RTL else x.replace("_"," ").title(),key="rv_status_v220")
    topics=["all"]+db.review_topics(ws_id)
    topic=f3.selectbox("الموضوع" if RTL else "Topic",topics,format_func=lambda x:("الكل" if RTL else "All") if x=="all" else x,key="rv_topic_v220")
    priority=f4.selectbox("الأولوية" if RTL else "Priority",["all","critical","high","normal","low"],format_func=lambda x:{"all":"الكل","critical":"حرجة","high":"عالية","normal":"عادية","low":"منخفضة"}.get(x,x) if RTL else x.title(),key="rv_priority_v220")

    f5,f6,f7,f8=st.columns(4)
    min_rating=f5.selectbox("أدنى نجوم" if RTL else "Min rating",[0,1,2,3,4,5],key="rv_min_rating_v220")
    resp_options=["all","unanswered","answered"]
    resp_default="unanswered" if page=="review_unanswered" else "all"
    response_filter=f6.selectbox("حالة الرد" if RTL else "Response",resp_options,index=resp_options.index(resp_default),format_func=lambda x:{"all":"الكل","unanswered":"بدون رد","answered":"تم الرد"}.get(x,x) if RTL else x.title(),key="rv_resp_v220")
    sort=f7.selectbox("الترتيب" if RTL else "Sort",["newest","oldest","priority","rating_low","rating_high","author_asc","author_desc","unanswered_first","answered_first"],format_func=lambda x:{"newest":"الأحدث ← الأقدم","oldest":"الأقدم ← الأحدث","priority":"الأولوية","rating_low":"التقييم الأقل ← الأعلى","rating_high":"التقييم الأعلى ← الأقل","author_asc":"الكاتب أ ← ي","author_desc":"الكاتب ي ← أ","unanswered_first":"بدون رد أولًا","answered_first":"تم الرد أولًا"}.get(x,x) if RTL else {"newest":"Newest → oldest","oldest":"Oldest → newest","priority":"Priority","rating_low":"Lowest rating → highest","rating_high":"Highest rating → lowest","author_asc":"Author A → Z","author_desc":"Author Z → A","unanswered_first":"Unanswered first","answered_first":"Answered first"}.get(x,x),key="rv_sort_v220")
    page_size=f8.selectbox("عدد الصفوف" if RTL else "Rows per page",[20,50,100],index=1,key="rv_size_v220")
    resp_value=None if response_filter=="all" else (response_filter=="answered")
    count=db.reviews_count(ws_id,since,None if status=="all" else status,None if sentiment=="all" else sentiment,None if branch=="__ALL__" else branch,text_q.strip() or None,
                           None if topic=="all" else topic,None if min_rating==0 else int(min_rating),None,resp_value,None if priority=="all" else priority)
    pages=max(1,math.ceil(count/int(page_size)))
    pg=st.number_input("الصفحة" if RTL else "Page",1,pages,min(int(st.session_state.get("rv_pg_v220",1) or 1),pages),1,key="rv_pg_v220")
    df=db.reviews_page(ws_id,since,None if status=="all" else status,None if sentiment=="all" else sentiment,None if branch=="__ALL__" else branch,text_q.strip() or None,int(page_size),(int(pg)-1)*int(page_size),
                       None if topic=="all" else topic,None if min_rating==0 else int(min_rating),None,resp_value,None if priority=="all" else priority,sort)
    r1,r2,r3,r4=st.columns(4); r1.metric("النتائج" if RTL else "Results",count); r2.metric("الصفحة" if RTL else "Page","{} / {}".format(pg,pages)); r3.metric("المعروض" if RTL else "Shown",len(df)); r4.metric("بدون رد" if RTL else "Unanswered",int((pd.to_numeric(df.get("responded",pd.Series(dtype=int)),errors="coerce").fillna(0)==0).sum()) if not df.empty else 0)

    if not df.empty:
        with st.expander("☑ "+("إجراءات جماعية للصفحة الحالية" if RTL else "Bulk actions for current page"),expanded=False):
            ids=df["id"].astype(int).tolist()
            bulk_ids=st.multiselect("حدد المراجعات" if RTL else "Select reviews",ids,format_func=lambda rid:"#{} · {} · ⭐{}".format(rid,str(df.loc[df["id"]==rid,"author"].iloc[0] or "—")[:35],df.loc[df["id"]==rid,"rating"].iloc[0]),key="rv_bulk_ids_v220")
            b1,b2,b3,b4=st.columns(4)
            if b1.button("💬 "+("يحتاج رد" if RTL else "Needs response"),disabled=not bulk_ids,use_container_width=True,key="rv_bulk_need"):
                db.bulk_review_action(bulk_ids,"needs_response"); st.rerun()
            if b2.button("⏳ "+("قيد المتابعة" if RTL else "In progress"),disabled=not bulk_ids,use_container_width=True,key="rv_bulk_prog"):
                db.bulk_review_action(bulk_ids,"in_progress"); st.rerun()
            if b3.button("✅ "+("تم الحل" if RTL else "Resolved"),disabled=not bulk_ids,use_container_width=True,key="rv_bulk_res"):
                db.bulk_review_action(bulk_ids,"resolved"); st.rerun()
            if b4.button("🚫 "+("تجاهل" if RTL else "Ignore"),disabled=not bulk_ids,use_container_width=True,key="rv_bulk_ign"):
                db.bulk_review_action(bulk_ids,"ignored"); st.rerun()

    for _,r in df.iterrows():
        rid=int(r["id"]); rating=max(0,min(5,int(safe_num(r.get("rating"))))); kind={"positive":"green","negative":"red","neutral":"amber"}.get(r.get("sentiment"),"gray"); conf=safe_num(r.get("analysis_confidence"),r.get("sentiment_score",0))*100
        priority_label={"critical":"حرجة","high":"عالية","normal":"عادية","low":"منخفضة"}.get(str(r.get("priority") or "normal"),str(r.get("priority") or "normal")) if RTL else str(r.get("priority") or "normal").title()
        st.markdown('<div class="rt-review"><b>{}</b> · {} · {} · {}<div class="text">{}</div><small>{}: {} · {}: {} · {}: {:.0f} · {} · {} {:.0f}% · {}: {}</small></div>'.format(html.escape(str(r.get("company_name") or "—")),"★"*rating+"☆"*(5-rating),status_badge(r.get("sentiment") or "?",kind),html.escape(str(r.get("author") or "?")),html.escape(str(r.get("text") or "")[:1200]),html.escape("اللغة" if RTL else "Language"),html.escape(str(r.get("language") or "unknown")),html.escape("الجانب" if RTL else "Aspect"),html.escape(str(r.get("aspect_primary") or r.get("topic") or "other")),html.escape("الاستعجال" if RTL else "Urgency"),safe_num(r.get("urgency"),0),html.escape(str(r.get("topic") or "other")),html.escape(str(r.get("analysis_method") or "rule-based-v3")),conf,html.escape("الأولوية" if RTL else "Priority"),html.escape(priority_label)),unsafe_allow_html=True)
        ac1,ac2,ac3,ac4=st.columns(4)
        if ac1.button("💬 "+("يحتاج رد" if RTL else "Needs response"),key="need220_{}".format(rid),use_container_width=True): db.review_action(rid,"needs_response"); invalidate_cache(); st.rerun()
        if ac2.button("⏳ "+("متابعة" if RTL else "In progress"),key="prog220_{}".format(rid),use_container_width=True): db.review_action(rid,"in_progress"); invalidate_cache(); st.rerun()
        if ac3.button("✅ "+("تم الحل" if RTL else "Resolved"),key="res220_{}".format(rid),use_container_width=True): db.review_action(rid,"resolved"); invalidate_cache(); st.rerun()
        if ac4.button("🔎 "+("فتح الفرع" if RTL else "Open branch"),key="br220_{}".format(rid),use_container_width=True): st.session_state.selected_branch_pk=r.get("pk"); nav_to("branch_workspace",company=r.get("pk"))
        p1,p2=st.columns([1.4,2.6])
        prio=p1.selectbox("الأولوية" if RTL else "Priority",["low","normal","high","critical"],index=["low","normal","high","critical"].index(str(r.get("priority") or "normal") if str(r.get("priority") or "normal") in ["low","normal","high","critical"] else "normal"),format_func=lambda x:{"low":"منخفضة","normal":"عادية","high":"عالية","critical":"حرجة"}.get(x,x) if RTL else x.title(),key="prio220_{}".format(rid))
        if p1.button("حفظ الأولوية" if RTL else "Save priority",key="saveprio220_{}".format(rid),use_container_width=True): db.set_review_priority(rid,prio); st.rerun()
        with p2.expander("🗒️ "+("ملاحظات داخلية" if RTL else "Internal notes"),expanded=False):
            note=st.text_input("ملاحظة" if RTL else "Note",key="note220_{}".format(rid))
            if st.button("إضافة الملاحظة" if RTL else "Add note",key="addnote220_{}".format(rid)) and note.strip():
                db.add_note(rid,note,ws_id,user.get("username") or user.get("display_name")); st.rerun()
            ndf=db.notes(rid,ws_id)
            if not ndf.empty: st.dataframe(ndf[[c for c in ["note","author","created_at"] if c in ndf]],use_container_width=True,hide_index=True)

# ---- BRANCHES ----
if page == "branches_all":
    page_header("🏢 "+t("all_branches"),"قائمة موحدة مع جودة البيانات وحداثتها ومؤشرات الأداء." if RTL else "Unified branch list with data quality, freshness and performance indicators.","branches_all")
    bdf=db.list_companies_with_stats(ws_id)

    if not bdf.empty:
        bdf["_network_v37"]=bdf.apply(lambda r:branch_network_key(r),axis=1)
        _network_counts=bdf["_network_v37"].fillna("—").astype(str).value_counts()
        with st.expander("🌐 "+("شبكة الفروع والتغطية — v37" if RTL else "Network & Coverage — v37"),expanded=True):
            _network_opts=_network_counts.index.tolist()
            _network_sel=st.selectbox("الشبكة / العلامة" if RTL else "Network / brand",_network_opts,
                format_func=lambda x:"{} · {} {}".format(x,int(_network_counts.get(x,0)),"فرع" if RTL else "branches"),
                key="network_selector_v37")
            _network_view=bdf[bdf["_network_v37"].astype(str)==str(_network_sel)].copy()
            _net=build_branch_network_intelligence(_network_view,30)
            _n1,_n2,_n3,_n4=st.columns(4)
            _n1.metric("الفروع المكتشفة" if RTL else "Discovered branches",_net.get("branches",0))
            _n2.metric("هوية مؤكدة" if RTL else "Strong identity",_net.get("strong_identity",0))
            _n3.metric("محددة على الخريطة" if RTL else "Geocoded",_net.get("geocoded",0))
            _n4.metric("تغطية الأدلة" if RTL else "Evidence coverage","{:.0f}%".format(float(_net.get("evidence_coverage_pct",0))))
            _n5,_n6,_n7=st.columns(3)
            _n5.metric("اتصال مكتمل 4/4" if RTL else "Contact complete 4/4",_net.get("contact_complete",0))
            _n6.metric("محدث ≤ 30 يوم" if RTL else "Fresh ≤ 30 days",_net.get("fresh",0))
            _n7.metric("هوية ضعيفة" if RTL else "Weak identity",_net.get("weak_identity",0))
            st.caption(("مؤشر تغطية الأدلة يقيس جودة ما تم اكتشافه وحفظه فقط، ولا يدّعي أن جميع الفروع الواقعية قد تم العثور عليها." if RTL else
                        "Evidence coverage measures the quality of saved/discovered evidence only; it does not claim absolute real-world branch completeness."))
            _net_cols=[c for c in ["name","rating","review_count","address","phone","mobile","email","website","place_id","cid","last_updated"] if c in _network_view.columns]
            st.dataframe(_network_view[_net_cols],use_container_width=True,hide_index=True,height=min(420,110+32*min(8,len(_network_view))))
            _net_map=build_map_figure(_network_view,hover_data=[c for c in ["rating","review_count","address"] if c in _network_view.columns],
                                      id_col="pk" if "pk" in _network_view.columns else None,fit_all=True)
            if _net_map is not None:
                st.plotly_chart(_net_map,use_container_width=True,key="network_map_v37")
            else:
                st.info("لا توجد إحداثيات كافية لرسم شبكة الفروع." if RTL else "Not enough coordinates to map this network yet.")
    with st.expander("🧬 "+("سلامة هوية الفروع — v29" if RTL else "Branch identity integrity — v29"),expanded=False):
        try:
            _ih=db.branch_identity_health(ws_id)
            _identity_error_v38=None
        except Exception as _exc:
            _identity_error_v38=safe_error_payload_v38(_exc,"branches_all.identity_health")
            record_local_error_v38(_identity_error_v38)
            _ih={"branches":len(bdf),"strong":0,"weak":0,"aliases":0,"identity_conflicts":0,"duplicate_candidates":0,"auto_merge_candidates":0}
            st.error("{} · {}".format(_identity_error_v38["user_message"],_identity_error_v38["reference"]))
        _i1,_i2,_i3,_i4,_i5=st.columns(5)
        _i1.metric("الفروع" if RTL else "Branches",_ih.get("branches",0))
        _i2.metric("هوية قوية" if RTL else "Strong ID",_ih.get("strong",0))
        _i3.metric("هوية ضعيفة" if RTL else "Weak ID",_ih.get("weak",0))
        _i4.metric("Aliases",_ih.get("aliases",0))
        _i5.metric("مرشح دمج" if RTL else "Merge candidates",_ih.get("duplicate_candidates",0))
        try:
            _dups=db.find_identity_duplicates(ws_id,200) if _identity_error_v38 is None else []
        except Exception as _exc:
            _p=safe_error_payload_v38(_exc,"branches_all.identity_duplicates")
            record_local_error_v38(_p)
            st.error("{} · {}".format(_p["user_message"],_p["reference"]))
            _dups=[]
        if _dups:
            _ddf=pd.DataFrame(_dups)
            st.dataframe(_ddf,use_container_width=True,hide_index=True,height=min(360,95+32*min(8,len(_ddf))))
            _auto=[x for x in _dups if x.get("auto_merge") and not x.get("strong_conflict")]
            st.caption(("الدمج التلقائي المقترح: {} · تعارضات الهوية لا تُدمج تلقائيًا." if RTL else "Suggested safe merges: {} · strong-ID conflicts are never auto-merged.").format(len(_auto)))
            _opts=["{} → {} | {}".format(x.get("pk_a"),x.get("pk_b"),x.get("reason")) for x in _dups]
            _sel=st.selectbox("معاينة حالة" if RTL else "Inspect candidate",range(len(_opts)),format_func=lambda i:_opts[i],key="identity_candidate_v29")
            _cand=_dups[int(_sel)]
            _dry=db.merge_branch_records(ws_id,_cand.get("source_pk") or _cand.get("pk_a"),_cand.get("target_pk") or _cand.get("pk_b"),"ui-dry-run",dry_run=True)
            st.caption(("المعاينة فقط: " if RTL else "Dry run: ")+json.dumps(_dry.get("relations",{}),ensure_ascii=False))
            _mc1,_mc2=st.columns(2)
            if _mc1.button("🔗 "+("دمج الحالة الآمنة" if RTL else "Merge safe candidate"),disabled=not bool(_cand.get("auto_merge")) or bool(_cand.get("strong_conflict")),use_container_width=True,key="identity_merge_v29"):
                _res=db.merge_branch_records(ws_id,_cand.get("source_pk") or _cand.get("pk_a"),_cand.get("target_pk") or _cand.get("pk_b"),"ui-approved")
                if _res.get("ok"):
                    invalidate_cache(); st.success("تم دمج سجل الفرع ونقل العلاقات بأمان." if RTL else "Branch records merged and relations moved safely."); st.rerun()
                else: st.error(str(_res.get("error") or _res))
            if _mc2.button("↻ "+("إعادة الفحص" if RTL else "Rescan"),use_container_width=True,key="identity_rescan_v29"):
                st.rerun()
        else:
            st.success("لا توجد حالات تكرار هوية واضحة حاليًا." if RTL else "No clear branch-identity duplicates detected.")
    if bdf.empty: st.info(t("no_data"))
    else:
        bdf["rating_num"]=pd.to_numeric(bdf.get("rating"),errors="coerce"); bdf["review_count_num"]=pd.to_numeric(bdf.get("review_count"),errors="coerce").fillna(0); bdf["confidence"]=pd.to_numeric(bdf.get("confidence_score"),errors="coerce").fillna(0)
        bdf["freshness"]=pd.to_datetime(bdf.get("last_updated"),errors="coerce"); now=pd.Timestamp.now(); bdf["age_days"]=(now-bdf["freshness"]).dt.days
        f1,f2,f3=st.columns([2,1,1]); qf=f1.text_input("بحث داخل الفروع" if RTL else "Filter branches",key="bf_v21"); minr=f2.selectbox("التقييم ≥" if RTL else "Rating ≥",[0,3,3.5,4,4.5],key="bf_r_v21"); stale=f3.checkbox("بيانات أقدم من 30 يوم" if RTL else "Older than 30 days",key="bf_stale")
        view=bdf.copy()
        if qf.strip():
            _mask=search_result_filter_mask(view,qf)
            view=view[_mask.reindex(view.index,fill_value=False)]
        if minr: view=view[view["rating_num"].fillna(0)>=float(minr)]
        if stale: view=view[view["age_days"].fillna(999)>30]
        cols=[c for c in ["pk","name","rating","review_count","stored_reviews","negative_reviews","responded_reviews","category","address","phone","email","confidence_score","last_updated"] if c in view]
        st.dataframe(view[cols],use_container_width=True,hide_index=True,height=min(720,120+34*min(len(view),17)))
        choices=view["pk"].astype(str).tolist(); pick=st.selectbox("اختر فرعًا" if RTL else "Select branch",choices,format_func=lambda x:company_options.get(x,x),key="open_branch_pick")
        c1,c2,c3=st.columns(3)
        if c1.button("🧭 "+("فتح ملف الفرع 360°" if RTL else "Open Branch 360°"),type="primary",use_container_width=True,key="open360"): st.session_state.selected_branch_pk=pick; nav_to("branch_workspace",company=pick)
        with c2.popover("⬇ "+("تصدير" if RTL else "Export"),use_container_width=True):
            st.markdown("**📦 "+("التصدير الشامل v40" if RTL else "v40 complete export")+"**")
            render_complete_export_controls_v40(db,ws_id,companies,view,pick,"branches")
            st.divider(); st.caption("التصدير السريع القديم" if RTL else "Legacy quick export")
            st.download_button("📄 PDF",dataframe_to_pdf_cached_v39(view,"جميع الفروع" if RTL else "All branches"),file_name="branches.pdf",mime="application/pdf",use_container_width=True,key="branches_pdf_v39")
            st.download_button("📊 Excel",to_excel(view),file_name="branches.xlsx",mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",use_container_width=True,key="branches_xlsx21")
            st.download_button("🗂️ CSV",to_csv(view),file_name="branches.csv",mime="text/csv",use_container_width=True,key="branches_csv_v39")
        if c3.button("↻ "+("تحديث الفرع المحدد" if RTL else "Refresh selected branch"),use_container_width=True,key="refresh_branch21"):
            br=db.get_company(pick,ws_id); target=(br or {}).get("maps_url") or (br or {}).get("name");
            if target:
                with st.spinner("Updating..."): scrape_and_store(db,target,100,bool((br or {}).get("maps_url")),ws_id,user["username"],target_pk=pick)
                invalidate_cache(); st.success("✅")


if page == "branch_workspace":
    page_header("🏢 "+("مساحة الفروع 360°" if RTL else "Unified Branch 360 Workspace"),
                "اختر الفرع مرة واحدة ثم راجع التعليقات والموقع والاتصال والأداء والسجل من مكان واحد." if RTL else
                "Choose a branch once, then review its reviews, location, contact data, performance and history in one place.","branch_workspace")
    st.markdown('<div class="rt-v31-workspace"><h3>{}</h3><p>{}</p></div>'.format(
        "مساحة عمل موحدة" if RTL else "Unified daily workspace",
        "اختيار واحد متزامن بين القائمة والخريطة والتفاصيل، مع إبقاء الصفحات القديمة متاحة من القوائم." if RTL else "One synchronized selection across list, map and details; legacy pages remain available from menus."),unsafe_allow_html=True)

    _saved=companies.copy() if isinstance(companies,pd.DataFrame) else pd.DataFrame()
    _live=st.session_state.get("selected_workspace_record_v31") or {}
    if not _saved.empty:
        _saved["_family"]=_saved.apply(lambda r:branch_family_key(r),axis=1)
    _nav,_detail=st.columns([0.82,2.18],gap="medium")
    _selected_pk=str(st.session_state.get("selected_branch_pk") or "")
    _selected_br={}
    with _nav:
        st.markdown('<div class="rt-v31-navigator"><h4>{}</h4><div class="rt-v30-inspector-sub">{}</div></div>'.format(
            "الفروع" if RTL else "Branches","بحث سريع داخل الفروع المحفوظة" if RTL else "Quick filter across saved branches"),unsafe_allow_html=True)
        _nq=st.text_input("بحث" if RTL else "Filter",placeholder="اسم، عنوان، نشاط..." if RTL else "Name, address, category...",key="branch_workspace_filter_v31",label_visibility="collapsed")
        _nv=_saved.copy()
        if not _nv.empty and _nq.strip():
            _mask=search_result_filter_mask(_nv.assign(result_group=""),_nq)
            _nv=_nv[_mask]
        _groups=int(_nv["_family"].nunique()) if not _nv.empty and "_family" in _nv else 0
        st.caption(("{} فرع · {} مجموعة" if RTL else "{} branches · {} groups").format(len(_nv),_groups))
        _opts=_nv["pk"].astype(str).tolist() if not _nv.empty else []
        if _live: _opts=["__LIVE__"]+_opts
        if _opts:
            _choice=("__LIVE__" if _live and not _selected_pk else (_selected_pk if _selected_pk in _opts else _opts[0]))
            _choice=st.selectbox("الفرع" if RTL else "Branch",_opts,index=_opts.index(_choice),
                format_func=lambda x:("⚡ "+str(_live.get("name") or ("نتيجة البحث الحية" if RTL else "Live search result"))) if x=="__LIVE__" else ("{} · {}".format(str(_nv.loc[_nv["pk"].astype(str)==str(x),"name"].iloc[0]),str(_nv.loc[_nv["pk"].astype(str)==str(x),"_family"].iloc[0])) if not _nv.loc[_nv["pk"].astype(str)==str(x)].empty else x),
                key="branch_workspace_select_v31",label_visibility="collapsed")
            if _choice=="__LIVE__":
                _selected_pk=""; st.session_state.selected_branch_pk=None; _selected_br={}
                st.info("عرض نتيجة البحث الحية؛ احفظها لإضافتها لقائمة الفروع." if RTL else "Showing the live search result; save it to add it to the branch list.")
            else:
                _selected_pk=str(_choice); st.session_state.selected_branch_pk=_selected_pk
                _selected_br=db.get_company(_selected_pk,ws_id) or {}
        else:
            st.info(t("no_data"))
        if _selected_br:
            _fam=branch_family_key(_selected_br)
            _siblings=_saved[_saved["_family"]==_fam] if not _saved.empty and "_family" in _saved else pd.DataFrame()
            if len(_siblings)>1:
                st.markdown("**{}:** {}".format("فروع المجموعة" if RTL else "Family branches",len(_siblings)))
                for _,_sr in _siblings.head(12).iterrows():
                    st.caption("• {} · ⭐ {}".format(_sr.get("name") or "—",_sr.get("rating") or "—"))

    with _detail:
        br=merge_branch_workspace_record(_selected_br,_live)
        pk=str(_selected_pk or br.get("pk") or "")
        if not br:
            st.markdown('<div class="rt-v31-detail-pane"><h3>{}</h3><div class="rt-v31-detail-sub">{}</div></div>'.format(
                "اختر فرعًا" if RTL else "Choose a branch","تظهر هنا كل بيانات الفرع وتعليقاته وموقعه وسجله." if RTL else "All branch data, reviews, location and history appear here."),unsafe_allow_html=True)
        else:
            _stored=bool(pk and db.get_company(pk,ws_id))
            _stored_reviews=db.get_reviews(pk,ws_id) if _stored else pd.DataFrame()
            _rev=merge_branch_reviews(_stored_reviews,br.get("preview_reviews") or [])
            _snaps=db.snapshots(ws_id,pk,36500) if _stored else pd.DataFrame()
            _intel=build_branch_review_intelligence(_rev)
            _title=html.escape(str(br.get("name") or "—")); _addr=html.escape(str(br.get("address") or "—"))
            st.markdown('<div class="rt-v31-detail-pane"><h3>{}</h3><div class="rt-v31-detail-sub">{} · ⭐ {} · {} {}</div></div>'.format(
                _title,_addr,html.escape(str(br.get("rating") or "—")),int(safe_num(br.get("review_count"),0)),"مراجعة Google" if RTL else "Google reviews"),unsafe_allow_html=True)
            _m1,_m2,_m3,_m4=st.columns(4)
            _m1.metric("⭐ "+("التقييم" if RTL else "Rating"),br.get("rating") or "—")
            _m2.metric("Google Reviews",int(safe_num(br.get("review_count"),0)))
            _m3.metric("المخزنة" if RTL else "Stored",_intel.get("total",0))
            _m4.metric("السلبية" if RTL else "Negative","{:.1f}%".format(_intel.get("negative_pct",0)))

            with st.popover("⋯ "+("عمليات الفرع" if RTL else "Branch actions")):
                if _stored and st.button("↻ "+("تحديث بيانات الفرع" if RTL else "Refresh branch"),use_container_width=True,key="v31_refresh_branch"):
                    _target=br.get("maps_url") or br.get("name")
                    if _target:
                        with st.spinner("Updating..."): scrape_and_store(db,_target,150,bool(br.get("maps_url")),ws_id,user["username"],target_pk=pk)
                        invalidate_cache(); st.rerun()
                if (not _stored) and st.button("💾 "+("حفظ هذا الفرع" if RTL else "Save this branch"),use_container_width=True,key="v31_save_live"):
                    _newpk=db.save_company(br,st.session_state.get("search_query_used",""),ws_id)
                    if _newpk:
                        persist_preview_reviews(db,_newpk,br,ws_id)
                        st.session_state.selected_branch_pk=str(_newpk); invalidate_cache(); st.success("تم الحفظ مع التعليقات المتاحة" if RTL else "Saved with available review samples"); st.rerun()
                if br.get("maps_url") or br.get("url"):
                    st.link_button("📍 Google Maps",br.get("maps_url") or br.get("url"),use_container_width=True)
                if br.get("website"):
                    st.link_button("🌐 "+("الموقع الإلكتروني" if RTL else "Website"),br.get("website"),use_container_width=True)
                st.divider(); st.markdown("**📦 "+("تصدير شامل للفرع" if RTL else "Complete branch export")+"**")
                _current_frame=pd.DataFrame([br])
                render_complete_export_controls_v40(db,ws_id,_saved,_current_frame,pk,"workspace")

            _sections=[("overview","نظرة عامة","Overview"),("reviews","التعليقات","Reviews"),("location","الموقع","Location"),("contact","الاتصال","Contact"),("performance","الأداء","Performance"),("history","السجل","History")]
            _section=st.radio("قسم مساحة العمل" if RTL else "Workspace section",[x[0] for x in _sections],horizontal=True,
                format_func=lambda x:next((a if RTL else e for k,a,e in _sections if k==x),x),key="branch_workspace_tab_v31",label_visibility="collapsed")

            if _section=="overview":
                st.markdown('<div class="rt-v31-intel"><div class="summary">{}</div></div>'.format(html.escape(_intel.get("summary_ar") if RTL else _intel.get("summary_en"))),unsafe_allow_html=True)
                if _stored:
                    _branch_actions=build_action_center_feed(db.actionable_reviews(ws_id,400),db.list_alerts(ws_id,status="all",limit=800),db.list_tickets(ws_id),db.branch_sync_states(ws_id),companies)
                    _branch_actions=[x for x in _branch_actions if str(x.get("pk") or "")==str(pk)][:4]
                    if _branch_actions:
                        st.markdown("#### ⚡ "+("ما يحتاج إجراء" if RTL else "Needs action"))
                        for _a in _branch_actions:
                            st.markdown('<div class="rt-v32-action-mini"><b>{}</b> · {}<br>{}</div>'.format(html.escape(str(_a.get("priority") or "normal").upper()),html.escape(str(_a.get("title") or "")),html.escape(str(_a.get("detail") or ""))[:420]),unsafe_allow_html=True)
                _o1,_o2=st.columns([1.25,1])
                with _o1:
                    render_branch_overview(dict(br,url=br.get("maps_url") or br.get("url"),last_updated=br.get("last_updated") or br.get("source_fetched_at")))
                with _o2:
                    _topics=_intel.get("top_topics") or []
                    st.markdown("#### "+("أكثر الموضوعات" if RTL else "Top topics"))
                    if _topics:
                        st.markdown("".join('<span class="rt-v31-topic">{} · {}</span>'.format(html.escape(str(k)),v) for k,v in _topics[:8]),unsafe_allow_html=True)
                    else: st.caption("لا توجد موضوعات مصنفة بعد." if RTL else "No classified topics yet.")
                    st.markdown("#### ⚠️ "+("الحالات العاجلة" if RTL else "Urgent reviews"))
                    _urg=_intel.get("urgent_reviews") or []
                    if not _urg: st.success("لا توجد مراجعات عاجلة غير مجاب عليها." if RTL else "No urgent unanswered reviews.")
                    for _u in _urg[:4]:
                        st.markdown('<div class="rt-v31-urgent"><b>⭐ {}</b> · {}<br>{}</div>'.format(html.escape(str(_u.get("rating") or "—")),html.escape(str(_u.get("author") or "—")),html.escape(str(_u.get("text") or "—"))[:600]),unsafe_allow_html=True)

            elif _section=="reviews":
                st.markdown("#### 🔄 Deep Review Sync · "+("مزامنة عميقة للتعليقات" if RTL else "Incremental review backfill"))
                if _stored:
                    _ds=db.get_deep_review_sync_state(pk,ws_id)
                    _d1,_d2,_d3,_d4=st.columns(4)
                    _d1.metric("المحفوظ" if RTL else "Stored",len(_stored_reviews))
                    _d2.metric("جديد تراكمي" if RTL else "Discovered",int(safe_num(_ds.get("discovered_new"),0)))
                    _d3.metric("دفعات" if RTL else "Batches",int(safe_num(_ds.get("batches"),0)))
                    _d4.metric("الحالة" if RTL else "Status",str(_ds.get("status") or "never"))
                    _dc1,_dc2=st.columns([1,2])
                    _batch=_dc1.selectbox("حجم الدفعة" if RTL else "Batch size",[100,250,500],index=1,key="v33_deep_review_batch")
                    if _dc2.button("🔄 "+("مزامنة عميقة الآن" if RTL else "Run Deep Review Sync"),use_container_width=True,key="v33_deep_review_sync"):
                        with st.spinner("جاري فحص Google والبحث عن تعليقات غير محفوظة..." if RTL else "Scanning Google for unseen reviews..."):
                            _ds_result=deep_sync_reviews_for_branch(db,pk,br,ws_id,user.get("username","") if isinstance(user,dict) else "",_batch)
                        if _ds_result.get("ok"):
                            st.success(("تمت إضافة {} تعليق جديد. أصبح المحفوظ {}." if RTL else "Added {} new reviews. Stored total is now {}.").format(_ds_result.get("new",0),_ds_result.get("known_after",0)))
                            invalidate_cache(); st.rerun()
                        else: st.error(str(_ds_result.get("error") or ("تعذر إكمال المزامنة" if RTL else "Deep sync failed")))
                    st.caption(("آخر مزامنة: {} · الهدف المعلن في Google: {}" if RTL else "Last sync: {} · Google reported total: {}").format(_ds.get("last_sync_at") or "—",int(safe_num(br.get("review_count"),0))))
                else:
                    st.info("احفظ الفرع أولًا لتفعيل المزامنة العميقة للتعليقات." if RTL else "Save the branch first to enable Deep Review Sync.")
                _rq1,_rq2=st.columns([2.4,1])
                _rq=_rq1.text_input("بحث داخل التعليقات" if RTL else "Search reviews",key="v31_review_query",placeholder="كلمة، كاتب، موضوع..." if RTL else "Word, author, topic...")
                _rs=_rq2.selectbox("المشاعر" if RTL else "Sentiment",["all","negative","neutral","positive"],format_func=lambda x:{"all":"الكل","negative":"سلبي","neutral":"محايد","positive":"إيجابي"}.get(x,x) if RTL else x.title(),key="v31_review_sent")
                _rv=_rev.copy()
                if not _rv.empty:
                    if _rq.strip():
                        _hay=_rv[[c for c in ["author","text","topic","owner_response"] if c in _rv]].fillna("").astype(str).agg(" ".join,axis=1).str.casefold()
                        _rv=_rv[_hay.str.contains(re.escape(_rq.strip().casefold()),regex=True,na=False)]
                    if _rs!="all" and "sentiment" in _rv: _rv=_rv[_rv["sentiment"].fillna("").astype(str).str.lower()==_rs]
                    st.caption(("{} تعليق ظاهر من {} تعليق متاح" if RTL else "{} visible reviews of {} available").format(len(_rv),len(_rev)))
                    for _ri,(_, _rr) in enumerate(_rv.head(100).iterrows()):
                        _author=html.escape(str(_rr.get("author") or "—")); _date=html.escape(str(_rr.get("review_date") or _rr.get("date") or ""))
                        _text=html.escape(str(_rr.get("text") or "—")); _owner=html.escape(str(_rr.get("owner_response") or _rr.get("response_text") or ""))
                        _rating=max(0,min(5,int(safe_num(_rr.get("rating"),0)))); _stars="★"*_rating + "☆"*(5-_rating)
                        _sent=html.escape(str(_rr.get("sentiment") or "—")); _topic=html.escape(str(_rr.get("topic") or "—")); _source=html.escape(str(_rr.get("review_source") or "stored"))
                        _owner_html=('<div class="owner"><b>{}:</b> {}</div>'.format("رد المالك" if RTL else "Owner response",_owner)) if _owner else ""
                        st.markdown('<div class="rt-v32-review-card"><div class="head"><b>{}</b><span class="stars">{}</span><span>{}</span></div><div class="body">{}</div><div class="tags"><span>{}</span><span>{}</span><span>{}</span></div>{}</div>'.format(
                            _author,html.escape(_stars),_date,_text,_sent,_topic,_source,_owner_html),unsafe_allow_html=True)
                        _rid_raw=_rr.get("id")
                        try: _rid_int=int(_rid_raw) if _rid_raw is not None and not pd.isna(_rid_raw) else None
                        except Exception: _rid_int=None
                        if _stored and _rid_int is not None and st.button("✍️ Response Studio · "+("تجهيز رد" if RTL else "Draft reply"),key="v33_reply_open_{}_{}".format(pk,_rid_int)):
                            st.session_state["v33_response_review_id"]=_rid_int
                            st.rerun()
                    if len(_rv)>100: st.info(("يتم عرض أول 100 تعليق؛ استخدم البحث لتضييق النتائج." if RTL else "Showing the first 100 reviews; use search to narrow the list."))
                else: st.info("لا توجد تعليقات متاحة لهذا الفرع بعد. حدّث بيانات الفرع لجلب عينات جديدة من Google." if RTL else "No reviews are available for this branch yet. Refresh the branch to fetch new Google samples.")
                st.markdown('<div class="rt-v31-intel"><div class="summary">{}</div></div>'.format(html.escape(_intel.get("summary_ar") if RTL else _intel.get("summary_en"))),unsafe_allow_html=True)
                if _stored and st.session_state.get("v33_response_review_id"):
                    _studio_id=int(st.session_state.get("v33_response_review_id"))
                    _studio_rows=_stored_reviews[_stored_reviews["id"]==_studio_id] if (not _stored_reviews.empty and "id" in _stored_reviews) else pd.DataFrame()
                    if not _studio_rows.empty:
                        _studio_review=_studio_rows.iloc[0].to_dict()
                        st.markdown("---")
                        st.markdown("#### ✍️ Response Studio · "+("استوديو الردود" if RTL else "Review response workspace"))
                        st.caption("لا يتم إرسال أي رد تلقائيًا إلى Google؛ المسودة تحفظ محليًا ويمكن نسخها بعد المراجعة." if RTL else "Nothing is auto-posted to Google. Drafts are saved locally and can be copied after review.")
                        _sc1,_sc2=st.columns(2)
                        _reply_lang=_sc1.selectbox("لغة الرد" if RTL else "Reply language",["ar","en"],format_func=lambda x:"العربية" if x=="ar" else "English",key="v33_reply_lang_{}".format(_studio_id))
                        _tone=_sc2.selectbox("النبرة" if RTL else "Tone",["professional","friendly","short","apology"],format_func=lambda x:{"professional":"احترافية","friendly":"ودية","short":"مختصرة","apology":"اعتذار"}.get(x,x) if RTL else x.title(),key="v33_reply_tone_{}".format(_studio_id))
                        _draft_key="v33_draft_text_{}".format(_studio_id)
                        _saved_drafts=db.list_review_response_drafts(ws_id,pk=pk,review_id=_studio_id,limit=1)
                        if _draft_key not in st.session_state:
                            if not _saved_drafts.empty: st.session_state[_draft_key]=str(_saved_drafts.iloc[0].get("draft_text") or "")
                            else: st.session_state[_draft_key]=generate_review_reply_draft(_studio_review,_reply_lang,_tone)
                        _sg1,_sg2=st.columns(2)
                        if _sg1.button("✨ "+("إنشاء اقتراح جديد" if RTL else "Generate suggestion"),use_container_width=True,key="v33_generate_{}".format(_studio_id)):
                            st.session_state[_draft_key]=generate_review_reply_draft(_studio_review,_reply_lang,_tone); st.rerun()
                        if _sg2.button("× "+("إغلاق الاستوديو" if RTL else "Close Studio"),use_container_width=True,key="v33_close_{}".format(_studio_id)):
                            st.session_state.pop("v33_response_review_id",None); st.rerun()
                        _draft=st.text_area("مسودة الرد" if RTL else "Reply draft",key=_draft_key,height=150)
                        _recent=db.list_review_response_drafts(ws_id,pk=pk,limit=80)
                        _similar=0.0
                        if not _recent.empty:
                            _similar=max([reply_similarity_score(_draft,x) for x in _recent.get("draft_text",pd.Series(dtype=str)).fillna("").astype(str).tolist() if x.strip()] or [0.0])
                        if _similar>=0.86: st.warning(("المسودة مشابهة بنسبة {:.0f}% لرد محفوظ سابقًا؛ يفضّل تخصيصها أكثر." if RTL else "This draft is {:.0f}% similar to a recent saved reply; consider personalizing it.").format(_similar*100))
                        if st.button("💾 "+("حفظ المسودة" if RTL else "Save draft"),use_container_width=True,key="v33_save_draft_{}".format(_studio_id)):
                            _did=db.save_review_response_draft(_studio_id,pk,_draft,ws_id,_reply_lang,_tone,_similar,"draft")
                            if _did: st.success(("تم حفظ المسودة #{}" if RTL else "Draft #{} saved").format(_did))
                            else: st.error("تعذر حفظ المسودة" if RTL else "Could not save draft")
                        st.markdown("**"+("نسخة جاهزة للنسخ" if RTL else "Copy-ready version")+"**")
                        st.code(_draft,language=None)

            elif _section=="location":
                _lat=br.get("latitude"); _lon=br.get("longitude")
                if _lat is not None and _lon is not None:
                    _geo=pd.DataFrame([dict(br,_map_id=pk or "live")])
                    _fig=build_map_figure(_geo,lat_col="latitude",lon_col="longitude",hover_name="name",hover_data=["rating","review_count","address"],zoom=15,title=("موقع الفرع" if RTL else "Branch location"),id_col="_map_id",selected_ids={pk or "live"},fit_all=False,focus_id=pk or "live")
                    if _fig is not None: chart(_fig,"branch_workspace_map_v31",480)
                else: st.info("لا توجد إحداثيات محفوظة لهذا الفرع." if RTL else "No coordinates are stored for this branch.")
                _lc1,_lc2=st.columns(2); _lc1.metric("Latitude",_lat if _lat is not None else "—"); _lc2.metric("Longitude",_lon if _lon is not None else "—")
                info_box("العنوان" if RTL else "Address",br.get("address") or "—")

            elif _section=="contact":
                _fields=[("الهاتف" if RTL else "Phone",br.get("phone")),("الجوال" if RTL else "Mobile",br.get("mobile")),("البريد" if RTL else "Email",br.get("email")),("الموقع الإلكتروني" if RTL else "Website",br.get("website")),("النشاط" if RTL else "Category",br.get("category")),("Place ID",br.get("place_id")),("CID",br.get("cid")),("آخر تحديث" if RTL else "Last update",br.get("last_updated") or br.get("source_fetched_at"))]
                _html=[]
                for _k,_v in _fields: _html.append('<div class="rt-v30-detail"><div class="k">{}</div><div class="v">{}</div></div>'.format(html.escape(str(_k)),html.escape(str(_v or "—"))))
                st.markdown('<div class="rt-v31-contact-grid">{}</div>'.format("".join(_html)),unsafe_allow_html=True)

            elif _section=="performance":
                _p1,_p2,_p3=st.columns(3); _p1.metric("إيجابي" if RTL else "Positive","{:.1f}%".format(_intel.get("positive_pct",0))); _p2.metric("سلبي" if RTL else "Negative","{:.1f}%".format(_intel.get("negative_pct",0))); _p3.metric("بدون رد" if RTL else "Unanswered",_intel.get("unanswered",0))
                if not _rev.empty:
                    _c1,_c2=st.columns(2)
                    with _c1:
                        _sv=pd.Series({"positive":_intel.get("positive",0),"neutral":_intel.get("neutral",0),"negative":_intel.get("negative",0)}).reset_index(); _sv.columns=["sentiment","count"]
                        chart(px.pie(_sv,names="sentiment",values="count",hole=.55,title="المشاعر" if RTL else "Sentiment"),"v31_sentiment",310)
                    with _c2:
                        _tv=pd.DataFrame(_intel.get("top_topics") or [],columns=["topic","count"])
                        if not _tv.empty: chart(px.bar(_tv,x="topic",y="count",title="الموضوعات" if RTL else "Topics"),"v31_topics",310)
                st.markdown("#### 📈 "+("تطور التقييم" if RTL else "Historical rating trend"))
                _tf1,_tf2=st.columns(2)
                _freq=_tf1.selectbox("التجميع" if RTL else "Aggregation",["monthly","quarterly","yearly","daily"],
                    format_func=lambda x:{"monthly":"شهري","quarterly":"ربع سنوي","yearly":"سنوي","daily":"كل لقطة"}.get(x,x) if RTL else {"monthly":"Monthly","quarterly":"Quarterly","yearly":"Yearly","daily":"Every snapshot"}.get(x,x),key="v40_history_frequency")
                _range=_tf2.selectbox("الفترة" if RTL else "Range",["3m","6m","12m","all"],index=2,
                    format_func=lambda x:{"3m":"آخر 3 أشهر","6m":"آخر 6 أشهر","12m":"آخر 12 شهرًا","all":"كل السنوات"}.get(x,x) if RTL else {"3m":"Last 3 months","6m":"Last 6 months","12m":"Last 12 months","all":"All years"}.get(x,x),key="v40_history_range")
                if isinstance(_snaps,pd.DataFrame) and len(_snaps)>=1:
                    _all_monthly=aggregate_rating_timeline_v40(_snaps,"monthly"); _trend=rating_trend_summary_v40(_all_monthly)
                    _visible_snap=filter_rating_snapshots_range_v40(_snaps,_range); _timeline=aggregate_rating_timeline_v40(_visible_snap,_freq)
                    _t1,_t2,_t3,_t4,_t5=st.columns(5)
                    _t1.metric("التقييم الحالي" if RTL else "Current",_trend.get("current_rating") if _trend.get("current_rating") is not None else "—",delta=("{:+.2f}".format(_trend.get("delta_previous",0)) if _trend.get("points",0)>1 else None))
                    _t2.metric("3 أشهر" if RTL else "3 months","—" if _trend.get("delta_3m") is None else "{:+.2f}".format(_trend.get("delta_3m")))
                    _t3.metric("6 أشهر" if RTL else "6 months","—" if _trend.get("delta_6m") is None else "{:+.2f}".format(_trend.get("delta_6m")))
                    _t4.metric("12 شهرًا" if RTL else "12 months","—" if _trend.get("delta_12m") is None else "{:+.2f}".format(_trend.get("delta_12m")))
                    _t5.metric("أعلى / أدنى" if RTL else "High / low","{} / {}".format(_trend.get("high_rating") or "—",_trend.get("low_rating") or "—"))
                    if not _timeline.empty:
                        _fig=px.line(_timeline,x="day",y="rating",markers=True,hover_data=[c for c in ["period","total","rating_delta","review_delta"] if c in _timeline.columns],title="مؤشر تغير التقييم عبر الزمن" if RTL else "Rating movement over time")
                        _fig.update_layout(yaxis_range=[1,5],hovermode="x unified")
                        _fig.update_traces(line={"width":3},marker={"size":7})
                        chart(_fig,"v40_rating_market_line",390)
                        _show=[c for c in ["period","day","rating","total","rating_delta","review_delta"] if c in _timeline]
                        st.dataframe(_timeline[_show],use_container_width=True,hide_index=True,height=min(320,90+32*min(len(_timeline),7)))
                    if len(_snaps)<2: st.info("تم حفظ أول نقطة تاريخية. سيظهر اتجاه التحسن/التراجع بعد المزامنات القادمة." if RTL else "The first historical point is saved. Trend direction appears after future syncs.")
                else:
                    st.info("لا توجد لقطة تاريخية محفوظة بعد. يبدأ التاريخ تلقائيًا عند حفظ/مزامنة الفرع." if RTL else "No historical snapshot is stored yet. History starts automatically when the branch is saved/synced.")

            elif _section=="history":
                if not _stored:
                    st.info("احفظ الفرع أولًا لعرض سجل المزامنة والتنبيهات والملاحظات." if RTL else "Save the branch first to show sync history, alerts and notes.")
                else:
                    _sync=db.branch_sync_state(pk,ws_id) or {}; _runs=db.sync_runs(ws_id,pk=pk,limit=100); _changes=db.change_events(ws_id,pk=pk,limit=200); _alerts=db.list_alerts(ws_id,status="all",limit=1000); _notes=db.branch_notes(pk,ws_id,200)
                    if not _alerts.empty and "pk" in _alerts: _alerts=_alerts[_alerts["pk"].astype(str)==str(pk)]
                    _h1,_h2,_h3=st.columns(3); _h1.metric("المزامنات" if RTL else "Sync runs",len(_runs)); _h2.metric("التغييرات" if RTL else "Changes",len(_changes)); _h3.metric("التنبيهات" if RTL else "Alerts",len(_alerts))
                    with st.expander("⏱️ "+("سجل المزامنة" if RTL else "Sync history"),expanded=True):
                        if _sync: st.caption(("آخر حالة: {} · آخر مزامنة: {}" if RTL else "Last status: {} · last sync: {}").format(_sync.get("status") or "—",_sync.get("last_sync_at") or "—"))
                        st.dataframe(_runs,use_container_width=True,hide_index=True,height=220) if not _runs.empty else st.caption("—")
                    with st.expander("🧠 "+("التغييرات والتنبيهات" if RTL else "Changes & alerts")):
                        if not _changes.empty: st.dataframe(_changes[[c for c in ["severity","event_type","metric","old_value","new_value","detail","created_at"] if c in _changes]],use_container_width=True,hide_index=True,height=240)
                        if not _alerts.empty: st.dataframe(_alerts[[c for c in ["severity","title","detail","status","created_at"] if c in _alerts]],use_container_width=True,hide_index=True,height=220)
                    with st.expander("📝 "+("ملاحظات الفريق" if RTL else "Team notes")):
                        _note=st.text_area("ملاحظة جديدة" if RTL else "New note",key="v31_note",height=90)
                        if st.button("💾 "+("حفظ الملاحظة" if RTL else "Save note"),key="v31_save_note") and _note.strip():
                            db.add_branch_note(pk,_note,ws_id,user.get("username") or user.get("display_name")); st.rerun()
                        if not _notes.empty: st.dataframe(_notes[[c for c in ["note","author","created_at"] if c in _notes]],use_container_width=True,hide_index=True,height=220)


if page == "branch_360":
    page_header("🧭 "+("ملف الفرع 360°" if RTL else "Branch 360°"),
                "لوحة إدارة موحدة للفرع: الهوية، المراجعات، الصحة، الاتجاه، التنبيهات والملاحظات." if RTL else
                "A unified branch workspace for identity, reviews, health, trend, alerts and notes.","branch_360")
    pk=st.session_state.get("selected_branch_pk")
    if not pk and not companies.empty: pk=str(companies.iloc[0]["pk"])
    opts=companies["pk"].astype(str).tolist() if not companies.empty else []
    if not opts:
        st.info(t("no_data"))
    else:
        if str(pk) not in opts: pk=opts[0]
        top_sel, top_refresh = st.columns([4.6,1.25])
        with top_sel:
            pk=st.selectbox("الفرع" if RTL else "Branch",opts,index=opts.index(str(pk)),
                            format_func=lambda x:company_options.get(x,x),key="branch360_select")
            st.session_state.selected_branch_pk=pk
        br=db.get_company(pk,ws_id) or {}
        rev=db.get_reviews(pk,ws_id)
        snaps=db.snapshots(ws_id,pk,36500)
        branch_trend_delta=None
        if isinstance(snaps,pd.DataFrame) and len(snaps)>=2:
            try:
                _ss=snaps.sort_values("day")
                branch_trend_delta=safe_num(_ss.iloc[-1].get("rating"))-safe_num(_ss.iloc[-2].get("rating"))
            except Exception:
                branch_trend_delta=None
        m=kpis(rev,br,trend_delta=branch_trend_delta)
        with top_refresh:
            if st.button("↻ "+("تحديث الفرع" if RTL else "Refresh branch"),type="primary",use_container_width=True,key="refresh360_top"):
                target=br.get("maps_url") or br.get("name")
                if target:
                    with st.spinner("جاري تحديث بيانات هذا الفرع فقط..." if RTL else "Refreshing this branch only..."):
                        scrape_and_store(db,target,150,bool(br.get("maps_url")),ws_id,user["username"],target_pk=pk)
                    invalidate_cache(); st.rerun()

        last_updated=br.get("last_updated") or br.get("source_fetched_at")
        age=None
        try:
            lu=pd.to_datetime(last_updated,errors="coerce")
            if pd.notna(lu): age=max(0,int((pd.Timestamp.now()-lu).total_seconds()//86400))
        except Exception: pass
        if age is None:
            fresh_label="غير معروف" if RTL else "Unknown"; fresh_cls="rt-fresh-warn"
        elif age <= 1:
            fresh_label=("محدث" if RTL else "Fresh"); fresh_cls="rt-fresh-ok"
        elif age <= 7:
            fresh_label=(("منذ {} أيام" if RTL else "{} days old").format(age)); fresh_cls="rt-fresh-warn"
        else:
            fresh_label=(("قديم — {} يوم" if RTL else "Stale — {} days").format(age)); fresh_cls="rt-fresh-bad"

        branch_title=html.escape(str(br.get("name") or company_options.get(pk,pk)))
        branch_meta=" · ".join([str(x) for x in [br.get("category"),br.get("address"),br.get("phone") or br.get("mobile")] if x]) or "—"
        st.markdown('<div class="rt-branch-hero"><h3>🏢 {}</h3><p>{}</p><p style="margin-top:6px">{}: <span class="{}">{}</span></p></div>'.format(
            branch_title,html.escape(branch_meta),"حداثة البيانات" if RTL else "Data freshness",fresh_cls,html.escape(fresh_label)),unsafe_allow_html=True)
        _sync_state=db.branch_sync_state(pk,ws_id) or {}
        if _sync_state:
            _ss1,_ss2,_ss3,_ss4=st.columns(4)
            _ss1.metric("حالة المزامنة" if RTL else "Sync status",str(_sync_state.get("status") or "—"))
            _ss2.metric("جديد منذ آخر مزامنة" if RTL else "New since last sync",int(_sync_state.get("new_reviews") or 0))
            _ss3.metric("مكرر/قديم" if RTL else "Duplicate/old",int(_sync_state.get("duplicate_reviews") or 0))
            _ss4.metric("آخر مزامنة" if RTL else "Last sync",str(_sync_state.get("last_sync_at") or "—")[:16])
            if _sync_state.get("status")=="error" and _sync_state.get("last_error"):
                st.warning(("آخر خطأ مزامنة: " if RTL else "Last sync error: ")+str(_sync_state.get("last_error"))[:500])
            elif str(_sync_state.get("changed_fields") or "") not in ("","[]"):
                st.info(("تغيّرات مكتشفة: " if RTL else "Detected changes: ")+str(_sync_state.get("changed_fields"))[:700])

        response_rate=safe_num(m.get("response_rate"),0); neg_rate=safe_num(m.get("neg_rate"),0)
        unanswered=int((pd.to_numeric(rev.get("responded",pd.Series(dtype=float)),errors="coerce").fillna(0)==0).sum()) if not rev.empty else 0
        metrics=[
            ("⭐ "+("التقييم" if RTL else "Rating"),br.get("rating") or "—"),
            ("Google Reviews",int(safe_num(br.get("review_count"),0))),
            (("المراجعات المخزنة" if RTL else "Stored reviews"),len(rev)),
            ("Health 3.0","{:.0f}".format(safe_num(m.get("health"),0))),
            (("نسبة الرد" if RTL else "Response rate"),"{:.0f}%".format(response_rate)),
            (("بدون رد" if RTL else "Unanswered"),unanswered),
        ]
        for chunk in (metrics[:3],metrics[3:]):
            cc=st.columns(3)
            for c,(lab,val) in zip(cc,chunk):
                with c: metric_card(lab,val)
        st.markdown('<div class="rt-health-box">💚 <b>Health 3.0:</b> {} · {} · {}% {}<br>{}</div>'.format(
            html.escape(str(round(safe_num(m.get("health"),0),1))),html.escape(str(m.get("health_grade") or "—")),
            html.escape(str(round(safe_num(m.get("health_data_quality"),0),0))),"جودة البيانات" if RTL else "data quality",
            html.escape(str(m.get("health_reason") or ("لا توجد ملاحظات كافية" if RTL else "No additional explanation")))),unsafe_allow_html=True)

        tabs=st.tabs(["نظرة عامة" if RTL else "Overview","المراجعات" if RTL else "Reviews","التحليل" if RTL else "Analytics","الاتجاه التاريخي" if RTL else "Trend","التنبيهات" if RTL else "Alerts","الملاحظات" if RTL else "Notes"])

        with tabs[0]:
            ov1,ov2=st.columns([2,1])
            with ov1:
                render_branch_overview(dict(br,url=br.get("maps_url") or br.get("url"),last_updated=last_updated))
            with ov2:
                st.markdown("#### "+("ملخص الحالة" if RTL else "Status summary"))
                info_box("Health 3.0", "{:.0f} · {}".format(safe_num(m.get("health"),0),m.get("health_grade") or "—"))
                info_box("جودة البيانات" if RTL else "Data quality", "{:.0f}%".format(safe_num(m.get("health_data_quality"),0)))
                info_box("السلبية" if RTL else "Negative", "{:.0f}%".format(neg_rate))
                info_box("نسبة الرد" if RTL else "Response rate", "{:.0f}%".format(response_rate))
                info_box("آخر تحديث" if RTL else "Last updated", last_updated or "—")
            if len(snaps)>=2:
                ss=snaps.sort_values("day")
                prev_row=ss.iloc[-2]; cur_row=ss.iloc[-1]
                d1,d2=st.columns(2)
                d1.metric("Δ "+("التقييم" if RTL else "Rating"),"{:+.2f}".format(safe_num(cur_row.get("rating"))-safe_num(prev_row.get("rating"))))
                d2.metric("Δ "+("إجمالي المراجعات" if RTL else "Total reviews"),"{:+.0f}".format(safe_num(cur_row.get("total"))-safe_num(prev_row.get("total"))))

        with tabs[1]:
            st.markdown("#### "+("إدارة مراجعات الفرع" if RTL else "Branch review workspace"))
            r1,r2,r3=st.columns([2.2,1.1,1.1])
            rv_q=r1.text_input("بحث في مراجعات هذا الفرع" if RTL else "Search this branch reviews",placeholder="نص المراجعة أو اسم الكاتب" if RTL else "Review text or author",key="b360_rq")
            rv_sent=r2.selectbox("المشاعر" if RTL else "Sentiment",["all","negative","neutral","positive"],format_func=lambda x:{"all":"الكل","negative":"سلبي","neutral":"محايد","positive":"إيجابي"}.get(x,x) if RTL else x.title(),key="b360_sent")
            rv_status=r3.selectbox("الحالة" if RTL else "Workflow",["all","new","responded","ignored"],format_func=lambda x:{"all":"الكل","new":"جديد","responded":"تم الرد","ignored":"متجاهل"}.get(x,x) if RTL else x.title(),key="b360_status")
            bpage_size=50
            total_branch_reviews=db.reviews_count(ws_id,pk=pk,text_query=rv_q.strip() or None,sentiment=None if rv_sent=="all" else rv_sent,status=None if rv_status=="all" else rv_status)
            max_pages=max(1,(total_branch_reviews+bpage_size-1)//bpage_size)
            bpage=st.number_input("الصفحة" if RTL else "Page",1,max_pages,min(int(st.session_state.get("b360_page",1)),max_pages),1,key="b360_page")
            rv_page=db.reviews_page(ws_id,pk=pk,text_query=rv_q.strip() or None,sentiment=None if rv_sent=="all" else rv_sent,status=None if rv_status=="all" else rv_status,limit=bpage_size,offset=(int(bpage)-1)*bpage_size)
            st.caption(("{} مراجعة مطابقة · صفحة {} من {}" if RTL else "{} matching reviews · page {} of {}").format(total_branch_reviews,int(bpage),max_pages))
            cols=[c for c in ["author","rating","text","review_date","sentiment","topic","workflow_status","responded"] if c in rv_page]
            st.dataframe(rv_page[cols] if not rv_page.empty else rv_page,use_container_width=True,hide_index=True,height=430)
            if not rv_page.empty:
                review_ids=rv_page["id"].astype(int).tolist()
                chosen_rid=st.selectbox("إجراء على مراجعة" if RTL else "Review action",review_ids,
                    format_func=lambda rid:("#{} · {} · ⭐{}".format(rid,str(rv_page.loc[rv_page["id"]==rid,"author"].iloc[0] or "—")[:35],rv_page.loc[rv_page["id"]==rid,"rating"].iloc[0])),key="b360_review_action")
                ra1,ra2,ra3=st.columns(3)
                if ra1.button("✅ "+("تم الرد" if RTL else "Mark responded"),use_container_width=True,key="b360_mark_resp"):
                    db.review_action(int(chosen_rid),"responded"); st.success("تم تحديث المراجعة" if RTL else "Review updated"); st.rerun()
                if ra2.button("⏳ "+("قيد المتابعة" if RTL else "In progress"),use_container_width=True,key="b360_mark_progress"):
                    db.review_action(int(chosen_rid),"in_progress"); st.success("تم تحديث الحالة" if RTL else "Status updated"); st.rerun()
                if ra3.button("🚫 "+("تجاهل" if RTL else "Ignore"),use_container_width=True,key="b360_mark_ignore"):
                    db.review_action(int(chosen_rid),"ignored"); st.success("تم تحديث الحالة" if RTL else "Status updated"); st.rerun()

        with tabs[2]:
            a1,a2,a3=st.columns(3)
            a1.metric("Health","{:.0f}".format(safe_num(m.get("health"),0)))
            a2.metric("السلبية" if RTL else "Negative","{:.1f}%".format(neg_rate))
            a3.metric("نسبة الرد" if RTL else "Response rate","{:.1f}%".format(response_rate))
            if not rev.empty:
                ch1,ch2=st.columns(2)
                with ch1:
                    sent=rev.get("sentiment",pd.Series(dtype=str)).fillna("neutral").value_counts().reset_index(); sent.columns=["sentiment","count"]
                    chart(px.pie(sent,names="sentiment",values="count",hole=.52,title="المشاعر" if RTL else "Sentiment"),"b360_sentiment_v219",320)
                with ch2:
                    tc=rev.get("topic",pd.Series(dtype=str)).fillna("other").value_counts().head(12).reset_index(); tc.columns=["topic","count"]
                    chart(px.bar(tc,x="topic",y="count",title="أكثر الموضوعات" if RTL else "Top topics"),"b360_topics_v219",320)
                bad=rev[rev.get("sentiment",pd.Series(index=rev.index,dtype=str)).fillna("")=="negative"].copy()
                if not bad.empty:
                    st.markdown("#### ⚠️ "+("أحدث المراجعات السلبية" if RTL else "Latest negative reviews"))
                    bad_cols=[c for c in ["author","rating","text","review_date","topic"] if c in bad]
                    st.dataframe(bad[bad_cols].head(10),use_container_width=True,hide_index=True)
            else:
                st.info("لا توجد مراجعات مخزنة لهذا الفرع بعد." if RTL else "No stored reviews for this branch yet.")

        with tabs[3]:
            st.markdown("#### 📈 "+("تطور التقييم — مؤشر زمني" if RTL else "Rating history — timeline indicator"))
            _bf1,_bf2=st.columns(2)
            _bfreq=_bf1.selectbox("التجميع" if RTL else "Aggregation",["monthly","quarterly","yearly","daily"],format_func=lambda x:{"monthly":"شهري","quarterly":"ربع سنوي","yearly":"سنوي","daily":"كل لقطة"}.get(x,x) if RTL else x.title(),key="b360_v40_freq")
            _brange=_bf2.selectbox("الفترة" if RTL else "Range",["3m","6m","12m","all"],index=3,format_func=lambda x:{"3m":"آخر 3 أشهر","6m":"آخر 6 أشهر","12m":"آخر 12 شهرًا","all":"كل السنوات"}.get(x,x) if RTL else {"3m":"Last 3 months","6m":"Last 6 months","12m":"Last 12 months","all":"All years"}.get(x,x),key="b360_v40_range")
            if len(snaps)>=1:
                _ss=filter_rating_snapshots_range_v40(snaps,_brange); _tl=aggregate_rating_timeline_v40(_ss,_bfreq); _sum=rating_trend_summary_v40(aggregate_rating_timeline_v40(snaps,"monthly"))
                _bm1,_bm2,_bm3,_bm4=st.columns(4)
                _bm1.metric("الحالي" if RTL else "Current",_sum.get("current_rating") or "—",delta=("{:+.2f}".format(_sum.get("delta_previous",0)) if _sum.get("points",0)>1 else None))
                _bm2.metric("من البداية" if RTL else "Since first","{:+.2f}".format(_sum.get("delta_all",0)))
                _bm3.metric("أفضل تغير" if RTL else "Best move","{:+.2f}".format(_sum.get("best_change",0)))
                _bm4.metric("أكبر تراجع" if RTL else "Largest decline","{:+.2f}".format(_sum.get("worst_change",0)))
                if not _tl.empty:
                    tr1,tr2=st.columns(2)
                    with tr1:
                        _f=px.line(_tl,x="day",y="rating",markers=True,hover_data=[c for c in ["period","total","rating_delta","review_delta"] if c in _tl],title="تاريخ التقييم" if RTL else "Rating history"); _f.update_layout(yaxis_range=[1,5],hovermode="x unified"); _f.update_traces(line={"width":3}); chart(_f,"b360_trend_rating_v40",350)
                    with tr2: chart(px.line(_tl,x="day",y="total",markers=True,title="نمو عدد المراجعات" if RTL else "Review count history"),"b360_trend_total_v40",350)
                    st.dataframe(_tl[[c for c in ["period","day","rating","total","rating_delta","review_delta"] if c in _tl]].tail(60),use_container_width=True,hide_index=True)
                if len(snaps)<2: st.info("هذه أول نقطة تاريخية؛ سيظهر اتجاه التغير بعد مزامنة لاحقة." if RTL else "This is the first historical point; trend direction appears after another sync.")
            else:
                st.info("لا توجد لقطة تاريخية محفوظة بعد." if RTL else "No historical snapshot is stored yet.")

        with tabs[4]:
            al=db.list_alerts(ws_id,status="all",limit=1000)
            al=al[al["pk"].astype(str)==str(pk)].copy() if not al.empty and "pk" in al else pd.DataFrame()
            if al.empty:
                st.success("لا توجد تنبيهات لهذا الفرع." if RTL else "No alerts for this branch.")
            else:
                new_alerts=al[al["status"].astype(str)!="resolved"] if "status" in al else al
                ac1,ac2,ac3=st.columns(3)
                ac1.metric("الإجمالي" if RTL else "Total",len(al))
                ac2.metric("المفتوحة" if RTL else "Open",len(new_alerts))
                ac3.metric("الحرجة" if RTL else "Critical",int((al.get("severity",pd.Series(dtype=str)).astype(str)=="critical").sum()))
                show_cols=[c for c in ["id","severity","title","detail","status","created_at","resolved_at"] if c in al]
                st.dataframe(al[show_cols],use_container_width=True,hide_index=True,height=330)
                open_ids=new_alerts["id"].astype(int).tolist() if not new_alerts.empty else []
                if open_ids:
                    aid=st.selectbox("حل تنبيه" if RTL else "Resolve alert",open_ids,
                        format_func=lambda x:"#{} · {}".format(x,str(new_alerts.loc[new_alerts["id"]==x,"title"].iloc[0])[:70]),key="b360_alert_resolve")
                    if st.button("✅ "+("حل التنبيه المحدد" if RTL else "Resolve selected alert"),type="primary",key="b360_resolve_alert"):
                        db.resolve_alert(int(aid)); st.success("تم حل التنبيه" if RTL else "Alert resolved"); st.rerun()

        with tabs[5]:
            st.markdown("#### "+("ملاحظات داخلية على الفرع" if RTL else "Internal branch notes"))
            note_text=st.text_area("ملاحظة جديدة" if RTL else "New note",placeholder="اكتب ملاحظة داخلية لفريق العمل..." if RTL else "Write an internal note for your team...",key="b360_note_text",height=110)
            if st.button("💾 "+("حفظ الملاحظة" if RTL else "Save note"),type="primary",key="b360_save_note"):
                if not note_text.strip(): st.warning("اكتب الملاحظة أولاً." if RTL else "Enter a note first.")
                elif db.add_branch_note(pk,note_text,ws_id,user.get("username") or user.get("display_name")):
                    st.success("تم حفظ الملاحظة." if RTL else "Note saved."); st.rerun()
                else: st.error("تعذر حفظ الملاحظة." if RTL else "Could not save note.")
            notes_df=db.branch_notes(pk,ws_id,300)
            if notes_df.empty:
                st.info("لا توجد ملاحظات على هذا الفرع." if RTL else "No notes for this branch yet.")
            else:
                ncols=[c for c in ["id","note","author","created_at"] if c in notes_df]
                st.dataframe(notes_df[ncols],use_container_width=True,hide_index=True,height=300)
                del_id=st.selectbox("حذف ملاحظة" if RTL else "Delete note",notes_df["id"].astype(int).tolist(),
                    format_func=lambda nid:"#{} · {}".format(nid,str(notes_df.loc[notes_df["id"]==nid,"note"].iloc[0])[:65]),key="b360_delete_note_select")
                if st.button("🗑️ "+("حذف الملاحظة المحددة" if RTL else "Delete selected note"),key="b360_delete_note"):
                    if db.delete_branch_note(int(del_id),pk,ws_id): st.success("تم حذف الملاحظة" if RTL else "Note deleted"); st.rerun()

if page == "map":
    page_header("🗺️ "+t("map"),"خريطة تفاعلية مرتبطة بملف الفرع مع Fit All وتلوين حسب الأداء." if RTL else "Interactive branch map with Fit All, performance coloring and Branch 360 linkage.","map")
    geo = companies.dropna(subset=["latitude","longitude"]).copy() if not companies.empty else pd.DataFrame()
    if geo.empty:
        st.info("لا توجد إحداثيات محفوظة للفروع." if RTL else "No saved branch coordinates are available.")
    else:
        bh=branch_health_table(companies,period_reviews)
        if not bh.empty:
            geo=geo.merge(bh[["pk","health"]],on="pk",how="left")
        else:
            geo["health"]=None
        geo["_map_id"]=geo["pk"].astype(str)
        m1,m2,m3=st.columns([1.2,2.4,1.1])
        color_mode=m1.selectbox("لون الخريطة حسب" if RTL else "Color map by",["rating","health","review_count"],format_func=lambda x:{"rating":"التقييم","health":"Health","review_count":"عدد المراجعات"}.get(x,x) if RTL else {"rating":"Rating","health":"Health","review_count":"Reviews"}.get(x,x),key="branches_map_color_v218")
        focus_options=["__ALL__"]+geo["_map_id"].tolist()
        current_pk=str(st.session_state.get("selected_branch_pk") or "")
        default_focus=current_pk if current_pk in focus_options else "__ALL__"
        focus_pk=m2.selectbox("التركيز على فرع" if RTL else "Focus branch",focus_options,index=focus_options.index(default_focus),format_func=lambda x:("كل الفروع — Fit All" if RTL else "All branches — Fit All") if x=="__ALL__" else "{} · ⭐ {}".format(str(geo.loc[geo["_map_id"]==x,"name"].iloc[0]),geo.loc[geo["_map_id"]==x,"rating"].iloc[0] if not geo.loc[geo["_map_id"]==x].empty else "—"),key="branches_map_focus_v218")
        m3.metric("الفروع" if RTL else "Branches",len(geo))
        selected_ids=[] if focus_pk=="__ALL__" else [focus_pk]
        fig=build_map_figure(geo,lat_col="latitude",lon_col="longitude",hover_name="name",hover_data=["rating","review_count","health","address","phone"],zoom=9,title=("خريطة الفروع" if RTL else "Branches map"),color_field=color_mode,id_col="_map_id",selected_ids=selected_ids,fit_all=(focus_pk=="__ALL__"),focus_id=None if focus_pk=="__ALL__" else focus_pk)
        if fig is None:
            st.info("تعذر رسم الخريطة الحالية." if RTL else "Could not render the current map.")
        else:
            clicked=chart_select(fig,"map_view_v218",620)
            if clicked:
                chosen=str(clicked[-1])
                hit=geo[geo["_map_id"]==chosen]
                if not hit.empty:
                    st.session_state.selected_branch_pk=str(hit.iloc[0]["pk"])
                    focus_pk=chosen
            selected_row=geo[geo["_map_id"]==str(focus_pk)] if focus_pk!="__ALL__" else pd.DataFrame()
            if not selected_row.empty:
                r=selected_row.iloc[0]
                c1,c2,c3,c4=st.columns(4)
                c1.metric("⭐ "+("التقييم" if RTL else "Rating"),r.get("rating") or "—")
                c2.metric("📝 "+("المراجعات" if RTL else "Reviews"),int(safe_num(r.get("review_count"),0)))
                c3.metric("💚 Health",("{:.0f}".format(safe_num(r.get("health"),0))))
                if c4.button("🧭 "+("فتح ملف 360°" if RTL else "Open Branch 360°"),type="primary",use_container_width=True,key="map_open_360_v218"):
                    st.session_state.selected_branch_pk=str(r.get("pk")); nav_to("branch_workspace",company=str(r.get("pk")))
                st.caption("{} · {}".format(r.get("address") or "—",r.get("phone") or r.get("mobile") or "—"))
            st.markdown("#### "+("دليل الفروع على الخريطة" if RTL else "Map branch index"))
            idx_cols=[c for c in ["name","rating","review_count","health","address"] if c in geo.columns]
            st.dataframe(geo[idx_cols].sort_values(color_mode,ascending=False,na_position="last"),use_container_width=True,hide_index=True,height=min(420,120+34*min(len(geo),9)))

if page == "compare":
    section_title("⚔️ " + t("compare"))
    if len(companies) >= 2:
        labels = {r["pk"]: r["name"] for _, r in companies.iterrows()}
        sel = st.multiselect("Select", list(labels),
                             default=list(labels)[:min(3, len(labels))],
                             format_func=lambda x: labels[x], key="cmp_sel")
        if len(sel) >= 2:
            rows = []
            for pk in sel:
                c = db.get_company(pk, ws_id)
                rev = db.get_reviews(pk, ws_id)
                k = kpis(rev, c or {})
                rows.append({"branch": c["name"], "rating": c.get("rating"),
                             "reviews": k["total"], "health": round(k["health"],1),
                             "pos%": round(k["pos_rate"],1), "neg%": round(k["neg_rate"],1)})
            st.dataframe(pd.DataFrame(rows).sort_values("health", ascending=False),
                         use_container_width=True, hide_index=True)

# ---- MARKET SHARE (v20.1) ----
if page == "market_share":
    section_title("📊 " + t("market_share"), t("benchmark"))
    all_with_comp = db.list_companies(ws_id, include_competitors=True)
    if all_with_comp.empty: st.info(t("no_data"))
    else:
        all_with_comp["is_cmp"] = pd.to_numeric(all_with_comp.get("is_competitor"), errors="coerce").fillna(0)
        data = all_with_comp.copy()
        data["rating_num"] = pd.to_numeric(data["rating"], errors="coerce").fillna(0)
        data["reviews_num"] = pd.to_numeric(data["review_count"], errors="coerce").fillna(0)
        total_reviews = data["reviews_num"].sum() or 1
        data["share_pct"] = data["reviews_num"] / total_reviews * 100
        c1, c2, c3 = st.columns(3)
        c1.metric("Total places", len(data))
        c2.metric("Our branches", int((data["is_cmp"] == 0).sum()))
        c3.metric("Competitors", int((data["is_cmp"] == 1).sum()))
        st.divider()
        st.markdown("##### 🏆 " + ("ترتيب حسب التقييم" if RTL else "Ranking by rating"))
        rank = data[["name","is_cmp","rating_num","reviews_num","share_pct"]].copy()
        rank["type"] = rank["is_cmp"].apply(lambda x: "🎯 Ours" if x == 0 else "⚔️ Comp")
        rank = rank.sort_values("rating_num", ascending=False).head(20)
        st.dataframe(rank[["name","type","rating_num","reviews_num","share_pct"]],
                     use_container_width=True, hide_index=True)
        st.divider()
        st.markdown("##### 📊 " + ("الحصة السوقية" if RTL else "Market share"))
        fig = px.pie(data, names="name", values="reviews_num", hole=0.55)
        chart(fig, "share_pie", 460)

# ---- ANALYTICS ----
if page == "performance":
    page_header("📊 "+("مركز التحليلات 3.0" if RTL else "Analytics Center 3.0"),
                "مقارنة الفترات، حركة الفروع، Heatmap للموضوعات، الاستجابة والشذوذ مع استنتاجات قابلة للتنفيذ." if RTL else "Period comparison, branch movement, topic heatmaps, response analytics and anomalies with actionable insights.","performance")
    if period_reviews.empty:
        st.info(t("no_data"))
    else:
        prev=previous_period_reviews(db,ws_id)
        m=kpis(period_reviews); pm=kpis(prev) if not prev.empty else None
        comp=branch_period_comparison(companies,period_reviews,prev)
        tabs=st.tabs(["نظرة عامة" if RTL else "Overview","الاتجاهات" if RTL else "Trends","الموضوعات × الفروع" if RTL else "Topics × branches","حركة الفروع" if RTL else "Branch movement","الاستجابة" if RTL else "Response","الشذوذ" if RTL else "Anomalies","استنتاجات" if RTL else "Insights"])
        with tabs[0]:
            review_delta=len(period_reviews)-len(prev) if not prev.empty else None
            k1,k2,k3=st.columns(3)
            k1.metric(t("avg_rating"),"{:.2f}".format(safe_num(m.get("avg"))),delta="{:+.2f}".format(safe_num(m.get("avg"))-safe_num(pm.get("avg"))) if pm else None)
            k2.metric("المراجعات" if RTL else "Reviews",len(period_reviews),delta=("{:+d}".format(int(review_delta)) if review_delta is not None else None))
            k3.metric("السلبية" if RTL else "Negative","{:.1f}%".format(m.get("neg_rate",0)),delta="{:+.1f}%".format(m.get("neg_rate",0)-pm.get("neg_rate",0)) if pm else None,delta_color="inverse")
            k4,k5=st.columns(2)
            k4.metric(t("response_rate"),"{:.1f}%".format(m.get("response_rate",0)),delta="{:+.1f}%".format(m.get("response_rate",0)-pm.get("response_rate",0)) if pm else None)
            k5.metric("Health 3.0",m.get("health",0))
            st.info(("سبب Health: " if RTL else "Health explanation: ")+m.get("health_reason",""))
            if not comp.empty:
                movers=comp.dropna(subset=["rating_delta"]).copy() if "rating_delta" in comp else pd.DataFrame()
                if not movers.empty:
                    left,right=st.columns(2)
                    with left:
                        st.markdown("#### 📈 "+("أكبر تحسن" if RTL else "Biggest improvement"))
                        st.dataframe(movers.sort_values("rating_delta",ascending=False).head(5)[["name","rating","rating_delta","review_delta","health"]],use_container_width=True,hide_index=True)
                    with right:
                        st.markdown("#### 📉 "+("أكبر تراجع" if RTL else "Biggest decline"))
                        st.dataframe(movers.sort_values("rating_delta",ascending=True).head(5)[["name","rating","rating_delta","negative_delta","health"]],use_container_width=True,hide_index=True)
        with tabs[1]:
            st.markdown("#### 📈 "+("مقارنة تاريخ التقييم بين الفروع" if RTL else "Compare branch rating history"))
            if not companies.empty:
                _cmp1,_cmp2=st.columns(2)
                _cmp_freq=_cmp1.selectbox("التجميع التاريخي" if RTL else "History aggregation",["monthly","quarterly","yearly","daily"],format_func=lambda x:{"monthly":"شهري","quarterly":"ربع سنوي","yearly":"سنوي","daily":"كل لقطة"}.get(x,x) if RTL else x.title(),key="v40_compare_history_freq")
                _cmp_range=_cmp2.selectbox("الفترة التاريخية" if RTL else "History range",["3m","6m","12m","all"],index=2,format_func=lambda x:{"3m":"آخر 3 أشهر","6m":"آخر 6 أشهر","12m":"آخر 12 شهرًا","all":"كل السنوات"}.get(x,x) if RTL else {"3m":"Last 3 months","6m":"Last 6 months","12m":"Last 12 months","all":"All years"}.get(x,x),key="v40_compare_history_range")
                _cmp_options=companies["pk"].astype(str).tolist()
                _cmp_sel=st.multiselect("الفروع للمقارنة" if RTL else "Branches to compare",_cmp_options,default=_cmp_options[:min(3,len(_cmp_options))],format_func=lambda x:company_options.get(x,x),max_selections=8,key="v40_compare_branches")
                _all_hist=db.snapshots(ws_id,None,36500)
                _rows=[]
                if isinstance(_all_hist,pd.DataFrame) and not _all_hist.empty and _cmp_sel:
                    for _cpk in _cmp_sel:
                        _sub=_all_hist[_all_hist["pk"].astype(str)==str(_cpk)].copy() if "pk" in _all_hist else pd.DataFrame()
                        _sub=filter_rating_snapshots_range_v40(_sub,_cmp_range); _tl=aggregate_rating_timeline_v40(_sub,_cmp_freq)
                        if not _tl.empty:
                            _tl=_tl.copy(); _tl["branch_name"]=company_options.get(str(_cpk),str(_cpk)); _rows.append(_tl)
                if _rows:
                    _cmp_hist=pd.concat(_rows,ignore_index=True)
                    _cf=px.line(_cmp_hist,x="day",y="rating",color="branch_name",markers=True,hover_data=[c for c in ["period","total","rating_delta","review_delta"] if c in _cmp_hist],title="حركة التقييم عبر الزمن" if RTL else "Rating movement over time")
                    _cf.update_layout(yaxis_range=[1,5],hovermode="x unified"); _cf.update_traces(line={"width":3})
                    chart(_cf,"v40_compare_rating_history",420)
                else:
                    st.caption("اختر فروعًا لديها لقطات تاريخية للمقارنة." if RTL else "Choose branches with stored snapshots to compare.")
            st.divider()
            d=period_reviews.copy(); d["day"]=pd.to_datetime(d["fetched_at"],errors="coerce").dt.date; d["r"]=pd.to_numeric(d["rating"],errors="coerce")
            g=d.groupby("day",as_index=False).agg(rating=("r","mean"),reviews=("id","count"),negative_pct=("sentiment",lambda x:(x=="negative").mean()*100),urgent=("urgency",lambda x:(pd.to_numeric(x,errors="coerce").fillna(0)>=60).sum()))
            chart(px.line(g,x="day",y="rating",markers=True,title="متوسط التقييم يوميًا" if RTL else "Daily rating"),"an3_trend",310)
            c1,c2=st.columns(2)
            with c1: chart(px.bar(g,x="day",y="reviews",title="حجم المراجعات" if RTL else "Review volume"),"an3_vol",280)
            with c2: chart(px.line(g,x="day",y="negative_pct",markers=True,title="نسبة السلبية" if RTL else "Negative share"),"an3_neg",280)
            if not companies.empty:
                trend_pk=st.selectbox("فرع لتقدير الاتجاه" if RTL else "Branch trend estimate",companies["pk"].astype(str).tolist(),format_func=lambda x:company_options.get(x,x),key="trend_branch22")
                snaps=db.snapshots(ws_id,trend_pk,180)
                if len(snaps)>=3:
                    ss=snaps.sort_values("day").tail(60); y=pd.to_numeric(ss["rating"],errors="coerce").dropna().values
                    if len(y)>=3:
                        x=np.arange(len(y),dtype=float); coef=np.polyfit(x,y,1); pred=np.polyval(coef,x); ss_res=float(np.sum((y-pred)**2)); ss_tot=float(np.sum((y-y.mean())**2)); r2=1-(ss_res/ss_tot) if ss_tot>0 else 0; next_val=float(np.polyval(coef,[len(y)])[0]); label=("توقع" if len(y)>=14 and r2>=.35 else "تقدير اتجاه") if RTL else ("Forecast" if len(y)>=14 and r2>=.35 else "Trend estimate")
                        tt1,tt2,tt3=st.columns(3); tt1.metric(label,"{:.2f}".format(next_val),delta="{:+.2f}".format(next_val-float(y[-1]))); tt2.metric("R²","{:.2f}".format(r2)); tt3.metric("Points",len(y))
                        if len(y)<14 or r2<.35: st.warning("البيانات غير كافية لتوقع قوي؛ المعروض تقدير اتجاه فقط." if RTL else "Insufficient evidence for a strong forecast; this is only a trend estimate.")
                else: st.info("تحتاج 3 لقطات زمنية على الأقل، ويفضل 14+ لتوقع موثوق." if RTL else "At least 3 snapshots are needed; 14+ are preferred for a forecast.")
        with tabs[2]:
            rows=[]
            for topic,cdf in period_reviews.groupby(period_reviews["topic"].fillna("other")):
                rows.append({"topic":topic,"count":len(cdf),"avg_rating":round(safe_num(pd.to_numeric(cdf["rating"],errors="coerce").mean()),2),"negative_pct":round((cdf["sentiment"]=="negative").mean()*100,1),"urgent":int((pd.to_numeric(cdf.get("urgency"),errors="coerce").fillna(0)>=60).sum())})
            td=pd.DataFrame(rows).sort_values("count",ascending=False)
            c1,c2=st.columns([1,1.35])
            with c1: st.dataframe(td,use_container_width=True,hide_index=True,height=420)
            with c2: chart(px.bar(td.head(15),x="topic",y="count",color="negative_pct",title="الموضوعات الأكثر تكرارًا" if RTL else "Most frequent topics"),"an3_topics",390)
            hm=topic_branch_matrix(period_reviews,10,15)
            if not hm.empty:
                fig=px.imshow(hm,aspect="auto",text_auto=True,title="Heatmap الموضوعات × الفروع" if RTL else "Topics × branches heatmap",labels={"x":"Topic","y":"Branch","color":"Reviews"})
                chart(fig,"an3_heatmap",520)
            else: st.info("لا توجد بيانات كافية لبناء Heatmap." if RTL else "Not enough data for a topic heatmap.")
        with tabs[3]:
            bh=branch_health_table(companies,period_reviews)
            if not comp.empty:
                show_cols=[c for c in ["name","current_reviews","previous_reviews","review_delta","rating","rating_delta","negative_pct","negative_delta","response_pct","response_delta","health"] if c in comp]
                st.dataframe(comp.sort_values(["health","rating"],ascending=[False,False])[show_cols],use_container_width=True,hide_index=True,height=470)
            if len(bh)>1:
                chart(px.scatter(bh,x="reviews",y="rating",size="reviews",color="health",hover_name="name",title="المراجعات مقابل التقييم" if RTL else "Reviews vs rating"),"an3_corr",360)
        with tabs[4]:
            resp=pd.to_numeric(period_reviews.get("responded"),errors="coerce").fillna(0)
            c1,c2,c3=st.columns(3); c1.metric("تم الرد" if RTL else "Responded",int((resp==1).sum())); c2.metric("بدون رد" if RTL else "Unanswered",int((resp==0).sum())); c3.metric(t("response_rate"),"{:.1f}%".format(m.get("response_rate",0)))
            rr=period_reviews.copy(); rr["day"]=pd.to_datetime(rr.get("fetched_at"),errors="coerce").dt.date
            daily_resp=rr.dropna(subset=["day"]).groupby("day").agg(total=("id","count"),responded=("responded",lambda x:(pd.to_numeric(x,errors="coerce").fillna(0)==1).sum())).reset_index()
            if not daily_resp.empty:
                daily_resp["response_pct"]=daily_resp["responded"]/daily_resp["total"].replace(0,np.nan)*100
                chart(px.line(daily_resp,x="day",y="response_pct",markers=True,title="نسبة الرد يوميًا" if RTL else "Daily response rate"),"an3_resp_rate",300)
            if "fetched_at" in period_reviews and "responded_at" in period_reviews:
                rrt=period_reviews.copy(); rrt["f"]=pd.to_datetime(rrt["fetched_at"],errors="coerce"); rrt["r"]=pd.to_datetime(rrt["responded_at"],errors="coerce"); rrt=rrt.dropna(subset=["f","r"]); rrt["hours"]=(rrt["r"]-rrt["f"]).dt.total_seconds()/3600; rrt=rrt[rrt["hours"]>=0]
                if not rrt.empty: st.metric("متوسط زمن الرد الوسيط" if RTL else "Median response time","{} h".format(round(rrt["hours"].median(),1))); chart(px.histogram(rrt,x="hours",nbins=30),"an3_response_hours",290)
        with tabs[5]:
            adf=detect_anomalies_v3(period_reviews)
            if not adf.empty:
                st.dataframe(adf,use_container_width=True,hide_index=True)
                sev=adf["severity"].value_counts(); c1,c2=st.columns(2); c1.metric("شذوذات عالية" if RTL else "High anomalies",int(sev.get("high",0))); c2.metric("متوسطة" if RTL else "Medium",int(sev.get("medium",0)))
            else: st.success("لا توجد شذوذات قوية ضمن البيانات الحالية." if RTL else "No strong anomalies in the current data.")
        with tabs[6]:
            ins=executive_insights(period_reviews,prev)
            if not ins: st.info("لا توجد بيانات كافية لاستنتاج تنفيذي موثوق." if RTL else "Not enough data for reliable executive insights.")
            for x in ins:
                st.markdown('<div class="rt-card {}"><b>{}</b><br>{}</div>'.format("rt-success" if x["severity"]=="good" else "rt-warning",html.escape(x["title"]),html.escape(x["detail"])),unsafe_allow_html=True)

if page == "executive":
    page_header("💡 "+t("executive"),"استنتاجات قابلة للتنفيذ مرتبطة بالبيانات الحالية." if RTL else "Actionable insights grounded in current data.","executive")
    ins=executive_insights(period_reviews,previous_period_reviews(db,ws_id))
    if not ins: st.info("لا توجد بيانات كافية لاستنتاج تنفيذي موثوق." if RTL else "Not enough data for reliable executive insights.")
    for x in ins: st.markdown('<div class="rt-card {}"><b>{}</b><br>{}</div>'.format("rt-success" if x["severity"]=="good" else "rt-warning",html.escape(x["title"]),html.escape(x["detail"])),unsafe_allow_html=True)

if page in {"aspects","wordcloud","forecast","anomalies","duplicates","sla"}:
    st.info("تم دمج هذه الأداة داخل «مركز التحليلات» في v21 لتقليل تشتت القوائم." if RTL else "This tool is now integrated into Analytics Center in v21 to reduce navigation clutter.")
    if st.button("📊 "+("فتح مركز التحليلات" if RTL else "Open Analytics Center"),type="primary",key="go_analytics_"+page): nav_to("performance")

# ---- AI (v19.0) ----
if page == "ai_settings":
    section_title("🤖 " + t("ai_settings"))
    status = ai_status()
    c1, c2, c3 = st.columns(3)
    c1.metric("AI Available", "✅" if status["available"] else "❌")
    c2.metric("Device", status["device"])
    c3.metric("Models", "AR:{} EN:{}".format(
        "✓" if status["arabic"] else "✗", "✓" if status["english"] else "✗"))
    if status.get("error"):
        st.warning("Error: " + status["error"])
    st.info("To enable AI: `pip install transformers torch` — "
            "then restart. First run downloads ~500MB of models.")
    st.divider()
    if st.button("🔍 Test AI", key="ai_test", disabled=not status["available"]):
        with st.spinner("Loading models..."):
            p = get_ai_pipeline()
            try:
                if p.get("ar"):
                    test = p["ar"]("الخدمة ممتازة جداً")
                    st.success("AR test: " + json.dumps(test, ensure_ascii=False))
                if p.get("en"):
                    test2 = p["en"]("Great service!")
                    st.success("EN test: " + json.dumps(test2, ensure_ascii=False))
            except Exception as e:
                st.error(str(e))

if page == "ai_analyze":
    page_header("✨ "+("Analysis Pipeline 3.0" if RTL else "Analysis Pipeline 3.0"),
                "اكتشاف اللغة ثم المشاعر والموضوعات والجوانب والاستعجال مع توضيح مصدر التحليل." if RTL else "Language detection, sentiment, topics, aspects and urgency with transparent provenance.","ai_analyze")
    status = ai_status()
    if period_reviews.empty:
        st.info("لا توجد مراجعات في الفترة الحالية." if RTL else "No reviews in the current period.")
    else:
        lang_counts=period_reviews.get("language",pd.Series(dtype=str)).fillna("unknown").value_counts()
        method_counts=period_reviews.get("analysis_method",pd.Series(dtype=str)).fillna("unknown").value_counts()
        a1,a2,a3=st.columns(3)
        a1.metric("المراجعات" if RTL else "Reviews",len(period_reviews))
        a2.metric("اللغات" if RTL else "Languages",int(len(lang_counts)))
        a3.metric("حرجة/عاجلة" if RTL else "Urgent",int((pd.to_numeric(period_reviews.get("urgency",pd.Series(dtype=float)),errors="coerce").fillna(0)>=60).sum()))
        limit = st.number_input("عدد المراجعات للمعالجة" if RTL else "Reviews to process",10,2000,min(100,max(10,len(period_reviews))),10,key="ai_limit")
        c1,c2=st.columns(2)
        if c1.button("🔎 "+("إعادة تحليل بالقواعد 3.0" if RTL else "Re-analyze with Rules 3.0"),type="primary",use_container_width=True,key="rule_run_v221"):
            with st.spinner("جاري التحليل..." if RTL else "Analyzing..."):
                n=reanalyze_rule_based_reviews(period_reviews.head(int(limit)).copy())
            st.success(("تم تحديث {} مراجعة" if RTL else "Updated {} reviews").format(n)); invalidate_cache(); st.rerun()
        if c2.button("🧠 "+("تعزيز التحليل بالذكاء الاصطناعي" if RTL else "Enhance with AI"),use_container_width=True,key="ai_run_v221",disabled=not status.get("available")):
            with st.spinner("Running AI..."):
                n=run_ai_on_reviews(period_reviews.head(int(limit)).copy())
            st.success(("تم تعزيز {} مراجعة" if RTL else "AI-enhanced {} reviews").format(n)); invalidate_cache(); st.rerun()
        if not status.get("available"):
            st.info("نماذج AI غير متاحة حاليًا؛ Rule-based 3.0 يعمل بالكامل ويظهر ذلك في حقل مصدر التحليل." if RTL else "AI models are unavailable; Rule-based 3.0 remains fully active and is shown in the analysis method field.")
        with st.expander("📊 "+("شفافية التحليل" if RTL else "Analysis transparency"),expanded=True):
            c1,c2=st.columns(2)
            with c1:
                if not lang_counts.empty: st.dataframe(lang_counts.rename("count").reset_index().rename(columns={"index":"language"}),use_container_width=True,hide_index=True)
            with c2:
                if not method_counts.empty: st.dataframe(method_counts.rename("count").reset_index().rename(columns={"index":"method"}),use_container_width=True,hide_index=True)
            st.caption(("اكتشاف اللغة: langdetect عند توفره + كشف نوع الحروف كخطة احتياطية." if RTL else "Language detection uses langdetect when available plus script heuristics as fallback."))

# ---- FOLLOWUP ----
if page == "action_center":
    page_header("⚡ "+("مركز الإجراءات" if RTL else "Smart Action Center"),
                "المراجعات والتنبيهات والتذاكر وتغيّرات المراقبة في قائمة عمل واحدة مرتبة حسب الأولوية." if RTL else
                "Reviews, alerts, tickets and monitoring changes in one prioritized work queue.","action_center")
    _ar=db.actionable_reviews(ws_id,1500); _aa=db.list_alerts(ws_id,status="all",limit=1500); _at=db.list_tickets(ws_id); _as=db.branch_sync_states(ws_id)
    _feed=build_action_center_feed(_ar,_aa,_at,_as,companies); _met=action_center_metrics(_feed)
    _m1,_m2,_m3,_m4,_m5=st.columns(5)
    _m1.metric("مفتوح" if RTL else "Open",_met["open"]); _m2.metric("حرج" if RTL else "Critical",_met["critical"])
    _m3.metric("مرتفع" if RTL else "High",_met["high"]); _m4.metric("بدون رد" if RTL else "Unanswered",_met["unanswered"]); _m5.metric("له تذكرة" if RTL else "Ticketed",_met["with_ticket"])
    f1,f2,f3=st.columns([1.1,1.1,2.2])
    _prio=f1.selectbox("الأولوية" if RTL else "Priority",["all","critical","high","normal","low"],format_func=lambda x:{"all":"الكل","critical":"حرج","high":"مرتفع","normal":"عادي","low":"منخفض"}.get(x,x) if RTL else x.title(),key="v32_action_priority")
    _kind=f2.selectbox("النوع" if RTL else "Type",["all","unanswered","review","alert","ticket","sync"],format_func=lambda x:{"all":"الكل","unanswered":"بدون رد","review":"مراجعة","alert":"تنبيه","ticket":"تذكرة","sync":"مراقبة"}.get(x,x) if RTL else ("Unanswered" if x=="unanswered" else x.title()),key="v32_action_type")
    _aq=f3.text_input("بحث" if RTL else "Search",placeholder="فرع، مشكلة، نص مراجعة..." if RTL else "Branch, issue, review text...",key="v32_action_query")
    _view=[]
    for _a in _feed:
        if _prio!="all" and _a.get("priority")!=_prio: continue
        if _kind=="unanswered":
            if _a.get("review_id") is None or bool(_a.get("responded")): continue
        elif _kind!="all" and _kind not in (_a.get("sources") or []): continue
        if _aq.strip():
            _blob=" ".join(str(_a.get(k) or "") for k in ["company_name","title","detail","priority_reason","owner"]).casefold()
            if _aq.strip().casefold() not in _blob: continue
        _view.append(_a)
    st.caption(("{} إجراء ظاهر من {}" if RTL else "{} visible actions of {}").format(len(_view),len(_feed)))
    if not _view: st.success("لا توجد إجراءات مطابقة الآن." if RTL else "No matching actions right now.")
    for _i,_a in enumerate(_view[:250]):
        _pk=str(_a.get("pk") or ""); _prio_label=str(_a.get("priority") or "normal"); _src=" + ".join(_a.get("sources") or [])
        st.markdown('<div class="rt-v32-action {}"><div class="head"><b>{}</b><span>{}</span></div><div class="title">{}</div><div class="detail">{}</div><div class="meta">{} · {}</div></div>'.format(
            html.escape(_prio_label),html.escape(str(_a.get("company_name") or _pk or "—")),html.escape(_prio_label.upper()),html.escape(str(_a.get("title") or "")),html.escape(str(_a.get("detail") or ""))[:900],html.escape(_src),html.escape(str(_a.get("priority_reason") or ""))),unsafe_allow_html=True)
        c1,c2,c3=st.columns([1,1,1])
        if c1.button(("✍️ " if _a.get("review_id") is not None and not bool(_a.get("responded")) else "🏢 ")+("فتح الرد" if RTL and _a.get("review_id") is not None and not bool(_a.get("responded")) else "فتح الفرع" if RTL else "Open reply" if _a.get("review_id") is not None and not bool(_a.get("responded")) else "Open branch"),key="v32_open_{}".format(_i),use_container_width=True,disabled=not bool(_pk)):
            st.session_state.selected_branch_pk=_pk
            if _a.get("review_id") is not None: st.session_state["v33_response_review_id"]=int(_a.get("review_id"))
            nav_to("branch_workspace"); st.rerun()
        if _a.get("ticket_id"):
            if c2.button("🎫 "+("فتح التذكرة" if RTL else "Ticket exists"),key="v32_ticket_{}".format(_i),use_container_width=True): nav_to("tickets"); st.rerun()
        else:
            if c2.button("➕ "+("إنشاء تذكرة" if RTL else "Create ticket"),key="v32_ticket_{}".format(_i),use_container_width=True,disabled=not bool(_pk)):
                _tp="critical" if _a.get("priority")=="critical" else "high" if _a.get("priority")=="high" else "medium"
                _tid=db.create_ticket(_pk,_a.get("review_id"),_a.get("title") or "Action Center follow-up",_tp,ws_id,
                                      source_kind=("review" if _a.get("review_id") is not None else (_a.get("sources") or ["action"])[0]),
                                      source_id=_a.get("review_id") or _a.get("alert_id"),action_key=_a.get("action_key"))
                if _tid: st.success(("تم إنشاء التذكرة #{}" if RTL else "Created ticket #{}").format(_tid)); st.rerun()
        if c3.button("✓ "+("إغلاق" if RTL else "Resolve"),key="v32_resolve_{}".format(_i),use_container_width=True):
            if _a.get("alert_id") is not None: db.resolve_alert(int(_a.get("alert_id")))
            if _a.get("review_id") is not None:
                try: db.review_action(int(_a.get("review_id")),"resolved")
                except Exception: pass
            if _a.get("ticket_id") is not None: db.update_ticket(int(_a.get("ticket_id")),status="resolved")
            st.rerun()
        with st.popover("⋯ "+("تفاصيل" if RTL else "Details")):
            st.write("Action key:",_a.get("action_key")); st.write("Sources:",_a.get("sources")); st.write("Owner:",_a.get("owner") or "—"); st.write("Created:",_a.get("created_at") or "—")

if page == "alerts":

    section_title("🚨 " + t("alerts"))
    alerts = db.list_alerts(ws_id)
    if alerts.empty: st.success("No alerts")
    else:
        for _, a in alerts.iterrows():
            st.markdown('<div class="rt-alert sev-{}"><b>{}</b> — <span style="color:{}">{}</span></div>'.format(
                html.escape(str(a["severity"])), html.escape(str(a["title"])),
                C["muted"], html.escape(str(a.get("company_name") or ""))),
                unsafe_allow_html=True)
            if a["status"] != "resolved" and st.button("✓ "+("حل التنبيه" if RTL else "Resolve"), key="ra_{}".format(a["id"])):
                db.resolve_alert(int(a["id"])); st.rerun()

if page == "tickets":
    section_title("🎫 " + t("tickets"))
    tk = db.list_tickets(ws_id)
    if tk.empty: st.info(t("no_data"))
    else: st.dataframe(tk, use_container_width=True, hide_index=True)

if page == "monitoring":
    section_title("🤖 " + t("monitoring"))
    if not companies.empty:
        c1, c2 = st.columns([3, 1])
        pk = c1.selectbox(t("branch"), companies["pk"].tolist(),
                          format_func=lambda x: company_options.get(x, x), key="mn_pk")
        iv = c2.selectbox("Interval (h)", [1,2,3,6,12,24], index=3, key="mn_iv")
        if st.button("➕ "+("إضافة للمراقبة" if RTL else "Add to monitoring"), key="mn_add"): 
            db.add_monitored(pk, company_options.get(pk, pk), iv, ws_id)
            st.toast("✅")
    mons = db.list_monitored(ws_id)
    if not mons.empty: st.dataframe(mons, use_container_width=True, hide_index=True)
    sync_states=db.branch_sync_states(ws_id)
    if not sync_states.empty:
        st.markdown("#### "+("ذكاء المزامنة 23.5" if RTL else "Branch Sync Intelligence 23.5"))
        s1,s2,s3,s4=st.columns(4)
        s1.metric("فروع تمت مزامنتها" if RTL else "Synced branches",len(sync_states))
        s2.metric("مراجعات جديدة" if RTL else "New reviews",int(pd.to_numeric(sync_states.get("new_reviews"),errors="coerce").fillna(0).sum()))
        s3.metric("تغيّرات مكتشفة" if RTL else "Detected changes",int(sync_states.get("changed_fields",pd.Series(dtype=str)).fillna("").astype(str).ne("[]").sum()))
        s4.metric("أخطاء" if RTL else "Errors",int(sync_states.get("status",pd.Series(dtype=str)).fillna("").eq("error").sum()))
        show_cols=[c for c in ["name","status","new_reviews","duplicate_reviews","current_rating","current_review_count","changed_fields","last_sync_at","last_error"] if c in sync_states.columns]
        st.dataframe(sync_states[show_cols],use_container_width=True,hide_index=True)
    _qc=db.sync_queue_counts(ws_id)
    st.caption(("طابور 23.5: انتظار {queued} · يعمل {running} · إعادة {retry_wait} · فشل {failed}" if RTL else "23.5 queue: queued {queued} · running {running} · retry {retry_wait} · failed {failed}").format(**_qc))

if page == "notif_center":
    section_title("🔔 " + t("notifications"))
    notifs = db.list_notifs(ws_id, "all", 100)
    if notifs.empty: st.info("No notifications")
    else:
        for _, n in notifs.iterrows():
            is_read = bool(n.get("read_at"))
            with st.container(border=True):
                c1, c2 = st.columns([5, 1])
                c1.markdown(("" if is_read else "**") + str(n.get("title") or "") +
                            ("" if is_read else "**"))
                c1.caption(str(n.get("created_at") or "")[:16])
                c1.markdown(str(n.get("body") or ""))
                if not is_read and c2.button("✓ "+("تمت القراءة" if RTL else "Mark read"), key="nc_r_{}".format(n["id"])):
                    db.mark_notif_read(int(n["id"])); st.rerun()

# ---- v20 PAGES ----
if page == "workspaces":
    section_title("🏢 " + t("workspaces"))
    if not permission(role, "admin"):
        st.error("❌ Access denied")
    else:
        with st.expander("➕ New workspace"):
            c1, c2 = st.columns(2)
            n = c1.text_input("Name", key="ws_n")
            ic = c2.text_input("Icon", value="🏢", key="ws_i")
            desc = st.text_input("Description", key="ws_d")
            if st.button("💾 "+("حفظ مساحة العمل" if RTL else "Save workspace"), key="ws_sv") and n.strip():
                if db.add_workspace(n.strip(), desc, ic):
                    db.log_activity(user["username"], ws_id, "create_workspace", n)
                    st.success("✅"); st.rerun()
                else: st.error("Already exists")
        wsd = db.list_workspaces()
        for _, w in wsd.iterrows():
            with st.container(border=True):
                c1, c2 = st.columns([4, 1])
                c1.markdown("{} **{}** — {} br · {} rev · {} usr".format(
                    w.get("icon","🏢"), w["name"], w["branch_count"],
                    w["review_count"], w["user_count"]))
                if w["id"] != 1 and c2.button("🗑️ "+("حذف" if RTL else "Delete"), key="ws_d_{}".format(w["id"])):
                    db.delete_workspace(int(w["id"])); st.rerun()

if page == "users":
    section_title("👥 " + t("users"))
    if not permission(role, "admin"):
        st.error("❌ Access denied")
    else:
        with st.expander("➕ New user"):
            c1, c2 = st.columns(2)
            un = c1.text_input("Username", key="u_un")
            dn = c2.text_input("Display name", key="u_dn")
            c3, c4 = st.columns(2)
            pw = c3.text_input("Password", type="password", key="u_pw")
            rl = c4.selectbox("Role", ["viewer","agent","manager","admin"], index=1, key="u_role")
            c5, c6 = st.columns(2)
            em = c5.text_input("Email", key="u_email")
            wsd = db.list_workspaces()
            if not wsd.empty:
                wd = c6.selectbox("Workspace", wsd["id"].tolist(),
                    format_func=lambda x: wsd[wsd["id"]==x]["name"].iloc[0] if not wsd[wsd["id"]==x].empty else str(x),
                    key="u_ws")
            else: wd = 1
            if st.button("💾 "+("حفظ المستخدم" if RTL else "Save user"), key="u_sv") and un.strip() and pw.strip():
                if db.add_user(un.strip(), pw, dn, rl, int(wd), em):
                    db.log_activity(user["username"], ws_id, "create_user", un)
                    st.success("✅"); st.rerun()
                else: st.error("Already exists")
        users_df = db.list_users()
        st.dataframe(users_df, use_container_width=True, hide_index=True)

if page == "sync_control":
    page_header("⏱️ "+("طابور المزامنة الحية 23.5" if RTL else "Live Sync Queue 23.5"),
                "كل فرع له مهمة مستقلة، تقدم مستقل وإعادة محاولة مستقلة؛ تعطل فرع لا يوقف بقية الفروع." if RTL else
                "Each branch has an independent job, progress and retry policy; one failed branch never blocks the rest.")
    _sched=db.scheduler_status(ws_id); _policy=db.get_sync_policy(ws_id)
    sm1,sm2,sm3,sm4=st.columns(4)
    sm1.metric("مستحق الآن" if RTL else "Due now",_sched.get("due",0))
    sm2.metric("أولوية عالية" if RTL else "High priority",_sched.get("high_priority",0))
    sm3.metric("فروع متتبعة" if RTL else "Tracked",_sched.get("tracked",0))
    sm4.metric("حد الدفعة" if RTL else "Batch limit",_sched.get("batch_limit",5))
    with st.expander("⚙️ "+("سياسة الجدولة والتنبيهات" if RTL else "Scheduler & alert policy"),expanded=False):
        pc1,pc2,pc3,pc4=st.columns(4)
        _base=pc1.number_input("الفترة الأساسية/ساعة" if RTL else "Base interval (h)",0.25,168.0,float(_policy.get("base_interval_hours") or 6),0.25,key="sp_base_235")
        _min=pc2.number_input("أقل فترة" if RTL else "Min interval",0.25,72.0,float(_policy.get("min_interval_hours") or 1),0.25,key="sp_min_235")
        _max=pc3.number_input("أقصى فترة" if RTL else "Max interval",1.0,720.0,float(_policy.get("max_interval_hours") or 24),1.0,key="sp_max_235")
        _batch=pc4.number_input("حد الدفعة" if RTL else "Batch limit",1,100,int(_policy.get("batch_limit") or 5),1,key="sp_batch_235")
        pc5,pc6,pc7,pc8=st.columns(4)
        _drop=pc5.number_input("تنبيه هبوط التقييم" if RTL else "Rating drop alert",0.01,2.0,float(_policy.get("rating_drop_threshold") or .2),.05,key="sp_drop_235")
        _neg=pc6.number_input("سلبيات جديدة للتنبيه" if RTL else "New negatives alert",1,100,int(_policy.get("negative_review_threshold") or 2),1,key="sp_neg_235")
        _spike=pc7.number_input("قفزة المراجعات" if RTL else "Review spike",1,1000,int(_policy.get("review_spike_threshold") or 10),1,key="sp_spike_235")
        _archive=pc8.number_input("أرشفة بعد/يوم" if RTL else "Archive after days",1,365,int(_policy.get("archive_days") or 30),1,key="sp_archive_235")
        _tog1,_tog2=st.columns(2)
        _enabled=_tog1.toggle("تشغيل الجدولة الذكية" if RTL else "Enable adaptive scheduler",value=bool(_policy.get("enabled",1)),key="sp_enabled_235")
        _contact_alert=_tog2.toggle("تنبيه عند تغير بيانات الاتصال" if RTL else "Alert on contact changes",value=bool(_policy.get("alert_contact_change",1)),key="sp_contact_235")
        if st.button("💾 "+("حفظ سياسة الجدولة" if RTL else "Save scheduler policy"),key="save_sync_policy_235"):
            db.save_sync_policy(ws_id,{"enabled":_enabled,"base_interval_hours":_base,"min_interval_hours":_min,"max_interval_hours":_max,"batch_limit":_batch,
                "archive_days":_archive,"rating_drop_threshold":_drop,"negative_review_threshold":_neg,"review_spike_threshold":_spike,
                "alert_contact_change":_contact_alert}); st.success("✅"); st.rerun()

    counts=db.sync_queue_counts(ws_id)
    q1,q2,q3,q4,q5=st.columns(5)
    q1.metric("بانتظار" if RTL else "Queued",counts.get("queued",0))
    q2.metric("يعمل" if RTL else "Running",counts.get("running",0))
    q3.metric("إعادة محاولة" if RTL else "Retry wait",counts.get("retry_wait",0))
    q4.metric("مكتمل" if RTL else "Done",counts.get("done",0))
    q5.metric("فشل" if RTL else "Failed",counts.get("failed",0))
    a1,a2,a3,a4=st.columns(4)
    if a1.button("➕ "+("إضافة الفروع المراقبة" if RTL else "Queue monitored branches"),type="primary",use_container_width=True,key="queue_monitored_234"):
        ids=sync_worker.enqueue_workspace(ws_id,force=True); st.success(("تمت إضافة {} مهمة" if RTL else "Queued {} jobs").format(len(ids))); st.rerun()
    if a2.button("▶️ "+("معالجة 5 الآن" if RTL else "Process 5 now"),use_container_width=True,key="process_queue_234"):
        with st.spinner("مزامنة الفروع..." if RTL else "Synchronizing branches..."):
            done=sync_worker.process_once(limit=5,ws_id=ws_id)
        st.success(("تمت معالجة {} مهمة" if RTL else "Processed {} jobs").format(len(done))); st.rerun()
    if sync_worker.is_running():
        if a3.button("⏹️ "+("إيقاف الخلفية" if RTL else "Stop background"),use_container_width=True,key="stop_sync_234"):
            sync_worker.stop(); st.rerun()
        a4.success("🟢 "+("الخلفية تعمل" if RTL else "Background running"))
    else:
        if a3.button("▶️ "+("تشغيل الخلفية" if RTL else "Start background"),use_container_width=True,key="start_sync_234"):
            sync_worker.start(); st.rerun()
        a4.info("⚪ "+("الخلفية متوقفة" if RTL else "Background stopped"))

    def _render_sync_queue_234():
        qdf=db.sync_queue_rows(ws_id,limit=250)
        if qdf.empty:
            st.info("لا توجد مهام مزامنة بعد." if RTL else "No sync jobs yet.")
            return
        view=qdf.copy()
        view["progress"] = pd.to_numeric(view.get("progress_pct"),errors="coerce").fillna(0).clip(0,100).map(lambda x:"{:.0f}%".format(x))
        view["attempt"] = view.apply(lambda r:"{}/{}".format(int(r.get("attempts") or 0),int(r.get("max_attempts") or 3)),axis=1)
        cols=[c for c in ["id","name","status","stage","progress","attempt","new_reviews","duplicate_reviews","queued_at","started_at","completed_at","next_attempt_at","error"] if c in view.columns]
        st.dataframe(view[cols],use_container_width=True,hide_index=True,height=min(620,120+34*min(15,len(view))))
        failed=qdf[qdf["status"].isin(["failed","retry_wait"])] if "status" in qdf else pd.DataFrame()
        if not failed.empty:
            opts=failed["id"].astype(int).tolist(); rid=st.selectbox("المهمة" if RTL else "Job",opts,
                format_func=lambda x:"#{} · {}".format(x,str(failed[failed["id"]==x]["name"].iloc[0] or "")),key="retry_queue_pick_234")
            if st.button("↻ "+("إعادة المحاولة الآن" if RTL else "Retry now"),key="retry_queue_234"):
                db.retry_sync(int(rid)); st.rerun()
    if hasattr(st,"fragment"):
        @st.fragment(run_every=2.0)
        def _live_queue_fragment_234(): _render_sync_queue_234()
        _live_queue_fragment_234()
    else:
        _render_sync_queue_234()

if page == "review_delta":
    page_header("🆕 "+("مركز المراجعات الجديدة" if RTL else "Review Delta Center"),
                "يعرض المراجعات التي دخلت من دورات المزامنة فقط، مع فصل الجديد عن السجل التاريخي." if RTL else
                "Shows reviews added by synchronization runs only, separating new arrivals from historical data.")
    d1,d2=st.columns([2,2])
    mode_label=d1.selectbox("النطاق" if RTL else "Window",
        ["latest","24h","7d","all"],
        format_func=lambda x:{"latest":"آخر مزامنة لكل فرع","24h":"آخر 24 ساعة","7d":"آخر 7 أيام","all":"كل ما اكتشفته المزامنة"}.get(x,x) if RTL else
                             {"latest":"Latest sync per branch","24h":"Last 24 hours","7d":"Last 7 days","all":"All sync-discovered reviews"}.get(x,x),key="delta_mode_234")
    delta_branches=[None]+companies["pk"].astype(str).tolist() if not companies.empty else [None]
    delta_pk=d2.selectbox("الفرع" if RTL else "Branch",delta_branches,
        format_func=lambda x:("كل الفروع" if RTL else "All branches") if x is None else company_options.get(x,x),key="delta_pk_234")
    delta=db.review_delta(ws_id,mode_label,delta_pk,limit=3000)
    if delta.empty:
        st.info("لا توجد مراجعات جديدة في هذا النطاق حتى الآن." if RTL else "No newly synchronized reviews in this window yet.")
    else:
        sent=delta.get("sentiment",pd.Series(dtype=str)).fillna("neutral")
        urgency=pd.to_numeric(delta.get("urgency",pd.Series(dtype=float)),errors="coerce").fillna(0)
        rating=pd.to_numeric(delta.get("rating",pd.Series(dtype=float)),errors="coerce")
        m1,m2,m3,m4,m5=st.columns(5)
        m1.metric("مراجعات جديدة" if RTL else "New reviews",len(delta))
        m2.metric("الفروع" if RTL else "Branches",int(delta["pk"].nunique()) if "pk" in delta else 0)
        m3.metric("سلبية" if RTL else "Negative",int((sent=="negative").sum()))
        m4.metric("عاجلة" if RTL else "Urgent",int((urgency>=60).sum()))
        m5.metric("متوسط النجوم" if RTL else "Avg stars","{:.2f}".format(float(rating.mean())) if rating.notna().any() else "—")
        f1,f2=st.columns(2)
        sentiment_filter=f1.selectbox("المشاعر" if RTL else "Sentiment",["all","negative","neutral","positive"],key="delta_sent_234")
        text_filter=f2.text_input("بحث داخل الجديد" if RTL else "Search new reviews",placeholder="كلمة أو اسم..." if RTL else "keyword or author...",key="delta_text_234")
        show=delta.copy()
        if sentiment_filter!="all" and "sentiment" in show: show=show[show["sentiment"]==sentiment_filter]
        if text_filter.strip():
            q=text_filter.strip().lower(); mask=show.get("text",pd.Series("",index=show.index)).fillna("").astype(str).str.lower().str.contains(re.escape(q),regex=True) | show.get("author",pd.Series("",index=show.index)).fillna("").astype(str).str.lower().str.contains(re.escape(q),regex=True)
            show=show[mask]
        cols=[c for c in ["company_name","rating","author","text","sentiment","topic","urgency","priority","review_date","sync_completed_at"] if c in show.columns]
        st.dataframe(show[cols],use_container_width=True,hide_index=True,height=min(650,120+36*min(14,len(show))))
        runs=db.sync_runs(ws_id,delta_pk,limit=50)
        with st.expander("سجل دورات المزامنة" if RTL else "Sync run history",expanded=False):
            if not runs.empty:
                st.dataframe(runs[[c for c in ["id","name","status","new_reviews","duplicate_reviews","started_at","completed_at","error"] if c in runs]],use_container_width=True,hide_index=True)

if page == "change_intelligence":
    page_header("🧠 "+("ذكاء التغييرات 2.0" if RTL else "Change Intelligence 2.0"),
                "مركز موحد للتغييرات المهمة منذ المزامنة: التقييم، عدد المراجعات، بيانات الاتصال والمراجعات السلبية الجديدة." if RTL else
                "Unified important changes from synchronization: rating, review-count, contact data and newly negative reviews.")
    ci1,ci2,ci3=st.columns(3)
    _window=ci1.selectbox("الفترة" if RTL else "Window",[24,168,720,None],format_func=lambda x:("كل السجل" if RTL else "All history") if x is None else (("آخر {} ساعة" if RTL else "Last {} hours").format(x) if x<168 else (("آخر 7 أيام" if RTL else "Last 7 days") if x==168 else ("آخر 30 يوم" if RTL else "Last 30 days"))),key="ci_window_235")
    _sev=ci2.selectbox("الأهمية" if RTL else "Severity",[None,"critical","warning","info"],format_func=lambda x:("الكل" if RTL else "All") if x is None else x,key="ci_sev_235")
    _pk_opts=[None]+companies["pk"].astype(str).tolist() if not companies.empty else [None]
    _pk=ci3.selectbox("الفرع" if RTL else "Branch",_pk_opts,format_func=lambda x:("كل الفروع" if RTL else "All branches") if x is None else company_options.get(x,x),key="ci_pk_235")
    ev=db.change_events(ws_id,since_hours=_window,pk=_pk,severity=_sev,limit=3000)
    if ev.empty:
        st.info("لا توجد تغييرات مسجلة في هذا النطاق." if RTL else "No recorded changes in this window.")
    else:
        c1,c2,c3,c4,c5=st.columns(5)
        c1.metric("إجمالي التغييرات" if RTL else "Changes",len(ev))
        c2.metric("حرجة" if RTL else "Critical",int((ev["severity"]=="critical").sum()))
        c3.metric("تحذير" if RTL else "Warnings",int((ev["severity"]=="warning").sum()))
        c4.metric("هبوط تقييم" if RTL else "Rating drops",int(((ev["event_type"]=="rating_delta") & (pd.to_numeric(ev["delta"],errors="coerce")<0)).sum()))
        c5.metric("سلبيات جديدة" if RTL else "New negatives",int(pd.to_numeric(ev.loc[ev["event_type"]=="new_negative_reviews","delta"],errors="coerce").fillna(0).sum()))
        show=ev.copy(); show["when"]=show["created_at"]
        cols=[c for c in ["when","name","severity","event_type","metric","old_value","new_value","delta","detail","maps_url"] if c in show]
        _ci_cfg={"maps_url":st.column_config.LinkColumn("Google Maps",display_text="فتح الخريطة" if RTL else "Open map")} if "maps_url" in cols else {}
        st.dataframe(show[cols],use_container_width=True,hide_index=True,height=min(680,130+35*min(15,len(show))),column_config=_ci_cfg)
        if "event_type" in ev:
            summary=ev.groupby(["event_type","severity"],dropna=False).size().reset_index(name="count").sort_values("count",ascending=False)
            with st.expander("ملخص الأنواع" if RTL else "Event summary",expanded=False): st.dataframe(summary,use_container_width=True,hide_index=True)

if page == "delivery_center":
    page_header("📨 "+("Scheduled Insights & Alert Delivery 3.0" if not RTL else "الملخصات المجدولة وتسليم التنبيهات 3.0"),
                "تجميع التغييرات المهمة، منع التكرار، Quiet Hours، Retry مستقل وOutbox موحد؛ روابط Google Maps مرفقة بالتغييرات." if RTL else
                "Digest important changes, deduplicate noise, honor quiet hours, retry channels independently and keep one auditable outbox with Google Maps links.")
    pol=db.get_delivery_policy(ws_id); ds=db.delivery_status(ws_id)
    m1,m2,m3,m4=st.columns(4); m1.metric("بالانتظار" if RTL else "Queued",ds.get("queued",0)); m2.metric("إعادة محاولة" if RTL else "Retry",ds.get("retry_wait",0)); m3.metric("تم الإرسال" if RTL else "Sent",ds.get("sent",0)); m4.metric("فشل" if RTL else "Failed",ds.get("failed",0))
    with st.expander("⚙️ "+("سياسة التسليم" if RTL else "Delivery policy"),expanded=True):
        p1,p2,p3=st.columns(3)
        en=p1.toggle("تفعيل الملخصات" if RTL else "Enable digests",value=bool(pol.get("enabled",1)),key="del_en_236")
        freq=p2.selectbox("التكرار" if RTL else "Frequency",["hourly","daily","weekly"],index=["hourly","daily","weekly"].index(str(pol.get("digest_frequency") or "daily")),key="del_freq_236")
        sev=p3.selectbox("أدنى أهمية" if RTL else "Minimum severity",["info","warning","critical"],index=["info","warning","critical"].index(str(pol.get("min_severity") or "warning")),key="del_sev_236")
        q1,q2,q3=st.columns(3)
        qs=q1.text_input("بداية Quiet Hours" if RTL else "Quiet start",value=str(pol.get("quiet_start") or "22:00"),key="del_qs_236")
        qe=q2.text_input("نهاية Quiet Hours" if RTL else "Quiet end",value=str(pol.get("quiet_end") or "07:00"),key="del_qe_236")
        retries=q3.number_input("أقصى محاولات" if RTL else "Max retries",1,10,int(pol.get("max_retries") or 3),key="del_retry_236")
        available=["telegram","slack","teams","whatsapp","email","webhook"]
        try: selected=json.loads(pol.get("channels_json") or "[]")
        except Exception: selected=[]
        channels=st.multiselect("قنوات الإرسال" if RTL else "Delivery channels",available,default=[x for x in selected if x in available],help=("اتركها فارغة لاستخدام جميع القنوات المهيأة تلقائيًا." if RTL else "Leave empty to auto-use all configured channels."),key="del_channels_236")
        if st.button("💾 "+("حفظ السياسة" if RTL else "Save policy"),type="primary",key="save_del_pol_236"):
            ok=db.save_delivery_policy(ws_id,{"enabled":en,"digest_frequency":freq,"min_severity":sev,"quiet_start":qs,"quiet_end":qe,"channels_json":channels,"max_retries":int(retries)})
            st.success("تم الحفظ" if RTL else "Saved") if ok else st.error("تعذر الحفظ" if RTL else "Save failed")
    d=db.build_change_digest(ws_id,freq,persist=False)
    st.markdown("#### "+("معاينة الملخص الحالي" if RTL else "Current digest preview"))
    if not d.get("event_count"):
        st.info("لا توجد تغييرات ضمن مستوى الأهمية والفترة الحالية." if RTL else "No changes match the current window and severity policy.")
    else:
        x1,x2,x3=st.columns(3); x1.metric("الأحداث" if RTL else "Events",d.get("event_count")); x2.metric("الفروع" if RTL else "Branches",d.get("branch_count")); x3.metric("أعلى أهمية" if RTL else "Max severity",d.get("severity_max"))
        st.caption(d.get("summary"))
        preview=pd.DataFrame(d.get("events") or [])
        if not preview.empty:
            pcols=[c for c in ["name","severity","event_type","detail","created_at","maps_url"] if c in preview.columns]
            pcfg={"maps_url":st.column_config.LinkColumn("Google Maps",display_text="فتح" if RTL else "Open")} if "maps_url" in pcols else {}
            st.dataframe(preview[pcols].tail(30),use_container_width=True,hide_index=True,column_config=pcfg)
    a,b=st.columns(2)
    if a.button("➕ "+("إنشاء الملخص ووضعه في Outbox" if RTL else "Build digest & queue outbox"),type="primary",use_container_width=True,key="queue_digest_236"):
        dd=db.build_change_digest(ws_id,freq,persist=True); n=db.queue_digest_outbox(ws_id,dd,channels if channels else None); st.success(("تمت إضافة {} رسالة" if RTL else "Queued {} message(s)").format(n)); st.rerun()
    if b.button("▶️ "+("إرسال المستحق الآن" if RTL else "Process due outbox now"),use_container_width=True,key="process_outbox_236"):
        res=process_delivery_outbox(db,ws_id,limit=20); st.info(json.dumps(res,ensure_ascii=False)); st.rerun()
    out=db.list_outbox(ws_id,"all",500)
    st.markdown("#### Outbox")
    if out.empty: st.caption("لا توجد رسائل بعد." if RTL else "No delivery items yet.")
    else:
        of=st.selectbox("الحالة" if RTL else "Status",["all","queued","retry_wait","sent","failed"],key="outbox_status_236")
        show=out if of=="all" else out[out["status"]==of]
        st.dataframe(show[[c for c in ["id","severity","channel","status","attempts","queued_at","sent_at","next_attempt_at","error","subject"] if c in show.columns]],use_container_width=True,hide_index=True,height=min(620,120+34*min(14,len(show))))
    runs=db.list_digest_runs(ws_id,100)
    with st.expander("سجل الملخصات" if RTL else "Digest history",expanded=False):
        if runs.empty: st.caption("—")
        else: st.dataframe(runs[[c for c in ["id","frequency","severity_max","event_count","branch_count","window_start","window_end","status","created_at"] if c in runs.columns]],use_container_width=True,hide_index=True)


if page == "reports_builder":
    page_header("📄 " + ("تقارير 3.0" if RTL else "Reports 3.0"),
                  "تقرير إداري جاهز من البيانات الحالية: مقارنة زمنية، أفضل الفروع، الفروع التي تحتاج انتباه، Insights وملفات PDF / Excel / CSV." if RTL else
                  "Management-ready reporting from live data: period comparison, top/attention branches, insights and PDF / Excel / CSV exports.")

    all_sec = {
        "summary": "الملخص التنفيذي" if RTL else "Executive summary",
        "branches": "الفروع" if RTL else "Branches",
        "reviews": "المراجعات" if RTL else "Reviews",
        "alerts": "التنبيهات" if RTL else "Alerts",
        "tickets": "التذاكر" if RTL else "Tickets",
    }
    preset=st.radio("نوع التقرير" if RTL else "Report type",["executive","full","operations"],horizontal=True,
                    format_func=lambda x:{"executive":"تنفيذي","full":"شامل","operations":"تشغيلي"}.get(x,x) if RTL else {"executive":"Executive","full":"Full","operations":"Operations"}.get(x,x),key="report_preset_v224")
    preset_sections={"executive":["summary","branches"],"full":["summary","branches","reviews","alerts","tickets"],"operations":["summary","branches","alerts","tickets"]}
    templates_df = db.list_report_templates(ws_id)
    tmpl_options = ["__new__"] + (templates_df["id"].astype(str).tolist() if not templates_df.empty else [])
    tmpl_pick = st.selectbox("القالب" if RTL else "Template", tmpl_options,
                             format_func=lambda x: ("قالب جديد" if RTL else "New template") if x == "__new__" else
                             str(templates_df.loc[templates_df["id"].astype(str)==x, "name"].iloc[0]),
                             key="rb_template_pick")
    default_sections = list(preset_sections.get(preset,["summary","branches"]))
    if tmpl_pick != "__new__":
        try:
            row = templates_df[templates_df["id"].astype(str)==tmpl_pick].iloc[0]
            loaded = json.loads(row.get("sections_json") or "[]")
            if loaded: default_sections = [x for x in loaded if x in all_sec]
        except Exception:
            pass

    c1, c2 = st.columns([2.2, 1])
    with c1:
        st.info(("سيتم إنشاء الأقسام تلقائيًا حسب نوع التقرير المختار." if RTL else "Sections are selected automatically from the report type."))
        with st.expander("تخصيص الأقسام" if RTL else "Customize sections",expanded=False):
            sel = st.multiselect("أقسام التقرير" if RTL else "Report sections", list(all_sec.keys()),
                                 default=default_sections, format_func=lambda x: all_sec[x], key="rb_sel_real")
    with c2:
        report_name = st.text_input("اسم التقرير" if RTL else "Report name",
                                    value=("تقرير السمعة 3.0" if RTL else "Reputation Report 3.0"), key="rb_report_name")

    datasets = report_datasets(db, ws_id, companies, period_reviews)
    summary_df = report_summary_frame(datasets)
    report_kpi = kpis(datasets["reviews"])
    s1, s2, s3 = st.columns(3)
    s1.metric("الفروع" if RTL else "Branches", len(datasets["branches"]))
    s2.metric("المراجعات" if RTL else "Reviews", len(datasets["reviews"]))
    s3.metric("متوسط التقييم" if RTL else "Avg rating", "{:.2f}".format(safe_num(report_kpi.get("avg"))))
    s4, s5 = st.columns(2)
    s4.metric("التنبيهات" if RTL else "Alerts", len(datasets["alerts"]))
    s5.metric("التذاكر" if RTL else "Tickets", len(datasets["tickets"]))
    st.caption(("الفترة الحالية: " if RTL else "Current period: ") + report_period_label())

    btn1, btn2, btn3 = st.columns([1.2, 1, 1])
    with btn1:
        tname = st.text_input("اسم القالب (اختياري)" if RTL else "Template name (optional)", key="rb_template_name")
        if st.button("💾 حفظ القالب" if RTL else "💾 Save template", use_container_width=True, key="rb_save_real"):
            if not tname.strip():
                st.warning("اكتب اسم القالب أولًا." if RTL else "Enter a template name first.")
            elif not sel:
                st.warning("اختر قسمًا واحدًا على الأقل." if RTL else "Select at least one section.")
            elif db.save_report_template(tname.strip(), sel, ws_id):
                st.success("تم حفظ القالب." if RTL else "Template saved.")
            else:
                st.error("تعذر حفظ القالب." if RTL else "Could not save template.")
    with btn2:
        if tmpl_pick != "__new__" and st.button("🗑️ حذف القالب" if RTL else "🗑️ Delete template", use_container_width=True, key="rb_delete_template"):
            if db.delete_report_template(int(tmpl_pick), ws_id):
                st.success("تم حذف القالب." if RTL else "Template deleted.")
                st.rerun()
    with btn3:
        st.metric("الأقسام المختارة" if RTL else "Selected sections", len(sel))

    if not sel:
        st.info("اختر قسمًا واحدًا على الأقل لإنشاء التقرير." if RTL else "Select at least one section to generate the report.")
    else:
        st.markdown("#### 👁️ " + ("معاينة البيانات قبل الإنشاء" if RTL else "Data preview before generation"))
        if "summary" in sel:
            st.dataframe(summary_df, use_container_width=True, hide_index=True, height=360)
        p1, p2 = st.columns(2)
        with p1:
            if "branches" in sel and not datasets["branches"].empty:
                st.caption(("أول 20 فرعًا من {}" if RTL else "First 20 of {} branches").format(len(datasets["branches"])))
                cols = [c for c in ["name","rating","review_count","address","phone","email"] if c in datasets["branches"].columns]
                st.dataframe(datasets["branches"][cols].head(20), use_container_width=True, hide_index=True)
        with p2:
            if "reviews" in sel and not datasets["reviews"].empty:
                st.caption(("أول 20 مراجعة من {}" if RTL else "First 20 of {} reviews").format(len(datasets["reviews"])))
                cols = [c for c in ["company_name","author","rating","sentiment","text"] if c in datasets["reviews"].columns]
                st.dataframe(datasets["reviews"][cols].head(20), use_container_width=True, hide_index=True)

        report_signature = json.dumps({
            "sections": sorted(sel), "period": st.session_state.get("period_key"),
            "days": st.session_state.get("period_custom_days"), "name": report_name.strip(),
            "counts": [len(datasets["branches"]), len(datasets["reviews"]), len(datasets["alerts"]), len(datasets["tickets"])],
        }, ensure_ascii=False, sort_keys=True)
        gen1, gen2 = st.columns([2, 1])
        with gen1:
            generate_now = st.button("📄 إنشاء التقرير" if RTL else "📄 Generate report",
                                     type="primary", use_container_width=True, key="rb_generate")
        with gen2:
            st.caption("يتم الإنشاء عند الطلب لتقليل استهلاك الموارد." if RTL else "Generated on demand for better performance.")
        if generate_now:
            started = time.perf_counter()
            try:
                stamp = datetime.now().strftime("%Y%m%d_%H%M")
                safe_name = (re.sub(r"[^\w-]+", "_", report_name.strip() or "report", flags=re.UNICODE).strip("_") or "report")[:60]
                bundle = {
                    "signature": report_signature,
                    "pdf": build_report_pdf(sel, datasets, title=report_name.strip()),
                    "excel": build_report_excel(sel, datasets),
                    "csv_zip": build_report_csv_zip(sel, datasets),
                    "stamp": stamp, "safe_name": safe_name,
                    "elapsed_ms": round((time.perf_counter()-started)*1000, 1),
                    "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                }
                st.session_state["report_bundle"] = bundle
            except Exception as e:
                st.session_state.pop("report_bundle", None)
                st.error(("تعذر إنشاء ملفات التقرير: " if RTL else "Could not generate report files: ") + str(e))

        bundle = st.session_state.get("report_bundle")
        if isinstance(bundle, dict):
            if bundle.get("signature") != report_signature:
                st.warning("تغيرت إعدادات التقرير أو البيانات. اضغط «إنشاء / تحديث التقرير» للحصول على ملفات حديثة." if RTL else
                           "Report settings or data changed. Generate/refresh the report to get current files.")
            else:
                d1, d2, d3 = st.columns(3)
                d1.download_button("📄 تنزيل PDF فعلي" if RTL else "📄 Download real PDF", bundle["pdf"],
                                   file_name="{}_{}.pdf".format(bundle["safe_name"], bundle["stamp"]), mime="application/pdf",
                                   use_container_width=True, type="primary", key="rb_pdf")
                d2.download_button("📊 تنزيل Excel كامل" if RTL else "📊 Download full Excel", bundle["excel"],
                                   file_name="{}_{}.xlsx".format(bundle["safe_name"], bundle["stamp"]),
                                   mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                                   use_container_width=True, key="rb_excel")
                d3.download_button("🗂️ تنزيل CSV ZIP" if RTL else "🗂️ Download CSV ZIP", bundle["csv_zip"],
                                   file_name="{}_{}_csv.zip".format(bundle["safe_name"], bundle["stamp"]), mime="application/zip",
                                   use_container_width=True, key="rb_csv_zip")
                st.success(("تم إنشاء التقرير من البيانات الفعلية في {} مللي ثانية · وقت الإنشاء {}." if RTL else
                            "Report generated from live data in {} ms · generated at {}.").format(
                                bundle.get("elapsed_ms"), bundle.get("generated_at")))

if page == "activity_log":
    section_title("📜 " + t("activity_log"))
    if not permission(role, "manager"):
        st.error("❌ Access denied")
    else:
        acts = db.list_activity(ws_id if not permission(role, "admin") else None, 300)
        if acts.empty: st.info(t("no_data"))
        else: st.dataframe(acts, use_container_width=True, hide_index=True)

if page == "schema_doctor":
    section_title("🩺 " + t("schema_doctor"))
    report = db.schema_report()
    c1, c2, c3 = st.columns(3)
    c1.metric("Schema Version", str(report.get("version") or "—"))
    c2.metric("Tables", len(report.get("tables", {})))
    c3.metric("Warnings", len(report.get("warnings", [])))
    if report.get("warnings"):
        with st.expander("⚠️ Migration warnings", expanded=True):
            for w in report["warnings"]: st.text(w)
    else:
        st.success("✅ Database schema is healthy")
    rows = []
    for tname, info in report.get("tables", {}).items():
        if "error" in info:
            rows.append({"Table": tname, "Status": "❌ " + info["error"]})
        else:
            status = "✅" if info.get("has_ws") or tname in ("workspaces","users","schema_version","migration_log","brand_settings") else "⚠️ no ws_id"
            rows.append({"Table": tname, "Rows": info.get("rows", 0),
                         "Columns": len(info.get("columns", [])), "Status": status})
    st.dataframe(pd.DataFrame(rows), use_container_width=True, hide_index=True)
    if st.button("🔧 "+("إعادة فحص وإصلاح المخطط" if RTL else "Re-run schema repair"), type="primary", key="doctor_repair"): 
        try:
            with st.spinner("..."):
                db._heal_schema(); db._create_indexes()
            st.success("✅ Done")
            st.rerun()
        except Exception as _exc:
            _p=safe_error_payload_v38(_exc,"schema_doctor.repair")
            record_local_error_v38(_p)
            st.error("{} · {}".format(_p["user_message"],_p["reference"]))

if page == "api_page":
    section_title("🔌 " + t("api"))
    key = api_key() or "Not generated yet (run the app once)"
    st.code(key, language=None)
    c1, c2 = st.columns(2)
    if c1.button("📋 "+("عرض مفتاح API" if RTL else "Show API key"), key="api_show"): 
        st.success("Key shown above")
    st.markdown("### Endpoints")
    st.code("""GET  http://localhost:8502/api/v1/health
GET  http://localhost:8502/api/v1/branches?workspace_id=1
GET  http://localhost:8502/api/v1/branches/<pk>?workspace_id=1
GET  http://localhost:8502/api/v1/reviews?workspace_id=1&limit=100
GET  http://localhost:8502/api/v1/alerts?workspace_id=1&status=new
GET  http://localhost:8502/api/v1/stats?workspace_id=1&days=30
POST http://localhost:8502/api/v1/webhook/zapier
POST http://localhost:8502/api/v1/webhook/make""", language="bash")
    st.markdown("### Auth")
    st.code('Header: X-API-Key: <your-key>\nOR: ?api_key=<your-key>', language="bash")

if page == "whitelabel":
    section_title("🎨 " + t("whitelabel"))
    if not permission(role, "admin"):
        st.error("❌ Access denied")
    else:
        b = db.get_brand()
        c1, c2 = st.columns(2)
        bn = c1.text_input("Brand name", value=b.get("brand_name", "Reviews Tracker"), key="wl_bn")
        bt = c2.text_input("Tagline", value=b.get("brand_tagline", ""), key="wl_bt")
        c3, c4 = st.columns(2)
        pc = c3.color_picker("Primary color", value=b.get("primary_color", "#2563EB"), key="wl_pc")
        ac = c4.color_picker("Accent color", value=b.get("accent_color", "#10B981"), key="wl_ac")
        lu = st.text_input("Logo URL", value=b.get("logo_url", ""), key="wl_lu")
        cd = st.text_input("Custom domain", value=b.get("custom_domain", ""), key="wl_cd")
        ft = st.text_area("Footer text", value=b.get("footer_text", ""), key="wl_ft")
        if st.button("💾 "+("حفظ الهوية" if RTL else "Save brand"), type="primary", key="wl_sv"): 
            ok = db.save_brand({"brand_name": bn, "brand_tagline": bt,
                                "primary_color": pc, "accent_color": ac,
                                "logo_url": lu, "custom_domain": cd,
                                "footer_text": ft})
            if ok: st.success("✅"); st.rerun()
            else: st.error("Failed")

if page == "team":
    section_title("🎯 " + t("team"))
    with st.expander("➕ New member"):
        c1, c2, c3 = st.columns(3)
        n = c1.text_input("Name", key="tm_n")
        r = c2.selectbox("Role", ["agent","manager","supervisor"], key="tm_r")
        e = c3.text_input("Email", key="tm_e")
        if st.button("💾 "+("حفظ عضو الفريق" if RTL else "Save team member"), key="tm_sv") and n.strip():
            try:
                with db._c() as c:
                    c.execute("""INSERT INTO team_members(workspace_id,name,role,email,active,created_at)
                        VALUES(?,?,?,?,1,?)""",
                        (ws_id, n.strip(), r, e.strip(), datetime.now().isoformat()))
                st.rerun()
            except Exception as ex: st.error(str(ex))
    try:
        with db._c() as c:
            team = pd.read_sql_query("SELECT * FROM team_members WHERE workspace_id=? AND active=1 ORDER BY id DESC",
                                      c, params=(ws_id,))
        if team.empty: st.info("No members")
        else: st.dataframe(team, use_container_width=True, hide_index=True)
    except Exception: pass

if page == "templates_lib":
    section_title("📚 " + t("templates"))
    with st.expander("➕ New template"):
        c1, c2 = st.columns(2)
        n = c1.text_input("Name", key="tpl_n")
        cat = c2.selectbox("Category", ["positive","negative","neutral"], key="tpl_c")
        content = st.text_area("Content", key="tpl_txt")
        if st.button("💾 "+("حفظ القالب" if RTL else "Save template"), key="tpl_sv") and n.strip() and content.strip():
            try:
                with db._c() as c:
                    c.execute("""INSERT INTO response_templates(workspace_id,name,category,content,lang,created_at,updated_at)
                        VALUES(?,?,?,?,?,?,?)""",
                        (ws_id, n.strip(), cat, content.strip(), lang,
                         datetime.now().isoformat(), datetime.now().isoformat()))
                st.rerun()
            except Exception as ex: st.error(str(ex))
    try:
        with db._c() as c:
            tpls = pd.read_sql_query("SELECT * FROM response_templates WHERE workspace_id=? ORDER BY id DESC",
                                      c, params=(ws_id,))
        if tpls.empty: st.info("No templates")
        else:
            for _, tr in tpls.iterrows():
                with st.expander("📄 {} · {}".format(tr["name"], tr.get("category",""))):
                    st.code(tr["content"], language=None)
    except Exception: pass

# ---- SYSTEM ----
if page == "settings":
    page_header("⚙️ "+t("settings"),"إعدادات بسيطة وواضحة: لغة، مظهر، وأوزان Health 3.0." if RTL else "Simple settings for language, appearance and Health 3.0 weights.","settings")
    st.markdown("#### 👁️ "+("وضوح الواجهة" if RTL else "Visual clarity"))
    st.info("تم اعتماد الواجهة الفاتحة فقط؛ v30.0 يعيد بناء مساحة البحث بأسلوب مبسط وفخم مع Query Expansion وResult Fusion ولوحة تفاصيل فرع، مع الحفاظ على الهوية المركزية والكاش والاستئناف والبحث المرن بدون مفتاح." if RTL else "v30.0 rebuilds the search workspace with Query Expansion, Result Fusion and a premium branch inspector while preserving canonical identity, Smart Cache, Resume and resilient keyless search.")
    c1,c2,c3=st.columns(3)
    nl=c1.selectbox(t("language"),["ar","en"],index=0 if lang=="ar" else 1,format_func=lambda x:"العربية" if x=="ar" else "English",key="s_l21")
    fs_opts=[1.0,1.08,1.12,1.18,1.25]; current_fs=min(fs_opts,key=lambda x:abs(x-UI_FONT_SCALE))
    nfs=c2.selectbox("حجم الخط" if RTL else "Font size",fs_opts,index=fs_opts.index(current_fs),format_func=lambda x:{1.0:"عادي",1.08:"واضح",1.12:"واضح +",1.18:"كبير",1.25:"كبير جدًا"}.get(x,str(x)) if RTL else {1.0:"Normal",1.08:"Readable",1.12:"Readable +",1.18:"Large",1.25:"Extra large"}.get(x,str(x)),key="ui_font_scale_setting")
    ndensity=c3.selectbox("تباعد العناصر" if RTL else "Control spacing",["compact","comfortable"],index=0 if UI_DENSITY=="compact" else 1,format_func=lambda x:("مضغوط" if x=="compact" else "مريح") if RTL else x.title(),key="ui_density_setting")
    nhc=st.checkbox("حدود واضحة عالية التباين" if RTL else "High-contrast borders",value=UI_HIGH_CONTRAST,key="ui_high_contrast_setting")
    st.caption("خطوط النظام المحلية مستخدمة لضمان وضوح العربية حتى بدون إنترنت." if RTL else "Local system fonts are used for sharp text even without internet access.")
    st.markdown("#### 💚 Health 3.0")
    hw=current_health_weights(); w1,w2,w3=st.columns(3)
    wr=w1.number_input("Rating %",0.0,100.0,float(hw["rating"]),1.0,key="hw_rating")
    wsnt=w2.number_input("Sentiment %",0.0,100.0,float(hw["sentiment"]),1.0,key="hw_sent")
    wresp=w3.number_input("Response %",0.0,100.0,float(hw["response"]),1.0,key="hw_resp")
    w4,w5=st.columns(2)
    wtr=w4.number_input("Trend %",0.0,100.0,float(hw["trend"]),1.0,key="hw_trend")
    wcr=w5.number_input("Critical %",0.0,100.0,float(hw["critical"]),1.0,key="hw_crit")
    st.caption(("Health 3.0 يطبّع الأوزان تلقائيًا على المؤشرات المتوفرة فقط؛ المؤشر المفقود لا يُحسب صفرًا. جودة البيانات تظهر منفصلة عن الدرجة." if RTL else "Health 3.0 normalizes weights only across available indicators; missing indicators are not scored as zero. Data quality is reported separately from the score."))
    if st.button("💾 "+("حفظ الإعدادات" if RTL else "Save settings"),type="primary",key="save_settings21"):
        st.session_state.lang=nl; st.session_state.theme="light"
        db.set_pref(_ui_pref_key,{"font_scale":float(nfs),"density":ndensity,"high_contrast":bool(nhc)})
        db.set_pref("health_weights_{}".format(ws_id),{"rating":wr,"sentiment":wsnt,"response":wresp,"trend":wtr,"critical":wcr})
        db.log_activity(user["username"],ws_id,"settings","lang={} theme=light font_scale={} density={}".format(nl,nfs,ndensity)); st.rerun()

if page == "connections":
    section_title("🔗 " + ("الاتصالات" if RTL else "Connections"))
    st.markdown("### 🌐 Google + Google Maps")
    gkey=CFG.get("GOOGLE_MAPS_API_KEY") or CFG.get("GOOGLE_PLACES_API_KEY")
    st.success("⚡ "+("البحث الحي بدون مفتاح مفعّل: Google Search + Google Maps عبر المتصفح." if RTL else "Keyless live search is enabled: Google Search + Google Maps through the browser."))
    if gkey:
        st.caption("Google Places API is also configured as an optional additional provider." if not RTL else "Google Places API مضاف أيضًا كمصدر اختياري إضافي.")
    else:
        st.caption("لا تحتاج إلى API Key لتشغيل البحث الأساسي. المفتاح اختياري فقط للمصدر الرسمي الإضافي." if RTL else "No API key is required for normal search. A key is optional only for the additional official provider.")
    with st.expander("🔑 "+("Google Places API اختياري" if RTL else "Optional Google Places API"),expanded=False):
        new_gkey=st.text_input("Google Maps / Places API Key",type="password",placeholder="AIza...",key="google_places_key_input")
        cga,cgb=st.columns(2)
        if cga.button("💾 "+("حفظ المفتاح محليًا" if RTL else "Save key locally"),use_container_width=True,key="save_google_key"):
            if not new_gkey.strip(): st.warning("أدخل المفتاح أولًا" if RTL else "Enter the key first")
            else:
                ok,msg=save_local_secret("GOOGLE_MAPS_API_KEY",new_gkey.strip())
                st.success("تم حفظ المفتاح ويمكن استخدامه فورًا." if RTL else "Key saved and available immediately.") if ok else st.error(msg)
        if cgb.button("🧪 "+("اختبار الاتصال الحي" if RTL else "Test live connection"),use_container_width=True,key="test_google_live"):
            with st.spinner("Testing Google..." if not RTL else "جاري اختبار Google..."):
                gs=test_google_live_connection()
            (st.success if gs.get("search") else st.warning)("Google Search: "+str(gs.get("search_detail")))
            (st.success if gs.get("maps") else st.error)("Google Maps: "+str(gs.get("maps_detail")))
            if gkey or new_gkey.strip():
                (st.success if gs.get("places") else st.warning)("Google Places API: "+str(gs.get("places_detail")))
            else:
                st.info("Google Places API: "+str(gs.get("places_detail")))
        st.caption("يُحفظ المفتاح في .streamlit/secrets.toml داخل البرنامج ولا يظهر بعد الحفظ." if RTL else "The key is stored in the app's .streamlit/secrets.toml and is not displayed after saving.")
    st.divider()
    st.markdown("### 🏢 " + ("مراجعات فروعك الموثقة" if RTL else "Verified owned-location reviews"))
    st.caption(
        "اختبار رسمي للقراءة فقط عبر Google Business Profile API. يتطلب رمز OAuth صالحًا وصلاحية إدارة الفرع. "
        "لا يُحفظ الرمز في قاعدة البيانات أو ملفات المشروع."
        if RTL else
        "Official read-only Business Profile check for locations you manage. "
        "Requires an authorized OAuth token; the token is not saved in the database or project files."
    )
    with st.form("gbp_v41_preview_form", clear_on_submit=True):
        gbp_parent = st.text_input(
            "Account / Location", placeholder="accounts/123456789/locations/987654321",
            key="gbp_v41_location_name")
        gbp_token = st.text_input(
            "OAuth Access Token", type="password", key="gbp_v41_access_token")
        gbp_submit = st.form_submit_button(
            "اختبار جلب المراجعات" if RTL else "Test owned reviews", type="primary")
    if gbp_submit:
        try:
            from business_profile_v41 import list_owned_location_reviews
            with st.spinner("Google Business Profile API"):
                gbp_rows, gbp_meta = list_owned_location_reviews(
                    str(gbp_parent or "").strip(),str(gbp_token or "").strip(),max_pages=5)
            st.session_state["gbp_v41_preview"]={
                "pages":gbp_meta.get("pages"),"total":gbp_meta.get("total_review_count"),
                "returned":gbp_meta.get("returned_reviews"),"complete":gbp_meta.get("complete"),
                "rating":gbp_meta.get("average_rating"),"rows":gbp_rows[:20],
            }
        except Exception as exc:
            # Provider intentionally sanitizes remote errors to avoid token leakage.
            st.session_state.pop("gbp_v41_preview",None)
            st.error(("تعذر جلب البيانات: " if RTL else "Unable to retrieve reviews: ")
                     + type(exc).__name__)
    gbp_preview=st.session_state.get("gbp_v41_preview")
    if isinstance(gbp_preview,dict):
        gc1,gc2,gc3=st.columns(3)
        gc1.metric("التقييم العام" if RTL else "Aggregate rating",
                   gbp_preview.get("rating") if gbp_preview.get("rating") is not None else "—")
        gc2.metric("عدد المراجعات" if RTL else "Total reviews",
                   gbp_preview.get("total") if gbp_preview.get("total") is not None else "—")
        gc3.metric("مراجعات مجلوبة" if RTL else "Fetched",
                   gbp_preview.get("returned",0))
        if not gbp_preview.get("complete",False):
            st.warning("النتائج جزئية بسبب حد الصفحات للاختبار." if RTL else "Partial results; test page limit reached.")
        if gbp_preview.get("rows"):
            st.dataframe(pd.DataFrame(gbp_preview["rows"]),use_container_width=True,hide_index=True)
        if st.button("مسح المعاينة" if RTL else "Clear preview",key="gbp_v41_clear"):
            st.session_state.pop("gbp_v41_preview",None); st.rerun()
    st.divider()
    st.markdown("### 📱 " + t("whatsapp"))
    st.caption("Meta Cloud API — Configure in `.streamlit/secrets.toml`:")
    st.code("""WHATSAPP_TOKEN = "EAAG..."
WHATSAPP_PHONE_ID = "1234567890"
WHATSAPP_TO = "+966501234567\"""", language="toml")
    wa_status = "✅ Configured" if CFG.get("WHATSAPP_TOKEN") and CFG.get("WHATSAPP_PHONE_ID") else "❌ Not configured"
    st.markdown("Status: " + wa_status)
    if st.button("🧪 Test WhatsApp", key="test_wa"):
        ok, msg = send_whatsapp("🧪 Reviews Tracker test message")
        st.success("Sent: " + msg) if ok else st.error(msg)
    st.divider()
    st.markdown("### 💬 Slack")
    st.code('# SLACK_WEBHOOK_URL = "https://hooks.slack.com/services/..."', language="toml")
    slack_status = "✅ Configured" if CFG.get("SLACK_WEBHOOK_URL") else "❌ Not configured"
    st.markdown("Status: " + slack_status)
    if st.button("🧪 Test Slack", key="test_slack"):
        ok, msg = send_slack("🧪 Reviews Tracker test")
        st.success("Sent: " + msg) if ok else st.error(msg)
    st.divider()
    st.markdown("### 👥 Microsoft Teams")
    st.code('# TEAMS_WEBHOOK_URL = "https://outlook.office.com/webhook/..."', language="toml")
    teams_status = "✅ Configured" if CFG.get("TEAMS_WEBHOOK_URL") else "❌ Not configured"
    st.markdown("Status: " + teams_status)
    if st.button("🧪 Test Teams", key="test_teams"):
        ok, msg = send_teams("🧪 Reviews Tracker test")
        st.success("Sent: " + msg) if ok else st.error(msg)
    st.divider()
    st.markdown("### ✈️ Telegram")
    st.code('# TELEGRAM_BOT_TOKEN = "..."\n# TELEGRAM_CHAT_ID = "..."', language="toml")
    tg_status = "✅ Configured" if CFG.get("TELEGRAM_BOT_TOKEN") else "❌ Not configured"
    st.markdown("Status: " + tg_status)
    if st.button("🧪 Test Telegram", key="test_tg"):
        ok, msg = send_telegram("🧪 Reviews Tracker test")
        st.success("Sent: " + msg) if ok else st.error(msg)
    st.info("💡 All credentials go in `.streamlit/secrets.toml`. Restart the app to reload.")

if page == "diagnostics":
    page_header("🩺 "+("حالة النظام" if RTL else "System Status"),"اختبارات حقيقية لـ SQLite والملفات والأداء مع إصلاح عند الطلب." if RTL else "Real SQLite, file and performance checks with on-demand repair.","diagnostics")
    _v39_preflight=final_preflight_v39(APP_DIR,DB_PATH)
    with st.expander("🏁 "+("فحص الإغلاق النهائي — v39" if RTL else "Final release preflight — v39"),expanded=True):
        st.success("جميع فحوص الإغلاق المحلية ناجحة." if RTL else "All local release checks passed.") if _v39_preflight.get("ok") else st.warning("توجد عناصر تحتاج مراجعة." if RTL else "Some release checks need attention.")
        st.dataframe(pd.DataFrame(_v39_preflight.get("checks") or []),use_container_width=True,hide_index=True)
        _rr=st.session_state.get("v39_runtime_recovery") or {}
        st.caption(("استعادة حالات التشغيل: {} متوقفة · {} عملية حية محفوظة." if RTL else "Runtime recovery: {} stale recovered · {} live preserved.").format(_rr.get("recovered",0),_rr.get("kept_live",0)))
    st.markdown("#### 🛡️ "+("الفحص المحلي الشامل — v38" if RTL else "Local Health Check — v38"))
    _v38_health=build_local_health_check_v38(db,APP_DIR,DB_PATH)
    _v38_icons={"ok":"✅","warning":"⚠️","error":"❌"}
    _v38_cols=st.columns(min(3,max(1,len(_v38_health))))
    for _i,_r in enumerate(_v38_health):
        _v38_cols[_i % len(_v38_cols)].markdown("**{} {}**  \n{}".format(_v38_icons.get(_r.get("status"),"ℹ️"),_r.get("label"),_r.get("detail") or "—"))
    if any(_r.get("status")=="error" for _r in _v38_health):
        st.warning("يوجد عنصر محلي يحتاج إصلاحًا. استخدم زر فحص وإصلاح أدناه." if RTL else "A local component needs repair. Use Check & repair below.")
    mons=db.list_monitored(ws_id); ai=ai_status(); integrity=db.integrity()
    try:
        with db._c() as c: fk_rows=c.execute("PRAGMA foreign_key_check").fetchall(); quick=c.execute("PRAGMA quick_check").fetchone()[0]
    except Exception as e: fk_rows=[str(e)]; quick="error"
    rh=runtime_health_snapshot(db,ws_id)
    _last_meta=((st.session_state.get("last_search_job") or {}).get("meta") or {}) if isinstance(st.session_state.get("last_search_job"),dict) else {}
    _last_pf=st.session_state.get("search_preflight_result") if isinstance(st.session_state.get("search_preflight_result"),dict) else {}
    _last_rel=_last_meta.get("search_reliability") or _last_pf.get("reliability") or {}
    _diag_log=APP_DIR / "worker_diagnostics.jsonl"
    _browser_health_file=APP_DIR / "browser_health.json"
    _selector_health_file=APP_DIR / "selector_health.json"
    try:
        _browser_health=json.loads(_browser_health_file.read_text(encoding="utf-8")) if _browser_health_file.exists() else {}
    except Exception:
        _browser_health={}
    try:
        _selector_health=json.loads(_selector_health_file.read_text(encoding="utf-8")) if _selector_health_file.exists() else {}
    except Exception:
        _selector_health={}
    _selector_rows=[]
    for _sn,_sr in (_selector_health.get("strategies") or {}).items():
        _selector_rows.append({"Strategy":_sn,"Score":round(float(_sr.get("score") or 0),1),
                               "Attempts":int(_sr.get("attempts") or 0),"Successes":int(_sr.get("successes") or 0),
                               "Failures":int(_sr.get("failures") or 0),"Records":int(_sr.get("records") or 0),
                               "Last error":str(_sr.get("last_error") or "")[:120]})
    _selector_rows.sort(key=lambda r:(-r["Score"],r["Strategy"]))
    vals=[("Version",VERSION),("Schema",SCHEMA_VERSION),("Python",sys.version.split()[0]),("OS",platform.platform()),("DB Integrity",integrity),("DB Quick Check",quick),("Foreign Key Issues",len(fk_rows)),("DB Size","{} MB".format(rh.get("db_mb",0))),("WAL Size","{} MB".format(rh.get("wal_mb",0))),("Job artifacts",rh.get("job_files",0)),("Stale DB jobs",rh.get("stale_jobs",0)),("Worker","OK" if WORKER.exists() else "Missing"),("Daemon","OK" if DAEMON.exists() else "Missing"),("API","OK" if API_FILE.exists() else "Missing"),("AI","Available" if ai["available"] else "Rule-based fallback"),("Language detector","langdetect + script" if LANGDETECT_AVAILABLE else "script heuristic"),("Monitored",len(mons)),("Outbox queued",db.delivery_status(ws_id).get("queued",0)+db.delivery_status(ws_id).get("retry_wait",0)),("Branches",rh.get("branches",len(companies))),("Reviews",rh.get("reviews",len(period_reviews))),("Search engine",_last_meta.get("browser_engine","—")),
          ("Async search",bool(_last_meta.get("async_mode"))),("Discovery concurrency",_last_meta.get("async_discovery_concurrency","—")),
          ("Detail concurrency",_last_meta.get("async_detail_concurrency","—")),("Last search reason",_last_meta.get("termination_reason","—")),
          ("Browser engines tried"," → ".join(map(str,_last_meta.get("browser_engines_tried") or [])) or "—"),
          ("Browser failover",("Yes → "+str(_last_meta.get("browser_failover_engine") or "—")) if _last_meta.get("browser_failover_used") else "No"),
          ("Preferred browser",_browser_health.get("preferred_engine") or "—"),("Browser health",_browser_health.get("last_status") or "—"),
          ("Selector strategy"," → ".join(map(str,_last_meta.get("selector_strategy_order") or [])) or "—"),
          ("Best selector",_last_meta.get("best_selector_strategy") or ((_selector_rows[0]["Strategy"] if _selector_rows else "—"))),
          ("Selector health entries",len(_selector_rows)),
          ("Preflight ready",_last_pf.get("ready","—")),("Preflight reason",_last_pf.get("reason","—")),
          ("Search reliability score",_last_rel.get("score","—")),("Search reliability grade",_last_rel.get("grade","—")),
          ("Worker diagnostics","{} KB".format(round(_diag_log.stat().st_size/1024,1)) if _diag_log.exists() else "0 KB")]
    st.dataframe(pd.DataFrame(vals,columns=["Check","Status"]),use_container_width=True,hide_index=True)
    b1,b2,b3=st.columns(3)
    if b1.button("🧪 "+("اختبار أداء فعلي" if RTL else "Run benchmark"),type="primary",use_container_width=True,key="diag_bench21"):
        df,meta=run_performance_benchmark(db,ws_id); st.session_state.perf_bench_df=df; st.session_state.perf_bench_meta=meta
    if b2.button("🔧 "+("فحص وإصلاح" if RTL else "Check & repair"),use_container_width=True,key="diag_repair21"):
        try:
            with st.spinner("Repairing..."):
                db._heal_schema(); db._create_indexes(); ok,msg=db.optimize()
            invalidate_cache(); st.success(("اكتمل الإصلاح: " if RTL else "Repair completed: ")+str(msg)) if ok else st.error(str(msg))
        except Exception as _exc:
            _p=safe_error_payload_v38(_exc,"diagnostics.repair")
            record_local_error_v38(_p)
            st.error("{} · {}".format(_p["user_message"],_p["reference"]))
    if b3.button("💾 "+("نسخة احتياطية" if RTL else "Backup"),use_container_width=True,key="diag_bk21"): bp=backup_create(); st.success(bp.name)
    st.markdown("#### 🧰 "+("حزمة تشخيص Windows / البحث" if RTL else "Windows / Search diagnostic bundle"))
    _dq=str(st.session_state.get("search_query_used") or "البيك الرياض")
    if st.button("🧰 "+("إنشاء حزمة تشخيص" if RTL else "Build diagnostic bundle"),key="diag_build_field_bundle",use_container_width=True):
        with st.spinner("Diagnostics..."):
            st.session_state["field_diag_bundle_result"]=build_field_diagnostic_bundle(_dq,st.session_state.get("lang","ar"),90)
    _bd=st.session_state.get("field_diag_bundle_result")
    if isinstance(_bd,dict):
        _bp=Path(str(_bd.get("path") or ""))
        if _bd.get("ok") and _bp.exists():
            st.success("تم إنشاء حزمة تشخيص منزوعة الأسرار." if RTL else "Redacted diagnostic bundle created.")
            st.download_button("⬇️ "+("تحميل حزمة التشخيص" if RTL else "Download diagnostics"),_bp.read_bytes(),file_name=_bp.name,mime="application/zip",key="diag_download_field_bundle",use_container_width=True)
        elif _bd.get("error"):
            st.error(str(_bd.get("error")))

    st.markdown("#### 🗺️ "+("اختبار Google Maps الحي" if RTL else "Live Google Maps probe"))
    _pb1,_pb2=st.columns([2,1])
    with _pb1:
        if st.button("🧩 "+("إعادة تعلم استراتيجيات الاستخراج" if RTL else "Reset extraction learning"),key="diag_reset_selector_health",use_container_width=True):
            try:
                if _selector_health_file.exists(): _selector_health_file.unlink()
                st.success("تم مسح سجل صحة المحددات. سيعيد المحرك تعلم أفضل استراتيجية تلقائيًا." if RTL else "Selector health cleared. The engine will relearn the best extraction strategy automatically.")
            except Exception as _e:
                st.error(str(_e))
    with _pb2:
        if st.button("♻️ "+("إعادة اختيار المتصفح" if RTL else "Reset browser preference"),key="diag_reset_browser_pref",use_container_width=True):
            try:
                if _browser_health_file.exists(): _browser_health_file.unlink()
                st.success("تم مسح تفضيل المتصفح. سيعيد المحرك اختبار المتصفحات في البحث التالي." if RTL else "Browser preference cleared. The next search will test browsers again.")
            except Exception as _e:
                st.error(str(_e))
    _probe_default=str(st.session_state.get("search_query_used") or "البيك الرياض")
    _probe_q=st.text_input("عبارة الاختبار" if RTL else "Probe query",value=_probe_default,key="diag_google_maps_probe_q")
    if st.button("▶️ "+("تشغيل الاختبار الحي" if RTL else "Run live probe"),key="diag_google_maps_probe_btn",use_container_width=True):
        with st.spinner("Google Maps..."):
            st.session_state["diag_google_maps_probe_result"]=run_google_maps_live_probe(_probe_q,st.session_state.get("lang","ar"),75)
    _probe=st.session_state.get("diag_google_maps_probe_result")
    if isinstance(_probe,dict):
        if _probe.get("ok") and int(_probe.get("results") or 0)>0:
            st.success(("وصلت {} نتيجة حقيقية · {} · Async {}" if RTL else "{} real result(s) · {} · Async {}").format(_probe.get("results"),_probe.get("browser_engine") or "—",bool(_probe.get("async_mode"))))
        elif _probe.get("ok"):
            st.warning(("اتصل المحرك لكن عاد بصفر نتائج · السبب {} · المتصفح {}" if RTL else "Engine connected but returned zero results · reason {} · browser {}").format(_probe.get("no_result_reason") or _probe.get("termination_reason") or "unknown",_probe.get("browser_engine") or "—"))
        else:
            st.error(str(_probe.get("error") or _probe.get("stderr_tail") or "Probe failed"))
        _probe_rows=[("Results",_probe.get("results",0)),("Browser",_probe.get("browser_engine")),("Async",_probe.get("async_mode")),
                     ("Browser engines tried"," → ".join(map(str,_probe.get("browser_engines_tried") or [])) or "—"),
                     ("Browser failover",("Yes → "+str(_probe.get("browser_failover_engine") or "—")) if _probe.get("browser_failover_used") else "No"),
                     ("Termination",_probe.get("termination_reason")),("No-result reason",_probe.get("no_result_reason")),
                     ("Providers",", ".join(_probe.get("providers") or [])),("Discovery x",_probe.get("discovery_concurrency")),
                     ("Details x",_probe.get("detail_concurrency")),
                     ("Selector order"," → ".join(map(str,_probe.get("selector_strategy_order") or [])) or "—"),
                     ("Best selector",_probe.get("best_selector_strategy") or "—"),("Elapsed ms",_probe.get("elapsed_ms"))]
        st.dataframe(pd.DataFrame(_probe_rows,columns=["Probe","Value"]),use_container_width=True,hide_index=True)
        _pt=_probe.get("extraction_telemetry") or {}
        if _pt.get("strategies"):
            _pt_rows=[]
            for _n,_r in _pt.get("strategies",{}).items():
                _pt_rows.append({"Strategy":_n,"Attempts":_r.get("attempts",0),"Records":_r.get("count",0),"Errors":_r.get("errors",0),"Elapsed ms":round(float(_r.get("elapsed_ms") or 0),1)})
            st.dataframe(pd.DataFrame(_pt_rows),use_container_width=True,hide_index=True)
        if _probe.get("errors"): st.code("\n".join(map(str,_probe.get("errors")[:12])))
    if _selector_rows:
        st.markdown("#### 🧩 "+("صحة استراتيجيات الاستخراج" if RTL else "Extraction strategy health"))
        st.dataframe(pd.DataFrame(_selector_rows),use_container_width=True,hide_index=True)
    df=st.session_state.get("perf_bench_df"); meta=st.session_state.get("perf_bench_meta")
    if isinstance(df,pd.DataFrame) and not df.empty:
        m1,m2,m3,m4=st.columns(4); m1.metric("Average","{} ms".format(meta.get("avg_ms"))); m2.metric("P95","{} ms".format(meta.get("p95_ms"))); m3.metric("Worst","{} ms".format(meta.get("worst_ms"))); m4.metric("Grade",meta.get("grade")); st.dataframe(df,use_container_width=True,hide_index=True)
    jobs=db.list_search_jobs(ws_id,30)
    if not jobs.empty:
        st.markdown("#### "+("سجل البحث الفعلي" if RTL else "Real search job log")); st.dataframe(jobs[[c for c in ["job_id","query","status","elapsed_ms","results","discovered","tiles","errors","started_at"] if c in jobs]],use_container_width=True,hide_index=True)

if page == "backups":
    section_title("💾 " + t("backups"))
    st.caption("نسخ SQLite متحقق منها قبل اعتمادها، مع SHA256 واستعادة آمنة ونسخة Rollback تلقائية." if RTL else "Verified SQLite backups with SHA256, safe restore, and automatic rollback backup.")
    if st.button("➕ "+("إنشاء نسخة احتياطية متحققة" if RTL else "Create verified backup"), type="primary", use_container_width=True, key="bkp_new"):
        try:
            p = backup_create(); info=backup_validate_v39(p)
            st.success("✅ {} · SHA256 {}…".format(p.name,str(info.get("sha256") or "")[:16]))
        except Exception as exc:
            _bp=safe_error_payload_v38(exc,"backup.create.v39"); record_local_error_v38(_bp); st.error("{} · {}".format(_bp["user_message"],_bp["reference"]))
    for i, p in enumerate(backup_list()[:20]):
        info=backup_validate_v39(p)
        cc1, cc2, cc3 = st.columns([4.6, 1, 1.4])
        status="✅" if info.get("ok") else "❌"
        cc1.write("{} {} · {:.2f} MB · SHA256 {}…".format(status,p.name,p.stat().st_size/1024/1024,str(info.get("sha256") or "—")[:16]))
        cc2.download_button("⬇", p.read_bytes(), file_name=p.name,mime="application/octet-stream", key="dl_bk_{}".format(i),use_container_width=True)
        with cc3.popover("⋯ "+("استعادة" if RTL else "Restore"),use_container_width=True):
            st.caption("Integrity: {} · SHA256: {}".format(info.get("integrity"),info.get("sha256") or "—"))
            confirm=st.checkbox("أؤكد استعادة هذه النسخة" if RTL else "I confirm restoring this backup",key="restore_confirm_v39_{}".format(i),disabled=not info.get("ok"))
            if st.button("♻️ "+("استعادة النسخة" if RTL else "Restore backup"),key="restore_backup_v39_{}".format(i),use_container_width=True,disabled=(not confirm or not info.get("ok"))):
                try:
                    result=restore_verified_backup_v39(p,DB_PATH,BACKUP_DIR); invalidate_cache()
                    st.success(("تمت الاستعادة بنجاح. نسخة الرجوع: " if RTL else "Restore completed. Rollback backup: ")+Path(result.get("rollback_path") or "").name)
                    st.session_state.pop("search_results",None); st.session_state.pop("search_meta",None)
                    st.rerun()
                except Exception as exc:
                    _rp=safe_error_payload_v38(exc,"backup.restore.v39"); record_local_error_v38(_rp); st.error("{} · {}".format(_rp["user_message"],_rp["reference"]))


st.divider()
ft = BRAND.get("footer_text") or "Reviews Tracker {} · AI · WhatsApp · REST API · White-label".format(VERSION)
st.markdown('<div class="rt-footer">{}</div>'.format(html.escape(ft)), unsafe_allow_html=True)

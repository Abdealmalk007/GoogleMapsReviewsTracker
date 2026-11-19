#!/usr/bin/env python3
"""Google/Google Maps worker v40.0: Google-style query expansion, result fusion/grouping, resumable search sessions, smart cache, data freshness, resilient async discovery, and review synchronization."""
import sys, json, re, argparse, traceback, math, os, time, uuid, html as html_lib, asyncio, unicodedata, shutil, socket, platform, zipfile, threading, hashlib
import urllib.request, urllib.parse
from datetime import datetime, timedelta
from pathlib import Path
from urllib.parse import quote_plus, unquote, urlparse, urljoin

APP_DIR = Path(__file__).resolve().parent
GEOCODE_CACHE = APP_DIR / "geocode_cache.json"
DIAG_LOG = APP_DIR / "worker_diagnostics.jsonl"
BROWSER_HEALTH_FILE = APP_DIR / "browser_health.json"
SELECTOR_HEALTH_FILE = APP_DIR / "selector_health.json"
BRANCH_CACHE_FILE = APP_DIR / "branch_detail_cache.json"
CONTACT_CACHE_FILE = APP_DIR / "contact_cache.json"
BRANCH_CACHE_VERSION = 1
DETAIL_CACHE_TTL_HOURS = max(12, min(int(os.environ.get("RT_DETAIL_CACHE_TTL_HOURS", "168") or 168), 2160))
CONTACT_CACHE_TTL_HOURS = max(24, min(int(os.environ.get("RT_CONTACT_CACHE_TTL_HOURS", "336") or 336), 4320))
BRANCH_CACHE_MAX_ENTRIES = max(500, min(int(os.environ.get("RT_BRANCH_CACHE_MAX_ENTRIES", "6000") or 6000), 25000))
CONTACT_CACHE_MAX_ENTRIES = max(200, min(int(os.environ.get("RT_CONTACT_CACHE_MAX_ENTRIES", "2500") or 2500), 10000))
_CACHE_LOCK = threading.RLock()
_CACHE_MEMORY = {}
DISCOVERY_EXTRACTION_STRATEGIES = ("semantic_anchors", "feed_cards", "legacy_cards", "raw_html")
SEARCH_CHECKPOINT_VERSION = 1

def _atomic_json_write(path, payload):
    p=Path(path)
    p.parent.mkdir(parents=True,exist_ok=True)
    tmp=p.with_name(p.name+".tmp")
    tmp.write_text(json.dumps(payload,ensure_ascii=False,indent=2,default=str),encoding="utf-8")
    os.replace(str(tmp),str(p))
    return True

def _search_resume_fingerprint(query="", **params):
    """Stable identity for a resumable search. Any material parameter change creates a new session."""
    def norm(v):
        if isinstance(v,bool): return v
        if isinstance(v,(int,float)): return round(float(v),6)
        if isinstance(v,(list,tuple)): return [norm(x) for x in v]
        if isinstance(v,dict): return {str(k):norm(v[k]) for k in sorted(v)}
        if isinstance(v,str): return re.sub(r"\s+"," ",v.strip()).casefold()
        return v
    payload={"query":norm(query)}
    payload.update({str(k):norm(v) for k,v in sorted(params.items())})
    blob=json.dumps(payload,ensure_ascii=False,sort_keys=True,separators=(",",":"),default=str)
    return hashlib.sha256(blob.encode("utf-8")).hexdigest()[:32]

def _checkpoint_job_key(kind, query="", center=None, idx=None):
    q=re.sub(r"\s+"," ",str(query or "").strip()).casefold()
    c=None
    if center is not None:
        try:
            c=[round(float(center[0]),5),round(float(center[1]),5)]
        except Exception:
            c=str(center)
    blob=json.dumps({"kind":str(kind or ""),"query":q,"center":c,"idx":idx},ensure_ascii=False,sort_keys=True,separators=(",",":"),default=str)
    return str(kind or "job")+":"+hashlib.sha1(blob.encode("utf-8")).hexdigest()[:20]

def _new_search_checkpoint(fingerprint="", query=""):
    return {"version":SEARCH_CHECKPOINT_VERSION,"fingerprint":str(fingerprint or ""),"query":str(query or ""),
            "status":"new","phase":"init","created_at":datetime.now().isoformat(timespec="seconds"),
            "updated_at":datetime.now().isoformat(timespec="seconds"),"completed_jobs":[],"completed_details":[],
            "results":[],"tile_stats":[],"resume_count":0,"compatible":True}

def _load_search_checkpoint(path, fingerprint=None, query=""):
    base=_new_search_checkpoint(fingerprint or "",query)
    if not path:
        return base
    p=Path(path)
    try:
        if not p.exists(): return base
        data=json.loads(p.read_text(encoding="utf-8"))
        if not isinstance(data,dict) or int(data.get("version") or 0)!=SEARCH_CHECKPOINT_VERSION:
            base["compatible"]=False; base["reset_reason"]="version_mismatch"; return base
        if fingerprint and str(data.get("fingerprint") or "")!=str(fingerprint):
            base["compatible"]=False; base["reset_reason"]="fingerprint_mismatch"; return base
        data.setdefault("completed_jobs",[]); data.setdefault("completed_details",[]); data.setdefault("results",[]); data.setdefault("tile_stats",[])
        data["compatible"]=True
        return data
    except Exception as exc:
        _diag("search_checkpoint_load_failed",exc,path=str(p))
        base["compatible"]=False; base["reset_reason"]="load_error"
        return base

def _save_search_checkpoint(path, checkpoint):
    if not path: return False
    try:
        data=dict(checkpoint or {})
        data["version"]=SEARCH_CHECKPOINT_VERSION
        data["updated_at"]=datetime.now().isoformat(timespec="seconds")
        # Bound file growth while preserving the strongest known records.
        data["results"]=list(data.get("results") or [])[:3000]
        data["tile_stats"]=list(data.get("tile_stats") or [])[-240:]
        data["completed_jobs"]=list(dict.fromkeys(map(str,data.get("completed_jobs") or [])))[-500:]
        data["completed_details"]=list(dict.fromkeys(map(str,data.get("completed_details") or [])))[-3000:]
        return _atomic_json_write(path,data)
    except Exception as exc:
        _diag("search_checkpoint_save_failed",exc,path=str(path))
        return False

def _checkpoint_snapshot(checkpoint, all_found, tile_stats, completed_jobs, completed_details, phase="running", status="running", results_override=None):
    cp=dict(checkpoint or {})
    cp["phase"]=str(phase or "running"); cp["status"]=str(status or "running")
    cp["completed_jobs"]=sorted(set(map(str,completed_jobs or [])))
    cp["completed_details"]=sorted(set(map(str,completed_details or [])))
    cp["tile_stats"]=list(tile_stats or [])
    rows=results_override if results_override is not None else list((all_found or {}).values())
    cp["results"]=list(rows or [])
    cp["result_count"]=len(cp["results"]); cp["completed_job_count"]=len(cp["completed_jobs"]); cp["completed_detail_count"]=len(cp["completed_details"])
    return cp

def _load_selector_health(path=None):
    p=Path(path or SELECTOR_HEALTH_FILE)
    try:
        if p.exists():
            data=json.loads(p.read_text(encoding="utf-8"))
            if isinstance(data,dict):
                data.setdefault("strategies",{})
                return data
    except Exception as exc:
        _diag("selector_health_load_failed",exc,path=str(p))
    return {"version":1,"updated_at":None,"strategies":{}}

def _save_selector_health(data,path=None):
    p=Path(path or SELECTOR_HEALTH_FILE)
    try:
        p.parent.mkdir(parents=True,exist_ok=True)
        payload=dict(data or {})
        payload["version"]=1
        payload["updated_at"]=datetime.now().isoformat(timespec="seconds")
        p.write_text(json.dumps(payload,ensure_ascii=False,indent=2,default=str),encoding="utf-8")
        return True
    except Exception as exc:
        _diag("selector_health_save_failed",exc,path=str(p))
        return False

def _selector_health_score(row):
    if not isinstance(row,dict) or int(row.get("attempts") or 0)<=0:
        return 50.0
    attempts=max(1,int(row.get("attempts") or 0))
    successes=max(0,int(row.get("successes") or 0))
    records=max(0,int(row.get("records") or 0))
    consecutive=max(0,int(row.get("consecutive_failures") or 0))
    success_rate=min(1.0,successes/attempts)
    avg_records=records/attempts
    return round(success_rate*70.0 + min(avg_records,20.0)*1.5 - min(consecutive,5)*10.0,3)

def _selector_strategy_order(strategies,path=None):
    declared=list(strategies or [])
    health=_load_selector_health(path)
    stats=health.get("strategies") or {}
    indexed=list(enumerate(declared))
    def _key(item):
        idx,name=item
        row=stats.get(name)
        score=_selector_health_score(row) if row else 50.0
        return (-score,idx)
    return [name for _,name in sorted(indexed,key=_key)]

def _record_selector_strategy(name,count=0,error="",elapsed_ms=0,path=None):
    data=_load_selector_health(path)
    stats=data.setdefault("strategies",{})
    row=stats.setdefault(str(name),{})
    row["attempts"]=int(row.get("attempts") or 0)+1
    got=max(0,int(count or 0))
    row["records"]=int(row.get("records") or 0)+got
    row["total_elapsed_ms"]=round(float(row.get("total_elapsed_ms") or 0.0)+float(elapsed_ms or 0.0),3)
    row["last_elapsed_ms"]=round(float(elapsed_ms or 0.0),3)
    row["last_count"]=got
    row["last_attempt_at"]=datetime.now().isoformat(timespec="seconds")
    if got>0:
        row["successes"]=int(row.get("successes") or 0)+1
        row["consecutive_failures"]=0
        row["last_success_at"]=row["last_attempt_at"]
    else:
        row["failures"]=int(row.get("failures") or 0)+1
        row["consecutive_failures"]=int(row.get("consecutive_failures") or 0)+1
    if error:
        row["last_error"]=str(error)[:300]
        if got>0:
            row["failures"]=int(row.get("failures") or 0)+1
    else:
        row["last_error"]=""
    row["score"]=_selector_health_score(row)
    _save_selector_health(data,path)
    return dict(row)

def _new_extraction_telemetry(query=""):
    return {"query":clean(query) if "clean" in globals() else str(query or "").strip(),"started_at":time.perf_counter(),"strategies":{},"strategy_order":[]}

def _telemetry_strategy_event(telemetry,name,count=0,error="",elapsed_ms=0):
    if not isinstance(telemetry,dict):
        return
    row=telemetry.setdefault("strategies",{}).setdefault(str(name),{"attempts":0,"count":0,"errors":0,"elapsed_ms":0.0})
    row["attempts"]+=1
    row["count"]+=max(0,int(count or 0))
    if error:
        row["errors"]+=1
        row["last_error"]=str(error)[:220]
    row["elapsed_ms"]=round(float(row.get("elapsed_ms") or 0.0)+float(elapsed_ms or 0.0),3)

def _telemetry_summary(telemetry):
    if not isinstance(telemetry,dict):
        return {}
    rows=telemetry.get("strategies") or {}
    return {"query":telemetry.get("query") or "","strategy_order":list(telemetry.get("strategy_order") or []),
            "strategies":rows,"total_records_seen":sum(int(v.get("count") or 0) for v in rows.values()),
            "total_elapsed_ms":round(sum(float(v.get("elapsed_ms") or 0.0) for v in rows.values()),3)}

def _aggregate_extraction_telemetry(metas):
    out={"jobs":0,"strategies":{},"total_records_seen":0,"total_elapsed_ms":0.0,"best_strategy":"","strategy_order":[]}
    for meta in metas or []:
        tele=(meta or {}).get("extraction_telemetry") or {}
        if not tele: continue
        out["jobs"]+=1
        if not out["strategy_order"] and tele.get("strategy_order"):
            out["strategy_order"]=list(tele.get("strategy_order") or [])
        out["total_records_seen"]+=int(tele.get("total_records_seen") or 0)
        out["total_elapsed_ms"]+=float(tele.get("total_elapsed_ms") or 0.0)
        for name,row in (tele.get("strategies") or {}).items():
            dst=out["strategies"].setdefault(name,{"attempts":0,"count":0,"errors":0,"elapsed_ms":0.0})
            dst["attempts"]+=int(row.get("attempts") or 0)
            dst["count"]+=int(row.get("count") or 0)
            dst["errors"]+=int(row.get("errors") or 0)
            dst["elapsed_ms"]+=float(row.get("elapsed_ms") or 0.0)
            if row.get("last_error"): dst["last_error"]=row.get("last_error")
    out["total_elapsed_ms"]=round(out["total_elapsed_ms"],3)
    for row in out["strategies"].values(): row["elapsed_ms"]=round(row["elapsed_ms"],3)
    if out["strategies"]:
        out["best_strategy"]=max(out["strategies"],key=lambda n:(out["strategies"][n].get("count",0),-out["strategies"][n].get("errors",0)))
    return out

def _selector_health_snapshot(path=None):
    data=_load_selector_health(path)
    rows=[]
    for name,row in (data.get("strategies") or {}).items():
        item={"strategy":name,**dict(row)}
        item["score"]=_selector_health_score(row)
        attempts=max(1,int(row.get("attempts") or 0))
        item["success_rate_pct"]=round(100.0*int(row.get("successes") or 0)/attempts,1)
        item["avg_records"]=round(float(row.get("records") or 0)/attempts,2)
        rows.append(item)
    rows.sort(key=lambda r:(-float(r.get("score") or 0),str(r.get("strategy") or "")))
    return rows

# Hot-path regular expressions are compiled once.
RE_WS = re.compile(r"\s+")
RE_ZERO_WIDTH = re.compile(r"[\u200B-\u200D\u2060\uFEFF]")
RE_RATING = re.compile(r"([1-5](?:[.,]\d)?)")
RE_LATLNG_AT = re.compile(r"/@(-?\d+\.\d+),(-?\d+\.\d+)")
RE_LATLNG_BANG = re.compile(r"!3d(-?\d+\.\d+)!4d(-?\d+\.\d+)")
RE_CID = re.compile(r"[?&]cid=(\d+)", re.I)
RE_PLACE_ID = re.compile(r"place_id[=:]([A-Za-z0-9_\-]+)", re.I)
RE_FTID = re.compile(r"!1s([^!/?&]+)")
RE_EMAIL = re.compile(r"(?i)(?<![\w.+-])([A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,})(?![\w.-])")
RE_SAUDI_MOBILE = re.compile(r"(?<!\d)(?:\+?966[\s\-()]*)?0?5[0-9](?:[\s\-()]?\d){7}(?!\d)")
RE_HREF = re.compile(r"(?is)href=[\"']([^\"'#]+)")
RE_MAPS_PLACE_URL = re.compile(r"https?://(?:www\.)?google\.[^\s\"'<>]+/maps/place/[^\s\"'<>]+", re.I)
RE_MAPS_REL_PLACE = re.compile(r"/maps/place/[^\s\"'<>]+", re.I)
RE_MAPS_CID_URL = re.compile(r"https?://(?:(?:www\.)?google\.[^/]+/maps\?[^\s\"'<>]*cid=\d+|maps\.google\.[^/]+/\?[^\s\"'<>]*cid=\d+)", re.I)
RE_MAPS_REL_CID = re.compile(r"/maps\?[^\s\"'<>]*cid=\d+", re.I)

ASYNC_DISCOVERY_CONCURRENCY = max(1, min(int(os.environ.get("RT_DISCOVERY_CONCURRENCY", "3") or 3), 6))
ASYNC_DETAIL_CONCURRENCY = max(1, min(int(os.environ.get("RT_DETAIL_CONCURRENCY", "4") or 4), 8))
ASYNC_CONTACT_CONCURRENCY = max(1, min(int(os.environ.get("RT_CONTACT_CONCURRENCY", "8") or 8), 12))
ASYNC_DISCOVERY_JOB_CAP = max(6, min(int(os.environ.get("RT_DISCOVERY_JOB_CAP", "36") or 36), 72))

def _diag(event, error=None, level="warning", **payload):
    try:
        row={"ts":datetime.now().isoformat(timespec="seconds"),"level":level,"event":event}
        if error is not None:
            row["error"]="{}: {}".format(type(error).__name__, str(error)[:500]) if isinstance(error,BaseException) else str(error)[:500]
        row.update({k:v for k,v in payload.items() if v is not None})
        with DIAG_LOG.open("a",encoding="utf-8") as f:
            f.write(json.dumps(row,ensure_ascii=False,default=str)+"\n")
    except Exception:
        pass

def _sanitize_contact_text(value):
    text=str(value or "")
    text=RE_ZERO_WIDTH.sub("", text)
    text="".join(ch for ch in text if unicodedata.category(ch) != "Cf")
    return RE_WS.sub(" ", text).strip()


def _ascii_digits(value):
    table=str.maketrans("٠١٢٣٤٥٦٧٨٩۰۱۲۳۴۵۶۷۸۹", "01234567890123456789")
    return str(value or "").translate(table).replace("٫", ".").replace("٬", ",").replace("،", ",")


def _normalize_saudi_phone(value):
    text=_ascii_digits(_sanitize_contact_text(value))
    digits=re.sub(r"\D", "", text)
    if digits.startswith("00966"):
        digits=digits[2:]
    if digits.startswith("966") and len(digits)>=11:
        digits="0"+digits[3:]
    elif len(digits)==9 and digits.startswith(("5","1")):
        digits="0"+digits
    return digits or None


def _classify_phone_number(value):
    n=_normalize_saudi_phone(value) or ""
    if re.fullmatch(r"05\d{8}",n): return "mobile"
    if re.fullmatch(r"01[1-7]\d{7}",n): return "phone"
    return "unknown"


def _split_phone_fields(value):
    n=_normalize_saudi_phone(value)
    if not n: return {"phone":None,"mobile":None}
    kind=_classify_phone_number(n)
    if kind=="mobile": return {"phone":None,"mobile":n}
    return {"phone":n,"mobile":None}


def _valid_contact_email(value):
    e=str(value or "").strip().lower().strip("<>[](){}.,;:\'\"")
    if not re.fullmatch(r"[a-z0-9.!#$%&'*+/=?^_`{|}~-]+@[a-z0-9.-]+\.[a-z]{2,}",e,re.I): return None
    local,domain=e.rsplit("@",1)
    blocked=("example.com","example.org","sentry.io","wixpress.com","cloudflare.com","schema.org","googleapis.com","gstatic.com","facebook.com")
    if domain in blocked or any(domain.endswith("."+d) for d in blocked): return None
    if local in {"noreply","no-reply","donotreply","do-not-reply","mailer-daemon"}: return None
    return e



def _normalize_official_website(value):
    raw=str(value or "").strip()
    if not raw: return None
    try:
        u=urlparse(raw)
        host=(u.netloc or "").lower().replace("www.","")
        if host.endswith("google.com") and u.path.startswith("/url"):
            qs=urllib.parse.parse_qs(u.query); target=(qs.get("q") or qs.get("url") or [None])[0]
            return _normalize_official_website(target) if target else None
        if "google." in host and ("/maps" in u.path or host.startswith("maps.")): return None
        if u.scheme in ("http","https") and u.netloc:
            return raw.split("#",1)[0]
    except Exception:
        return None
    return None

def _extract_contact_profile_from_html(html_text, final_url, fallback_phone=None):
    raw=str(html_text or ""); ascii_raw=_ascii_digits(raw)
    profile={"phone":None,"mobile":None,"email":None,"website":None,"whatsapp":None,
             "emails":[],"phones":[],"field_sources":{},"contact_source":"official_website","contact_confidence":0}
    host=""
    try:
        u=urlparse(str(final_url or "").strip()); host=(u.netloc or "").lower().replace("www.","")
        if u.scheme in ("http","https") and host and "google." not in host and "goo.gl" not in host:
            profile["website"]="{}://{}".format(u.scheme,u.netloc)
            profile["field_sources"]["website"]="official_website"
    except Exception:
        pass
    numbers=[]
    if fallback_phone: numbers.append((fallback_phone,"google_maps"))
    hrefs=re.findall(r"(?is)href=[\"']([^\"']+)",raw)
    email_candidates=[]; whatsapp_candidates=[]
    for href in hrefs:
        h=str(href or "").strip(); hl=h.lower()
        if hl.startswith("mailto:"):
            email_candidates.append((unquote(h.split(":",1)[1].split("?",1)[0]),"mailto"))
        elif hl.startswith("tel:"):
            numbers.append((unquote(h.split(":",1)[1].split("?",1)[0]),"tel"))
        if "wa.me/" in hl or "whatsapp.com/send" in hl or "api.whatsapp.com/send" in hl:
            m=re.search(r"(?:wa\.me/|[?&]phone=)(\+?\d{8,15})",_ascii_digits(h),re.I)
            if m: whatsapp_candidates.append((m.group(1),"whatsapp_link"))
    for m in re.finditer(r"(?i)\"(?:telephone|phone|mobile)\"\s*:\s*\"([^\"]+)\"",ascii_raw):
        numbers.append((m.group(1),"json_ld"))
    generic_phone_re=r"(?<!\d)(?:(?:\+?966|00966)[\s\-()]*)?(?:0?(?:5\d|1[1-7]))(?:[\s\-()]?\d){7,8}(?!\d)"
    for m in re.finditer(generic_phone_re,ascii_raw): numbers.append((m.group(0),"page_text"))
    for e in re.findall(r"(?i)(?<![\w.+-])([A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,})(?![\w.-])",raw):
        email_candidates.append((e,"page_text"))
    seen=set()
    for value,src in numbers:
        n=_normalize_saudi_phone(value)
        if not n or n in seen: continue
        seen.add(n); profile["phones"].append(n); kind=_classify_phone_number(n)
        if kind=="mobile" and not profile["mobile"]:
            profile["mobile"]=n; profile["field_sources"]["mobile"]=src
        elif kind=="phone" and not profile["phone"]:
            profile["phone"]=n; profile["field_sources"]["phone"]=src
    seen_e=set()
    for value,src in email_candidates:
        e=_valid_contact_email(value)
        if not e or e in seen_e: continue
        seen_e.add(e); profile["emails"].append(e)
        if not profile["email"]:
            profile["email"]=e; profile["field_sources"]["email"]=src
    for value,src in whatsapp_candidates:
        n=_normalize_saudi_phone(value)
        if n and _classify_phone_number(n)=="mobile":
            profile["whatsapp"]=n; profile["field_sources"]["whatsapp"]=src; break
    score=0
    if profile.get("website"): score+=25
    if profile.get("phone") or profile.get("mobile"): score+=25
    if profile.get("email"): score+=30
    if profile.get("whatsapp"): score+=10
    if host and profile.get("email") and profile["email"].split("@")[-1].endswith(host): score+=10
    profile["contact_confidence"]=min(100,score)
    return profile


def _extract_saudi_mobile(value):
    txt=_sanitize_contact_text(value)
    m=RE_SAUDI_MOBILE.search(txt)
    if not m:
        return None
    raw=re.sub(r"[^\d+]", "", m.group(0))
    return raw or None


def _classify_navigation_error(error):
    text=clean(error).lower()
    if "err_blocked_by_administrator" in text or "blocked by administrator" in text:
        return "environment_blocked"
    if any(x in text for x in ("err_name_not_resolved","dns_probe_finished","temporary failure in name resolution","name resolution")):
        return "network_error"
    if any(x in text for x in ("err_internet_disconnected","err_network_changed","connection refused","connection timed out")):
        return "network_error"
    return ""


def _classify_browser_page(url, title="", body=""):
    """Classify browser state without treating network errors as empty Google results."""
    u=clean(url).lower(); t=clean(title).lower(); b=clean(body).lower(); text=t+" "+b
    if u.startswith("chrome-error://") or any(x in text for x in ("err_name_not_resolved","dns_probe_finished","this site can’t be reached","this site can't be reached","تعذر الوصول إلى هذا الموقع")):
        return "network_error"
    if any(x in text for x in ("captcha","recaptcha","verify you are human","تحقق من أنك لست برنامج")):
        return "captcha"
    if any(x in text for x in ("unusual traffic","automated queries","زيارات غير معتادة","طلبات آلية")):
        return "unusual_traffic"
    if any(x in text for x in ("too many requests","rate limit","http 429")):
        return "rate_limit"
    return "ok"


def _system_browser_candidates(which_func=shutil.which):
    """Return installed Chromium-family executables Playwright can launch directly."""
    out=[]
    for label,cmd in (("chrome-exe","google-chrome"),("chrome-exe","google-chrome-stable"),
                      ("edge-exe","microsoft-edge"),("edge-exe","msedge"),
                      ("chromium-exe","chromium"),("chromium-exe","chromium-browser")):
        try:
            path=which_func(cmd)
        except Exception:
            path=None
        if path and (label,path) not in out:
            out.append((label,path))
    if os.name == "nt":
        roots=[os.environ.get("PROGRAMFILES"),os.environ.get("PROGRAMFILES(X86)"),os.environ.get("LOCALAPPDATA")]
        rels=[("chrome-exe",r"Google\Chrome\Application\chrome.exe"),
              ("edge-exe",r"Microsoft\Edge\Application\msedge.exe")]
        for root in roots:
            if not root: continue
            for label,rel in rels:
                cand=str(Path(root)/rel)
                if Path(cand).exists() and (label,cand) not in out:
                    out.append((label,cand))
    return out

def _load_browser_health(path=None):
    p=Path(path or BROWSER_HEALTH_FILE)
    try:
        if p.exists():
            data=json.loads(p.read_text(encoding="utf-8"))
            return data if isinstance(data,dict) else {}
    except Exception as exc:
        _diag("browser_health_load_failed",exc,path=str(p))
    return {}


def _save_browser_health(data,path=None):
    p=Path(path or BROWSER_HEALTH_FILE)
    try:
        p.write_text(json.dumps(data,ensure_ascii=False,indent=2,default=str),encoding="utf-8")
        return True
    except Exception as exc:
        _diag("browser_health_save_failed",exc,path=str(p))
        return False


def _order_browser_candidates(candidates,health=None):
    """Prefer the last browser that reached Google successfully, preserving all fallbacks."""
    rows=list(candidates or [])
    health=health or {}
    pe=clean(health.get("preferred_engine")); px=clean(health.get("preferred_executable"))
    def score(item):
        name,exe=item
        if pe and name==pe and px and clean(exe)==px: return 0
        if pe and name==pe: return 1
        return 2
    return sorted(rows,key=score)


def _record_browser_health(engine,status,reason="",executable="",path=None):
    data=_load_browser_health(path)
    now=datetime.now().isoformat(timespec="seconds")
    status=clean(status).lower() or "unknown"
    data["last_engine"]=clean(engine); data["last_status"]=status; data["last_checked_at"]=now
    if executable: data["last_executable"]=clean(executable)
    if status in {"ok","success","healthy"}:
        data["preferred_engine"]=clean(engine)
        if executable: data["preferred_executable"]=clean(executable)
        data["success_count"]=int(data.get("success_count") or 0)+1
        data["last_success_at"]=now; data["last_failure_reason"]=""
    elif status in {"failed","error","unhealthy"}:
        data["failure_count"]=int(data.get("failure_count") or 0)+1
        data["last_failure_at"]=now; data["last_failure_reason"]=clean(reason)[:300]
    _save_browser_health(data,path)
    return data


def _should_browser_failover(result_count,termination_reason):
    return bool(_search_retry_policy(termination_reason,attempt=1,result_count=result_count).get("try_next_browser"))

def _calculate_search_reliability(meta):
    """Return an explainable 0-100 reliability score for one search/preflight run."""
    meta=dict(meta or {})
    reason=clean(meta.get("termination_reason") or meta.get("no_result_reason")).lower()
    results=max(0,int(meta.get("total_unique") or meta.get("results") or 0))
    browser=clean(meta.get("browser_engine"))
    providers=list(meta.get("providers") or [])
    errors=list(meta.get("errors") or [])
    score=20.0; reasons=[]
    if browser and browser not in {"none","—","unknown"}:
        score+=15; reasons.append("browser-launched")
    if results>0:
        score+=35; reasons.append("real-results")
        if results>=5: score+=5
    if len(set(map(str,providers)))>=2:
        score+=10; reasons.append("multi-provider")
    elif providers:
        score+=4
    if clean(meta.get("best_selector_strategy")):
        score+=6; reasons.append("selector-healthy")
    comp=float(meta.get("avg_data_completeness") or 0)
    score+=min(7.0,max(0.0,comp)/100.0*7.0)
    if meta.get("browser_failover_used") and results>0:
        score+=2; reasons.append("failover-recovered")
    if errors:
        score-=min(12.0,len(errors)*2.0)
    hard_block=reason in {"environment_blocked","network_error"}
    external_block=reason in {"google_blocked","captcha","unusual_traffic","rate_limit"}
    if hard_block:
        score=min(score,20.0); reasons.append(reason)
    elif external_block:
        score=min(score,30.0); reasons.append(reason)
    elif reason in {"browser_or_layout_no_results","layout_no_results","empty_results"} and results<=0:
        score=min(score,45.0); reasons.append("layout-zero")
    score=round(max(0.0,min(100.0,score)),1)
    grade="A" if score>=90 else "B" if score>=75 else "C" if score>=60 else "D" if score>=40 else "F"
    return {"score":score,"grade":grade,"healthy":bool(score>=70 and not hard_block and not external_block),
            "hard_block":bool(hard_block),"external_block":bool(external_block),"reason":reason or "ok","signals":reasons}


def _search_retry_policy(reason,attempt=1,result_count=0):
    """Deterministic retry/failover policy; never retries systemic network or Google blocks."""
    reason=clean(reason).lower() or "completed"; attempt=max(1,int(attempt or 1)); results=max(0,int(result_count or 0))
    if results>0:
        return {"action":"accept","retry_same_browser":False,"try_next_browser":False,"backoff_seconds":0.0}
    if reason in {"network_error","environment_blocked","google_blocked","captcha","unusual_traffic","rate_limit"}:
        return {"action":"stop","retry_same_browser":False,"try_next_browser":False,"backoff_seconds":0.0}
    if reason in {"browser_or_layout_no_results","layout_no_results","empty_results","completed"}:
        return {"action":"next_browser","retry_same_browser":False,"try_next_browser":True,"backoff_seconds":0.0}
    if reason in {"navigation_error","timeout","deadline_reached","probe_timeout"} and attempt<=1:
        return {"action":"retry_same","retry_same_browser":True,"try_next_browser":False,"backoff_seconds":0.8}
    if reason in {"navigation_error","timeout","deadline_reached","probe_timeout"}:
        return {"action":"next_browser","retry_same_browser":False,"try_next_browser":True,"backoff_seconds":0.0}
    return {"action":"stop","retry_same_browser":False,"try_next_browser":False,"backoff_seconds":0.0}


def _build_preflight_report_from_checks(checks):
    checks=dict(checks or {})
    state=clean(checks.get("maps_page_state") or "ok").lower()
    hard_block=state in {"environment_blocked","network_error"}
    external_block=state in {"captcha","unusual_traffic","rate_limit","google_blocked"}
    browser_ok=bool(checks.get("browser_launched")) or bool(checks.get("browser_candidates")) or bool(checks.get("playwright"))
    dns_ok=bool(checks.get("dns_google")) and bool(checks.get("dns_maps"))
    ready=bool(dns_ok and browser_ok and not hard_block and not external_block)
    pseudo_meta={
        "results":int(checks.get("maps_results") or 0),"browser_engine":checks.get("browser_engine") or (checks.get("browser_candidates") or [""])[0],
        "termination_reason":state if state!="ok" else "completed","providers":["google_maps_browser"] if checks.get("google_http") or checks.get("maps_loaded") else [],
        "best_selector_strategy":checks.get("best_selector_strategy") or "","errors":checks.get("errors") or [],
        "avg_data_completeness":100 if int(checks.get("maps_results") or 0)>0 else 0,
    }
    reliability=_calculate_search_reliability(pseudo_meta)
    if ready and reliability["score"]<55:
        reliability["score"]=55.0; reliability["grade"]="D"
    return {"ready":ready,"hard_block":hard_block,"external_block":external_block,"checks":checks,
            "reliability":reliability,"reason":state if state!="ok" else ("ready" if ready else "preflight_incomplete")}


_SECRET_PATTERNS=(
    re.compile(r"AIza[0-9A-Za-z_\-]{10,}"),
    re.compile(r"(?i)(bearer\s+)[A-Za-z0-9._\-]{10,}"),
    re.compile(r"(?i)(token|password|secret|api[_-]?key)(\s*[:=]\s*)[^\s,}\]]+"),
)

def _redact_diagnostic_text(value):
    text=str(value or "")
    for pat in _SECRET_PATTERNS:
        if pat.pattern.lower().startswith('(?i)(bearer'):
            text=pat.sub(lambda m:m.group(1)+"[REDACTED]",text)
        elif 'token|password|secret' in pat.pattern:
            text=pat.sub(lambda m:m.group(1)+m.group(2)+"[REDACTED]",text)
        else:
            text=pat.sub("[REDACTED]",text)
    return text

def _redact_diagnostic_obj(obj,key=""):
    sensitive=any(x in str(key).lower() for x in ("token","password","secret","api_key","apikey","webhook"))
    if sensitive:
        return "[REDACTED]"
    if isinstance(obj,dict): return {k:_redact_diagnostic_obj(v,k) for k,v in obj.items()}
    if isinstance(obj,list): return [_redact_diagnostic_obj(v,key) for v in obj]
    if isinstance(obj,tuple): return tuple(_redact_diagnostic_obj(v,key) for v in obj)
    if isinstance(obj,str): return _redact_diagnostic_text(obj)
    return obj


def _create_field_diagnostic_bundle(output_path,preflight=None,last_meta=None,extra_text=""):
    """Create a support ZIP containing diagnostics only; secrets and DB contents are excluded."""
    out=Path(output_path); out.parent.mkdir(parents=True,exist_ok=True)
    system_info={
        "created_at":datetime.now().isoformat(timespec="seconds"),"platform":platform.platform(),"python":sys.version,
        "os_name":os.name,"browser_candidates":[{"engine":n,"path":p} for n,p in _system_browser_candidates()],
        "proxy_configured":{k:bool(os.environ.get(k)) for k in ("HTTP_PROXY","HTTPS_PROXY","ALL_PROXY","NO_PROXY")},
        "playwright_async_available":bool(async_playwright is not None),
    }
    entries={
        "preflight.json":json.dumps(_redact_diagnostic_obj(preflight or {}),ensure_ascii=False,indent=2,default=str),
        "last_search_meta.json":json.dumps(_redact_diagnostic_obj(last_meta or {}),ensure_ascii=False,indent=2,default=str),
        "system.json":json.dumps(_redact_diagnostic_obj(system_info),ensure_ascii=False,indent=2,default=str),
        "notes.txt":_redact_diagnostic_text(extra_text),
    }
    for name,path in (("browser_health.json",BROWSER_HEALTH_FILE),("selector_health.json",SELECTOR_HEALTH_FILE)):
        try:
            if Path(path).exists(): entries[name]=_redact_diagnostic_text(Path(path).read_text(encoding="utf-8",errors="ignore"))
        except Exception as exc:
            entries[name+".error.txt"]=_redact_diagnostic_text(exc)
    try:
        if DIAG_LOG.exists():
            lines=DIAG_LOG.read_text(encoding="utf-8",errors="ignore").splitlines()[-1200:]
            entries["worker_diagnostics_tail.jsonl"]=_redact_diagnostic_text("\n".join(lines))
    except Exception as exc:
        entries["worker_diagnostics_tail.error.txt"]=_redact_diagnostic_text(exc)
    with zipfile.ZipFile(out,"w",zipfile.ZIP_DEFLATED) as z:
        for name,text in entries.items(): z.writestr(name,str(text))
    return str(out)

def _progress(event, **payload):
    try:
        data = {"event": event, "ts": datetime.now().isoformat(timespec="seconds")}
        data.update(payload)
        sys.stderr.write("RT_PROGRESS|" + json.dumps(data, ensure_ascii=False) + "\n")
        sys.stderr.flush()
    except Exception:
        pass

def _load_geocode_cache():
    try:
        if GEOCODE_CACHE.exists():
            data = json.loads(GEOCODE_CACHE.read_text(encoding="utf-8"))
            return data if isinstance(data, dict) else {}
    except Exception:
        pass
    return {}

def _save_geocode_cache(data):
    try:
        GEOCODE_CACHE.write_text(json.dumps(data, ensure_ascii=False), encoding="utf-8")
    except Exception:
        pass

def _goto_retry(page, url, timeout=45000, attempts=3):
    last = None
    for i in range(max(1, attempts)):
        try:
            page.goto(url, wait_until="domcontentloaded", timeout=timeout)
            return True
        except Exception as e:
            last = e
            if i + 1 < attempts:
                page.wait_for_timeout(int(500 * (2 ** i)))
    if last:
        raise last
    return False


def _launch_browser(p):
    """Launch the most Google-compatible browser available on Windows/desktop.

    System Chrome/Edge are tried first because they tend to match Google Maps' current
    rendering path better than a stale bundled engine. Bundled Chromium and Firefox
    remain fallbacks. No anti-CAPTCHA bypass is attempted.
    """
    errors=[]
    chromium_args=["--disable-dev-shm-usage","--no-sandbox",
                   "--disable-background-networking","--disable-default-apps","--no-first-run"]
    for name,exe in _system_browser_candidates():
        try:
            browser=p.chromium.launch(executable_path=exe,headless=True,args=chromium_args)
            _progress("browser",engine=name,status="ok",executable=exe)
            return browser,name
        except Exception as e:
            errors.append("{}: {}".format(name,str(e)[:180])); _diag("system_browser_launch_failed",e,engine=name,executable=exe)
    attempts=[
        ("chrome", lambda: p.chromium.launch(channel="chrome", headless=True, args=chromium_args)),
        ("msedge", lambda: p.chromium.launch(channel="msedge", headless=True, args=chromium_args)),
        ("chromium", lambda: p.chromium.launch(headless=True, args=chromium_args)),
        ("firefox", lambda: p.firefox.launch(headless=True, args=["--no-remote"])),
    ]
    for name,starter in attempts:
        try:
            browser=starter()
            _progress("browser", engine=name, status="ok")
            return browser,name
        except Exception as e:
            errors.append("{}: {}".format(name,str(e)[:180]))
            _progress("browser", engine=name, status="failed", error=str(e)[:180])
    raise RuntimeError("No Playwright browser available | "+" | ".join(errors))

def _google_block_reason(page):
    try:
        title=clean(page.title()).lower()
    except Exception:
        title=""
    try:
        body=clean(page.locator("body").inner_text(timeout=1200)).lower()[:12000]
    except Exception:
        body=""
    text=title+" "+body
    checks=[
        ("captcha",["captcha","recaptcha","verify you are human","تحقق من أنك لست برنامج"]),
        ("unusual_traffic",["unusual traffic","automated queries","زيارات غير معتادة","طلبات آلية"]),
        ("rate_limit",["too many requests","429","rate limit"]),
    ]
    for reason,terms in checks:
        if any(t in text for t in terms): return reason
    return ""


def clean(t):
    text = _sanitize_contact_text(t)
    return RE_WS.sub(" ", text).strip()


def parse_rating(s):
    if not s: return None
    text=_ascii_digits(s)
    m = RE_RATING.search(text)
    if not m: return None
    try: return float(m.group(1).replace(",", "."))
    except Exception: return None


def first_text(p, sels, t=800):
    for s in sels:
        try:
            loc = p.locator(s)
            if loc.count() > 0:
                v = loc.first.inner_text(timeout=t)
                if v: return clean(v)
        except Exception: pass
    return ""


def first_attr(p, sels, name, t=800):
    for s in sels:
        try:
            loc = p.locator(s)
            if loc.count() > 0:
                v = loc.first.get_attribute(name, timeout=t)
                if v: return v
        except Exception: pass
    return ""


def extract_latlng(page):
    try:
        for pat in (RE_LATLNG_AT, RE_LATLNG_BANG):
            m = pat.search(page.url)
            if m: return float(m.group(1)), float(m.group(2))
    except Exception: pass
    return None, None


def _latlng_from_url(url):
    try:
        for pat in (RE_LATLNG_AT, RE_LATLNG_BANG):
            m = pat.search(url or "")
            if m: return float(m.group(1)), float(m.group(2))
    except Exception: pass
    return None, None


def haversine(lat1, lon1, lat2, lon2):
    try:
        r = 6371.0088
        p1, p2 = math.radians(lat1), math.radians(lat2)
        dp = math.radians(lat2 - lat1); dl = math.radians(lon2 - lon1)
        a = math.sin(dp/2)**2 + math.cos(p1)*math.cos(p2)*math.sin(dl/2)**2
        return 2*r*math.asin(math.sqrt(a))
    except Exception: return None


def zoom_for(radius):
    try: r = max(.5, float(radius or 10))
    except Exception: r = 10
    for th, z in [(1,16),(2,15),(5,14),(10,13),(20,12),(40,11),(80,10)]:
        if r <= th: return z
    return 9


def geocode(text, lang="ar"):
    text = clean(text)
    if not text: return None, None, None
    key = "{}|{}".format(lang, text.lower())
    cache = _load_geocode_cache()
    item = cache.get(key)
    if isinstance(item, dict):
        try:
            cached_at = datetime.fromisoformat(item.get("ts"))
            if datetime.now() - cached_at < timedelta(days=30):
                return float(item["lat"]), float(item["lon"]), item.get("display_name")
        except Exception:
            pass
    # Prefer Google Geocoding when the user configured a Google Maps key; fall back
    # to Nominatim so searching still works without extra credentials.
    google_key = clean(os.environ.get("GOOGLE_MAPS_API_KEY") or os.environ.get("GOOGLE_PLACES_API_KEY"))
    if google_key:
        try:
            gurl = "https://maps.googleapis.com/maps/api/geocode/json?" + urllib.parse.urlencode(
                {"address": text, "key": google_key, "language": lang})
            req = urllib.request.Request(gurl, headers={"User-Agent": "ReviewsTracker/37.0"})
            with urllib.request.urlopen(req, timeout=8) as r:
                gdata = json.loads(r.read(512000).decode("utf-8", "ignore"))
            if gdata.get("status") == "OK" and gdata.get("results"):
                first = gdata["results"][0]; loc = (first.get("geometry") or {}).get("location") or {}
                lat, lon = float(loc["lat"]), float(loc["lng"]); label = first.get("formatted_address")
                cache[key] = {"lat": lat, "lon": lon, "display_name": label, "source":"google_geocoding", "ts": datetime.now().isoformat()}
                if len(cache) > 500: cache = dict(list(cache.items())[-400:])
                _save_geocode_cache(cache)
                _progress("geocode_provider", provider="google_geocoding", text=text)
                return lat, lon, label
        except Exception:
            pass
    try:
        url = "https://nominatim.openstreetmap.org/search?" + urllib.parse.urlencode(
            {"q": text, "format": "jsonv2", "limit": 1, "accept-language": lang})
        req = urllib.request.Request(url, headers={"User-Agent": "ReviewsTracker/37.0"})
        with urllib.request.urlopen(req, timeout=8) as r:
            data = json.loads(r.read(512000).decode("utf-8", "ignore"))
        if data:
            lat, lon, label = float(data[0]["lat"]), float(data[0]["lon"]), data[0].get("display_name")
            cache[key] = {"lat": lat, "lon": lon, "display_name": label, "source":"nominatim", "ts": datetime.now().isoformat()}
            if len(cache) > 500:
                cache = dict(list(cache.items())[-400:])
            _save_geocode_cache(cache)
            _progress("geocode_provider", provider="nominatim", text=text)
            return lat, lon, label
    except Exception:
        pass
    return None, None, None




def _public_result_record(rec):
    """Small JSON-safe projection used for streaming live discoveries to the UI."""
    keys = ["name","rating","review_count","address","phone","mobile","email","website","category",
            "cid","place_id","latitude","longitude","url","maps_url","distance_km","match_score",
            "match_reason","relevance_guard","validation_score","validation_status","result_score","data_completeness","provider_count","providers",
            "confidence_score","source","source_fetched_at","detail_fetched_at","contact_checked_at","preview_reviews","preview_review_count",
            "cache_hit","cache_age_hours","freshness_score","freshness_status","whatsapp","contact_source","contact_confidence","field_sources"]
    out = {k: rec.get(k) for k in keys if rec.get(k) is not None}
    if out.get("url") and not out.get("maps_url"):
        out["maps_url"] = out.get("url")
    try: out["relevance"] = _relevance_band(rec.get("match_score"))
    except Exception: pass
    return out


def _emit_live_result(rec, provider="google_maps"):
    try:
        _progress("result", provider=provider, record=_public_result_record(rec))
    except Exception:
        pass


def google_places_api_search(query, lang="ar", city="", area="", activity="",
                             max_results=40, lat=None, lng=None, radius_km=None):
    """Use the official Google Places API (New) when an API key is configured.

    This path is intentionally optional: the worker always keeps the Google Maps
    browser engine as a fallback so the desktop app still works without a key.
    """
    key = clean(os.environ.get("GOOGLE_MAPS_API_KEY") or os.environ.get("GOOGLE_PLACES_API_KEY"))
    if not key:
        return [], {"available": False, "used": False, "provider": "google_places_api"}
    text_query = " ".join(x for x in [clean(activity), clean(query), clean(area), clean(city)] if x) or clean(query)
    if not text_query:
        return [], {"available": True, "used": False, "provider": "google_places_api", "error": "empty_query"}
    endpoint = "https://places.googleapis.com/v1/places:searchText"
    field_mask = ",".join([
        "places.id","places.displayName","places.formattedAddress","places.rating","places.userRatingCount",
        "places.location","places.googleMapsUri","places.websiteUri","places.nationalPhoneNumber",
        "places.internationalPhoneNumber","places.primaryTypeDisplayName","places.reviews","nextPageToken"
    ])
    wanted = max(1, min(int(max_results or 40), 60))
    records = []; page_token = None; calls = 0; seen_places = set()
    try:
        while len(records) < wanted and calls < 3:
            body = {
                "textQuery": text_query,
                "languageCode": "ar" if str(lang).lower().startswith("ar") else "en",
                "pageSize": min(20, wanted - len(records)),
            }
            if page_token:
                body["pageToken"] = page_token
            if lat is not None and lng is not None:
                try:
                    radius_m = min(50000.0, max(1000.0, float(radius_km or 25) * 1000.0))
                    body["locationBias"] = {"circle": {"center": {"latitude": float(lat), "longitude": float(lng)}, "radius": radius_m}}
                except Exception:
                    pass
            req = urllib.request.Request(
                endpoint, data=json.dumps(body, ensure_ascii=False).encode("utf-8"), method="POST",
                headers={
                    "Content-Type": "application/json; charset=utf-8",
                    "X-Goog-Api-Key": key,
                    "X-Goog-FieldMask": field_mask,
                    "User-Agent": "ReviewsTracker/37.0",
                })
            with urllib.request.urlopen(req, timeout=12) as resp:
                payload = json.loads(resp.read(2_000_000).decode("utf-8", "ignore"))
            calls += 1
            for place in payload.get("places") or []:
                loc = place.get("location") or {}
                display = place.get("displayName") or {}
                ptype = place.get("primaryTypeDisplayName") or {}
                preview_reviews = []
                for rv in (place.get("reviews") or [])[:5]:
                    txt_obj = rv.get("text") or rv.get("originalText") or {}
                    author_obj = rv.get("authorAttribution") or {}
                    review_text = clean(txt_obj.get("text") if isinstance(txt_obj, dict) else txt_obj)
                    if not review_text and isinstance(rv.get("originalText"), dict):
                        review_text = clean((rv.get("originalText") or {}).get("text"))
                    preview_reviews.append({
                        "review_id": clean(rv.get("name") or rv.get("googleMapsUri")),
                        "author": clean(author_obj.get("displayName")),
                        "rating": rv.get("rating"),
                        "text": review_text,
                        "date": rv.get("publishTime") or rv.get("relativePublishTimeDescription"),
                        "google_maps_uri": rv.get("googleMapsUri"),
                        "source": "google_places_api",
                    })
                phone_fields=_split_phone_fields(place.get("nationalPhoneNumber") or place.get("internationalPhoneNumber"))
                rec = {
                    "name": clean(display.get("text")),
                    "rating": place.get("rating"),
                    "review_count": place.get("userRatingCount"),
                    "address": place.get("formattedAddress"),
                    "phone": phone_fields.get("phone"),
                    "mobile": phone_fields.get("mobile"), "email": None,
                    "website": _normalize_official_website(place.get("websiteUri")),
                    "category": clean(ptype.get("text")),
                    "place_id": place.get("id"),
                    "cid": None,
                    "latitude": loc.get("latitude"), "longitude": loc.get("longitude"),
                    "url": place.get("googleMapsUri"),
                    "maps_url": place.get("googleMapsUri"),
                    "source": "google_places_api",
                    "source_fetched_at": datetime.now().isoformat(),
                    "details_loaded": True,
                    "preview_reviews": preview_reviews,
                    "preview_review_count": len(preview_reviews),
                    "review_sample_only": True,
                    "review_sample_limit": 5,
                    "review_source_access": "places_preview",
                }
                if rec.get("name"):
                    identity = clean(rec.get("place_id") or rec.get("maps_url") or
                                     rec.get("name") + "|" + str(rec.get("address"))).casefold()
                    if identity in seen_places:
                        continue
                    seen_places.add(identity)
                    records.append(rec)
                    _emit_live_result(rec, "google_places_api")
                    if len(records) >= wanted: break
            page_token = payload.get("nextPageToken")
            if not page_token: break
            time.sleep(.25)
        return records[:wanted], {"available": True, "used": True, "provider": "google_places_api", "calls": calls, "count": len(records)}
    except Exception as e:
        return records[:wanted], {"available": True, "used": bool(records), "provider": "google_places_api",
                                  "calls": calls, "count": len(records), "error": "{}: {}".format(type(e).__name__, str(e)[:240])}


def extract_contact_profile(website, fallback_phone=None, timeout=2.8, subpage_timeout=1.8, max_subpages=1):
    """Bounded official-site contact enrichment; cache-first and safe for parallel execution."""
    base=_extract_contact_profile_from_html("",website or "",fallback_phone)
    if fallback_phone: base["contact_source"]="google_maps"
    if not website:
        return base
    cached=_contact_cache_lookup(website)
    if cached:
        for k in ("phone","mobile","email","whatsapp","contact_source","contact_confidence"):
            if cached.get(k) not in (None,"") and base.get(k) in (None,""): base[k]=cached.get(k)
        fs=dict(base.get("field_sources") or {}); fs.update(cached.get("field_sources") or {}); base["field_sources"]=fs
        base["contact_cache_hit"]=True; base["contact_checked_at"]=cached.get("checked_at")
        return base
    def fetch(url,tm):
        req=urllib.request.Request(url,headers={"User-Agent":"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36","Accept-Language":"ar,en;q=0.8"})
        with urllib.request.urlopen(req,timeout=tm) as r:
            return r.geturl(),r.read(500000).decode("utf-8","ignore")
    def merge(dst,src):
        for k in ("phone","mobile","email","website","whatsapp"):
            if not dst.get(k) and src.get(k): dst[k]=src[k]
        dst["emails"]=list(dict.fromkeys(list(dst.get("emails") or [])+list(src.get("emails") or [])))
        dst["phones"]=list(dict.fromkeys(list(dst.get("phones") or [])+list(src.get("phones") or [])))
        fs=dict(dst.get("field_sources") or {}); fs.update(src.get("field_sources") or {}); dst["field_sources"]=fs
        dst["contact_confidence"]=max(int(dst.get("contact_confidence") or 0),int(src.get("contact_confidence") or 0))
        return dst
    try:
        final_url,txt=fetch(website,timeout); merge(base,_extract_contact_profile_from_html(txt,final_url,fallback_phone))
        if not (base.get("email") and base.get("mobile")) and int(max_subpages or 0)>0:
            root=(urlparse(final_url).netloc or "").lower().replace("www.",""); candidates=[]
            for href in re.findall(r"(?is)href=[\"']([^\"']+)",txt):
                full=urljoin(final_url,str(href).strip()); u=urlparse(full); host=(u.netloc or "").lower().replace("www.","")
                hay=(str(href)+" "+full).lower()
                if u.scheme not in ("http","https") or host!=root: continue
                if not any(k in hay for k in ("contact","contact-us","about","support","اتصل","تواصل","about-us")): continue
                if full not in candidates: candidates.append(full)
                if len(candidates)>=int(max_subpages): break
            for u in candidates:
                try:
                    u2,t2=fetch(u,subpage_timeout); merge(base,_extract_contact_profile_from_html(t2,u2,fallback_phone))
                except Exception as exc: _diag("contact_subpage_fetch_failed",exc,url=u)
    except Exception as exc:
        _diag("contact_fetch_failed",exc,website=website)
    base["contact_checked_at"]=datetime.now().isoformat(timespec="seconds")
    try: _contact_cache_store(website,base.get("mobile"),base.get("email"),base.get("contact_source") or "official_website",profile=base)
    except Exception as exc: _diag("contact_cache_store_failed",exc,website=website)
    return base


def extract_contacts(website, fallback_phone=None):
    """Compatibility wrapper returning (mobile, email, source)."""
    def scan(txt, src=None):
        # Retained local compatibility helper from v35; the new parser returns a richer profile.
        return _extract_contact_profile_from_html(txt,website or "",fallback_phone)
    profile=extract_contact_profile(website,fallback_phone)
    fs=profile.get("field_sources") or {}
    source=fs.get("email") or fs.get("mobile") or profile.get("contact_source") or "website"
    return profile.get("mobile"),profile.get("email"),source


def extract_info(page, with_contacts=True):
    """Extract a branch record. Contact crawling can be disabled for fast listing passes."""
    lat, lng = extract_latlng(page)
    info = {"name":"","rating":None,"review_count":None,"address":None,
            "phone":None,"mobile":None,"email":None,"email_source":None,
            "contact_checked_at":None,"website":None,"category":None,"cid":None,"place_id":None,
            "latitude":lat,"longitude":lng,"url":page.url,
            "source":"google_maps_detail","source_fetched_at":datetime.now().isoformat()}
    info["name"] = first_text(page, ["h1.DUwDvf","h1[class*='fontHeadlineLarge']","h1"])
    info["rating"] = parse_rating(first_text(page, ["div.F7nice span[aria-hidden='true']","span.MW4etd"]))
    if info["rating"] is None:
        info["rating"] = parse_rating(first_attr(page,
            ["div.F7nice span[role='img']","span[role='img'][aria-label*='star' i]"], "aria-label"))
    cnt = first_text(page, ["div.F7nice button","button.HHrUdb"])
    m = re.search(r"([\d.,\s]+)", _ascii_digits(cnt or ""))
    if m:
        d = re.sub(r"[^\d]", "", m.group(1))
        if d: info["review_count"] = int(d)
    a = first_text(page, ["button[data-item-id='address']",
        "button[data-item-id='address'] div.fontBodyMedium"])
    if a:
        for p in ["العنوان","Address"]:
            if a.startswith(p): a = a[len(p):].strip(": -")
        info["address"] = a
    p = first_text(page, ["button[data-item-id^='phone:']",
        "button[data-item-id^='phone:'] div.fontBodyMedium"])
    if p:
        for pfx in ["الهاتف","Phone"]:
            if p.startswith(pfx): p = p[len(pfx):].strip(": -")
        pf=_split_phone_fields(p); info["phone"]=pf.get("phone"); info["mobile"]=pf.get("mobile")
    info["website"] = _normalize_official_website(first_attr(page, ["a[data-item-id='authority']"], "href")) or None
    info["category"] = first_text(page, ["button[jsaction*='category']","button.DkEaL"])
    try:
        m = RE_CID.search(page.url)
        if m: info["cid"] = m.group(1)
        m = RE_PLACE_ID.search(page.url)
        if m: info["place_id"] = m.group(1)
        if not info.get("place_id"):
            m = RE_FTID.search(page.url)
            if m: info["place_id"] = "ftid:" + unquote(m.group(1))
    except Exception: pass
    if with_contacts:
        try:
            mobile, email, src = extract_contacts(info.get("website"), info.get("phone"))
            info["mobile"] = mobile; info["email"] = email; info["email_source"] = src
            info["contact_checked_at"] = datetime.now().isoformat()
        except Exception as exc:
            _diag("contact_extract_failed", exc, url=info.get("website"))
    return info


def _review_norm_text(value):
    text=unicodedata.normalize("NFKD",clean(value).casefold())
    text="".join(ch for ch in text if not unicodedata.combining(ch))
    text=re.sub(r"[^\w\u0600-\u06ff]+"," ",text,flags=re.UNICODE)
    return re.sub(r"\s+"," ",text).strip()

def review_identity_key(review):
    review=dict(review or {})
    rid=clean(review.get("review_id"))
    if rid and not rid.startswith("idx-"):
        return "rid:"+rid
    author=_review_norm_text(review.get("author"))
    try: rating="{:g}".format(float(review.get("rating")))
    except Exception: rating=clean(review.get("rating"))
    dstr=_review_norm_text(review.get("date") or review.get("review_date"))
    text=_review_norm_text(review.get("text"))
    raw="{}|{}|{}|{}".format(author,rating,dstr,text)
    return "fp:"+hashlib.sha256(raw.encode("utf-8")).hexdigest()

def filter_unseen_reviews(records, known_review_keys=None):
    known=set(str(x) for x in (known_review_keys or set()) if x)
    emitted=set(); out=[]
    for row in records or []:
        item=dict(row or {}); key=review_identity_key(item)
        if key in known or key in emitted:
            continue
        item["review_identity_key"]=key
        emitted.add(key); out.append(item)
    return out

def load_known_review_keys(path):
    if not path: return set()
    try:
        data=json.loads(Path(path).read_text(encoding="utf-8"))
        if isinstance(data,dict): data=data.get("known_review_keys") or data.get("keys") or []
        return {str(x) for x in (data or []) if x}
    except Exception as exc:
        _diag("known_reviews_load_failed",exc,path=str(path)); return set()

def extract_reviews(page, max_reviews, known_review_keys=None):
    reviews, seen, prev, stall = [], set(), -1, 0
    emitted_keys=set()
    known=set(str(x) for x in (known_review_keys or set()) if x)
    try: page.wait_for_selector("div[data-review-id]", timeout=10000)
    except Exception: return reviews
    while len(reviews) < max_reviews and stall < 4:
        cards = page.locator("div[data-review-id]")
        n = cards.count()
        if n == prev: stall += 1
        else: stall, prev = 0, n
        for i in range(n):
            if len(reviews) >= max_reviews: break
            try: card = cards.nth(i)
            except Exception: continue
            try: rid = card.get_attribute("data-review-id", timeout=500) or ""
            except Exception: rid = ""
            if not rid: rid = "idx-{}-{}".format(i, n)
            if rid in seen: continue
            seen.add(rid)
            for sel in ["button[aria-label*='المزيد']","button[aria-label*='More' i]","button.w8nwRe"]:
                try:
                    m = card.locator(sel)
                    if m.count() > 0: m.first.click(timeout=200); break
                except Exception: pass
            author = first_text(card, [".d4r55",".WNxzHc"], timeout=500)
            text = first_text(card, [".wiI7pd",".MyEned"], timeout=500)
            dstr = first_text(card, [".rsqaWe",".xRkPPb"], timeout=500)
            rating = None
            for s in [".kvMYJc","span.kvMYJc","[role='img'][aria-label*='star' i]"]:
                try:
                    l = card.locator(s)
                    if l.count() > 0:
                        rating = parse_rating(l.first.get_attribute("aria-label", timeout=500) or "")
                        if rating: break
                except Exception: pass
            owner_resp = ""
            for sel in ['.CDe7pd', 'div[class*="response"] .wiI7pd', '.owner-response']:
                try:
                    loc = card.locator(sel)
                    if loc.count() > 0:
                        owner_resp = clean(loc.first.inner_text(timeout=500)); break
                except Exception: pass
            if text or author:
                item={"review_id": rid, "author": author, "rating": rating,
                      "text": text, "date": dstr, "language": "",
                      "owner_response": owner_resp}
                key=review_identity_key(item)
                if key not in known and key not in emitted_keys:
                    emitted_keys.add(key)
                    item["review_identity_key"]=key
                    reviews.append(item)
        _progress("reviews", current=len(reviews), total=max_reviews)
        if len(reviews) >= max_reviews: break
        try:
            page.evaluate("""() => {const c=document.querySelectorAll('div[data-review-id]');
                if(c.length) c[c.length-1].scrollIntoView({behavior:'instant',block:'end'});}""")
        except Exception: pass
        page.wait_for_timeout(700)
    return reviews


def setup_page(browser, lang="ar", engine="firefox"):
    locale = {"ar":"ar-SA","en":"en-US","ur":"ur-PK","fr":"fr-FR"}.get(lang, "en-US")
    if engine in ("chrome","msedge","chromium"):
        ua="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/153.0.0.0 Safari/537.36"
    else:
        ua="Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:129.0) Gecko/20100101 Firefox/129.0"
    ctx = browser.new_context(locale=locale,
        viewport={"width":1500,"height":960}, user_agent=ua,
        extra_http_headers={"Accept-Language": locale+",en;q=0.8"})
    page = ctx.new_page()
    page.set_default_timeout(6000)
    # Images/fonts/media are not needed for listing extraction; scripts/XHR are kept.
    page.route("**/*", lambda r: r.abort() if r.request.resource_type in ["image","font","media"] else r.continue_())
    return page


def dismiss_consent(page):
    for label in ["Accept all","I agree","Agree","قبول الكل","أوافق","موافقة","Accept","Tout accepter","Alle akzeptieren"]:
        for sel in ['button:has-text("{}")'.format(label),'[role="button"]:has-text("{}")'.format(label)]:
            try:
                loc = page.locator(sel)
                if loc.count() > 0 and loc.first.is_visible():
                    loc.first.click(timeout=1000); page.wait_for_timeout(300); return True
            except Exception: pass
    return False


def open_reviews(page):
    for sel in ['button[jsaction*="pane.reviewChart.moreReviews"]',
                'button[aria-label*="مراجعة"]','button[aria-label*="review" i]',
                'button:has-text("المراجعات")','button:has-text("Reviews")']:
        try:
            loc = page.locator(sel)
            if loc.count() > 0 and loc.first.is_visible():
                loc.first.click(timeout=4000); page.wait_for_timeout(1200); return True
        except Exception: continue
    return False


def sort_reviews_newest(page):
    """Best-effort Google Maps review sort. Failure is harmless; extraction continues."""
    sort_clicked=False
    for sel in ['button[aria-label*="Sort reviews" i]','button[aria-label*="فرز" i]','button[data-value*="Sort" i]']:
        try:
            loc=page.locator(sel)
            if loc.count()>0 and loc.first.is_visible():
                loc.first.click(timeout=2500); page.wait_for_timeout(350); sort_clicked=True; break
        except Exception: pass
    if not sort_clicked: return False
    for label in ['Newest','Most recent','الأحدث','الأحدث أولاً','الأكثر حداثة']:
        for sel in ['div[role="menuitemradio"]:has-text("{}")'.format(label),'div[role="menuitem"]:has-text("{}")'.format(label),'text="{}"'.format(label)]:
            try:
                loc=page.locator(sel)
                if loc.count()>0 and loc.first.is_visible():
                    loc.first.click(timeout=2500); page.wait_for_timeout(700); return True
            except Exception: pass
    return False


def _norm_place_text(value):
    value = clean(value).lower()
    value = re.sub(r"[إأآٱ]","ا",value).replace("ى","ي").replace("ة","ه")
    value = re.sub(r"[ًٌٍَُِّْـ]","",value)
    value = re.sub(r"[^\w\u0600-\u06ff]+"," ",value,flags=re.UNICODE)
    return re.sub(r"\s+"," ",value).strip()


def _url_place_token(url):
    for pat, prefix in ((RE_CID,"cid:"),(RE_FTID,"ftid:")):
        m = pat.search(url or "")
        if m: return prefix + unquote(m.group(1))
    return ""


def _place_aliases(item):
    """Return multiple stable identities so the same branch can merge even when one pass exposes CID and another Place ID."""
    aliases = []
    pid = clean(item.get("place_id"))
    cid = clean(item.get("cid"))
    if pid: aliases.append("pid:" + pid)
    if cid: aliases.append("cid:" + cid)
    tok = _url_place_token(item.get("url") or item.get("maps_url"))
    if tok: aliases.append(tok)
    name = _norm_place_text(item.get("name"))
    lat, lng = item.get("latitude"), item.get("longitude")
    try:
        if lat is not None and lng is not None:
            # ~11m coordinate bucket. Name is retained to avoid merging neighboring unrelated businesses.
            aliases.append("geo:{}:{:.4f}:{:.4f}".format(name, float(lat), float(lng)))
    except Exception:
        pass
    addr = _norm_place_text(item.get("address"))
    if name and addr:
        aliases.append("addr:{}:{}".format(name, addr))
    # Name alone is only a last-resort identity. Never use it alongside stronger aliases,
    # otherwise all branches of the same chain would collapse into one record.
    if name and not aliases:
        aliases.append("name:" + name)
    # Keep order but remove duplicates/empties.
    out = []
    for a in aliases:
        if a and a not in out: out.append(a)
    return out


def _place_key(item):
    aliases = _place_aliases(item)
    return aliases[0] if aliases else "unknown:{}".format(id(item))


def _parse_iso_dt(value):
    try:
        if not value:
            return None
        text=str(value).strip().replace("Z","+00:00")
        dt=datetime.fromisoformat(text)
        if getattr(dt,"tzinfo",None) is not None:
            dt=dt.astimezone().replace(tzinfo=None)
        return dt
    except Exception:
        return None


def _freshness_info(value, now=None, ttl_hours=None):
    """Return an explainable freshness score for cached detail data."""
    now=now or datetime.now(); ttl=float(ttl_hours or DETAIL_CACHE_TTL_HOURS)
    dt=_parse_iso_dt(value)
    if dt is None:
        return {"score":0.0,"status":"unknown","age_hours":None,"fresh":False}
    age=max(0.0,(now-dt).total_seconds()/3600.0)
    if age <= 24:
        score=100.0
    elif age <= ttl:
        score=100.0-(age-24.0)/max(1.0,ttl-24.0)*25.0
    else:
        score=max(0.0,75.0-(age-ttl)/max(1.0,720.0-ttl)*75.0)
    status="fresh" if age <= ttl else "stale"
    return {"score":round(max(0.0,min(100.0,score)),1),"status":status,"age_hours":round(age,1),"fresh":bool(age<=ttl)}


def _cache_file_signature(p):
    try:
        st=p.stat(); return (int(st.st_mtime_ns),int(st.st_size))
    except Exception: return None


def _load_simple_cache(path, max_entries=6000):
    p=Path(path); key=str(p.resolve()) if p.is_absolute() else str(p)
    with _CACHE_LOCK:
        sig=_cache_file_signature(p); memo=_CACHE_MEMORY.get(key)
        if memo and memo.get("signature")==sig and isinstance(memo.get("data"),dict):
            return memo["data"]
        try:
            if p.exists():
                data=json.loads(p.read_text(encoding="utf-8"))
                if isinstance(data,dict):
                    data.setdefault("version",1); data.setdefault("entries",{})
                    if isinstance(data.get("entries"),dict):
                        _CACHE_MEMORY[key]={"signature":sig,"data":data}
                        return data
        except Exception as exc:
            _diag("cache_load_failed",exc,path=str(p))
        data={"version":1,"updated_at":None,"entries":{}}
        _CACHE_MEMORY[key]={"signature":sig,"data":data}
        return data


def _save_simple_cache(data,path,max_entries=6000):
    p=Path(path); key=str(p.resolve()) if p.is_absolute() else str(p)
    with _CACHE_LOCK:
        try:
            p.parent.mkdir(parents=True,exist_ok=True)
            payload=dict(data or {}); entries=payload.get("entries") if isinstance(payload.get("entries"),dict) else {}
            if len(entries)>int(max_entries):
                def _stamp(item):
                    row=item[1] if isinstance(item[1],dict) else {}
                    return str(row.get("cached_at") or row.get("checked_at") or "")
                entries=dict(sorted(entries.items(),key=_stamp,reverse=True)[:int(max_entries)])
            payload["entries"]=entries; payload["version"]=1; payload["updated_at"]=datetime.now().isoformat(timespec="seconds")
            tmp=p.with_suffix(p.suffix+".tmp")
            tmp.write_text(json.dumps(payload,ensure_ascii=False,indent=2,default=str),encoding="utf-8")
            tmp.replace(p)
            _CACHE_MEMORY[key]={"signature":_cache_file_signature(p),"data":payload}
            return True
        except Exception as exc:
            _diag("cache_save_failed",exc,path=str(p)); return False


def _cache_aliases(rec):
    """Stable cache identities, preferring strong Google IDs and avoiding brand-name-only aliases."""
    rec=rec or {}; out=[]
    pid=clean(rec.get("place_id")); cid=clean(rec.get("cid")); tok=_url_place_token(rec.get("url") or rec.get("maps_url"))
    if pid:
        out.append(pid if pid.startswith(("ftid:","cid:")) else "pid:"+pid)
    if cid: out.append("cid:"+cid)
    if tok: out.append(tok)
    # If Google exposed any strong identity, do not mix it with coordinate/address aliases.
    if not out:
        for a in _place_aliases(rec):
            if a and not a.startswith("name:") and a not in out: out.append(a)
    ded=[]
    for a in out:
        if a and a not in ded: ded.append(a)
    return ded


def _cache_identity_key(rec):
    aliases=_cache_aliases(rec)
    return aliases[0] if aliases else ""


_BRANCH_CACHE_FIELDS=("name","rating","review_count","address","phone","mobile","email","email_source","website","category",
                      "cid","place_id","latitude","longitude","url","maps_url","details_loaded","preview_reviews","preview_review_count",
                      "source","source_fetched_at","detail_fetched_at","contact_checked_at","review_samples_fetched_at","field_sources")


def _branch_cache_store_many(records,path=None):
    p=Path(path or BRANCH_CACHE_FILE); rows=list(records or [])
    if not rows: return 0
    with _CACHE_LOCK:
        data=_load_simple_cache(p,BRANCH_CACHE_MAX_ENTRIES); entries=data.setdefault("entries",{}); alias_map=data.setdefault("aliases",{}); stored=0
        now=datetime.now().isoformat(timespec="seconds")
        for rec in rows:
            aliases=_cache_aliases(rec); key=aliases[0] if aliases else ""
            if not key: continue
            row={k:rec.get(k) for k in _BRANCH_CACHE_FIELDS if rec.get(k) not in (None,"",[],{})}
            row["cached_at"]=now; row["cache_key"]=key
            row.setdefault("detail_fetched_at",rec.get("detail_fetched_at") or (now if rec.get("details_loaded") else None))
            entries[key]=row
            for alias in aliases: alias_map[alias]=key
            stored+=1
        if not stored: return 0
        return stored if _save_simple_cache(data,p,BRANCH_CACHE_MAX_ENTRIES) else 0


def _branch_cache_store(rec,path=None):
    return bool(_branch_cache_store_many([rec],path))


def _branch_cache_lookup(rec,path=None):
    aliases=_cache_aliases(rec)
    if not aliases: return None
    data=_load_simple_cache(Path(path or BRANCH_CACHE_FILE),BRANCH_CACHE_MAX_ENTRIES)
    entries=data.get("entries") or {}; alias_map=data.get("aliases") or {}
    for alias in aliases:
        canonical=alias_map.get(alias) or alias
        row=entries.get(canonical)
        if not isinstance(row,dict): continue
        if _strong_identity_conflict(rec,row): continue
        return dict(row)
    return None


def _apply_branch_cache(rec,path=None):
    base=dict(rec or {}); cached=_branch_cache_lookup(base,path)
    if not cached:
        base["cache_hit"]=False
        info=_freshness_info(base.get("detail_fetched_at"))
        base["freshness_score"]=info["score"]; base["freshness_status"]=info["status"]
        base["cache_age_hours"]=info["age_hours"]
        return base
    # Cached record is the older base; current discovery fields win when they are present.
    merged=_merge(cached,base)
    finfo=_freshness_info(cached.get("detail_fetched_at") or cached.get("cached_at"))
    merged["cache_hit"]=True; merged["cache_key"]=cached.get("cache_key") or _cache_identity_key(base)
    merged["cache_age_hours"]=finfo["age_hours"]; merged["freshness_score"]=finfo["score"]; merged["freshness_status"]=finfo["status"]
    merged["cache_updated_at"]=cached.get("cached_at")
    return merged


def _should_refresh_details(rec,now=None,review_samples=0):
    rec=rec or {}; now=now or datetime.now()
    if not rec.get("details_loaded"): return True
    info=_freshness_info(rec.get("detail_fetched_at") or rec.get("cache_updated_at"),now=now)
    if not info.get("fresh"): return True
    # A checked contact page is evidence even when no phone/email exists.
    contact_ts=rec.get("contact_checked_at")
    if rec.get("website") and not contact_ts and not (rec.get("phone") or rec.get("mobile") or rec.get("email")): return True
    if contact_ts and not _freshness_info(contact_ts,now=now,ttl_hours=CONTACT_CACHE_TTL_HOURS).get("fresh"): return True
    if int(review_samples or 0)>0 and int(rec.get("preview_review_count") or 0)<min(int(review_samples),3): return True
    return False


def _enrichment_priority(rec,now=None):
    rec=rec or {}; now=now or datetime.now()
    validation=float(rec.get("validation_score") or 0)
    score=validation
    if not rec.get("details_loaded"): score+=45.0
    finfo=_freshness_info(rec.get("detail_fetched_at") or rec.get("cache_updated_at"),now=now)
    if finfo["status"]=="stale": score+=32.0
    elif finfo["status"]=="unknown": score+=22.0
    if rec.get("website") and not rec.get("contact_checked_at"): score+=12.0
    if not (rec.get("phone") or rec.get("mobile") or rec.get("email") or rec.get("website")): score+=8.0
    return round(score,2)


def _contact_cache_key(website):
    try:
        u=urlparse(clean(website)); host=(u.netloc or "").lower().replace("www.","")
        path=re.sub(r"/+","/",u.path or "/").rstrip("/")
        if host:
            return host+(path if path else "")
        return clean(website).lower()
    except Exception: return clean(website).lower()


def _contact_cache_lookup(website,path=None):
    key=_contact_cache_key(website)
    if not key: return None
    data=_load_simple_cache(Path(path or CONTACT_CACHE_FILE),CONTACT_CACHE_MAX_ENTRIES)
    row=(data.get("entries") or {}).get(key)
    if not isinstance(row,dict): return None
    info=_freshness_info(row.get("checked_at"),ttl_hours=CONTACT_CACHE_TTL_HOURS)
    if not info.get("fresh"): return None
    out=dict(row); out["freshness_score"]=info["score"]; return out


def _contact_cache_store(website,mobile=None,email=None,source=None,path=None,profile=None):
    key=_contact_cache_key(website)
    if not key: return False
    p=Path(path or CONTACT_CACHE_FILE)
    with _CACHE_LOCK:
        data=_load_simple_cache(p,CONTACT_CACHE_MAX_ENTRIES)
        row={"website":clean(website),"mobile":mobile,"email":email,"source":source or "website",
             "checked_at":datetime.now().isoformat(timespec="seconds")}
        if isinstance(profile,dict):
            for k in ("phone","mobile","email","whatsapp","contact_source","contact_confidence","field_sources","emails","phones"):
                if profile.get(k) not in (None,"",[],{}): row[k]=profile.get(k)
        data.setdefault("entries",{})[key]=row
        return _save_simple_cache(data,p,CONTACT_CACHE_MAX_ENTRIES)


def _field_provenance(rec, source=None):
    src = source or rec.get("source") or "google_maps"
    prev = rec.get("field_sources") if isinstance(rec.get("field_sources"), dict) else {}
    out = dict(prev)
    for k in ["name","rating","review_count","address","phone","mobile","email","website","category",
              "cid","place_id","latitude","longitude","url","whatsapp","contact_source","contact_confidence"]:
        if rec.get(k) not in (None, "", [], {}):
            out.setdefault(k, src)
    return out


def _source_list(rec):
    vals=[]
    raw=rec.get("providers")
    if isinstance(raw,(list,tuple,set)):
        vals.extend([clean(x) for x in raw if clean(x)])
    src=clean(rec.get("source"))
    if src: vals.append(src)
    out=[]
    for x in vals:
        if x and x not in out: out.append(x)
    return out


def _source_rank_v38(source):
    s=str(source or "").strip().lower()
    if any(x in s for x in ("official_website","contact_page","json_ld","website")): return 5
    if "google_places" in s or "places_api" in s: return 4
    if "google_maps" in s or s=="detail": return 3
    if "google_search" in s or "search" in s: return 2
    if s in ("seed","cache"): return 1
    return 1


def _value_quality_v38(field, value):
    if value in (None,"",[],{}): return 0.0
    if field=="rating":
        try:
            x=float(value); return 3.0 if 0.0 < x <= 5.0 else 0.0
        except Exception: return 0.0
    if field=="review_count":
        try: return max(0.0,float(value))
        except Exception: return 0.0
    if field in ("contact_confidence","data_completeness","validation_score","confidence_score"):
        try: return max(0.0,float(value))
        except Exception: return 0.0
    return 1.0


def _should_replace_field_v38(field, old_value, new_value, old_source="", new_source=""):
    nq=_value_quality_v38(field,new_value); oq=_value_quality_v38(field,old_value)
    if nq<=0: return False
    if oq<=0: return True
    if field=="review_count": return nq>=oq
    if field=="rating": return _source_rank_v38(new_source)>=_source_rank_v38(old_source)
    if field in ("contact_confidence","data_completeness","validation_score","confidence_score"): return nq>=oq
    if field in ("phone","mobile","email","website","whatsapp","address","category"):
        return _source_rank_v38(new_source)>=_source_rank_v38(old_source)
    return True


def _merge(old, new):
    if old is None:
        out = dict(new)
        out["field_sources"] = _field_provenance(out)
        out["providers"] = _source_list(out)
        out["provider_count"] = len(out["providers"])
        return out
    out = dict(old)
    old_sources = out.get("field_sources") if isinstance(out.get("field_sources"), dict) else _field_provenance(out)
    new_sources = _field_provenance(new)
    # Fields where fresher/more detailed detail-page data should win when present.
    prefer_new = {"rating","review_count","distance_km","match_score","confidence_score","phone","mobile","email",
                  "website","category","address","cid","place_id","latitude","longitude","url","contact_checked_at",
                  "preview_reviews","preview_review_count","match_reason","result_score","data_completeness","whatsapp","contact_source","contact_confidence","email_source"}
    for k, v in new.items():
        if k in ("field_sources","providers","provider_count") or v in (None,"",[],{}):
            continue
        old_src=old_sources.get(k) or old.get("source") or ""
        new_src=new_sources.get(k) or new.get("source") or ""
        replace = out.get(k) in (None,"",[],{})
        if not replace and k in prefer_new:
            replace=_should_replace_field_v38(k,out.get(k),v,old_src,new_src)
        if replace:
            out[k] = v
            if k in new_sources: old_sources[k] = new_sources[k]
    out["field_sources"] = old_sources
    providers=[]
    for src in _source_list(old)+_source_list(new):
        if src and src not in providers: providers.append(src)
    out["providers"]=providers
    out["provider_count"]=len(providers)
    # Keep earliest discovery time but latest detail timestamp separately.
    times = [x for x in [old.get("source_fetched_at"), new.get("source_fetched_at")] if x]
    if times: out["source_fetched_at"] = min(times)
    latest = [x for x in [old.get("detail_fetched_at"), new.get("detail_fetched_at"), new.get("source_fetched_at")] if x]
    if latest: out["detail_fetched_at"] = max(latest)
    return out

def _strong_identity_conflict(a,b):
    """Never merge two branches when the same strong identity type explicitly disagrees."""
    a=a or {}; b=b or {}
    for key in ("place_id","cid"):
        av=clean(a.get(key)); bv=clean(b.get(key))
        if av and bv and av != bv:
            return True
    at=_url_place_token(a.get("url") or a.get("maps_url")); bt=_url_place_token(b.get("url") or b.get("maps_url"))
    if at and bt and at.split(":",1)[0] == bt.split(":",1)[0] and at != bt:
        return True
    return False


def _near_duplicate_candidate(store,rec):
    """Find the same physical branch when names/coordinates differ slightly and no strong ID conflicts."""
    if not store or not clean(rec.get("name")):
        return None
    rlat,rlng=rec.get("latitude"),rec.get("longitude")
    raddr=_norm_place_text(rec.get("address"))
    for key,old in store.items():
        if _strong_identity_conflict(old,rec):
            continue
        name_score=_brand_score_reason(rec.get("name"),old.get("name"))[0]
        if name_score < .78:
            continue
        olat,olng=old.get("latitude"),old.get("longitude")
        if rlat is not None and rlng is not None and olat is not None and olng is not None:
            dist=haversine(float(rlat),float(rlng),float(olat),float(olng))
            if dist is not None and dist <= .040:
                return key
        oaddr=_norm_place_text(old.get("address"))
        if raddr and oaddr and name_score >= .88:
            if raddr == oaddr or (len(raddr)>=8 and raddr in oaddr) or (len(oaddr)>=8 and oaddr in raddr):
                return key
    return None


def _upsert_place(store, alias_index, rec, stats=None):
    """Merge by stable identity while protecting distinct nearby chain branches."""
    aliases = _place_aliases(rec)
    canonical = None
    for a in aliases:
        if a in alias_index:
            candidate=alias_index[a]
            old=store.get(candidate)
            if old is not None and _strong_identity_conflict(old,rec):
                if isinstance(stats,dict): stats["strong_identity_conflicts"]=int(stats.get("strong_identity_conflicts") or 0)+1
                continue
            canonical = candidate
            break
    if canonical is None:
        canonical=_near_duplicate_candidate(store,rec)
        if canonical is not None and isinstance(stats,dict):
            stats["near_duplicate_merges"]=int(stats.get("near_duplicate_merges") or 0)+1
    if canonical is None:
        # A Google Search seed may initially have only a name + Maps URL while the
        # subsequent Maps card exposes coordinates/CID/Place ID.
        n=_norm_place_text(rec.get("name"))
        seed_key=alias_index.get("name:"+n) if n else None
        seed_rec=store.get(seed_key) if seed_key else None
        if (seed_rec and not _strong_identity_conflict(seed_rec,rec) and
                str(seed_rec.get("source") or "").startswith("google_search_browser") and
                all(str(a).startswith("name:") for a in _place_aliases(seed_rec))):
            canonical=seed_key
    existed=canonical is not None
    if canonical is None:
        canonical = aliases[0] if aliases else "row:{}".format(len(store) + 1)
        store[canonical] = _merge(None, rec)
        if isinstance(stats,dict): stats["inserted"]=int(stats.get("inserted") or 0)+1
    else:
        store[canonical] = _merge(store.get(canonical), rec)
        if isinstance(stats,dict): stats["duplicates_suppressed"]=int(stats.get("duplicates_suppressed") or 0)+1
    # Recompute aliases after merge because detail pages may add CID/Place ID/address.
    # Do not let a conflicting weak alias overwrite another branch's strong identity mapping.
    for a in _place_aliases(store[canonical]):
        prev=alias_index.get(a)
        if prev and prev != canonical and _strong_identity_conflict(store.get(prev),store.get(canonical)):
            continue
        alias_index[a] = canonical
    for a in aliases:
        prev=alias_index.get(a)
        if prev and prev != canonical and _strong_identity_conflict(store.get(prev),rec):
            continue
        alias_index[a] = canonical
    return canonical


_GENERIC_NAME_TOKENS = {
    "فرع","فروع","شركة","شركه","مؤسسة","مؤسسه","محل","متجر","مطعم","كافيه","مقهى","مقهي",
    "branch","branches","company","store","shop","restaurant","cafe","location","locations","ksa","saudi"
}


def _brand_core(value):
    n=_norm_place_text(value)
    toks=[t for t in n.split() if t and t not in _GENERIC_NAME_TOKENS]
    return " ".join(toks) or n


def _brand_score_reason(brand, name):
    """Partial-name tolerant score that still resists unrelated Google suggestions."""
    b=_brand_core(brand); n=_brand_core(name)
    if not b: return 1.0,"empty_query"
    if not n: return 0.0,"missing_name"
    if b==n: return 1.0,"exact"
    if b in n: return .97,"query_phrase_in_name"
    if n in b and len(n)>=3: return .92,"name_phrase_in_query"
    bt=[x for x in b.split() if x]; nt=[x for x in n.split() if x]
    if not bt or not nt: return 0.0,"no_tokens"
    # A typed fragment such as «البي» should strongly match «البيك».
    prefix_hits=sum(1 for q in bt if any(t.startswith(q) or q.startswith(t) for t in nt))
    exact_hits=len(set(bt)&set(nt))
    prefix_ratio=prefix_hits/max(1,len(bt)); exact_ratio=exact_hits/max(1,len(bt))
    if prefix_ratio>=1.0 and all(len(q)>=2 for q in bt):
        return max(.88,.94 if exact_ratio>=.5 else .88),"token_prefix"
    try:
        import difflib
        seq=difflib.SequenceMatcher(None,b,n).ratio()
    except Exception:
        seq=0.0
    token_overlap=len(set(bt)&set(nt))/max(1,len(set(bt)|set(nt)))
    score=max(seq*.86, token_overlap*.92, prefix_ratio*.86)
    reason="fuzzy"
    if token_overlap>=.5: reason="token_overlap"
    elif prefix_ratio>=.5: reason="partial_prefix"
    return max(0.0,min(.91,score)),reason


def _brand_score(brand, name):
    return _brand_score_reason(brand,name)[0]


def _data_completeness(rec):
    checks=[
        bool(clean(rec.get("name"))), rec.get("rating") is not None, rec.get("review_count") not in (None,""),
        bool(clean(rec.get("address"))), bool(rec.get("phone") or rec.get("mobile")), bool(clean(rec.get("website"))),
        rec.get("latitude") is not None and rec.get("longitude") is not None,
        bool(rec.get("place_id") or rec.get("cid") or _url_place_token(rec.get("url") or rec.get("maps_url"))),
        bool(rec.get("details_loaded")), bool(rec.get("preview_reviews")),
    ]
    return round(100.0*sum(1 for x in checks if x)/len(checks),1)


def _branch_validation(query,rec):
    """Explainable branch validation independent from popularity."""
    rec=rec or {}
    match=float(rec.get("match_score") if rec.get("match_score") is not None else _brand_score(query,rec.get("name")))
    comp=float(rec.get("data_completeness") if rec.get("data_completeness") is not None else _data_completeness(rec))
    providers=_source_list(rec)
    identity=24.0 if rec.get("place_id") or rec.get("cid") else (12.0 if _url_place_token(rec.get("url") or rec.get("maps_url")) else 0.0)
    geo=10.0 if rec.get("latitude") is not None and rec.get("longitude") is not None else 0.0
    address=7.0 if clean(rec.get("address")) else 0.0
    provider_signal=10.0 if len(providers)>=2 else (4.0 if providers else 0.0)
    details=7.0 if rec.get("details_loaded") else 0.0
    score=match*40.0 + identity + geo + address + provider_signal + details + min(10.0,comp*.10)
    if clean(query) and match < .25: score=min(score,32.0)
    elif clean(query) and match < .45: score=min(score,46.0)
    elif clean(query) and match < .58: score=min(score,58.0)
    score=round(max(0.0,min(100.0,score)),1)
    status="confirmed" if score>=75 else ("probable" if score>=52 else "weak")
    return {"score":score,"status":status,"match":round(match,4),"identity":identity,"providers":len(providers),"completeness":comp}


def _detail_enrichment_budget(results,auto_mode=False,details=False):
    """v30 detail budget: fully enrich small relevant branch sets, bound broad searches."""
    if not details or not results: return 0
    rows=list(results); n=len(rows)
    relevant=0; strong=0
    for r in rows[:min(n,120)]:
        group=str(r.get("result_group") or "")
        try: match=float(r.get("match_score") or 0)
        except Exception: match=0.0
        if group in {"strong","confirmed","related"} or match>=.58: relevant+=1
        try:
            if _branch_validation("",r)["score"]>=70: strong+=1
        except Exception: pass
    if auto_mode:
        # For the normal branded-search case, users expect every discovered branch to have details.
        if n<=60 and relevant>=max(1,int(n*.45)):
            return n
        base=int(20 + math.sqrt(n)*2.7 + min(relevant,60)*.35 + min(strong,30)*.20)
        target=max(base,min(96,relevant+10))
        return min(n,max(20,min(96,target)))
    return min(n,max(20,min(80,int(24 + math.sqrt(n)*2.5 + min(strong,30)*.30))))


def _result_group(query, rec):
    """v30 fusion band; additional results stay visible instead of being discarded."""
    rec=rec or {}
    try: match=float(rec.get("match_score") if rec.get("match_score") is not None else _brand_score(query,rec.get("name")))
    except Exception: match=0.0
    status=clean(rec.get("validation_status") or "weak").lower()
    strong_id=bool(rec.get("place_id") or rec.get("cid") or _url_place_token(rec.get("url") or rec.get("maps_url")))
    if match>=.78 and (status=="confirmed" or strong_id): return "strong"
    if match>=.58 and (status=="confirmed" or strong_id): return "confirmed"
    if match>=.38 or status=="probable": return "related"
    return "additional"


def _apply_result_metrics(query, rec):
    """v28 relevance-first ranking with validation and data-freshness telemetry."""
    score,reason=_brand_score_reason(query,rec.get("name"))
    rec["match_score"]=round(float(score),4)
    rec["match_reason"]=reason
    providers=_source_list(rec)
    rec["providers"]=providers
    rec["provider_count"]=len(providers)
    comp=_data_completeness(rec); rec["data_completeness"]=comp
    reviews=0
    try: reviews=max(0,int(rec.get("review_count") or 0))
    except Exception: reviews=0
    review_signal=min(5.0, math.log10(reviews+1)*1.7) if reviews else 0.0
    identity=7.0 if rec.get("place_id") or rec.get("cid") else (3.5 if _url_place_token(rec.get("url") or rec.get("maps_url")) else 0.0)
    provider_bonus=min(5.0,max(0,len(providers)-1)*2.5)
    rating_bonus=0.0
    try: rating_bonus=max(0.0,min(5.0,float(rec.get("rating") or 0)))
    except Exception: pass
    final_score=min(100.0, score*68.0 + comp*.10 + identity + provider_bonus + review_signal + rating_bonus)
    # relevance_guard prevents a very popular unrelated place from outranking the intended brand.
    relevance_guard="ok"
    if clean(query) and score < .25:
        final_score=min(final_score,34.0); relevance_guard="very_low"
    elif clean(query) and score < .45:
        final_score=min(final_score,48.0); relevance_guard="low"
    elif clean(query) and score < .58:
        final_score=min(final_score,62.0); relevance_guard="medium"
    rec["relevance_guard"]=relevance_guard
    validation=_branch_validation(query,rec)
    rec["validation_score"]=validation["score"]
    rec["validation_status"]=validation["status"]
    rec["result_group"]=_result_group(query,rec)
    rec["result_group_rank"]={"strong":0,"confirmed":1,"related":2,"additional":3}.get(rec["result_group"],3)
    finfo=_freshness_info(rec.get("detail_fetched_at") or rec.get("cache_updated_at"))
    rec["freshness_score"]=finfo["score"]; rec["freshness_status"]=finfo["status"]; rec["cache_age_hours"]=finfo["age_hours"]
    final_score=min(100.0, final_score*.90 + validation["score"]*.10)
    rec["result_score"]=round(final_score,1)
    return rec


def _rank_key(query, rec):
    _apply_result_metrics(query,rec)
    dist=float(rec.get("distance_km")) if rec.get("distance_km") is not None else 1e9
    return (int(rec.get("result_group_rank") if rec.get("result_group_rank") is not None else 3),
            -float(rec.get("result_score") or 0),-float(rec.get("validation_score") or 0),-float(rec.get("match_score") or 0),
            -int(rec.get("provider_count") or 0),-float(rec.get("data_completeness") or 0),
            dist,-int(rec.get("review_count") or 0))

def _offset(lat, lng, nk, ek):
    dlat = nk / 110.574
    dlng = ek / max(.15, 111.320 * math.cos(math.radians(lat)))
    return lat + dlat, lng + dlng


def _coverage(center_lat, center_lng, radius_km=None, max_tiles=36):
    max_tiles = max(1, min(int(max_tiles or 36), 81))
    rad = float(radius_km) if radius_km else 25.0
    south, west = _offset(center_lat, center_lng, -rad, -rad)
    north, east = _offset(center_lat, center_lng, rad, rad)
    h = haversine(south, center_lng, north, center_lng) or 1
    w = haversine(center_lat, west, center_lat, east) or 1
    spacing = 6.0
    rows = max(1, int(math.ceil(h/spacing))+1)
    cols = max(1, int(math.ceil(w/spacing))+1)
    while rows*cols > max_tiles:
        if rows >= cols and rows > 1: rows -= 1
        elif cols > 1: cols -= 1
        else: break
    pts = []
    for ri in range(rows):
        fr = .5 if rows == 1 else ri/(rows-1)
        lat = south + (north-south)*fr
        for ci in range(cols):
            fc = .5 if cols == 1 else ci/(cols-1)
            lng = west + (east-west)*fc
            if radius_km and (haversine(center_lat, center_lng, lat, lng) or 0) > float(radius_km)*1.12:
                continue
            pts.append((lat, lng))
    pts.sort(key=lambda p: haversine(center_lat, center_lng, p[0], p[1]) or 0)
    if not pts or abs(pts[0][0]-center_lat) > .002:
        pts.insert(0, (center_lat, center_lng))
    return pts[:max_tiles]


def _adaptive_subdivision_centers(tile_stats,center_lat,center_lng,radius_km,max_children,seen=None):
    """Create smaller follow-up viewports only around saturated parent tiles."""
    max_children=max(0,int(max_children or 0))
    if max_children<=0: return []
    seen=set(seen or set()); out=[]
    saturated=[x for x in (tile_stats or []) if x.get("saturated") and x.get("center")]
    if not saturated: return out
    base=max(.45,min(4.5,float(radius_km or 18)/max(3.0,math.sqrt(max(1,len(tile_stats)))*2.2)))
    offsets=[(-1,-1),(-1,1),(1,-1),(1,1),(0,-1),(0,1),(-1,0),(1,0)]
    for stat in saturated:
        try: lat,lng=float(stat["center"][0]),float(stat["center"][1])
        except Exception: continue
        step=max(.35,base*(.55 if not stat.get("adaptive_child") else .38))
        added=0
        for ns,es in offsets:
            nl,ng=_offset(lat,lng,ns*step,es*step)
            if center_lat is not None and center_lng is not None and radius_km:
                d=haversine(float(center_lat),float(center_lng),nl,ng)
                if d is not None and d>float(radius_km)*1.06: continue
            key=(round(nl,5),round(ng,5))
            if key in seen: continue
            seen.add(key); out.append((nl,ng)); added+=1
            if len(out)>=max_children: break
        stat["subdivided"]=max(int(stat.get("subdivided") or 0),added)
        if len(out)>=max_children: break
    return out


def _coverage_quality_summary(tile_stats,discovered_count,discover_cap,termination_reason,unresolved_saturated=0,failed_tiles=0):
    rows=list(tile_stats or []); total=len(rows)
    failures=max(int(failed_tiles or 0),sum(1 for x in rows if x.get("error")))
    unresolved=max(int(unresolved_saturated or 0),sum(1 for x in rows if x.get("saturated") and int(x.get("subdivided") or 0)<=0))
    resolved=max(0,total-failures-unresolved)
    cap_hit=int(discovered_count or 0)>=max(1,int(discover_cap or 1))
    complete=bool(termination_reason in {"completed","completed_via_browser_failover"} and not cap_hit and failures==0 and unresolved==0)
    if complete:
        percent=100.0
    elif total:
        percent=55.0*(resolved/total) + 20.0*((total-unresolved)/total) + (10.0 if not cap_hit else 0.0)
        if termination_reason in {"deadline_reached","network_error","environment_blocked","google_blocked"}: percent-=12.0
        if cap_hit: percent=min(percent,82.0)
        percent=max(5.0,min(94.0,percent))
    else:
        percent=90.0 if termination_reason in {"completed","completed_via_browser_failover"} and not cap_hit else 35.0
    return {"percent":round(percent,1),"complete":complete,"failed":failures,"unresolved":unresolved,"cap_hit":cap_hit,"tiles":total}


def _feed_end(page):
    try:
        feed = page.locator('div[role="feed"]')
        txt = (feed.inner_text(timeout=600) if feed.count() else "").lower()
        return any(x in txt for x in ["reached the end", "نهاية القائمة"])
    except Exception: return False



def _normalize_maps_href(href):
    """Decode Google/HTML/JSON escaped Maps links into an absolute URL."""
    href = clean(href)
    if not href:
        return ""
    try:
        href = html_lib.unescape(href)
        href = href.replace("\\u003d", "=").replace("\\u0026", "&").replace("\\u003f", "?")
        href = href.replace("\\/", "/")
        href = _unwrap_google_href(href)
        if href.startswith("/maps/") or href.startswith("/maps?"):
            href = "https://www.google.com" + href
        if href.startswith("https://google.com/"):
            href = href.replace("https://google.com/","https://www.google.com/",1)
        if href.startswith("https://maps.google.com/"):
            tail = href[len("https://maps.google.com/"):]
            href = "https://www.google.com/maps" + (("?" + tail[1:]) if tail.startswith("?") else ("/" + tail if tail else ""))
    except Exception:
        pass
    return href


def _record_from_maps_href(href, q, name="", center=None, source="google_maps_anchor"):
    href=_normalize_maps_href(href)
    low=href.lower()
    is_maps_entity = ("/maps/place/" in low or "/maps?" in low or RE_CID.search(href) is not None or RE_FTID.search(href) is not None)
    if not href or not is_maps_entity or "/dir/" in low:
        return None
    name=clean(name)
    if not name:
        m=re.search(r"/maps/place/([^/?#]+)",href)
        if m:
            try: name=clean(unquote(m.group(1)).replace("+"," "))
            except Exception: name=""
    if not name:
        return None
    if _norm_place_text(name) in {"maps","google maps","خرائط google","خرائط جوجل","directions","الاتجاهات"}:
        return None
    lat,lon=_latlng_from_url(href); tok=_url_place_token(href)
    return {"name":name,"url":href,"maps_url":href,"latitude":lat,"longitude":lon,
            "place_id":tok[5:] if tok.startswith("ftid:") else None,
            "cid":tok[4:] if tok.startswith("cid:") else None,
            "search_query":q,"search_center":center,"source":source,
            "source_fetched_at":datetime.now().isoformat(),
            "match_score":_brand_score(q,name) if clean(q) else 1.0}


def _extract_global_maps_links(page, q, max_results=100, center=None, source="google_maps_global"):
    """Extract Maps entities without relying on Google's current card class names.

    Two paths are used: semantic anchors in the live DOM, then raw HTML/JSON URL
    fragments. This is intentionally redundant because Google frequently changes
    result wrappers while the place URLs remain present.
    """
    found={}; aliases={}; wanted=max(1,min(int(max_results or 100),500))
    selectors='a[href*="/maps/place/"],a[href*="/maps?cid="],a[href*="google.com/maps/place/"],a[href*="maps.google."]'
    try:
        anchors=page.locator(selectors); n=min(anchors.count(),wanted*8)
        for i in range(n):
            if len(found)>=wanted: break
            a=anchors.nth(i)
            try: href=a.get_attribute("href",timeout=300) or ""
            except Exception: href=""
            name=""
            for attr in ("aria-label","title"):
                try:
                    name=clean(a.get_attribute(attr,timeout=220) or "")
                    if name: break
                except Exception: pass
            if not name:
                try: name=clean(a.inner_text(timeout=260) or "")
                except Exception: pass
            if not name:
                try:
                    parent=a.locator("xpath=ancestor::*[self::div or self::article][1]")
                    txt=clean(parent.inner_text(timeout=260) or "") if parent.count() else ""
                    if txt: name=txt.split("\n",1)[0][:180]
                except Exception: pass
            rec=_record_from_maps_href(href,q,name,center,source)
            if rec: _upsert_place(found,aliases,rec)
    except Exception:
        pass
    if len(found)<wanted:
        try:
            raw=page.content()
            raw=html_lib.unescape(raw).replace("\\u003d","=").replace("\\u0026","&").replace("\\/","/")
            pats=(RE_MAPS_PLACE_URL, RE_MAPS_REL_PLACE, RE_MAPS_CID_URL, RE_MAPS_REL_CID)
            seen_urls=set()
            for pat in pats:
                for m in pat.finditer(raw):
                    if len(found)>=wanted: break
                    href=_normalize_maps_href(m.group(0).rstrip("\\,;"))
                    if href in seen_urls: continue
                    seen_urls.add(href)
                    rec=_record_from_maps_href(href,q,"",center,source+"_html")
                    if rec: _upsert_place(found,aliases,rec)
                if len(found)>=wanted: break
        except Exception:
            pass
    return list(found.values())


def _google_result_title_expansions(page, query, limit=8):
    """Read ordinary Google result/local-pack headings as extra Maps queries."""
    out=[]; base=_norm_place_text(query)
    for sel in ['a h3','[role="main"] h3','div[role="heading"]','h3']:
        try:
            loc=page.locator(sel); n=min(loc.count(),80)
            for i in range(n):
                try: txt=clean(loc.nth(i).inner_text(timeout=220) or "")
                except Exception: continue
                if not txt or len(txt)>180: continue
                score=_brand_score(query,txt) if base else 0
                if score < .34: continue
                # Remove common Google result suffixes while preserving the business name.
                txt=re.split(r"\s+[|–—-]\s+",txt,1)[0].strip()
                if txt and txt not in out: out.append(txt)
                if len(out)>=int(limit): return out
        except Exception:
            continue
    return out


def _card(item, q, center=None):
    """Extract a Google Maps result card using stable/generic selectors.

    Google changes CSS class names frequently, so this intentionally prefers
    semantic attributes and href patterns, while retaining the legacy selectors
    as fallbacks.
    """
    href = name = ""
    link = None
    for sel in [
        'a.hfpxzc',
        'a[href*="/maps/place/"]',
        'a[href*="/maps?cid="]',
        'a[href*="google.com/maps/place/"]',
    ]:
        try:
            loc = item.locator(sel).first
            if loc.count() > 0:
                link = loc
                href = loc.get_attribute("href", timeout=650) or ""
                name = loc.get_attribute("aria-label", timeout=650) or ""
                if not name:
                    try: name = loc.inner_text(timeout=500) or ""
                    except Exception: pass
                if href or name:
                    break
        except Exception:
            continue
    if not name:
        name = first_text(item, ['.qBF1Pd','.fontHeadlineSmall','[role="heading"]'], timeout=500)
    if not name:
        try: name = item.get_attribute("aria-label", timeout=400) or ""
        except Exception: pass
    if not name and href:
        m = re.search(r"/maps/place/([^/]+)/", href)
        if m: name = unquote(m.group(1)).replace("+", " ")
    name = clean(name)
    if not name:
        return None

    rating = None
    for sel in ['span.MW4etd','span[aria-label*="star" i]','[role="img"][aria-label*="star" i]','[role="img"][aria-label*="نجم"]']:
        try:
            loc=item.locator(sel).first
            if loc.count()>0:
                raw=(loc.get_attribute('aria-label',timeout=350) or '')
                if not raw:
                    try: raw=loc.inner_text(timeout=350) or ''
                    except Exception: pass
                rating=parse_rating(raw)
                if rating is not None: break
        except Exception:
            continue

    reviews = None
    for sel in ['span.UY7F9','span[aria-label*="review" i]','span[aria-label*="مراجعة"]','span[aria-label*="تقييم"]']:
        cnt = first_text(item, [sel], timeout=400)
        if cnt:
            m = re.search(r"([\d,٬،.]+)", cnt)
            if m:
                try: reviews = int(re.sub(r"[^\d]", "", m.group(1)))
                except Exception: pass
                if reviews is not None: break

    addr = first_text(item, ['.W4Efsd:last-child .W4Efsd','.W4Efsd','[data-value="Address"]','button[data-item-id*="address"]'], timeout=500)
    rlat, rlng = _latlng_from_url(href)
    tok = _url_place_token(href)
    pid = tok[5:] if tok.startswith("ftid:") else None
    cid = tok[4:] if tok.startswith("cid:") else None
    return {"name":name,"rating":rating,"review_count":reviews,"address":addr,
            "url":href,"place_id":pid,"cid":cid,"latitude":rlat,"longitude":rlng,
            "search_query":q,"search_center":center,"source":"google_maps_card",
            "source_fetched_at":datetime.now().isoformat()}


def _collect_search_cards(page, q, lat=None, lng=None, max_results=80, max_scrolls=60, stall_limit=6, wait_ms=650):
    found = {}; alias_index = {}; prev_n = -1; stall = 0; scrolls = 0; end = False
    while scrolls < int(max_scrolls) and stall < int(stall_limit) and len(found) < int(max_results):
        try:
            feed = page.locator('div[role="feed"]').first
            if feed.count() > 0:
                items = feed.locator('div.Nv2PK, div[role="article"]')
            else:
                items = page.locator('div.Nv2PK, div[role="article"]')
        except Exception:
            items = page.locator('div.Nv2PK, div[role="article"]')
        try: n = items.count()
        except Exception: n = 0
        before = len(found)
        for i in range(n):
            if len(found) >= int(max_results): break
            try:
                rec = _card(items.nth(i), q, center=(lat,lng) if lat is not None else None)
                if not rec: continue
                _upsert_place(found, alias_index, rec)
            except Exception:
                continue

        # If Google changed the result-card wrapper, use global DOM + raw HTML place URLs.
        if n == 0 or (len(found) == before and scrolls >= 1):
            try:
                extras=_extract_global_maps_links(page,q,max_results=max_results,
                    center=(lat,lng) if lat is not None else None,source="google_maps_fallback")
                for rec in extras:
                    if len(found)>=int(max_results): break
                    _upsert_place(found,alias_index,rec)
            except Exception:
                pass

        end = _feed_end(page)
        if end: break
        if n == prev_n and len(found) == before: stall += 1
        else: stall = 0
        prev_n = n; scrolls += 1
        try:
            page.evaluate("""() => {const f=document.querySelector('div[role="feed"]');if(f) f.scrollTop=f.scrollHeight;}""")
        except Exception:
            try: page.mouse.wheel(0, 1800)
            except Exception: pass
        page.wait_for_timeout(int(wait_ms))
    return list(found.values()), {"end_reached":bool(end),"saturated":len(found) >= int(max_results) and not end}


def _search_view(page, q, lat=None, lng=None, zoom=13, max_results=80, lang="ar", fast=False):
    """Search one viewport with URL/API/search-box fallbacks.

    A zero-result parse no longer immediately means a zero-result search: the
    worker retries through alternative Google Maps entry URLs and finally the
    visible search box when available.
    """
    encoded = quote_plus(q)
    urls = []
    base = "https://www.google.com/maps/search/" + encoded
    if lat is not None and lng is not None:
        urls.append(base + "/@{:.6f},{:.6f},{}z?hl={}".format(lat, lng, int(zoom), lang))
    urls.append(base + "?hl=" + lang)
    urls.append("https://www.google.com/maps/search/?api=1&query={}&hl={}".format(encoded, lang))

    last_stat = {"end_reached":False,"saturated":False}
    for attempt_url in urls:
        try:
            _goto_retry(page, attempt_url, timeout=45000, attempts=3)
            dismiss_consent(page)
            blocked=_google_block_reason(page)
            if blocked:
                _progress("google_block", reason=blocked, url=page.url[:300])
                raise RuntimeError("google_block:"+blocked)
            try: page.wait_for_selector('div[role="feed"], a[href*="/maps/place/"], h1.DUwDvf, #searchboxinput', timeout=9000)
            except Exception: page.wait_for_timeout(1200)
            if "/maps/place/" in page.url and page.locator('div[role="feed"]').count() == 0:
                info = extract_info(page, with_contacts=False); info["url"] = page.url
                if clean(info.get("name")):
                    return [info], {"end_reached":True,"saturated":False,"entry":"place"}
            seed=_extract_global_maps_links(page,q,max_results=max_results,
                    center=(lat,lng) if lat is not None else None,source="google_maps_url_seed")
            recs, stat = _collect_search_cards(page, q, lat, lng, max_results,
                                                max_scrolls=14 if fast else 60,
                                                stall_limit=3 if fast else 6,
                                                wait_ms=260 if fast else 650)
            if seed:
                merged={}; ma={}
                for r in seed+recs: _upsert_place(merged,ma,r)
                recs=list(merged.values())[:int(max_results)]
            stat["entry"] = "url"
            stat["global_seed"] = len(seed)
            if recs:
                return recs, stat
            last_stat = stat
        except Exception:
            continue

    # Final fallback: type directly into the Maps search box.
    try:
        _goto_retry(page, "https://www.google.com/maps?hl=" + lang, timeout=45000, attempts=2)
        dismiss_consent(page)
        sb = page.locator('#searchboxinput, input[aria-label*="Search" i], input[aria-label*="بحث"]').first
        if sb.count() > 0:
            sb.fill(q, timeout=2500)
            sb.press("Enter")
            page.wait_for_timeout(1600)
            try: page.wait_for_selector('div[role="feed"], a[href*="/maps/place/"], h1.DUwDvf', timeout=8500)
            except Exception: pass
            if "/maps/place/" in page.url and page.locator('div[role="feed"]').count() == 0:
                info = extract_info(page, with_contacts=False); info["url"] = page.url
                if clean(info.get("name")):
                    return [info], {"end_reached":True,"saturated":False,"entry":"searchbox_place"}
            seed=_extract_global_maps_links(page,q,max_results=max_results,
                    center=(lat,lng) if lat is not None else None,source="google_maps_searchbox_seed")
            recs, stat = _collect_search_cards(page, q, lat, lng, max_results,
                                                max_scrolls=14 if fast else 60,
                                                stall_limit=3 if fast else 6,
                                                wait_ms=260 if fast else 650)
            if seed:
                merged={}; ma={}
                for r in seed+recs: _upsert_place(merged,ma,r)
                recs=list(merged.values())[:int(max_results)]
            stat["entry"] = "searchbox"
            stat["global_seed"] = len(seed)
            if recs: return recs, stat
            last_stat = stat
    except Exception:
        pass
    return [], last_stat


def _unwrap_google_href(href):
    """Normalize a Google Search redirect/local link into a direct URL."""
    href = clean(href)
    if not href:
        return ""
    try:
        if href.startswith("/url?"):
            qs = urllib.parse.parse_qs(urllib.parse.urlparse(href).query)
            href = (qs.get("q") or qs.get("url") or [href])[0]
        if href.startswith("/"):
            href = urllib.parse.urljoin("https://www.google.com", href)
    except Exception:
        pass
    return href


def google_autocomplete_suggestions(query, lang="ar", limit=8):
    """Keyless Google Search suggestions for partial company/store names.

    Uses Google Search's public suggestion response only as a query-expansion hint.
    Failure is non-fatal: the normal Google Search + Maps browser pipeline continues.
    """
    q = clean(query)
    if not q:
        return []
    wanted = max(1, min(int(limit or 8), 12))
    try:
        url = "https://suggestqueries.google.com/complete/search?" + urllib.parse.urlencode({
            "client":"firefox", "hl":str(lang or "ar"), "q":q
        })
        req = urllib.request.Request(url, headers={
            "User-Agent":"Mozilla/5.0 (Windows NT 10.0; Win64; x64) ReviewsTracker/37.0",
            "Accept":"application/json,text/plain,*/*"
        })
        with urllib.request.urlopen(req, timeout=4.5) as r:
            data = json.loads(r.read(256000).decode("utf-8","ignore"))
        raw = data[1] if isinstance(data, list) and len(data) > 1 and isinstance(data[1], list) else []
        base = _norm_place_text(q)
        out = []
        for item in raw:
            txt = clean(item)
            if not txt:
                continue
            nt = _norm_place_text(txt)
            # Keep suggestions related to the typed fragment; this prevents a short
            # partial name from expanding into unrelated popular queries.
            if base and base not in nt and not any(tok and tok in nt for tok in base.split()):
                continue
            if txt not in out:
                out.append(txt)
            if len(out) >= wanted:
                break
        _progress("suggestions", query=q, count=len(out), suggestions=out[:wanted])
        return out[:wanted]
    except Exception as e:
        _progress("suggestions", query=q, count=0, error=str(e)[:160])
        return []


def _query_expansion_plan(query, lang="ar", city="", area="", suggestions=None):
    """Build a Google-style discovery plan while preserving the exact typed intent.

    Suggestions are additive hints; v30 never replaces what the user typed.
    """
    base=clean(query)
    loc=" ".join(x for x in [clean(area),clean(city)] if x)
    out=[]; seen=set()
    def add(value,origin,weight):
        value=clean(value); key=_norm_place_text(value)
        if not value or not key or key in seen: return
        seen.add(key); out.append({"query":value,"origin":origin,"weight":round(float(weight),3)})
    add(base,"typed",1.0)
    if loc: add(" ".join(x for x in [base,loc] if x),"typed_location",.99)
    if suggestions is None:
        suggestions=google_autocomplete_suggestions(base,lang=lang,limit=8) if len(base)>=2 else []
    for sug in suggestions or []:
        sug=clean(sug)
        if not sug: continue
        add(sug,"suggestion",.96)
        if loc: add(" ".join([sug,loc]),"suggestion_location",.95)
    low=_norm_place_text(base)
    has_branch_word=any(w in low for w in ["فرع","فروع","branch","branches","location","locations"])
    if base and not has_branch_word:
        if str(lang).lower().startswith("ar"):
            add("فروع {}".format(base),"branch_intent",.91)
            if loc: add("فروع {} {}".format(base,loc),"branch_location",.90)
        add("{} branches".format(base),"branch_intent",.89)
        if loc: add("{} branches {}".format(base,loc),"branch_location",.88)
        add("{} locations".format(base),"location_intent",.87)
    return out[:14]


def _query_variant_weight(plan, query):
    target=_norm_place_text(query)
    for row in plan or []:
        if _norm_place_text(row.get("query"))==target:
            try: return float(row.get("weight") or 1.0)
            except Exception: return 1.0
    return 1.0


def _auto_query_variants(query, lang="ar", city="", area=""):
    """Backward-compatible list view over the v30 query expansion plan."""
    return [row["query"] for row in _query_expansion_plan(query,lang=lang,city=city,area=area)]

def _relevance_band(score):
    try: s = float(score or 0)
    except Exception: s = 0
    if s >= .78: return "strong"
    if s >= .58: return "related"
    return "weak"


def google_search_browser_seed(page, query, lang="ar", city="", area="", activity="", max_results=20):
    """Fast, keyless Google Search pass that emits Maps/local business links immediately.

    This intentionally uses only the public Google Search UI through the same browser
    engine as Maps. It does not require an API key and does not attempt to bypass
    consent, CAPTCHA, rate limits or other access controls. If Google does not expose
    local/Maps links in the result page, the normal Maps search continues immediately.
    """
    q = " ".join(x for x in [clean(activity), clean(query), clean(area), clean(city)] if x) or clean(query)
    if not q:
        return [], {"provider":"google_search_browser","used":False,"count":0,"error":"empty_query"}
    wanted = max(1, min(int(max_results or 20), 60))
    found = {}; aliases = {}; errors=[]
    urls = [
        "https://www.google.com/search?" + urllib.parse.urlencode({"q":q,"hl":lang}),
        "https://www.google.com/search?" + urllib.parse.urlencode({"q":q,"hl":lang,"tbm":"lcl"}),
    ]
    started=time.perf_counter()
    for url in urls:
        if len(found) >= wanted:
            break
        try:
            _goto_retry(page,url,timeout=22000,attempts=2)
            dismiss_consent(page)
            blocked=_google_block_reason(page)
            if blocked:
                _progress("google_block", provider="google_search_browser", reason=blocked, url=page.url[:300])
                raise RuntimeError("google_block:"+blocked)
            page.wait_for_timeout(500)
            try:
                page.wait_for_selector('a[href*="/maps/"], a[href*="google.com/maps"], a[href*="maps.google"]',timeout=3500)
            except Exception:
                pass
            anchors=page.locator('a')
            n=min(anchors.count(),700)
            for i in range(n):
                if len(found)>=wanted:
                    break
                a=anchors.nth(i)
                try:
                    href=_unwrap_google_href(a.get_attribute('href',timeout=250) or '')
                except Exception:
                    continue
                low=href.lower()
                if not href or not (('google.' in low and '/maps' in low) or 'maps.google.' in low):
                    continue
                if '/dir/' in low or '/maps/dir' in low:
                    continue
                try:
                    name=clean(a.get_attribute('aria-label',timeout=200) or '')
                except Exception:
                    name=''
                if not name:
                    try: name=clean(a.inner_text(timeout=250) or '')
                    except Exception: name=''
                # Avoid generic Google navigation anchors.
                if not name or _norm_place_text(name) in {'maps','google maps','خرائط google','خرائط جوجل','directions','الاتجاهات'}:
                    m=re.search(r'/maps/place/([^/?#]+)',href)
                    if m: name=clean(unquote(m.group(1)).replace('+',' '))
                if not name:
                    continue
                lat,lon=_latlng_from_url(href)
                tok=_url_place_token(href)
                rec={"name":name,"url":href,"maps_url":href,"latitude":lat,"longitude":lon,
                     "place_id":tok[5:] if tok.startswith('ftid:') else None,
                     "cid":tok[4:] if tok.startswith('cid:') else None,
                     "search_query":q,"source":"google_search_browser","source_fetched_at":datetime.now().isoformat(),
                     "match_score":_brand_score(query,name) if clean(query) else 1.0}
                before=len(found); _upsert_place(found,aliases,rec)
                if len(found)>before:
                    _emit_live_result(rec,'google_search_browser')
            # Google Search can expose Maps links outside ordinary anchors or only in raw HTML.
            for rec in _extract_global_maps_links(page,query,max_results=wanted,source="google_search_maps_seed"):
                if len(found)>=wanted: break
                before=len(found); _upsert_place(found,aliases,rec)
                if len(found)>before: _emit_live_result(rec,"google_search_browser")
            title_expansions=_google_result_title_expansions(page,query,8)
            if found:
                break
        except Exception as e:
            errors.append(str(e)[:180])
    return list(found.values()), {"provider":"google_search_browser","used":True,"count":len(found),
                                  "elapsed_ms":round((time.perf_counter()-started)*1000,1),"errors":errors[-3:],
                                  "title_expansions":locals().get("title_expansions",[])}


def _enrich_branch(page, rec, lang="ar", review_samples=0):
    """Open one Maps branch only when the persistent cache cannot satisfy the request."""
    base=_apply_branch_cache(rec)
    if base.get("cache_hit") and not _should_refresh_details(base,review_samples=review_samples):
        base["cache_served"]=True; base["network_fetch"]=False
        return base,None
    url = clean(base.get("url") or base.get("maps_url"))
    if not url:
        return dict(base), "missing_url"
    try:
        _goto_retry(page, url, timeout=35000, attempts=3)
        dismiss_consent(page)
        blocked=_google_block_reason(page)
        if blocked:
            _progress("google_block", provider="google_maps_detail", reason=blocked, url=page.url[:300])
            raise RuntimeError("google_block:"+blocked)
        try: page.wait_for_selector("h1.DUwDvf, h1", timeout=6500)
        except Exception: page.wait_for_timeout(700)
        info = extract_info(page, with_contacts=True)
        info["url"] = page.url or url
        info["source"] = "google_maps_detail"
        info["detail_fetched_at"] = datetime.now().isoformat()
        merged = _merge(base, info)
        merged["details_loaded"] = True
        merged["cache_served"]=False; merged["network_fetch"]=True; merged["cache_refreshed"]=bool(base.get("cache_hit"))
        merged["field_sources"] = _field_provenance(merged, "google_maps_detail")
        if int(review_samples or 0) > 0 and int(merged.get("review_count") or 0) > 0:
            try:
                if open_reviews(page):
                    samples=extract_reviews(page,max(1,min(int(review_samples),8)))
                    if samples:
                        merged["preview_reviews"]=samples
                        merged["preview_review_count"]=len(samples)
                        merged["review_samples_fetched_at"]=datetime.now().isoformat()
            except Exception:
                pass
        try: _branch_cache_store(merged)
        except Exception as exc: _diag("branch_cache_store_failed",exc,url=url)
        _apply_result_metrics("",merged)
        return merged, None
    except Exception as e:
        base["details_loaded"] = False
        return base, str(e)[:160]


def _enrich_results(page, results, lang="ar"):
    enriched, errors = [], []
    total = len(results)
    for i, rec in enumerate(results):
        item, err = _enrich_branch(page, rec, lang, review_samples=(3 if auto_mode and i < 8 else 0))
        item["detail_index"] = i + 1
        item["detail_total"] = total
        enriched.append(item)
        if err: errors.append("{}: {}".format(clean(rec.get("name"))[:80], err))
    return enriched, errors


def _confidence_score(query, rec):
    """Confidence = brand-match confidence + identity strength + location + detail/contact evidence."""
    match = max(0.0, min(1.0, float(rec.get("match_score") or _brand_score(query, rec.get("name")))))
    components = {
        "name_match": round(match * 45.0, 1),
        "identity": 0.0,
        "location": 0.0,
        "details": 0.0,
        "contact": 0.0,
        "evidence": 0.0,
    }
    if rec.get("place_id") and rec.get("cid"): components["identity"] = 18.0
    elif rec.get("place_id") or rec.get("cid") or _url_place_token(rec.get("url") or rec.get("maps_url")): components["identity"] = 14.0
    if rec.get("latitude") is not None and rec.get("longitude") is not None: components["location"] += 7.0
    if clean(rec.get("address")): components["location"] += 5.0
    if rec.get("details_loaded"): components["details"] += 7.0
    if rec.get("category"): components["details"] += 2.0
    if rec.get("phone") or rec.get("mobile"): components["contact"] += 4.0
    if rec.get("website"): components["contact"] += 3.0
    if rec.get("email"): components["contact"] += 2.0
    if rec.get("rating") is not None: components["evidence"] += 2.0
    if rec.get("review_count") not in (None, ""): components["evidence"] += 2.0
    # Low name similarity is a strong warning even if the listing is otherwise complete.
    score = sum(components.values())
    if query and match < .50: score = min(score, 49.0)
    elif query and match < .65: score = min(score, 64.0)
    rec["confidence_components"] = components
    return round(max(0.0, min(100.0, score)), 1)


# === v25.0 async-first Google/Maps browser engine ===
try:
    from playwright.async_api import async_playwright, TimeoutError as PlaywrightAsyncTimeoutError
except Exception:
    async_playwright = None
    PlaywrightAsyncTimeoutError = Exception


async def _async_launch_browser(p, exclude=None):
    """Launch the best available browser, preferring the last browser that reached Google.

    ``exclude`` is used by v25.0 browser failover so a second attempt is genuinely a
    different engine/executable rather than immediately reopening the same browser.
    """
    errors=[]; exclude={clean(x) for x in (exclude or set()) if clean(x)}
    health=_load_browser_health()
    chromium_args=["--disable-dev-shm-usage","--no-sandbox","--disable-background-networking","--disable-default-apps","--no-first-run"]
    candidates=_order_browser_candidates(_system_browser_candidates(),health)
    for name,exe in candidates:
        key="{}|{}".format(name,exe)
        if name in exclude or key in exclude: continue
        try:
            browser=await p.chromium.launch(executable_path=exe,headless=True,args=chromium_args)
            _progress("browser",engine=name,status="ok",executable=exe,async_mode=True,cached_preferred=(health.get("preferred_engine")==name))
            return browser,name
        except Exception as exc:
            errors.append("{}: {}".format(name,str(exc)[:180])); _diag("async_system_browser_launch_failed",exc,engine=name,executable=exe)
            _record_browser_health(name,"failed","launch: "+str(exc)[:180],exe)
    attempts=[
        ("chrome", p.chromium, {"channel":"chrome","headless":True,"args":chromium_args}),
        ("msedge", p.chromium, {"channel":"msedge","headless":True,"args":chromium_args}),
        ("chromium", p.chromium, {"headless":True,"args":chromium_args}),
        ("firefox", p.firefox, {"headless":True,"args":["--no-remote"]}),
    ]
    # If a channel/browser was the last healthy choice, try it first.
    preferred=clean(health.get("preferred_engine"))
    attempts.sort(key=lambda x: 0 if preferred and x[0]==preferred else 1)
    for name,browser_type,kwargs in attempts:
        if name in exclude: continue
        try:
            browser=await browser_type.launch(**kwargs)
            _progress("browser",engine=name,status="ok",async_mode=True,cached_preferred=(preferred==name))
            return browser,name
        except Exception as exc:
            errors.append("{}: {}".format(name,str(exc)[:180]))
            _diag("async_browser_launch_failed",exc,engine=name)
            _record_browser_health(name,"failed","launch: "+str(exc)[:180])
            _progress("browser",engine=name,status="failed",error=str(exc)[:180],async_mode=True)
    raise RuntimeError("No Playwright browser available | "+" | ".join(errors))


async def _async_setup_context(browser, lang="ar", engine="chromium"):
    locale={"ar":"ar-SA","en":"en-US","ur":"ur-PK","fr":"fr-FR"}.get(lang,"en-US")
    ua=("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/153.0.0.0 Safari/537.36") if engine in ("chrome","msedge","chromium") else (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:129.0) Gecko/20100101 Firefox/129.0")
    ctx=await browser.new_context(locale=locale,viewport={"width":1500,"height":960},user_agent=ua,
                                  extra_http_headers={"Accept-Language":locale+",en;q=0.8"})
    ctx.set_default_timeout(6000)
    async def _route_handler(route):
        try:
            if route.request.resource_type in {"image","font","media"}:
                await route.abort()
            else:
                await route.continue_()
        except Exception as exc:
            _diag("route_handler_failed",exc,level="debug")
    try:
        await ctx.route("**/*",_route_handler)
    except Exception as exc:
        _diag("context_route_setup_failed",exc)
    return ctx


async def _async_goto_retry(page,url,timeout=35000,attempts=2):
    last=None
    for i in range(max(1,int(attempts or 1))):
        try:
            await page.goto(url,wait_until="domcontentloaded",timeout=timeout)
            return True
        except Exception as exc:
            last=exc
            _diag("async_goto_failed",exc,url=url,attempt=i+1)
            if i+1<attempts:
                await asyncio.sleep(min(1.5,.35*(2**i)))
    if last:
        raise last
    return False


async def _async_google_block_reason(page):
    try:
        title=clean(await page.title()).lower()
    except Exception:
        title=""
    try:
        body=clean(await page.locator("body").inner_text(timeout=1500)).lower()[:16000]
    except Exception:
        body=""
    state=_classify_browser_page(page.url,title,body)
    return "" if state in ("ok","network_error") else state


async def _async_dismiss_consent(page):
    labels=["Accept all","I agree","Agree","قبول الكل","أوافق","موافقة","Accept","Tout accepter","Alle akzeptieren"]
    for label in labels:
        for sel in ('button:has-text("{}")'.format(label),'[role="button"]:has-text("{}")'.format(label)):
            try:
                loc=page.locator(sel)
                if await loc.count()>0 and await loc.first.is_visible():
                    await loc.first.click(timeout=1200)
                    try:
                        await loc.first.wait_for(state="hidden",timeout=1200)
                    except Exception:
                        pass
                    return True
            except Exception as exc:
                _diag("consent_click_failed",exc,level="debug",selector=sel)
    return False


async def _async_first_text(scope, selectors, timeout=900):
    for sel in selectors:
        try:
            loc=scope.locator(sel)
            if await loc.count()>0:
                value=await loc.first.inner_text(timeout=timeout)
                if value:
                    return clean(value)
        except Exception:
            continue
    return ""


async def _async_first_attr(scope, selectors, name, timeout=900):
    for sel in selectors:
        try:
            loc=scope.locator(sel)
            if await loc.count()>0:
                value=await loc.first.get_attribute(name,timeout=timeout)
                if value:
                    return value
        except Exception:
            continue
    return ""


def _records_from_maps_html(raw,q,max_results=100,center=None,source="google_maps_html"):
    found={}; aliases={}
    raw=html_lib.unescape(str(raw or "")).replace("\\u003d","=").replace("\\u0026","&").replace("\\/","/")
    for pat in (RE_MAPS_PLACE_URL,RE_MAPS_REL_PLACE,RE_MAPS_CID_URL,RE_MAPS_REL_CID):
        for match in pat.finditer(raw):
            if len(found)>=max_results:
                break
            href=_normalize_maps_href(match.group(0).rstrip("\\,;"))
            name=""
            m=re.search(r"/maps/place/([^/?#]+)",href,re.I)
            if m:
                try:
                    name=clean(unquote(m.group(1)).replace("+"," "))
                except Exception:
                    name=""
            if not name and RE_CID.search(href):
                name=clean(q)
            rec=_record_from_maps_href(href,q,name,center,source)
            if rec:
                _upsert_place(found,aliases,rec)
        if len(found)>=max_results:
            break
    return list(found.values())


async def _async_strategy_semantic_anchors(page,q,wanted,center,source):
    out=[]
    selector='a[href*="/maps/place/"],a[href*="/maps?cid="],a[href*="google.com/maps"],a[href*="maps.google."]'
    anchors=page.locator(selector)
    n=min(await anchors.count(),wanted*10)
    for i in range(n):
        if len(out)>=wanted: break
        a=anchors.nth(i)
        try: href=await a.get_attribute("href",timeout=350) or ""
        except Exception: href=""
        name=""
        for attr in ("aria-label","title"):
            try:
                name=clean(await a.get_attribute(attr,timeout=250) or "")
                if name: break
            except Exception: pass
        if not name:
            try: name=clean(await a.inner_text(timeout=300) or "")
            except Exception: pass
        rec=_record_from_maps_href(href,q,name,center,source)
        if rec: out.append(rec)
    return out


async def _async_strategy_feed_cards(page,q,wanted,center,source):
    out=[]
    items=page.locator('div[role="feed"] div[role="article"],div[role="feed"] div.Nv2PK,div[role="article"]')
    n=min(await items.count(),wanted*6)
    for i in range(n):
        if len(out)>=wanted: break
        item=items.nth(i)
        link=item.locator('a[href*="/maps/place/"],a[href*="/maps?cid="],a[href*="maps.google."]').first
        try:
            if await link.count()<=0: continue
            href=await link.get_attribute("href",timeout=350) or ""
        except Exception:
            continue
        name=""
        for getter in ((link,"aria-label"),(item,"aria-label")):
            try:
                name=clean(await getter[0].get_attribute(getter[1],timeout=250) or "")
                if name: break
            except Exception: pass
        if not name:
            try:
                heading=item.locator('[role="heading"],.fontHeadlineSmall,.qBF1Pd').first
                if await heading.count()>0: name=clean(await heading.inner_text(timeout=300) or "")
            except Exception: pass
        rec=_record_from_maps_href(href,q,name,center,source)
        if rec: out.append(rec)
    return out


async def _async_strategy_legacy_cards(page,q,wanted,center,source):
    out=[]
    links=page.locator('a.hfpxzc')
    n=min(await links.count(),wanted*5)
    for i in range(n):
        if len(out)>=wanted: break
        a=links.nth(i)
        try: href=await a.get_attribute('href',timeout=350) or ''
        except Exception: href=''
        try: name=clean(await a.get_attribute('aria-label',timeout=250) or '')
        except Exception: name=''
        if not name:
            try: name=clean(await a.inner_text(timeout=250) or '')
            except Exception: pass
        rec=_record_from_maps_href(href,q,name,center,source)
        if rec: out.append(rec)
    return out


async def _async_strategy_raw_html(page,q,wanted,center,source):
    raw=await page.content()
    return _records_from_maps_html(raw,q,wanted,center,source)


async def _async_extract_global_maps_links(page,q,max_results=100,center=None,source="google_maps_async",telemetry=None):
    """Extract Maps entities using health-ranked strategies with persistent telemetry.

    Strategies that keep producing usable entities rise in priority. Strategies that
    repeatedly return zero or raise errors are automatically demoted, but remain as
    fallbacks so a later Google layout change can make them healthy again.
    """
    found={}; aliases={}; wanted=max(1,min(int(max_results or 100),500))
    telemetry=telemetry if isinstance(telemetry,dict) else _new_extraction_telemetry(q)
    order=_selector_strategy_order(DISCOVERY_EXTRACTION_STRATEGIES)
    telemetry["strategy_order"]=list(order)
    runners={
        "semantic_anchors":_async_strategy_semantic_anchors,
        "feed_cards":_async_strategy_feed_cards,
        "legacy_cards":_async_strategy_legacy_cards,
        "raw_html":_async_strategy_raw_html,
    }
    for strategy in order:
        if len(found)>=wanted: break
        runner=runners.get(strategy)
        if runner is None: continue
        started=time.perf_counter(); rows=[]; error=""
        try:
            rows=await runner(page,q,wanted,center,source+"_"+strategy)
            for rec in rows:
                if isinstance(rec,dict): rec.setdefault("selector_strategy",strategy)
                _upsert_place(found,aliases,rec)
                if len(found)>=wanted: break
        except Exception as exc:
            error="{}: {}".format(type(exc).__name__,str(exc)[:220])
            _diag("async_extract_strategy_failed",exc,query=q,strategy=strategy)
        elapsed=round((time.perf_counter()-started)*1000,3)
        _telemetry_strategy_event(telemetry,strategy,count=len(rows),error=error,elapsed_ms=elapsed)
        _record_selector_strategy(strategy,count=len(rows),error=error,elapsed_ms=elapsed)
    return list(found.values())


async def _async_google_title_expansions(page,query,limit=8):
    out=[]; base=_norm_place_text(query)
    for sel in ('a h3','[role="main"] h3','div[role="heading"]','h3'):
        try:
            loc=page.locator(sel); n=min(await loc.count(),80)
            for i in range(n):
                try:
                    txt=clean(await loc.nth(i).inner_text(timeout=260) or "")
                except Exception:
                    continue
                if not txt or len(txt)>180:
                    continue
                score=_brand_score(query,txt) if base else 0
                if score<.34:
                    continue
                txt=re.split(r"\s+[|–—-]\s+",txt,1)[0].strip()
                if txt and txt not in out:
                    out.append(txt)
                if len(out)>=limit:
                    return out
        except Exception:
            continue
    return out


async def _async_maps_discovery_job(context,q,lat=None,lng=None,zoom=13,max_results=100,lang="ar",semaphore=None,source="google_maps_async"):
    sem=semaphore or asyncio.Semaphore(1)
    async with sem:
        page=await context.new_page()
        started=time.perf_counter(); found={}; aliases={}; blocked=""; scrolls=0; stall=0
        telemetry=_new_extraction_telemetry(q)
        try:
            if lat is not None and lng is not None:
                url="https://www.google.com/maps/search/{}/@{:.7f},{:.7f},{}z?hl={}".format(quote_plus(q),float(lat),float(lng),int(zoom),lang)
            else:
                url="https://www.google.com/maps/search/?api=1&query={}&hl={}".format(quote_plus(q),lang)
            await _async_goto_retry(page,url,timeout=35000,attempts=2)
            await _async_dismiss_consent(page)
            try:
                _title=await page.title(); _body=await page.locator("body").inner_text(timeout=1200)
            except Exception:
                _title=""; _body=""
            page_state=_classify_browser_page(page.url,_title,_body)
            if page_state=="network_error":
                _diag("google_maps_network_error",page_state,url=page.url,query=q)
                return [],{"query":q,"page_state":page_state,"elapsed_ms":round((time.perf_counter()-started)*1000,1),"scrolls":0,"url":page.url,
                           "extraction_telemetry":_telemetry_summary(telemetry),"selector_strategy_order":list(telemetry.get("strategy_order") or [])}
            blocked=await _async_google_block_reason(page)
            if blocked:
                _progress("google_block",provider=source,reason=blocked,url=page.url[:300])
                return [],{"query":q,"blocked":blocked,"page_state":blocked,"elapsed_ms":round((time.perf_counter()-started)*1000,1),"scrolls":0,
                           "extraction_telemetry":_telemetry_summary(telemetry),"selector_strategy_order":list(telemetry.get("strategy_order") or [])}
            selector='div[role="feed"],a[href*="/maps/place/"],a[href*="/maps?cid="],a[href*="maps.google."]'
            try:
                await page.wait_for_selector(selector,timeout=6500,state="attached")
            except Exception:
                pass
            max_scrolls=10 if max_results<=100 else 16
            last_count=-1
            for step in range(max_scrolls+1):
                scrolls=step
                for rec in await _async_extract_global_maps_links(page,q,max_results,center=(lat,lng),source=source,telemetry=telemetry):
                    _upsert_place(found,aliases,rec)
                    if len(found)>=max_results:
                        break
                if len(found)>=max_results:
                    break
                anchor_sel='a[href*="/maps/place/"],a[href*="/maps?cid="],a[href*="maps.google."]'
                try:
                    current=await page.locator(anchor_sel).count()
                except Exception:
                    current=len(found)
                if current==last_count:
                    stall+=1
                else:
                    stall=0; last_count=current
                if stall>=3:
                    break
                feed=page.locator('div[role="feed"]')
                try:
                    if await feed.count()>0:
                        text=clean(await feed.first.inner_text(timeout=500)).lower()
                        if any(x in text for x in ("reached the end","نهاية القائمة")):
                            break
                        await feed.first.evaluate("el => { el.scrollTop = el.scrollHeight; }")
                    else:
                        await page.evaluate("() => window.scrollTo(0, document.body.scrollHeight)")
                    try:
                        await page.wait_for_function("prev => document.querySelectorAll('a[href*=\\\"/maps/place/\\\"],a[href*=\\\"/maps?cid=\\\"],a[href*=\\\"maps.google.\\\"]').length > prev",arg=current,timeout=1400)
                    except Exception:
                        pass
                except Exception as exc:
                    _diag("async_maps_scroll_failed",exc,query=q)
                    break
            recs=list(found.values())
            saturated=bool(len(recs)>=max_results or (scrolls>=max_scrolls and stall<3))
            return recs,{"query":q,"blocked":"","elapsed_ms":round((time.perf_counter()-started)*1000,1),"scrolls":scrolls,"found":len(recs),"source":source,
                         "saturated":saturated,"subdivided":0,
                         "extraction_telemetry":_telemetry_summary(telemetry),"selector_strategy_order":list(telemetry.get("strategy_order") or [])}
        except Exception as exc:
            _diag("async_maps_job_failed",exc,query=q,lat=lat,lng=lng)
            nav_state=_classify_navigation_error(exc)
            return [],{"query":q,"error":str(exc)[:220],"page_state":nav_state or "navigation_error",
                       "elapsed_ms":round((time.perf_counter()-started)*1000,1),"scrolls":scrolls,"source":source,
                       "extraction_telemetry":_telemetry_summary(telemetry),"selector_strategy_order":list(telemetry.get("strategy_order") or [])}
        finally:
            try:
                await page.close()
            except Exception:
                pass


async def _async_google_seed_job(context,q,lang="ar",max_results=30,semaphore=None):
    sem=semaphore or asyncio.Semaphore(1)
    async with sem:
        page=await context.new_page(); started=time.perf_counter(); telemetry=_new_extraction_telemetry(q)
        try:
            url="https://www.google.com/search?"+urllib.parse.urlencode({"q":q,"hl":lang,"num":20})
            await _async_goto_retry(page,url,timeout=28000,attempts=2)
            await _async_dismiss_consent(page)
            try:
                _title=await page.title(); _body=await page.locator("body").inner_text(timeout=1200)
            except Exception:
                _title=""; _body=""
            page_state=_classify_browser_page(page.url,_title,_body)
            if page_state=="network_error":
                return [],{"query":q,"page_state":page_state,"title_expansions":[],"elapsed_ms":round((time.perf_counter()-started)*1000,1),"url":page.url,
                           "extraction_telemetry":_telemetry_summary(telemetry),"selector_strategy_order":list(telemetry.get("strategy_order") or [])}
            blocked=await _async_google_block_reason(page)
            if blocked:
                return [],{"query":q,"blocked":blocked,"page_state":blocked,"title_expansions":[],"elapsed_ms":round((time.perf_counter()-started)*1000,1),
                           "extraction_telemetry":_telemetry_summary(telemetry),"selector_strategy_order":list(telemetry.get("strategy_order") or [])}
            try:
                await page.wait_for_selector('a[href*="/maps"],a[href*="maps.google"],h3',timeout=4500,state="attached")
            except Exception:
                pass
            recs=await _async_extract_global_maps_links(page,q,max_results,source="google_search_browser_async",telemetry=telemetry)
            titles=await _async_google_title_expansions(page,q,8)
            return recs,{"query":q,"blocked":"","title_expansions":titles,"elapsed_ms":round((time.perf_counter()-started)*1000,1),"found":len(recs),
                         "extraction_telemetry":_telemetry_summary(telemetry),"selector_strategy_order":list(telemetry.get("strategy_order") or [])}
        except Exception as exc:
            _diag("async_google_seed_failed",exc,query=q)
            nav_state=_classify_navigation_error(exc)
            return [],{"query":q,"error":str(exc)[:220],"page_state":nav_state or "navigation_error",
                       "title_expansions":[],"elapsed_ms":round((time.perf_counter()-started)*1000,1),
                       "extraction_telemetry":_telemetry_summary(telemetry),"selector_strategy_order":list(telemetry.get("strategy_order") or [])}
        finally:
            try:
                await page.close()
            except Exception:
                pass


def _dns_probe(host):
    try:
        rows=socket.getaddrinfo(str(host),443,type=socket.SOCK_STREAM)
        return True, sorted({r[4][0] for r in rows if r and r[4]})[:4], ""
    except Exception as exc:
        return False, [], "{}: {}".format(type(exc).__name__,str(exc)[:220])


def _google_http_probe(url="https://www.google.com/generate_204",timeout=6.0):
    try:
        req=urllib.request.Request(url,headers={"User-Agent":"Mozilla/5.0 ReviewsTracker/37.0 FieldPreflight"})
        with urllib.request.urlopen(req,timeout=float(timeout)) as r:
            code=int(getattr(r,"status",200) or 200)
        return code in {200,204},code,""
    except Exception as exc:
        return False,None,"{}: {}".format(type(exc).__name__,str(exc)[:260])


async def async_field_preflight(query="مطعم الرياض",lang="ar",live=True):
    """Verify the actual Windows/browser/network path before a user search starts."""
    started=time.perf_counter(); query=clean(query) or "مطعم الرياض"
    dg,ips_g,err_g=await asyncio.to_thread(_dns_probe,"www.google.com")
    dm,ips_m,err_m=await asyncio.to_thread(_dns_probe,"maps.google.com")
    http_ok,http_code,http_err=await asyncio.to_thread(_google_http_probe)
    candidates=_system_browser_candidates()
    checks={
        "dns_google":bool(dg),"dns_maps":bool(dm),"dns_google_ips":ips_g,"dns_maps_ips":ips_m,
        "dns_google_error":err_g,"dns_maps_error":err_m,"google_http":bool(http_ok),"google_http_status":http_code,
        "google_http_error":http_err,"browser_candidates":[n for n,_ in candidates],
        "browser_candidate_paths":[p for _,p in candidates],"playwright":bool(async_playwright is not None),
        "proxy_configured":any(bool(os.environ.get(k)) for k in ("HTTP_PROXY","HTTPS_PROXY","ALL_PROXY")),
        "maps_page_state":"not_run","maps_results":0,"browser_launched":False,"browser_engine":"","errors":[],
    }
    if not live:
        report=_build_preflight_report_from_checks(checks); report["elapsed_ms"]=round((time.perf_counter()-started)*1000,1); return report
    if async_playwright is None:
        checks["maps_page_state"]="playwright_unavailable"; checks["errors"].append("playwright.async_api unavailable")
        report=_build_preflight_report_from_checks(checks); report["elapsed_ms"]=round((time.perf_counter()-started)*1000,1); return report
    browser=None; context=None
    try:
        async with async_playwright() as p:
            browser,engine=await _async_launch_browser(p)
            checks["browser_launched"]=True; checks["browser_engine"]=engine
            context=await _async_setup_context(browser,lang,engine)
            recs,meta=await _async_maps_discovery_job(context,query,max_results=5,lang=lang,semaphore=asyncio.Semaphore(1),source="google_maps_preflight")
            checks["maps_results"]=len(recs or [])
            checks["maps_page_state"]=clean((meta or {}).get("page_state") or (meta or {}).get("blocked") or "ok") or "ok"
            checks["maps_loaded"]=checks["maps_page_state"]=="ok"
            checks["best_selector_strategy"]=((meta or {}).get("extraction_telemetry") or {}).get("best_strategy") or ""
            checks["selector_strategy_order"]=(meta or {}).get("selector_strategy_order") or []
            if (meta or {}).get("error"): checks["errors"].append(str(meta.get("error"))[:260])
    except Exception as exc:
        state=_classify_navigation_error(exc) or "browser_launch_error"
        checks["maps_page_state"]=state; checks["errors"].append("{}: {}".format(type(exc).__name__,str(exc)[:300]))
        _diag("field_preflight_failed",exc,query=query,state=state)
    finally:
        if context is not None:
            try: await context.close()
            except Exception: pass
        if browser is not None:
            try: await browser.close()
            except Exception: pass
    report=_build_preflight_report_from_checks(checks)
    report["elapsed_ms"]=round((time.perf_counter()-started)*1000,1)
    report["query"]=query; report["selector_health"]=_selector_health_snapshot()[:8]; report["browser_health"]=_load_browser_health()
    return report


def field_preflight(query="مطعم الرياض",lang="ar",live=True):
    try:
        return asyncio.run(async_field_preflight(query,lang,live))
    except Exception as exc:
        checks={"dns_google":False,"dns_maps":False,"playwright":bool(async_playwright is not None),"browser_candidates":[],
                "maps_page_state":_classify_navigation_error(exc) or "preflight_error","errors":["{}: {}".format(type(exc).__name__,str(exc)[:300])]}
        report=_build_preflight_report_from_checks(checks); report["error"]=checks["errors"][0]; return report


async def _async_extract_info(page,with_contacts=True):
    lat,lng=_latlng_from_url(page.url)
    info={"name":"","rating":None,"review_count":None,"address":None,"phone":None,"mobile":None,
          "email":None,"email_source":None,"contact_checked_at":None,"website":None,"category":None,
          "cid":None,"place_id":None,"latitude":lat,"longitude":lng,"url":page.url,
          "source":"google_maps_detail_async","source_fetched_at":datetime.now().isoformat()}
    info["name"]=await _async_first_text(page,['[role="main"] h1','h1[aria-label]','h1','h1.DUwDvf'])
    rating_label=await _async_first_attr(page,['[role="img"][aria-label*="star" i]','[role="img"][aria-label*="نجمة"]','div.F7nice span[role="img"]'],'aria-label')
    info["rating"]=parse_rating(rating_label) or parse_rating(await _async_first_text(page,['span[aria-hidden="true"]','span.MW4etd']))
    cnt=await _async_first_attr(page,['button[aria-label*="review" i]','button[aria-label*="مراجعة"]'],'aria-label')
    if not cnt:
        cnt=await _async_first_text(page,['div.F7nice button','button.HHrUdb'])
    m=re.search(r"([\d.,\s]+)",_ascii_digits(cnt or ""))
    if m:
        digits=re.sub(r"[^\d]","",m.group(1))
        if digits:
            info["review_count"]=int(digits)
    address=await _async_first_text(page,["button[data-item-id='address']","button[aria-label^='Address' i]","button[aria-label^='العنوان']"])
    if address:
        for prefix in ("العنوان","Address"):
            if address.startswith(prefix):
                address=address[len(prefix):].strip(": -")
        info["address"]=address
    phone=await _async_first_text(page,["button[data-item-id^='phone:']","button[aria-label^='Phone' i]","button[aria-label^='الهاتف']"])
    if phone:
        for prefix in ("الهاتف","Phone"):
            if phone.startswith(prefix):
                phone=phone[len(prefix):].strip(": -")
        pf=_split_phone_fields(phone); info["phone"]=pf.get("phone"); info["mobile"]=pf.get("mobile")
    info["website"]=_normalize_official_website(await _async_first_attr(page,["a[data-item-id='authority']","a[aria-label*='Website' i]","a[aria-label*='الموقع']"],"href")) or None
    info["category"]=await _async_first_text(page,["button[jsaction*='category']","button[aria-label*='Category' i]","button.DkEaL"])
    m=RE_CID.search(page.url)
    if m:
        info["cid"]=m.group(1)
    m=RE_PLACE_ID.search(page.url)
    if m:
        info["place_id"]=m.group(1)
    if not info.get("place_id"):
        m=RE_FTID.search(page.url)
        if m:
            info["place_id"]="ftid:"+unquote(m.group(1))
    if with_contacts:
        try:
            mobile,email,src=await asyncio.to_thread(extract_contacts,info.get("website"),info.get("phone"))
            info["mobile"]=mobile; info["email"]=email; info["email_source"]=src
            info["contact_checked_at"]=datetime.now().isoformat()
        except Exception as exc:
            _diag("async_contact_extract_failed",exc,url=info.get("website"))
    return info


async def _async_open_reviews(page):
    selectors=['button[jsaction*="pane.reviewChart.moreReviews"]','button[aria-label*="review" i]',
               'button[aria-label*="مراجعة"]','button:has-text("المراجعات")','button:has-text("Reviews")']
    for sel in selectors:
        try:
            loc=page.locator(sel)
            if await loc.count()>0 and await loc.first.is_visible():
                await loc.first.click(timeout=3500)
                try:
                    await page.wait_for_selector('div[data-review-id]',timeout=5500,state="attached")
                except Exception:
                    pass
                return True
        except Exception:
            continue
    return False


async def _async_sort_reviews_newest(page):
    for sel in ['button[aria-label*="Sort reviews" i]','button[aria-label*="فرز"]','button[data-value*="Sort" i]']:
        try:
            loc=page.locator(sel)
            if await loc.count()>0 and await loc.first.is_visible():
                await loc.first.click(timeout=2200)
                for label in ('Newest','Most recent','الأحدث','الأحدث أولاً','الأكثر حداثة'):
                    opt=page.locator('div[role="menuitemradio"]:has-text("{}"),div[role="menuitem"]:has-text("{}")'.format(label,label))
                    if await opt.count()>0 and await opt.first.is_visible():
                        await opt.first.click(timeout=2200)
                        return True
                return False
        except Exception:
            continue
    return False


async def _async_extract_reviews(page,max_reviews,known_review_keys=None):
    reviews=[]; seen=set(); emitted_keys=set(); stall=0; prev=-1
    known=set(str(x) for x in (known_review_keys or set()) if x)
    try:
        await page.wait_for_selector('div[data-review-id]',timeout=6500,state="attached")
    except Exception:
        return reviews
    while len(reviews)<max_reviews and stall<3:
        cards=page.locator('div[data-review-id]')
        n=await cards.count()
        if n==prev:
            stall+=1
        else:
            stall=0; prev=n
        for i in range(n):
            if len(reviews)>=max_reviews:
                break
            card=cards.nth(i)
            try:
                rid=await card.get_attribute('data-review-id',timeout=450) or 'idx-{}-{}'.format(i,n)
            except Exception:
                rid='idx-{}-{}'.format(i,n)
            if rid in seen:
                continue
            seen.add(rid)
            author=await _async_first_text(card,['[aria-label][role="button"]','.d4r55','.WNxzHc'],500)
            text=await _async_first_text(card,['span[lang]','.wiI7pd','.MyEned'],500)
            dstr=await _async_first_text(card,['.rsqaWe','.xRkPPb'],500)
            rating_label=await _async_first_attr(card,['[role="img"][aria-label*="star" i]','[role="img"][aria-label*="نجمة"]','.kvMYJc'],'aria-label',500)
            rating=parse_rating(rating_label)
            owner_resp=await _async_first_text(card,['.CDe7pd','div[class*="response"] span[lang]','.owner-response'],500)
            if text or author:
                item={"review_id":rid,"author":author,"rating":rating,"text":text,"date":dstr,"language":"","owner_response":owner_resp}
                key=review_identity_key(item)
                if key not in known and key not in emitted_keys:
                    emitted_keys.add(key)
                    item["review_identity_key"]=key
                    reviews.append(item)
        _progress("reviews",current=len(reviews),total=max_reviews,async_mode=True)
        if len(reviews)>=max_reviews:
            break
        try:
            await page.evaluate("""() => {const c=document.querySelectorAll('div[data-review-id]'); if(c.length) c[c.length-1].scrollIntoView({block:'end'});}""")
            try:
                await page.wait_for_function("prev => document.querySelectorAll('div[data-review-id]').length > prev",arg=n,timeout=1500)
            except Exception:
                pass
        except Exception:
            break
    return reviews


async def _async_enrich_branch(context,rec,lang="ar",review_samples=0,semaphore=None,force_refresh=False):
    base=_apply_branch_cache(rec)
    if base.get("cache_hit") and not force_refresh and not _should_refresh_details(base,review_samples=review_samples):
        base["cache_served"]=True; base["network_fetch"]=False
        return base,None
    sem=semaphore or asyncio.Semaphore(1)
    async with sem:
        url=clean(base.get("url") or base.get("maps_url"))
        if not url:
            return dict(base),"missing_url"
        page=await context.new_page()
        base["network_fetch"]=True; base["cache_refreshed"]=bool(base.get("cache_hit"))
        try:
            await _async_goto_retry(page,url,timeout=32000,attempts=2)
            await _async_dismiss_consent(page)
            blocked=await _async_google_block_reason(page)
            if blocked:
                return base,"google_block:"+blocked
            try:
                await page.wait_for_selector("[role='main'] h1,h1,button[data-item-id='address'],a[data-item-id='authority']",timeout=5500,state="attached")
            except Exception:
                pass
            info=await _async_extract_info(page,with_contacts=False)
            info["url"]=page.url or url
            info["detail_fetched_at"]=datetime.now().isoformat()
            merged=_merge(base,info)
            merged["details_loaded"]=True
            merged["cache_served"]=False; merged["network_fetch"]=True; merged["cache_refreshed"]=bool(base.get("cache_hit"))
            if int(review_samples or 0)>0 and int(merged.get("review_count") or 0)>0:
                try:
                    if await _async_open_reviews(page):
                        await _async_sort_reviews_newest(page)
                        samples=await _async_extract_reviews(page,max(1,min(int(review_samples),8)))
                        if samples:
                            merged["preview_reviews"]=samples
                            merged["preview_review_count"]=len(samples)
                            merged["review_samples_fetched_at"]=datetime.now().isoformat()
                except Exception as exc:
                    _diag("async_review_preview_failed",exc,url=url)
            return merged,None
        except Exception as exc:
            _diag("async_detail_failed",exc,url=url)
            base["details_loaded"]=False
            return base,str(exc)[:180]
        finally:
            try:
                await page.close()
            except Exception:
                pass


def _contact_enrichment_priority(rec):
    rec=dict(rec or {})
    group={"strong":80,"confirmed":65,"related":35,"additional":5}.get(str(rec.get("result_group") or "additional"),5)
    try: match=float(rec.get("match_score") or 0.0)
    except Exception: match=0.0
    missing=sum(1 for k in ("mobile","email") if not rec.get(k))
    website=20 if rec.get("website") else -100
    checked_penalty=15 if rec.get("contact_checked_at") else 0
    return round(group+match*30+missing*14+website-checked_penalty,2)


def _contact_enrichment_budget(results):
    rows=list(results or []); n=len(rows)
    if not n: return 0
    eligible=[r for r in rows if r.get("website") and (not r.get("mobile") or not r.get("email"))]
    if n<=12: return len(eligible)
    strong=sum(1 for r in eligible if str(r.get("result_group") or "") in {"strong","confirmed"})
    return min(len(eligible),18,max(6,min(16,strong+6)))


async def _async_contact_enrich_record(rec,semaphore=None):
    item=dict(rec or {}); sem=semaphore or asyncio.Semaphore(1)
    if not item.get("website"): return item,None
    async with sem:
        try:
            profile=await asyncio.to_thread(extract_contact_profile,item.get("website"),item.get("phone") or item.get("mobile"))
            fs=dict(item.get("field_sources") or {}); fs.update(profile.get("field_sources") or {})
            for k in ("phone","mobile","email","website","whatsapp"):
                if profile.get(k) not in (None,"") and item.get(k) in (None,""): item[k]=profile.get(k)
            item["field_sources"]=fs
            item["contact_source"]=profile.get("contact_source") or item.get("contact_source") or "official_website"
            item["email_source"]=(profile.get("field_sources") or {}).get("email") or item.get("email_source")
            item["contact_confidence"]=max(float(item.get("contact_confidence") or 0),float(profile.get("contact_confidence") or 0))
            item["contact_checked_at"]=profile.get("contact_checked_at") or datetime.now().isoformat(timespec="seconds")
            item["contact_cache_hit"]=bool(profile.get("contact_cache_hit"))
            try: _branch_cache_store(item)
            except Exception as exc: _diag("contact_branch_cache_store_failed",exc,website=item.get("website"))
            return item,None
        except Exception as exc:
            _diag("async_contact_enrich_failed",exc,website=item.get("website"))
            return item,str(exc)[:180]


async def _async_browser_failover_discovery(p, primary_engine, queries, lang, discover_cap):
    """Try a genuinely different browser when the first engine rendered zero usable places."""
    browser=None; context=None; engine="none"; records=[]; metas=[]
    try:
        browser,engine=await _async_launch_browser(p,exclude={clean(primary_engine)})
        context=await _async_setup_context(browser,lang,engine)
        sem=asyncio.Semaphore(1)
        seen={}; aliases={}
        for rq in [clean(x) for x in (queries or []) if clean(x)][:3]:
            recs,meta=await _async_maps_discovery_job(context,rq,None,None,11,min(int(discover_cap or 50),180),lang,sem,"google_maps_browser_failover_async")
            metas.append(meta)
            if meta.get("blocked") or meta.get("page_state") in {"network_error","environment_blocked"}:
                break
            for rec in recs:
                _upsert_place(seen,aliases,rec)
            if seen:
                break
        records=list(seen.values())
        if records:
            _record_browser_health(engine,"ok")
        elif metas and all((m.get("page_state") in {None,"ok"}) and not m.get("blocked") for m in metas):
            _record_browser_health(engine,"failed","layout-zero")
        return records,{"used":True,"engine":engine,"count":len(records),"queries":[clean(x) for x in (queries or []) if clean(x)][:3],"metas":metas}
    except Exception as exc:
        _diag("browser_failover_failed",exc,primary_engine=primary_engine)
        return [],{"used":True,"engine":engine,"count":0,"error":str(exc)[:300],"queries":[clean(x) for x in (queries or []) if clean(x)][:3]}
    finally:
        if context is not None:
            try: await context.close()
            except Exception as exc: _diag("browser_failover_context_close_failed",exc)
        if browser is not None:
            try: await browser.close()
            except Exception as exc: _diag("browser_failover_browser_close_failed",exc)


async def async_search_places(query,max_results=100,lang="ar",city="",area="",activity="",lat=None,lng=None,
                              radius_km=None,deep=False,exact_name=False,max_tiles=36,details=False,
                              discover_max=None,auto_mode=False,refresh_stale_only=False,resume_file=None):
    if async_playwright is None:
        raise RuntimeError("playwright.async_api unavailable")
    job_id=uuid.uuid4().hex[:12]; started=time.perf_counter(); termination_reason="completed"; errors=[]
    hard_seconds=220 if auto_mode else (420 if deep else 180)
    deadline=time.monotonic()+hard_seconds
    max_results=max(1,min(int(max_results or 100),1000)); max_tiles=max(1,min(int(max_tiles or 36),81))
    if refresh_stale_only:
        details=True
    if auto_mode:
        deep=True; exact_name=False; details=True; max_results=max(max_results,350); max_tiles=max(max_tiles,49)
        discover_cap=max(max_results,min(int(discover_max or 1400),3000))
    else:
        discover_cap=max(max_results,min(int(discover_max or max(max_results*2,300)),3000))
    parts=[clean(activity),clean(query),clean(area),clean(city)]
    search_q=" ".join([x for i,x in enumerate(parts) if x and x not in parts[:i]]) or clean(query)
    resume_fingerprint=_search_resume_fingerprint(query,max_results=max_results,lang=lang,city=city,area=area,activity=activity,
        lat=lat,lng=lng,radius_km=radius_km,deep=bool(deep),exact_name=bool(exact_name),max_tiles=max_tiles,details=bool(details),
        discover_cap=discover_cap,auto_mode=bool(auto_mode),refresh_stale_only=bool(refresh_stale_only))
    checkpoint=_load_search_checkpoint(resume_file,resume_fingerprint,search_q)
    resumed=bool(checkpoint.get("compatible") and (checkpoint.get("results") or checkpoint.get("completed_jobs") or checkpoint.get("completed_details")))
    if resumed:
        checkpoint["resume_count"]=int(checkpoint.get("resume_count") or 0)+1
    elif not checkpoint.get("compatible"):
        checkpoint=_new_search_checkpoint(resume_fingerprint,search_q)
    checkpoint["fingerprint"]=resume_fingerprint; checkpoint["query"]=search_q; checkpoint["status"]="running"; checkpoint["phase"]="start"
    if resume_file: _save_search_checkpoint(resume_file,checkpoint)
    glat,glng,geocoded=lat,lng,None
    _progress("start",job_id=job_id,query=search_q,display_limit=max_results,discovery_limit=discover_cap,auto_mode=bool(auto_mode),async_mode=True,
              resumed=resumed,restored_results=len(checkpoint.get("results") or []),completed_jobs=len(checkpoint.get("completed_jobs") or []),
              resume_count=int(checkpoint.get("resume_count") or 0),checkpoint=str(resume_file or ""))
    if (glat is None or glng is None) and (area or city) and (deep or radius_km):
        try:
            glat,glng,geocoded=await asyncio.to_thread(geocode," ".join(x for x in [area,city] if x),lang)
        except Exception as exc:
            _diag("async_geocode_failed",exc,query=search_q)
    elif glat is not None and glng is not None:
        geocoded="{:.6f},{:.6f}".format(float(glat),float(glng))
    all_found={}; aliases={}; tile_stats=list(checkpoint.get("tile_stats") or []); browser_engine="none"; api_meta={"available":False,"used":False}; api_records=[]
    dedupe_stats={"duplicates_suppressed":0,"near_duplicate_merges":0,"strong_identity_conflicts":0,"inserted":0}
    completed_job_keys=set(map(str,checkpoint.get("completed_jobs") or [])); completed_detail_keys=set(map(str,checkpoint.get("completed_details") or []))
    for rec in list(checkpoint.get("results") or []):
        if isinstance(rec,dict):
            _apply_result_metrics(query,rec); _upsert_place(all_found,aliases,rec,dedupe_stats)
    if resumed and all_found:
        for rec in list(all_found.values())[:120]: _emit_live_result(rec,"resume_checkpoint")
        _progress("resume_loaded",job_id=job_id,found=len(all_found),completed_jobs=len(completed_job_keys),completed_details=len(completed_detail_keys),
                  resume_count=int(checkpoint.get("resume_count") or 0))
    _api_key=_checkpoint_job_key("places_api",search_q)
    if _api_key not in completed_job_keys:
        try:
            api_records,api_meta=await asyncio.to_thread(google_places_api_search,query,lang,city,area,activity,min(discover_cap,60),glat,glng,radius_km)
        except Exception as exc:
            _diag("google_places_optional_failed",exc)
        for rec in api_records:
            _apply_result_metrics(query,rec); _upsert_place(all_found,aliases,rec,dedupe_stats)
        if not api_meta.get("error") or api_records:
            completed_job_keys.add(_api_key)
            checkpoint=_checkpoint_snapshot(checkpoint,all_found,tile_stats,completed_job_keys,completed_detail_keys,"places_api")
            if resume_file: _save_search_checkpoint(resume_file,checkpoint)
    else:
        api_meta={"available":bool(os.environ.get("GOOGLE_MAPS_API_KEY") or os.environ.get("GOOGLE_PLACES_API_KEY")),"used":False,"resumed":True,"count":0}
    expansion_plan=_query_expansion_plan(query,lang=lang,city=city,area=area) if auto_mode else [{"query":search_q,"origin":"typed","weight":1.0}]
    variants=[row.get("query") for row in expansion_plan if clean(row.get("query"))]
    if not variants:
        variants=[search_q]; expansion_plan=[{"query":search_q,"origin":"typed","weight":1.0}]
    variants=variants[:14]
    if deep and glat is not None and glng is not None:
        centers=_coverage(glat,glng,radius_km=radius_km,max_tiles=min(max_tiles,32 if auto_mode else max_tiles))
    else:
        centers=[(glat,glng)] if glat is not None and glng is not None else [(None,None)]
    discovery_sem=asyncio.Semaphore(ASYNC_DISCOVERY_CONCURRENCY)
    detail_sem=asyncio.Semaphore(ASYNC_DETAIL_CONCURRENCY)
    contact_sem=asyncio.Semaphore(ASYNC_CONTACT_CONCURRENCY)
    google_web_meta={"used":True,"count":0,"queries":[]}; recovery_attempts=[]
    extraction_metas=[]
    browser_engines_tried=[]; browser_failover_meta={"used":False}; partial_results_preserved=False
    async with async_playwright() as p:
        browser,browser_engine=await _async_launch_browser(p)
        browser_engines_tried.append(browser_engine)
        context=None
        try:
            context=await _async_setup_context(browser,lang,browser_engine)
            tasks=[]
            skipped_completed_jobs=0
            for sq in variants[:4]:
                _jk=_checkpoint_job_key("seed",sq)
                if _jk in completed_job_keys:
                    skipped_completed_jobs+=1
                    continue
                async def _seed_task(sq=sq,jk=_jk):
                    recs,meta=await _async_google_seed_job(context,sq,lang,min(45,discover_cap),discovery_sem)
                    return jk,"seed",sq,recs,meta
                tasks.append(asyncio.create_task(_seed_task()))
            map_jobs=[]
            for ci,c in enumerate(centers):
                map_jobs.append((variants[0],c,ci))
            for vi,vq in enumerate(variants[1:],start=1):
                map_jobs.append((vq,centers[0],-vi))
            map_jobs=map_jobs[:ASYNC_DISCOVERY_JOB_CAP]
            for vq,(clat,clng),idx in map_jobs:
                _jk=_checkpoint_job_key("map",vq,(clat,clng) if clat is not None else None,idx)
                if _jk in completed_job_keys:
                    skipped_completed_jobs+=1
                    continue
                async def _map_task(vq=vq,clat=clat,clng=clng,idx=idx,jk=_jk):
                    recs,meta=await _async_maps_discovery_job(context,vq,clat,clng,zoom_for(radius_km or 18),
                                                             (min(140,discover_cap) if deep else min(discover_cap,max(20,max_results*3))),lang,discovery_sem,"google_maps_async")
                    meta["tile"]=idx; meta["center"]=[clat,clng] if clat is not None else None
                    return jk,"map",vq,recs,meta
                tasks.append(asyncio.create_task(_map_task()))
            if resumed:
                _progress("resume_plan",job_id=job_id,remaining_jobs=len(tasks),skipped_completed_jobs=skipped_completed_jobs,restored_results=len(all_found))
            completed=0
            for fut in asyncio.as_completed(tasks):
                if time.monotonic()>=deadline:
                    termination_reason="deadline_reached"
                    for t in tasks:
                        if not t.done(): t.cancel()
                    break
                try:
                    job_key,kind,sq,recs,meta=await fut
                except asyncio.CancelledError:
                    continue
                except Exception as exc:
                    errors.append("async_job: "+str(exc)[:180]); _diag("async_discovery_task_failed",exc); continue
                completed+=1
                extraction_metas.append(meta)
                if meta.get("blocked"):
                    errors.append("google_block:"+str(meta.get("blocked")))
                    termination_reason="google_blocked"
                elif meta.get("page_state") in ("network_error","environment_blocked"):
                    state=meta.get("page_state")
                    errors.append("{}:{}".format(state,meta.get("url") or sq))
                    if termination_reason=="completed": termination_reason=state
                if kind=="map":
                    tile_stats.append(meta)
                else:
                    google_web_meta["queries"].append(sq)
                    google_web_meta.setdefault("title_expansions",[]).extend([x for x in meta.get("title_expansions",[]) if x not in google_web_meta.get("title_expansions",[])])
                for rec in recs:
                    if radius_km and glat is not None and rec.get("latitude") is not None:
                        dist=haversine(glat,glng,rec.get("latitude"),rec.get("longitude")); rec["distance_km"]=dist
                        if dist is not None and dist>float(radius_km)*1.05:
                            continue
                    rec["query_origin_weight"]=_query_variant_weight(expansion_plan,rec.get("search_query") or sq)
                    _apply_result_metrics(query,rec)
                    before=len(all_found); _upsert_place(all_found,aliases,rec,dedupe_stats)
                    if len(all_found)>before:
                        _emit_live_result(rec,"google_maps_async" if kind=="map" else "google_search_browser_async")
                completed_job_keys.add(job_key)
                checkpoint=_checkpoint_snapshot(checkpoint,all_found,tile_stats,completed_job_keys,completed_detail_keys,"discovery")
                if resume_file: _save_search_checkpoint(resume_file,checkpoint)
                _progress("async_discovery",job_id=job_id,current=completed,total=len(tasks),found=len(all_found),provider=kind,
                          resumed=resumed,skipped_completed_jobs=skipped_completed_jobs)
                if len(all_found)>=discover_cap:
                    termination_reason="discovery_cap_reached"
                    for t in tasks:
                        if not t.done(): t.cancel()
                    break
            pending=[t for t in tasks if not t.done()]
            if pending:
                for t in pending: t.cancel()
                await asyncio.gather(*pending,return_exceptions=True)
            # v30: strong titles discovered in Google Search become bounded Maps expansion queries.
            _known_variant_norm={_norm_place_text(x) for x in variants}
            _title_variants=[]
            for _tv in google_web_meta.get("title_expansions",[]) or []:
                _tv=clean(_tv); _n=_norm_place_text(_tv)
                if not _tv or not _n or _n in _known_variant_norm or _brand_score(query,_tv)<.42: continue
                _known_variant_norm.add(_n); _title_variants.append(_tv)
                if len(_title_variants)>=3: break
            for _tv in _title_variants:
                if time.monotonic()>=deadline or len(all_found)>=discover_cap: break
                _jk=_checkpoint_job_key("title_expand",_tv)
                if _jk in completed_job_keys: continue
                try:
                    _recs,_meta=await _async_maps_discovery_job(context,_tv,None,None,11,min(160,discover_cap),lang,discovery_sem,"google_maps_title_expand_async")
                    extraction_metas.append(_meta)
                    for _rec in _recs:
                        _rec["query_origin_weight"]=.94
                        _apply_result_metrics(query,_rec); _upsert_place(all_found,aliases,_rec,dedupe_stats)
                    completed_job_keys.add(_jk)
                    checkpoint=_checkpoint_snapshot(checkpoint,all_found,tile_stats,completed_job_keys,completed_detail_keys,"title_expansion")
                    if resume_file: _save_search_checkpoint(resume_file,checkpoint)
                except Exception as _exc:
                    errors.append("title_expand: "+str(_exc)[:180])
            # v26: saturated Async viewports get a bounded second wave of smaller child viewports.
            if deep and glat is not None and glng is not None and time.monotonic()<deadline and len(all_found)<discover_cap:
                _seen={(round(c[0],5),round(c[1],5)) for c in centers if c[0] is not None}
                _remaining=max(0,min(max_tiles,ASYNC_DISCOVERY_JOB_CAP+24)-len([x for x in tile_stats if x.get("center")]))
                adaptive_children=_adaptive_subdivision_centers(tile_stats,glat,glng,radius_km,_remaining,_seen)
                if adaptive_children:
                    _progress("adaptive_subdivision_start",job_id=job_id,parents=sum(1 for x in tile_stats if x.get("saturated")),children=len(adaptive_children),found=len(all_found))
                    child_tasks=[]
                    for ci,(clat,clng) in enumerate(adaptive_children):
                        _idx=10000+ci; _jk=_checkpoint_job_key("adaptive",variants[0],(clat,clng),_idx)
                        if _jk in completed_job_keys:
                            skipped_completed_jobs+=1
                            continue
                        async def _child_task(ci=ci,clat=clat,clng=clng,jk=_jk):
                            recs,meta=await _async_maps_discovery_job(context,variants[0],clat,clng,zoom_for(max(.8,float(radius_km or 18)*.45)),
                                                                     min(110,discover_cap),lang,discovery_sem,"google_maps_adaptive_async")
                            meta["tile"]=10000+ci; meta["center"]=[clat,clng]; meta["adaptive_child"]=True
                            return jk,recs,meta
                        child_tasks.append(asyncio.create_task(_child_task()))
                    for fut in asyncio.as_completed(child_tasks):
                        if time.monotonic()>=deadline or len(all_found)>=discover_cap:
                            for t in child_tasks:
                                if not t.done(): t.cancel()
                            break
                        try: job_key,recs,meta=await fut
                        except Exception as exc:
                            errors.append("adaptive_job: "+str(exc)[:180]); continue
                        tile_stats.append(meta); extraction_metas.append(meta)
                        for rec in recs:
                            if radius_km and rec.get("latitude") is not None:
                                dist=haversine(glat,glng,rec.get("latitude"),rec.get("longitude")); rec["distance_km"]=dist
                                if dist is not None and dist>float(radius_km)*1.05: continue
                            _apply_result_metrics(query,rec)
                            before=len(all_found); _upsert_place(all_found,aliases,rec,dedupe_stats)
                            if len(all_found)>before: _emit_live_result(rec,"google_maps_adaptive_async")
                        completed_job_keys.add(job_key)
                        checkpoint=_checkpoint_snapshot(checkpoint,all_found,tile_stats,completed_job_keys,completed_detail_keys,"adaptive_discovery")
                        if resume_file: _save_search_checkpoint(resume_file,checkpoint)
                    _pending_children=[t for t in child_tasks if not t.done()]
                    if _pending_children:
                        for t in _pending_children: t.cancel()
                        await asyncio.gather(*_pending_children,return_exceptions=True)
                    _progress("adaptive_subdivision_done",job_id=job_id,children=len(adaptive_children),found=len(all_found))
            google_web_meta["count"]=sum(1 for r in all_found.values() if "google_search_browser_async" in _source_list(r))
            if not all_found and termination_reason not in ("google_blocked","network_error","environment_blocked"):
                try:
                    suggestions=await asyncio.to_thread(google_autocomplete_suggestions,query,lang,8)
                except Exception:
                    suggestions=[]
                candidates=[]
                for x in list(variants)+list(suggestions):
                    x=clean(x)
                    if x and x not in candidates:
                        candidates.append(x)
                for rq in candidates[:6]:
                    recovery_attempts.append(rq)
                    _jk=_checkpoint_job_key("recovery",rq)
                    if _jk in completed_job_keys:
                        skipped_completed_jobs+=1
                        if all_found: break
                        continue
                    recs,meta=await _async_maps_discovery_job(context,rq,None,None,11,min(180,discover_cap),lang,discovery_sem,"google_maps_recovery_async")
                    extraction_metas.append(meta)
                    if meta.get("blocked"):
                        termination_reason="google_blocked"; errors.append("google_block:"+str(meta.get("blocked"))); break
                    for rec in recs:
                        _apply_result_metrics(query,rec); _upsert_place(all_found,aliases,rec,dedupe_stats)
                    completed_job_keys.add(_jk)
                    checkpoint=_checkpoint_snapshot(checkpoint,all_found,tile_stats,completed_job_keys,completed_detail_keys,"recovery")
                    if resume_file: _save_search_checkpoint(resume_file,checkpoint)
                    if all_found:
                        break
            # v26.0: a browser can launch successfully yet render zero usable Maps links.
            # Retry the strongest queries once in a genuinely different browser engine.
            _failover_key=_checkpoint_job_key("browser_failover",search_q)
            if _should_browser_failover(len(all_found),termination_reason) and time.monotonic()<deadline and _failover_key not in completed_job_keys:
                fail_queries=[]
                for x in list(variants)+list(recovery_attempts):
                    x=clean(x)
                    if x and x not in fail_queries: fail_queries.append(x)
                if not fail_queries: fail_queries=[search_q]
                _progress("browser_failover_start",job_id=job_id,primary_engine=browser_engine,queries=fail_queries[:3])
                fail_records,browser_failover_meta=await _async_browser_failover_discovery(p,browser_engine,fail_queries,lang,discover_cap)
                extraction_metas.extend(browser_failover_meta.get("metas") or [])
                fe=clean(browser_failover_meta.get("engine"))
                if fe and fe not in browser_engines_tried: browser_engines_tried.append(fe)
                for rec in fail_records:
                    _apply_result_metrics(query,rec)
                    before=len(all_found); _upsert_place(all_found,aliases,rec,dedupe_stats)
                    if len(all_found)>before: _emit_live_result(rec,"google_maps_browser_failover_async")
                if fail_records:
                    termination_reason="completed_via_browser_failover"
                    _record_browser_health(browser_engine,"failed","layout-zero")
                    if fe: _record_browser_health(fe,"ok")
                    _progress("browser_failover_done",job_id=job_id,engine=fe,found=len(fail_records))
                else:
                    if browser_failover_meta.get("error"):
                        errors.append("browser_failover: "+clean(browser_failover_meta.get("error"))[:180])
                    fmetas=browser_failover_meta.get("metas") or []
                    if any(m.get("blocked") for m in fmetas): termination_reason="google_blocked"
                    elif any(m.get("page_state")=="environment_blocked" for m in fmetas): termination_reason="environment_blocked"
                    elif any(m.get("page_state")=="network_error" for m in fmetas): termination_reason="network_error"
                    else: termination_reason="browser_or_layout_no_results"
                completed_job_keys.add(_failover_key)
                checkpoint=_checkpoint_snapshot(checkpoint,all_found,tile_stats,completed_job_keys,completed_detail_keys,"browser_failover")
                if resume_file: _save_search_checkpoint(resume_file,checkpoint)
            if all_found and termination_reason not in {"google_blocked","network_error","environment_blocked"}:
                _record_browser_health(browser_engine,"ok")
            elif not all_found and termination_reason in {"completed","browser_or_layout_no_results"}:
                _record_browser_health(browser_engine,"failed","layout-zero")
            partial_results_preserved=bool(all_found) and termination_reason in {"google_blocked","network_error","environment_blocked","deadline_reached"}
            discovered=list(all_found.values())
            discovered.sort(key=lambda r:_rank_key(query,r))
            results=discovered[:max_results]
            # v28: apply persistent detail cache before deciding which pages deserve a network visit.
            results=[_apply_branch_cache(r) for r in results]
            cache_hits=sum(1 for r in results if r.get("cache_hit"))
            cache_stale_hits=sum(1 for r in results if r.get("cache_hit") and r.get("freshness_status")=="stale")
            cache_fresh_hits=sum(1 for r in results if r.get("cache_hit") and r.get("freshness_status")=="fresh")
            network_fetches=cache_refreshed=0; cache_saved_requests=0
            if details and results and time.monotonic()<deadline:
                detail_limit=_detail_enrichment_budget(results,auto_mode=auto_mode,details=details)
                candidates=[]; cache_satisfied=0
                for i,rec in enumerate(results):
                    _grp=str(rec.get("result_group") or "additional")
                    rs=(3 if auto_mode and _grp in {"strong","confirmed"} and i<80 else (2 if auto_mode and _grp=="related" and i<24 else 0))
                    detail_key=_cache_identity_key(rec) or _place_key(rec)
                    already_done=bool(detail_key in completed_detail_keys and rec.get("details_loaded"))
                    if refresh_stale_only:
                        needs=bool(rec.get("cache_hit") and rec.get("freshness_status")=="stale")
                        if rec.get("cache_hit") and rec.get("freshness_status")=="fresh": cache_satisfied+=1
                    else:
                        needs=_should_refresh_details(rec,review_samples=rs)
                        if rec.get("cache_hit") and not needs: cache_satisfied+=1
                    if already_done:
                        needs=False; cache_satisfied+=1
                    if needs: candidates.append((i,rec,rs,detail_key))
                candidates.sort(key=lambda x:_enrichment_priority(x[1]),reverse=True)
                selected=candidates[:detail_limit]
                cache_saved_requests=min(detail_limit,cache_satisfied)
                _progress("details_start",job_id=job_id,total=len(selected),eligible=len(candidates),cache_hits=cache_hits,saved=cache_saved_requests,found=len(discovered),async_mode=True)
                detail_tasks=[]
                for i,rec,rs,detail_key in selected:
                    async def _detail_task(i=i,rec=rec,rs=rs,detail_key=detail_key):
                        item,err=await _async_enrich_branch(context,rec,lang,rs,detail_sem,force_refresh=True)
                        return i,detail_key,item,err
                    detail_tasks.append(asyncio.create_task(_detail_task()))
                enriched=[dict(r) for r in results]
                done_details=0
                for fut in asyncio.as_completed(detail_tasks):
                    if time.monotonic()>=deadline:
                        termination_reason="deadline_reached"
                        for t in detail_tasks:
                            if not t.done(): t.cancel()
                        break
                    try:
                        i,detail_key,item,err=await fut
                    except asyncio.CancelledError:
                        continue
                    except Exception as exc:
                        errors.append("detail: "+str(exc)[:180]); continue
                    enriched[i]=item; done_details+=1
                    network_fetches+=1 if item.get("network_fetch") else 0
                    cache_refreshed+=1 if item.get("cache_refreshed") else 0
                    if err:
                        errors.append("{}: {}".format(clean(item.get("name"))[:80],err))
                    _apply_result_metrics(query,item); _emit_live_result(item,"google_maps_detail_async")
                    completed_detail_keys.add(detail_key)
                    checkpoint=_checkpoint_snapshot(checkpoint,all_found,tile_stats,completed_job_keys,completed_detail_keys,"details",
                                                    results_override=enriched)
                    if resume_file: _save_search_checkpoint(resume_file,checkpoint)
                    _progress("details",job_id=job_id,current=done_details,total=len(selected),cache_hits=cache_hits,network_fetches=network_fetches,found=len(discovered),async_mode=True,
                              resumed=resumed,completed_details=len(completed_detail_keys))
                pending_details=[t for t in detail_tasks if not t.done()]
                if pending_details:
                    for t in pending_details: t.cancel()
                    await asyncio.gather(*pending_details,return_exceptions=True)
                results=enriched
                reconciled={}; rec_aliases={}
                for rec in results:
                    _upsert_place(reconciled,rec_aliases,rec)
                results=list(reconciled.values()); results.sort(key=lambda r:_rank_key(query,r)); results=results[:max_results]
            contact_enriched_count=contact_cache_hits=0
            if results and time.monotonic()<deadline:
                cands=[(i,r) for i,r in enumerate(results) if r.get("website") and (not r.get("mobile") or not r.get("email"))]
                cands.sort(key=lambda x:_contact_enrichment_priority(x[1]),reverse=True)
                cands=cands[:_contact_enrichment_budget(results)]
                if cands:
                    _progress("contact_enrichment_start",job_id=job_id,total=len(cands),found=len(results),async_mode=True)
                    ctasks=[]
                    for i,rec in cands:
                        async def _ctask(i=i,rec=rec):
                            item,err=await _async_contact_enrich_record(rec,contact_sem); return i,item,err
                        ctasks.append(asyncio.create_task(_ctask()))
                    done_contacts=0
                    for fut in asyncio.as_completed(ctasks):
                        if time.monotonic()>=deadline:
                            for t in ctasks:
                                if not t.done(): t.cancel()
                            break
                        try: i,item,err=await fut
                        except asyncio.CancelledError: continue
                        except Exception as exc:
                            errors.append("contact: "+str(exc)[:180]); continue
                        results[i]=item; done_contacts+=1
                        if item.get("contact_cache_hit"): contact_cache_hits+=1
                        if item.get("mobile") or item.get("email") or item.get("whatsapp"): contact_enriched_count+=1
                        _apply_result_metrics(query,item); _emit_live_result(item,"contact_enrichment_async")
                        _progress("contact_enrichment",job_id=job_id,current=done_contacts,total=len(cands),found=len(results),async_mode=True)
                    pending_contacts=[t for t in ctasks if not t.done()]
                    if pending_contacts:
                        for t in pending_contacts: t.cancel()
                        await asyncio.gather(*pending_contacts,return_exceptions=True)
            for rec in results:
                _apply_result_metrics(query,rec)
                rec["confidence_score"]=_confidence_score(query,rec)
                rec["relevance"]=_relevance_band(rec.get("match_score"))
                rec["source_fetched_at"]=rec.get("source_fetched_at") or datetime.now().isoformat()
                rec["field_sources"]=_field_provenance(rec)
                rec["identity_alias_count"]=len(_place_aliases(rec))
            try:
                _branch_cache_store_many([r for r in results if r.get("details_loaded")])
            except Exception as exc:
                _diag("final_branch_cache_store_failed",exc,count=sum(1 for r in results if r.get("details_loaded")))
        finally:
            if context is not None:
                try: await context.close()
                except Exception as exc: _diag("async_context_close_failed",exc)
            try: await browser.close()
            except Exception as exc: _diag("async_browser_close_failed",exc)
    elapsed_ms=round((time.perf_counter()-started)*1000,1)
    results=locals().get("results",list(all_found.values())[:max_results])
    cache_hits=int(locals().get("cache_hits",0) or 0); cache_fresh_hits=int(locals().get("cache_fresh_hits",0) or 0); cache_stale_hits=int(locals().get("cache_stale_hits",0) or 0)
    network_fetches=int(locals().get("network_fetches",0) or 0); cache_refreshed=int(locals().get("cache_refreshed",0) or 0); cache_saved_requests=int(locals().get("cache_saved_requests",0) or 0)
    extraction_telemetry=_aggregate_extraction_telemetry(extraction_metas)
    selector_health=_selector_health_snapshot()
    _failed_tiles=sum(1 for x in tile_stats if x.get("error"))
    _unresolved_saturated=sum(1 for x in tile_stats if x.get("saturated") and int(x.get("subdivided") or 0)<=0)
    _coverage_summary=_coverage_quality_summary(tile_stats,len(all_found),discover_cap,termination_reason,_unresolved_saturated,_failed_tiles)
    _validation_counts={"confirmed":0,"probable":0,"weak":0}
    for _r in results:
        _apply_result_metrics(query,_r); _validation_counts[_r.get("validation_status") or "weak"]=_validation_counts.get(_r.get("validation_status") or "weak",0)+1
    meta={
        "job_id":job_id,"elapsed_ms":elapsed_ms,"total_unique":len(results),"total_discovered":len(all_found),
        "resumed":bool(resumed),"resume_count":int(checkpoint.get("resume_count") or 0),"restored_results":len(checkpoint.get("results") or []) if resumed else 0,
        "resume_skipped_jobs":int(locals().get("skipped_completed_jobs",0) or 0),"checkpoint_file":str(resume_file or ""),
        "checkpoint_completed_jobs":len(completed_job_keys),"checkpoint_completed_details":len(completed_detail_keys),
        "display_limit":max_results,"discovery_limit":discover_cap,"with_coordinates":sum(1 for r in results if r.get("latitude") is not None),
        "with_phone":sum(1 for r in results if r.get("phone") or r.get("mobile")),"with_email":sum(1 for r in results if r.get("email")),
        "with_website":sum(1 for r in results if r.get("website")),"with_mobile":sum(1 for r in results if r.get("mobile")),"with_whatsapp":sum(1 for r in results if r.get("whatsapp")),"contact_enriched_count":int(locals().get("contact_enriched_count",0) or 0),"contact_cache_hits":int(locals().get("contact_cache_hits",0) or 0),"details_loaded":sum(1 for r in results if r.get("details_loaded")),
        "searched_tiles":len(tile_stats),"planned_tiles":len(tile_stats),"deep":bool(deep),"details":bool(details),"geocoded_center":geocoded,
        "coverage_status":"async_adaptive","saturated_tiles":sum(1 for x in tile_stats if x.get("saturated")),
        "subdivided_tiles":sum(1 for x in tile_stats if int(x.get("subdivided") or 0)>0),"unresolved_saturated_tiles":_unresolved_saturated,
        "tile_retries":0,"failed_tiles":_failed_tiles,"deduped_aliases":len(aliases),
        "duplicates_suppressed":int(dedupe_stats.get("duplicates_suppressed") or 0),"near_duplicate_merges":int(dedupe_stats.get("near_duplicate_merges") or 0),
        "strong_identity_conflicts":int(dedupe_stats.get("strong_identity_conflicts") or 0),
        "truncated":len(all_found)>max_results or len(all_found)>=discover_cap,"coverage_complete":_coverage_summary["complete"],"coverage_percent":_coverage_summary["percent"],
        "validation_confirmed":_validation_counts.get("confirmed",0),"validation_probable":_validation_counts.get("probable",0),"validation_weak":_validation_counts.get("weak",0),
        "cache_hits":cache_hits,"cache_fresh_hits":cache_fresh_hits,"cache_stale_hits":cache_stale_hits,"network_fetches":network_fetches,
        "cache_refreshed":cache_refreshed,"cache_saved_requests":cache_saved_requests,"refresh_stale_only":bool(refresh_stale_only),
        "freshness_fresh":sum(1 for r in results if r.get("freshness_status")=="fresh"),"freshness_stale":sum(1 for r in results if r.get("freshness_status")=="stale"),
        "freshness_unknown":sum(1 for r in results if r.get("freshness_status") in (None,"","unknown")),
        "termination_reason":termination_reason,"browser_engine":browser_engine,"hard_deadline_seconds":hard_seconds,
        "browser_engines_tried":browser_engines_tried,"browser_failover_used":bool(browser_failover_meta.get("used")),
        "browser_failover_engine":browser_failover_meta.get("engine"),"browser_failover_count":int(browser_failover_meta.get("count") or 0),
        "partial_results_preserved":bool(partial_results_preserved),"browser_health":_load_browser_health(),
        "fallback_used":bool(recovery_attempts),"exact_match_relaxed":False,"google_places_api_available":bool(api_meta.get("available")),
        "google_places_api_used":bool(api_meta.get("used")),"google_places_api_count":len(api_records),
        "google_search_browser_count":google_web_meta.get("count",0),"google_search_browser_meta":google_web_meta,
        "query_expansion_count":len(expansion_plan),"query_expansion_plan":expansion_plan,
        "result_groups":{g:sum(1 for r in results if r.get("result_group")==g) for g in ("strong","confirmed","related","additional")},
        "early_enriched_count":sum(1 for r in results[:8] if r.get("details_loaded")),
        "early_review_samples":sum(int(r.get("preview_review_count") or 0) for r in results[:8]),
        "multi_provider_results":sum(1 for r in results if int(r.get("provider_count") or 0)>1),
        "avg_data_completeness":round(sum(float(r.get("data_completeness") or 0) for r in results)/max(1,len(results)),1),
        "recovery_attempts":recovery_attempts,"no_result_reason":("google_blocked" if termination_reason=="google_blocked" else ("environment_blocked" if termination_reason=="environment_blocked" else ("network_error" if termination_reason=="network_error" else ("browser_or_layout_no_results" if not results else "")))),
        "keyless_live":True,"providers":["google_search_browser_async","google_maps_async"]+(["google_places_api"] if api_meta.get("used") else []),
        "direct_google_fast":False,"auto_mode":bool(auto_mode),"auto_query_variants":variants,"partial_name_supported":True,
        "suggestion_expansions":[v for v in variants if _norm_place_text(v)!=_norm_place_text(search_q)][:8],
        "full_details_count":sum(1 for r in results if r.get("details_loaded")),"basic_results_count":sum(1 for r in results if not r.get("details_loaded")),
        "google_preview_reviews":sum(int(r.get("preview_review_count") or 0) for r in results),"errors":errors[:30],"tile_stats":tile_stats[:100],
        "async_mode":True,"async_discovery_concurrency":ASYNC_DISCOVERY_CONCURRENCY,"async_detail_concurrency":ASYNC_DETAIL_CONCURRENCY,
        "diagnostic_log":str(DIAG_LOG),"selector_strategy":"health-ranked semantic/feed/legacy/raw-html",
        "selector_strategy_order":_selector_strategy_order(DISCOVERY_EXTRACTION_STRATEGIES),
        "best_selector_strategy":extraction_telemetry.get("best_strategy") or "",
        "extraction_telemetry":extraction_telemetry,"selector_health":selector_health[:12],"captcha_bypass":False,
    }
    _cp_status="completed" if termination_reason in {"completed","completed_via_browser_failover","discovery_cap_reached"} else "partial"
    checkpoint=_checkpoint_snapshot(checkpoint,all_found,tile_stats,completed_job_keys,completed_detail_keys,"done",_cp_status,results_override=results)
    checkpoint["last_meta"]={k:v for k,v in meta.items() if k not in {"tile_stats","selector_health","extraction_telemetry"}}
    if resume_file: _save_search_checkpoint(resume_file,checkpoint)
    _progress("done",job_id=job_id,found=len(results),discovered=len(all_found),elapsed_ms=elapsed_ms,termination_reason=termination_reason,async_mode=True,
              resumed=bool(resumed),resume_count=int(checkpoint.get("resume_count") or 0),checkpoint_status=_cp_status,skipped_completed_jobs=int(locals().get("skipped_completed_jobs",0) or 0))
    return results,meta


async def async_scrape_place(query_or_url,max_reviews=100,lang="ar",is_url=False,known_review_keys=None):
    if async_playwright is None:
        raise RuntimeError("playwright.async_api unavailable")
    result={"info":{"name":"","rating":None,"review_count":None,"address":None,"phone":None,"mobile":None,"email":None,
                    "email_source":None,"website":None,"category":None,"cid":None,"place_id":None,"latitude":None,"longitude":None},
            "reviews":[],"error":None,"async_mode":True}
    async with async_playwright() as p:
        browser,engine=await _async_launch_browser(p); context=None; page=None
        try:
            context=await _async_setup_context(browser,lang,engine)
            target=clean(query_or_url)
            if not is_url:
                recs,meta=await _async_maps_discovery_job(context,target,None,None,13,8,lang,asyncio.Semaphore(1),"google_maps_scrape_search_async")
                if meta.get("blocked"):
                    raise RuntimeError("google_block:"+str(meta.get("blocked")))
                if not recs:
                    return result
                recs.sort(key=lambda r:_rank_key(target,r)); target=recs[0].get("url") or recs[0].get("maps_url")
            page=await context.new_page()
            await _async_goto_retry(page,target,timeout=35000,attempts=2)
            await _async_dismiss_consent(page)
            blocked=await _async_google_block_reason(page)
            if blocked:
                raise RuntimeError("google_block:"+blocked)
            try:
                await page.wait_for_selector("[role='main'] h1,h1,button[data-item-id='address']",timeout=6000,state="attached")
            except Exception:
                pass
            result["info"]=await _async_extract_info(page,with_contacts=True)
            if await _async_open_reviews(page):
                await _async_sort_reviews_newest(page)
                result["reviews"]=await _async_extract_reviews(page,max_reviews,known_review_keys)
            if result["info"].get("review_count") is None and result["reviews"] and not known_review_keys:
                result["info"]["review_count"]=len(result["reviews"])
            _progress("done",found=len(result["reviews"]),total=max_reviews,async_mode=True)
        except Exception as exc:
            _diag("async_scrape_failed",exc,query=query_or_url)
            result["error"]=str(exc)+"\n"+traceback.format_exc()
        finally:
            if page is not None:
                try: await page.close()
                except Exception: pass
            if context is not None:
                try: await context.close()
                except Exception: pass
            try: await browser.close()
            except Exception: pass
    return result


def _search_places_legacy(query, max_results=100, lang="ar", city="", area="", activity="",
                  lat=None, lng=None, radius_km=None, deep=False, exact_name=False,
                  max_tiles=36, details=False, discover_max=None, auto_mode=False):
    job_id = uuid.uuid4().hex[:12]
    started = time.perf_counter()
    # Bounded execution protects the desktop UI from browser/network stalls.
    hard_seconds = 260 if auto_mode else (480 if deep else 210)
    deadline = time.monotonic() + hard_seconds
    termination_reason = "completed"
    browser_engine = "none"
    max_results = max(1, min(int(max_results or 100), 1000))
    max_tiles = max(1, min(int(max_tiles or 36), 81))
    if auto_mode:
        # One-box automatic mode: optimize breadth and detail without exposing
        # technical knobs in the primary UI.
        deep = True
        exact_name = False
        details = True
        max_results = max(max_results, 350)
        max_tiles = max(max_tiles, 49)
        discover_cap = max(max_results, min(int(discover_max or 1400), 3000))
    else:
        discover_cap = max(max_results, min(int(discover_max or max(max_results * 2, 300)), 3000))
    parts = [clean(activity), clean(query), clean(area), clean(city)]
    search_q = " ".join([x for i, x in enumerate(parts) if x and x not in parts[:i]]) or clean(query)
    glat, glng, geocoded = lat, lng, None
    _progress("start", job_id=job_id, query=search_q, display_limit=max_results, discovery_limit=discover_cap, auto_mode=bool(auto_mode))
    # Quick keyless search does not need a separate geocoder: city/area stay in
    # the Google query, avoiding an extra network round-trip. Coverage/radius mode
    # geocodes only when coordinates are actually needed for tiling.
    if (glat is None or glng is None) and (area or city) and (deep or radius_km):
        _progress("geocode", job_id=job_id, text=" ".join(x for x in [area, city] if x))
        glat, glng, geocoded = geocode(" ".join(x for x in [area, city] if x), lang)
    elif glat is not None and glng is not None:
        geocoded = "{:.6f},{:.6f}".format(float(glat), float(glng))

    all_found = {}; alias_index = {}; tile_stats = []; errors = []; tile_retry_count = 0
    dedupe_stats={"duplicates_suppressed":0,"near_duplicate_merges":0,"strong_identity_conflicts":0,"inserted":0}
    google_web_meta={"used":False,"count":0}; google_web_count=0
    early_enriched_count=0; early_review_samples=0

    # Fast/live provider: official Google Places API when configured. Results are
    # streamed immediately to the UI, then reconciled with the Maps browser pass.
    api_records, api_meta = google_places_api_search(
        query, lang=lang, city=city, area=area, activity=activity,
        max_results=min(discover_cap, 60), lat=glat, lng=glng, radius_km=radius_km)
    _progress("provider", job_id=job_id, provider="google_places_api",
              available=bool(api_meta.get("available")), used=bool(api_meta.get("used")),
              count=len(api_records), error=api_meta.get("error"))
    for rec in api_records:
        if radius_km and glat is not None and rec.get("latitude") is not None:
            rec["distance_km"] = haversine(glat, glng, rec.get("latitude"), rec.get("longitude"))
            if rec.get("distance_km") is not None and rec["distance_km"] > float(radius_km) * 1.05:
                continue
        _apply_result_metrics(query, rec)
        _upsert_place(all_found, alias_index, rec, dedupe_stats)

    # Quick search uses the official Google provider directly when available. This
    # avoids launching a browser and returns in seconds. Deep/all-branches mode
    # continues to the Maps browser after showing these direct results instantly.
    if api_records and not deep:
        direct_results = list(all_found.values())[:max_results]
        for rec in direct_results:
            _apply_result_metrics(query,rec)
            rec["confidence_score"] = _confidence_score(query, rec)
            rec["source_fetched_at"] = rec.get("source_fetched_at") or datetime.now().isoformat()
            rec["field_sources"] = _field_provenance(rec)
            rec["identity_alias_count"] = len(_place_aliases(rec))
        direct_results.sort(key=lambda r: _rank_key(query,r))
        elapsed_ms = round((time.perf_counter() - started) * 1000, 1)
        meta = {
            "job_id": job_id, "elapsed_ms": elapsed_ms, "total_unique": len(direct_results),
            "total_discovered": len(all_found), "display_limit": max_results, "discovery_limit": discover_cap,
            "with_coordinates": sum(1 for r in direct_results if r.get("latitude") is not None),
            "with_phone": sum(1 for r in direct_results if r.get("phone") or r.get("mobile")),
            "with_email": sum(1 for r in direct_results if r.get("email")),
            "with_website": sum(1 for r in direct_results if r.get("website")),
            "details_loaded": len(direct_results), "searched_tiles": 0, "planned_tiles": 0,
            "deep": False, "details": True, "geocoded_center": geocoded,
            "coverage_status": "google_direct", "saturated_tiles": 0, "subdivided_tiles": 0,
            "unresolved_saturated_tiles": 0, "tile_retries": 0, "failed_tiles": 0,
            "deduped_aliases": len(alias_index), "duplicates_suppressed":int(dedupe_stats.get("duplicates_suppressed") or 0),
            "near_duplicate_merges":int(dedupe_stats.get("near_duplicate_merges") or 0),"strong_identity_conflicts":int(dedupe_stats.get("strong_identity_conflicts") or 0),
            "truncated": len(all_found) > max_results, "coverage_complete": True,"coverage_percent":100.0,
            "validation_confirmed":sum(1 for r in direct_results if r.get("validation_status")=="confirmed"),
            "validation_probable":sum(1 for r in direct_results if r.get("validation_status")=="probable"),
            "validation_weak":sum(1 for r in direct_results if r.get("validation_status")=="weak"),
            "fallback_used": False, "exact_match_relaxed": False,
            "google_places_api_available": True, "google_places_api_used": True,
            "google_places_api_count": len(api_records), "google_preview_reviews": sum(int(r.get("preview_review_count") or 0) for r in direct_results),
            "providers": ["google_places_api"], "direct_google_fast": True, "auto_mode": bool(auto_mode), "auto_query_variants": _auto_query_variants(query,lang,city,area),
            "early_enriched_count": len(direct_results), "early_review_samples": sum(int(r.get("preview_review_count") or 0) for r in direct_results),
            "multi_provider_results": sum(1 for r in direct_results if int(r.get("provider_count") or 0) > 1),
            "avg_data_completeness": round(sum(float(r.get("data_completeness") or 0) for r in direct_results)/max(1,len(direct_results)),1),
            "partial_name_supported": True, "suggestion_expansions": [],
            "termination_reason":"completed","browser_engine":"google_places_api","hard_deadline_seconds":hard_seconds,
            "errors": [], "tile_stats": [],
        }
        _progress("google_direct_done", job_id=job_id, found=len(direct_results),
                  review_samples=meta["google_preview_reviews"], elapsed_ms=elapsed_ms)
        _progress("done", job_id=job_id, found=len(direct_results), discovered=len(all_found), elapsed_ms=elapsed_ms)
        return direct_results, meta

    _progress("provider", job_id=job_id, provider="google_maps_browser", available=True, used=True, count=0)
    from playwright.sync_api import sync_playwright
    if deep and glat is not None and glng is not None:
        base_budget = min(max_tiles, max(9, int(math.ceil(max_tiles * .65))))
        centers = _coverage(glat, glng, radius_km=radius_km, max_tiles=base_budget)
    else:
        centers = [(glat, glng)] if glat is not None and glng is not None else [(None, None)]

    variants = _auto_query_variants(query, lang=lang, city=city, area=area) if auto_mode else [search_q]
    partial_expansions = [v for v in variants if _norm_place_text(v) != _norm_place_text(search_q)]
    if auto_mode:
        _progress("query_plan", job_id=job_id, query=query, variants=variants[:10], partial=bool(partial_expansions))
    if deep and clean(query) and not auto_mode:
        suffix = " ".join(x for x in [clean(area), clean(city)] if x)
        for bv in [
            ("فروع {} {}".format(clean(query), suffix) if lang == "ar" else "{} branches {}".format(clean(query), suffix)).strip(),
            ("{} {}".format(clean(query), clean(city))).strip(),
        ]:
            if bv and bv not in variants: variants.append(bv)

    per_view = (max(20,min(50,max_results)) if not deep else max(60, min(160, max_results if max_results > 60 else 80)))
    with sync_playwright() as p:
        browser,browser_engine = _launch_browser(p)
        page = setup_page(browser, lang, browser_engine)
        try:
            # Phase 22.5 keyless-fast path: public Google Search first, then Maps.
            # Results are emitted as soon as anchors are discovered, so the UI can
            # show businesses before the Maps pass finishes.
            _progress("provider", job_id=job_id, provider="google_search_browser", available=True, used=True, count=0, key_required=False)
            try:
                seed_queries = (variants[:5] if auto_mode else [query])
                seed_records=[]; seed_errors=[]; seed_elapsed=0.0; seed_title_expansions=[]
                for sq in seed_queries:
                    recs, smeta = google_search_browser_seed(
                        page, sq, lang=lang, city="", area="", activity="",
                        max_results=min(60 if auto_mode else 24, discover_cap))
                    seed_elapsed += float(smeta.get("elapsed_ms") or 0)
                    seed_errors.extend(smeta.get("errors") or [])
                    for _te in (smeta.get("title_expansions") or []):
                        if _te not in seed_title_expansions: seed_title_expansions.append(_te)
                    for rec in recs:
                        rec["auto_seed_query"] = sq
                        if radius_km and glat is not None and rec.get("latitude") is not None:
                            rec["distance_km"] = haversine(glat, glng, rec.get("latitude"), rec.get("longitude"))
                            if rec.get("distance_km") is not None and rec["distance_km"] > float(radius_km)*1.05:
                                continue
                        before=len(all_found); _upsert_place(all_found, alias_index, rec, dedupe_stats)
                        if len(all_found)>before: seed_records.append(rec)
                    if len(all_found)>=min(discover_cap,120): break
                google_web_meta={"used":True,"count":len(seed_records),"elapsed_ms":round(seed_elapsed,1),"errors":seed_errors[-5:],"queries":seed_queries,
                                 "title_expansions":seed_title_expansions[:12]}
                # Ordinary Google result titles are strong hints when the user typed only part of a name.
                for t in seed_title_expansions:
                    if t not in variants and len(variants)<14: variants.append(t)
                google_web_count=len(seed_records)
                _progress("provider", job_id=job_id, provider="google_search_browser", available=True, used=True,
                          count=google_web_count, elapsed_ms=google_web_meta.get("elapsed_ms"), key_required=False)
            except Exception as web_err:
                google_web_meta={"used":True,"count":0,"error":str(web_err)[:200]}
                _progress("provider", job_id=job_id, provider="google_search_browser", available=True, used=True,
                          count=0, error=str(web_err)[:200], key_required=False)
            # v23.3: enrich the strongest Google seeds immediately so useful phone/site/review
            # samples reach the UI while wider Maps coverage continues in the background.
            if auto_mode and all_found:
                early_candidates=sorted(list(all_found.values()),key=lambda r:_rank_key(query,r))[:6]
                for _seed in early_candidates:
                    if time.monotonic() >= deadline: break
                    try:
                        _item,_err=_enrich_branch(page,_seed,lang,review_samples=3)
                        _apply_result_metrics(query,_item)
                        _upsert_place(all_found,alias_index,_item,dedupe_stats)
                        early_enriched_count += 1 if _item.get("details_loaded") else 0
                        early_review_samples += int(_item.get("preview_review_count") or 0)
                        _emit_live_result(_item,"google_maps_early_detail")
                        _progress("early_detail",job_id=job_id,current=early_enriched_count,total=len(early_candidates),
                                  reviews=early_review_samples,found=len(all_found))
                    except Exception as _e:
                        errors.append("early_detail: {}".format(str(_e)[:160]))

            jobs = []
            seen_centers = set()
            next_tile_id = 0
            for vi, vq in enumerate(variants):
                if vi == 0:
                    for c in centers:
                        jobs.append((vq, c, next_tile_id, 0)); next_tile_id += 1
                        if c[0] is not None: seen_centers.add((round(c[0],5), round(c[1],5)))
                else:
                    jobs.append((vq, centers[0], -1, 0))

            ji = 0; primary = 0
            while ji < len(jobs):
                if time.monotonic() >= deadline:
                    termination_reason="deadline_reached"
                    errors.append("search_deadline_reached:{}s".format(hard_seconds))
                    _progress("deadline", job_id=job_id, found=len(all_found), seconds=hard_seconds)
                    break
                vq, (clat, clng), tile_idx, depth = jobs[ji]; ji += 1
                if len(all_found) >= discover_cap:
                    termination_reason="discovery_cap_reached"; break
                if tile_idx >= 0 and primary >= max_tiles:
                    termination_reason="tile_cap_reached"; break
                _progress("tile_start", job_id=job_id, current=primary+1, planned=max_tiles,
                          found=len(all_found), tile=tile_idx, depth=depth)
                try:
                    tr = max(1.0, min(8.0, float(radius_km or 18)/max(1, math.sqrt(max(1, len(centers))))))
                    recs = []; stat = {}; last_tile_error = None
                    for attempt in range(1, 4):
                        try:
                            recs, stat = _search_view(page, vq, clat, clng, zoom_for(tr), per_view, lang, fast=(auto_mode or not deep))
                            stat["attempts"] = attempt
                            break
                        except Exception as retry_err:
                            last_tile_error = retry_err
                            if attempt < 3:
                                tile_retry_count += 1
                                _progress("tile_retry", job_id=job_id, tile=tile_idx, attempt=attempt+1,
                                          current=primary+1, planned=max_tiles, found=len(all_found))
                                page.wait_for_timeout(450 * (2 ** (attempt-1)))
                    else:
                        raise last_tile_error or RuntimeError("tile search failed")
                    stat.update({"tile":tile_idx,"found":len(recs),"depth":depth,
                                 "center":[clat,clng] if clat is not None else None,
                                 "subdivided":0})
                    tile_stats.append(stat)
                    if tile_idx >= 0: primary += 1
                    for rec in recs:
                        if radius_km and glat is not None and rec.get("latitude") is not None:
                            dist = haversine(glat, glng, rec["latitude"], rec["longitude"])
                            rec["distance_km"] = dist
                            if dist is not None and dist > float(radius_km) * 1.05: continue
                        else:
                            rec["distance_km"] = None
                        _apply_result_metrics(query, rec)
                        before = len(all_found)
                        _upsert_place(all_found, alias_index, rec, dedupe_stats)
                        if len(all_found) > before:
                            _emit_live_result(rec, "google_maps_browser")

                    if (deep and stat.get("saturated") and depth < 3 and clat is not None and
                            primary < max_tiles and len(all_found) < discover_cap):
                        # Progressively smaller child tiles around a saturated parent.
                        step = max(.45, tr * (.58 if depth == 0 else .38 if depth == 1 else .26))
                        children = 0
                        for nk, ek in [(-step,-step),(-step,step),(step,-step),(step,step),(0,-step),(0,step),(-step,0),(step,0)]:
                            nl, ng = _offset(clat, clng, nk, ek)
                            if radius_km and glat is not None:
                                d = haversine(glat, glng, nl, ng)
                                if d is not None and d > float(radius_km) * 1.06: continue
                            keyc = (round(nl,5), round(ng,5))
                            if keyc in seen_centers: continue
                            if next_tile_id >= max_tiles: break
                            seen_centers.add(keyc)
                            jobs.append((vq, (nl,ng), next_tile_id, depth+1)); next_tile_id += 1
                            children += 1
                        stat["subdivided"] = children
                    _progress("tile_done", job_id=job_id, current=primary, planned=max_tiles,
                              found=len(all_found), saturated=bool(stat.get("saturated")),
                              subdivided=int(stat.get("subdivided") or 0))
                except Exception as e:
                    msg=str(e)[:180]
                    errors.append("tile {}: {}".format(tile_idx, msg))
                    _progress("tile_error", job_id=job_id, current=primary, planned=max_tiles, found=len(all_found), error=msg)
                    if "google_block:" in msg:
                        termination_reason="google_blocked"
                        _progress("google_block", job_id=job_id, reason=msg.split("google_block:",1)[-1][:80], found=len(all_found))
                        break

            # If viewport/exact discovery yielded nothing, automatically broaden once before returning zero.
            fallback_used = False
            if not all_found:
                fallback_queries = []
                for fq in [clean(query), " ".join(x for x in [clean(query), clean(city)] if x), search_q]:
                    if fq and fq not in fallback_queries: fallback_queries.append(fq)
                for fq in fallback_queries:
                    _progress("fallback_start", job_id=job_id, query=fq, found=len(all_found))
                    try:
                        recs, stat = _search_view(page, fq, None, None, 11, max(per_view, 100), lang, fast=(auto_mode or not deep))
                        stat.update({"tile":-2,"found":len(recs),"depth":0,"center":None,"subdivided":0,"fallback":True})
                        tile_stats.append(stat)
                        for rec in recs:
                            rec["distance_km"] = (haversine(glat, glng, rec.get("latitude"), rec.get("longitude"))
                                                  if glat is not None and rec.get("latitude") is not None else None)
                            _apply_result_metrics(query, rec)
                            before = len(all_found)
                            _upsert_place(all_found, alias_index, rec, dedupe_stats)
                            if len(all_found) > before:
                                _emit_live_result(rec, "google_maps_browser")
                        if all_found:
                            fallback_used = True
                            _progress("fallback_done", job_id=job_id, query=fq, found=len(all_found))
                            break
                    except Exception as e:
                        errors.append("fallback: {}".format(str(e)[:180]))

            # v23.3 last-chance recovery: if every normal path returned zero, retry the
            # strongest autocomplete/title variants directly in Google Maps and parse raw place URLs.
            recovery_attempts=[]
            if not all_found and termination_reason not in ("google_blocked",):
                candidates=[]
                for x in variants + google_autocomplete_suggestions(query,lang=lang,limit=8):
                    x=clean(x)
                    if x and x not in candidates: candidates.append(x)
                for rq in candidates[:8]:
                    recovery_attempts.append(rq)
                    _progress("recovery_start",job_id=job_id,query=rq)
                    try:
                        url="https://www.google.com/maps/search/?api=1&query={}&hl={}".format(quote_plus(rq),lang)
                        _goto_retry(page,url,timeout=32000,attempts=2); dismiss_consent(page)
                        blocked=_google_block_reason(page)
                        if blocked:
                            termination_reason="google_blocked"; errors.append("recovery_google_block:"+blocked); break
                        try: page.wait_for_timeout(900)
                        except Exception: pass
                        recs=_extract_global_maps_links(page,rq,max_results=min(180,discover_cap),source="google_maps_recovery")
                        if not recs:
                            recs,_rs=_collect_search_cards(page,rq,None,None,min(180,discover_cap),max_scrolls=10,stall_limit=3,wait_ms=300)
                        for rec in recs:
                            _apply_result_metrics(query,rec)
                            before=len(all_found); _upsert_place(all_found,alias_index,rec,dedupe_stats)
                            if len(all_found)>before: _emit_live_result(rec,"google_maps_recovery")
                        if all_found:
                            fallback_used=True
                            _progress("recovery_done",job_id=job_id,query=rq,found=len(all_found))
                            break
                    except Exception as e:
                        errors.append("recovery: {}".format(str(e)[:180]))

            discovered = list(all_found.values())
            # Exact-name is now a preference, not a destructive hard filter. If strict matches exist,
            # show them first; if not, keep the best broad matches instead of returning zero.
            exact_fallback = False
            if exact_name and clean(query) and discovered:
                strict = [r for r in discovered if float(r.get("match_score") or 0) >= 0.58]
                if strict:
                    discovered = strict + [r for r in discovered if r not in strict]
                else:
                    exact_fallback = True
            discovered.sort(key=lambda r: _rank_key(query,r))
            results = discovered[:max_results]
            detail_errors = []
            detail_limit = _detail_enrichment_budget(results,auto_mode=auto_mode,details=details)
            if details and results:
                _progress("details_start", job_id=job_id, total=detail_limit, found=len(discovered), all_results=len(results))
                enriched=[]
                for i, rec in enumerate(results):
                    if time.monotonic() >= deadline:
                        termination_reason="deadline_reached"
                        errors.append("detail_deadline_reached")
                        enriched.extend(results[i:])
                        break
                    if i < detail_limit:
                        item, err = _enrich_branch(page, rec, lang, review_samples=(3 if auto_mode and i < 8 else 0))
                        item["detail_index"] = i + 1; item["detail_total"] = detail_limit
                        if err: detail_errors.append("{}: {}".format(clean(rec.get("name"))[:80], err))
                        _emit_live_result(item, "google_maps_detail")
                        _progress("details", job_id=job_id, current=i+1, total=detail_limit, found=len(discovered))
                    else:
                        item = dict(rec); item["details_loaded"] = bool(item.get("details_loaded"))
                    enriched.append(item)
                results=enriched
                for rec in results:
                    if glat is not None and rec.get("latitude") is not None:
                        rec["distance_km"] = haversine(glat, glng, rec["latitude"], rec["longitude"])
                    _apply_result_metrics(query, rec)
                errors.extend(detail_errors[:20])
                # Details may reveal stronger identities (CID/Place ID). Reconcile again before final output.
                reconciled = {}; reconciled_aliases = {}
                for rec in results:
                    _upsert_place(reconciled, reconciled_aliases, rec)
                results = list(reconciled.values())
                results.sort(key=lambda r: _rank_key(query,r))
                results = results[:max_results]
            for rec in results:
                _apply_result_metrics(query,rec)
                rec["confidence_score"] = _confidence_score(query, rec)
                rec["relevance"] = _relevance_band(rec.get("match_score"))
                rec["source_fetched_at"] = rec.get("source_fetched_at") or datetime.now().isoformat()
                rec["field_sources"] = _field_provenance(rec)
                rec["identity_alias_count"] = len(_place_aliases(rec))
        finally:
            browser.close()

    elapsed_ms = round((time.perf_counter() - started) * 1000, 1)
    unresolved_saturated = sum(1 for x in tile_stats if x.get("saturated") and int(x.get("subdivided") or 0) == 0)
    successful_tiles = len([x for x in tile_stats if x.get("tile",-1) >= 0])
    failed_tiles = sum(1 for e in errors if str(e).startswith("tile "))
    _coverage_summary=_coverage_quality_summary(tile_stats,len(all_found),discover_cap,termination_reason,unresolved_saturated,failed_tiles)
    _validation_counts={"confirmed":0,"probable":0,"weak":0}
    for _r in results:
        _apply_result_metrics(query,_r); _validation_counts[_r.get("validation_status") or "weak"]=_validation_counts.get(_r.get("validation_status") or "weak",0)+1
    meta = {
        "job_id": job_id,
        "elapsed_ms": elapsed_ms,
        "total_unique": len(results),
        "total_discovered": len(all_found),
        "display_limit": max_results,
        "discovery_limit": discover_cap,
        "with_coordinates": sum(1 for r in results if r.get("latitude") is not None),
        "with_phone": sum(1 for r in results if r.get("phone") or r.get("mobile")),
        "with_email": sum(1 for r in results if r.get("email")),
        "with_website": sum(1 for r in results if r.get("website")),
        "details_loaded": sum(1 for r in results if r.get("details_loaded")),
        "searched_tiles": successful_tiles,
        "planned_tiles": max(len(centers), next_tile_id if deep else len(centers)), "deep": bool(deep), "details": bool(details),
        "geocoded_center": geocoded,
        "coverage_status": "maximum" if deep else "standard",
        "saturated_tiles": sum(1 for x in tile_stats if x.get("saturated")),
        "subdivided_tiles": sum(1 for x in tile_stats if int(x.get("subdivided") or 0) > 0),
        "unresolved_saturated_tiles": unresolved_saturated,
        "tile_retries": tile_retry_count,
        "failed_tiles": failed_tiles,
        "deduped_aliases": len(alias_index),
        "duplicates_suppressed":int(dedupe_stats.get("duplicates_suppressed") or 0),"near_duplicate_merges":int(dedupe_stats.get("near_duplicate_merges") or 0),
        "strong_identity_conflicts":int(dedupe_stats.get("strong_identity_conflicts") or 0),
        "truncated": len(discovered) > max_results or len(all_found) >= discover_cap,
        "coverage_complete": _coverage_summary["complete"],"coverage_percent":_coverage_summary["percent"],
        "validation_confirmed":_validation_counts.get("confirmed",0),"validation_probable":_validation_counts.get("probable",0),"validation_weak":_validation_counts.get("weak",0),
        "termination_reason": termination_reason,
        "browser_engine": browser_engine,
        "hard_deadline_seconds": hard_seconds,
        "fallback_used": bool(locals().get("fallback_used", False)),
        "exact_match_relaxed": bool(locals().get("exact_fallback", False)),
        "google_places_api_available": bool(api_meta.get("available")),
        "google_places_api_used": bool(api_meta.get("used")),
        "google_places_api_count": len(api_records),
        "google_search_browser_count": google_web_count,
        "google_search_browser_meta": google_web_meta,
        "early_enriched_count": early_enriched_count,
        "early_review_samples": early_review_samples,
        "multi_provider_results": sum(1 for r in results if int(r.get("provider_count") or 0) > 1),
        "avg_data_completeness": round(sum(float(r.get("data_completeness") or 0) for r in results)/max(1,len(results)),1),
        "recovery_attempts": locals().get("recovery_attempts", []),
        "no_result_reason": ("google_blocked" if termination_reason=="google_blocked" else ("browser_or_layout_no_results" if not results else "")),
        "keyless_live": True,
        "providers": (["google_search_browser"] if google_web_meta.get("used") else []) + (["google_places_api"] if api_meta.get("used") else []) + ["google_maps_browser"],
        "direct_google_fast": False,
        "auto_mode": bool(auto_mode),
        "auto_query_variants": variants,
        "partial_name_supported": True,
        "suggestion_expansions": partial_expansions[:8],
        "full_details_count": sum(1 for r in results if r.get("details_loaded")),
        "basic_results_count": sum(1 for r in results if not r.get("details_loaded")),
        "google_preview_reviews": sum(int(r.get("preview_review_count") or 0) for r in results),
        "errors": errors[:30],
        "tile_stats": tile_stats[:100],
    }
    _progress("finalizing", job_id=job_id, found=len(results), discovered=len(all_found), termination_reason=termination_reason)
    _progress("done", job_id=job_id, found=len(results), discovered=len(all_found), elapsed_ms=elapsed_ms, termination_reason=termination_reason)
    return results, meta


def _scrape_place_legacy(query_or_url, max_reviews=100, lang="ar", is_url=False, known_review_keys=None):
    from playwright.sync_api import sync_playwright
    result = {"info": {"name":"","rating":None,"review_count":None,"address":None,
        "phone":None,"mobile":None,"email":None,"email_source":None,"website":None,"category":None,
        "cid":None,"place_id":None,"latitude":None,"longitude":None},
        "reviews": [], "error": None}
    try:
        with sync_playwright() as p:
            browser,_engine = _launch_browser(p)
            page = setup_page(browser, lang, _engine)
            try:
                if is_url: url = query_or_url
                else:
                    url = "https://www.google.com/maps/search/?api=1&query=" + quote_plus(query_or_url) + "&hl=" + lang
                _goto_retry(page, url, timeout=45000, attempts=3)
                dismiss_consent(page)
                try: page.wait_for_selector("h1.DUwDvf, a.hfpxzc", timeout=8000)
                except Exception: page.wait_for_timeout(1500)
                if not is_url and "/maps/place/" not in page.url:
                    for sel in ['a.hfpxzc','a[href*="/maps/place/"]']:
                        try:
                            loc = page.locator(sel)
                            if loc.count() > 0:
                                loc.first.click(timeout=6000)
                                page.wait_for_selector("h1.DUwDvf", timeout=6000); break
                        except Exception: continue
                result["info"] = extract_info(page)
                if open_reviews(page):
                    sort_reviews_newest(page)
                    result["reviews"] = extract_reviews(page, max_reviews, known_review_keys)
                if result["info"].get("review_count") is None and result["reviews"] and not known_review_keys:
                    result["info"]["review_count"] = len(result["reviews"])
                _progress("finalizing", found=len(result["reviews"]), total=max_reviews)
                _progress("done", found=len(result["reviews"]), total=max_reviews)
            finally: browser.close()
    except Exception as e:
        result["error"] = str(e) + "\n" + traceback.format_exc()
    return result



def search_places(query, max_results=100, lang="ar", city="", area="", activity="",
                  lat=None, lng=None, radius_km=None, deep=False, exact_name=False,
                  max_tiles=36, details=False, discover_max=None, auto_mode=False, refresh_stale_only=False, resume_file=None):
    """Async-first public entrypoint; sync legacy path is fallback-only."""
    try:
        results,meta=asyncio.run(async_search_places(query,max_results,lang,city,area,activity,lat,lng,radius_km,deep,
                                                      exact_name,max_tiles,details,discover_max,auto_mode,refresh_stale_only,resume_file))
    except Exception as exc:
        _diag("async_search_entry_failed",exc,query=query)
        _progress("async_fallback",query=query,error=str(exc)[:220])
        if str(os.environ.get("RT_ALLOW_LEGACY_FALLBACK","1")).strip().lower() in {"0","false","no"}:
            raise
        results,meta=_search_places_legacy(query,max_results,lang,city,area,activity,lat,lng,radius_km,deep,
                                           exact_name,max_tiles,details,discover_max,auto_mode)
        meta=dict(meta or {}); meta["async_mode"]=False; meta["async_fallback_reason"]="{}: {}".format(type(exc).__name__,str(exc)[:240])
    meta=dict(meta or {})
    reliability=_calculate_search_reliability(meta)
    meta["search_reliability"]=reliability; meta["search_reliability_score"]=reliability["score"]; meta["search_reliability_grade"]=reliability["grade"]
    return results,meta


def scrape_place(query_or_url, max_reviews=100, lang="ar", is_url=False, known_review_keys=None):
    """Async-first scrape used by saved-branch sync; preserves legacy fallback."""
    try:
        return asyncio.run(async_scrape_place(query_or_url,max_reviews,lang,is_url,known_review_keys))
    except Exception as exc:
        _diag("async_scrape_entry_failed",exc,query=query_or_url)
        _progress("async_scrape_fallback",query=query_or_url,error=str(exc)[:220])
        return _scrape_place_legacy(query_or_url,max_reviews,lang,is_url,known_review_keys)

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("mode", choices=["search","scrape","scrape-url","preflight","diagnostics"])
    ap.add_argument("query")
    ap.add_argument("--max", type=int, default=100)
    ap.add_argument("--lang", default="ar")
    ap.add_argument("--city", default="")
    ap.add_argument("--area", default="")
    ap.add_argument("--activity", default="")
    ap.add_argument("--lat", type=float, default=None)
    ap.add_argument("--lng", type=float, default=None)
    ap.add_argument("--radius", type=float, default=None)
    ap.add_argument("--deep", action="store_true")
    ap.add_argument("--exact-name", action="store_true")
    ap.add_argument("--max-tiles", type=int, default=36)
    ap.add_argument("--details", action="store_true")
    ap.add_argument("--auto", action="store_true")
    ap.add_argument("--refresh-stale-only", action="store_true")
    ap.add_argument("--resume-file", default="")
    ap.add_argument("--discover-max", type=int, default=None)
    ap.add_argument("--output", default="")
    ap.add_argument("--known-reviews-file", default="")
    args = ap.parse_args()
    known_review_keys=load_known_review_keys(args.known_reviews_file) if args.known_reviews_file else set()
    if args.mode == "search":
        places, meta = search_places(args.query, args.max, args.lang, args.city, args.area,
                                     args.activity, args.lat, args.lng, args.radius,
                                     args.deep, args.exact_name, args.max_tiles, args.details, args.discover_max, args.auto, args.refresh_stale_only, args.resume_file or None)
        res = {"results": places, "meta": meta}
    elif args.mode == "scrape":
        res = scrape_place(args.query, args.max, args.lang, is_url=False, known_review_keys=known_review_keys)
    elif args.mode == "scrape-url":
        res = scrape_place(args.query, args.max, args.lang, is_url=True, known_review_keys=known_review_keys)
    elif args.mode == "preflight":
        res = field_preflight(args.query,args.lang,live=True)
    else:
        pf=field_preflight(args.query,args.lang,live=True)
        out=args.output or str(APP_DIR / ("ReviewsTracker_Field_Diagnostics_"+datetime.now().strftime("%Y%m%d_%H%M%S")+".zip"))
        path=_create_field_diagnostic_bundle(out,preflight=pf,last_meta={},extra_text="Field diagnostics generated by Reviews Tracker 40.0")
        res={"ok":True,"path":path,"preflight":pf}
    payload = json.dumps(res, ensure_ascii=False)
    try:
        sys.stdout.buffer.write(payload.encode("utf-8")); sys.stdout.buffer.flush()
    except Exception: print(payload)


if __name__ == "__main__": main()

#!/usr/bin/env python3
"""Windows launcher for GoogleMapsReviewsTracker v45; supports worker subprocess routing."""
from __future__ import annotations
import os,runpy,sys
from pathlib import Path

ROOT=Path(getattr(sys,"_MEIPASS",Path(__file__).resolve().parent)).resolve()
APP=ROOT/"app.py"
CHILD_SCRIPTS={"worker.py","api.py","daemon.py"}
def _initialize_user_data_directory()->None:
    # User data must survive binary upgrades. An explicit override is reserved
    # for isolated QA, portable setups and controlled migrations.
    portable_home = Path(sys.executable).resolve().parent if getattr(sys,"frozen",False) else None
    portable_flag = bool(portable_home and (portable_home / "PORTABLE_MODE.flag").is_file())
    explicit = os.environ.get("REVIEWS_TRACKER_DATA_DIR")
    if explicit:
        data_dir = Path(explicit).expanduser().resolve()
    elif portable_flag:
        data_dir = portable_home / "Data"
    else:
        local = Path(os.environ.get("LOCALAPPDATA") or (Path.home() / "AppData" / "Local"))
        data_dir = local / "ProTech" / "GoogleMapsReviewsTracker"
    try:
        data_dir.mkdir(parents=True, exist_ok=True)
    except OSError as exc:
        raise RuntimeError("Cannot create writable Reviews Tracker data directory: " +
                           str(data_dir)) from exc
    if not explicit and not portable_flag and not (data_dir / "reviews.db").exists() and (ROOT / "reviews.db").is_file():
        # Atomic SQLite migration reads the old database with WAL support and
        # never deletes the original database from the previous installation.
        import sqlite3
        from contextlib import closing
        temp = data_dir / ".reviews_v43_import.tmp"
        if temp.exists():
            raise RuntimeError("A previous migration is incomplete. Check "+str(temp))
        try:
            with closing(sqlite3.connect("file:"+(ROOT/"reviews.db").as_posix()+"?mode=ro", uri=True)) as src:
                with closing(sqlite3.connect(str(temp))) as dst:
                    src.backup(dst)
                    if dst.execute("PRAGMA quick_check").fetchone()[0] != "ok":
                        raise RuntimeError("Legacy database verification failed")
            os.replace(temp, data_dir / "reviews.db")
        except Exception:
            if temp.exists():
                temp.unlink()
            raise
    if not explicit and not portable_flag and (ROOT / ".api_key").is_file() and not (data_dir / ".api_key").exists():
        import shutil
        shutil.copy2(ROOT / ".api_key", data_dir / ".api_key")
    os.environ["REVIEWS_TRACKER_DATA_DIR"] = str(data_dir)

def main()->int:
    _initialize_user_data_directory()
    if len(sys.argv)>=2 and Path(sys.argv[1]).name.lower() in CHILD_SCRIPTS:
        script=ROOT/Path(sys.argv[1]).name
        if not script.is_file():
            print("Required application component missing.",file=sys.stderr)
            return 2
        sys.argv=[str(script),*sys.argv[2:]]
        runpy.run_path(str(script),run_name="__main__")
        return 0
    if not APP.is_file():
        print("Application data files are missing.",file=sys.stderr)
        return 2
    os.environ["STREAMLIT_GLOBAL_DEVELOPMENT_MODE"]="false"
    os.environ.setdefault("STREAMLIT_BROWSER_GATHER_USAGE_STATS","false")
    os.environ.setdefault("STREAMLIT_SERVER_HEADLESS","false")
    from streamlit.web import cli as stcli
    sys.argv=["streamlit","run",str(APP),"--server.address","127.0.0.1",
              "--server.port","8597","--browser.gatherUsageStats","false"]
    stcli.main()
    return 0
if __name__=="__main__":
    raise SystemExit(main())

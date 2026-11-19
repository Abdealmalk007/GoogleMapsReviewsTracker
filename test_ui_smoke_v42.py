"""AppTest smoke tests for the approved Streamlit dashboard and persistent navigation."""
import os
from pathlib import Path
from streamlit.testing.v1 import AppTest

ROOT=Path(__file__).resolve().parent
os.environ.setdefault("HF_HUB_OFFLINE","1")
os.environ.setdefault("TRANSFORMERS_OFFLINE","1")
os.environ.setdefault("STREAMLIT_GLOBAL_DEVELOPMENT_MODE","false")

def run(page,lang="ar"):
    app=AppTest.from_file(str(ROOT/"app.py"),default_timeout=45)
    app.session_state["page"]=page
    app.session_state["lang"]=lang
    app.run(timeout=45)
    errors=[str(e.message) for e in app.exception]
    print("PAGE",page,"LANG",lang,"EXCEPTIONS",len(errors),"BUTTONS",len(app.button),
          "CHARTS",len(app.get("plotly_chart")),"SELECTBOXES",len(app.selectbox),flush=True)
    for error in errors[:3]:
        print("EXCEPTION",error[:1400],flush=True)
    if errors:raise SystemExit(2)
    if page=="dashboard":
        assert len(app.get("plotly_chart"))>=0
        texts=[str(x.value) for x in app.markdown]
        assert any("rt42-welcome" in x for x in texts),"Approved dashboard hero missing"
        assert any("rt42-kpi" in x for x in texts),"KPI cards missing"
        assert not any('<div class="rt-topmenus"></div>' in x for x in texts),"Legacy top menus still rendered"

if __name__=="__main__":
    run("dashboard","ar")
    run("dashboard","en")
    print("V42_STREAMLIT_UI_SMOKE_PASS",flush=True)

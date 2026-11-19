#!/usr/bin/env python3
"""S3 smoke: dashboard + connections page, local API security, secret exclusion."""
from pathlib import Path
from streamlit.testing.v1 import AppTest
import ast,unittest
ROOT=Path(__file__).resolve().parent
class FullUISmoke(unittest.TestCase):
    def test_dashboard(self):
        app=AppTest.from_file(str(ROOT/"app.py"),default_timeout=70)
        app.session_state["page"]="dashboard"
        app.session_state["_last_page_loaded"]=True
        app.run(timeout=70)
        self.assertFalse(app.exception,[str(x.message)[:500] for x in app.exception])
        self.assertGreaterEqual(len(app.button),15)
    def test_connections_form(self):
        app=AppTest.from_file(str(ROOT/"app.py"),default_timeout=40)
        app.session_state["page"]="connections"
        app.session_state["_last_page_loaded"]=True
        app.run(timeout=40)
        self.assertFalse(app.exception,[str(x.message)[:500] for x in app.exception])
        found=[widget.key for widget in app.text_input]
        self.assertIn("gbp_v41_location_name",found)
        self.assertIn("gbp_v41_access_token",found)
    def test_security_and_arabic_navigation(self):
        code=(ROOT/"api.py").read_text(encoding="utf-8")
        self.assertNotIn('print("API Key:',code)
        self.assertIn('"127.0.0.1"',code)
        app=(ROOT/"app.py").read_text(encoding="utf-8")
        nav=app[app.index("TOP_MENUS=["):app.index("st.markdown(",app.index("TOP_MENUS=["))]
        self.assertIn("الفروع والبحث",nav)
        self.assertIn('("connections",',nav)
        self.assertNotIn('"??????"',nav)
        for name in ["app.py","worker.py","api.py","daemon.py","business_profile_v41.py"]:
            ast.parse((ROOT/name).read_text(encoding="utf-8"))
if __name__=="__main__": unittest.main(verbosity=2)

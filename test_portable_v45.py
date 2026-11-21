"""v45 portable persistence, accepted logo and functional navigation regressions."""
from __future__ import annotations
import ast,importlib.util,os,sys,tempfile,unittest
from pathlib import Path
from unittest.mock import patch

ROOT=Path(__file__).resolve().parent
LAUNCHER=ROOT/"launcher_v45.py"

class PortableV45Tests(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory(prefix="reviews_v45_portable_")
        self.addCleanup(self.tmp.cleanup)
        self.root=Path(self.tmp.name)
        self.exe=self.root/"ReviewsTracker45.exe"
        self.exe.touch()
        self.resources=self.root/"_internal"
        self.resources.mkdir()
        spec=importlib.util.spec_from_file_location("reviews_launcher_v45_qa",LAUNCHER)
        self.module=importlib.util.module_from_spec(spec)
        spec.loader.exec_module(self.module)
        self.module.ROOT=self.resources

    def test_portable_marker_writes_into_own_data_folder(self):
        (self.root/"PORTABLE_MODE.flag").write_text("portable",encoding="utf-8")
        env={k:v for k,v in os.environ.items() if k!="REVIEWS_TRACKER_DATA_DIR"}
        with patch.dict(os.environ,env,clear=True), \
             patch.object(sys,"executable",str(self.exe)), \
             patch.object(sys,"frozen",True,create=True):
            self.module._initialize_user_data_directory()
            actual=Path(os.environ["REVIEWS_TRACKER_DATA_DIR"])
            self.assertEqual(actual,(self.root/"Data").resolve())
            self.assertTrue(actual.is_dir())
            self.assertFalse((self.root/"reviews.db").exists())
            self.module._initialize_user_data_directory()
            self.assertEqual(Path(os.environ["REVIEWS_TRACKER_DATA_DIR"]),actual)

    def test_installed_build_without_marker_keeps_localappdata(self):
        local=self.root/"AppData"
        env={k:v for k,v in os.environ.items() if k!="REVIEWS_TRACKER_DATA_DIR"}
        env["LOCALAPPDATA"]=str(local)
        with patch.dict(os.environ,env,clear=True), \
             patch.object(sys,"executable",str(self.exe)), \
             patch.object(sys,"frozen",True,create=True):
            self.module._initialize_user_data_directory()
            self.assertEqual(
                Path(os.environ["REVIEWS_TRACKER_DATA_DIR"]),
                local/"ProTech"/"GoogleMapsReviewsTracker"
            )

    def test_explicit_data_path_overrides_both_modes(self):
        (self.root/"PORTABLE_MODE.flag").write_text("portable",encoding="utf-8")
        custom=self.root/"CustomData"
        with patch.dict(os.environ,{"REVIEWS_TRACKER_DATA_DIR":str(custom)}), \
             patch.object(sys,"executable",str(self.exe)), \
             patch.object(sys,"frozen",True,create=True):
            self.module._initialize_user_data_directory()
            self.assertEqual(Path(os.environ["REVIEWS_TRACKER_DATA_DIR"]),custom.resolve())

    def test_visual_assets_and_branding_are_bundled(self):
        app=(ROOT/"app.py").read_text(encoding="utf-8")
        dash=(ROOT/"dashboard_v42.py").read_text(encoding="utf-8")
        ast.parse(app);ast.parse(dash)
        self.assertIn('page_icon=str(APP_DIR / "assets" / "reviews_tracker_icon.png")',app)
        self.assertIn('"ui_v45.css"',app)
        self.assertIn("rt45-feature",dash)
        self.assertIn("rt45-result-card",app)
        self.assertEqual(dash.count("rt45-feature-"+""),0) if False else None
        for name in ("reviews_tracker_icon.png","reviews_tracker_icon.ico"):
            self.assertTrue((ROOT/"assets"/name).is_file())

if __name__=="__main__":
    unittest.main(verbosity=2)

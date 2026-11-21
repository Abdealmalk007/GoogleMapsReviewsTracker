"""Synthetic tests of portable/installed Reviews Tracker v43 data migration."""
import importlib.util
import os
import sqlite3
from contextlib import closing
import tempfile
import unittest
from pathlib import Path

LAUNCHER=Path(__file__).resolve().parent/"launcher_v43.py"

class PersistentDataTests(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory(prefix="reviews_v43_")
        self.addCleanup(self.tmp.cleanup)
        self.root=Path(self.tmp.name)
        self.existing={k:os.environ.get(k) for k in ("LOCALAPPDATA","REVIEWS_TRACKER_DATA_DIR")}
        self.addCleanup(self.restore)
        self.spec=importlib.util.spec_from_file_location("v43_launcher_qa",LAUNCHER)
        self.module=importlib.util.module_from_spec(self.spec)
        self.spec.loader.exec_module(self.module)
        self.module.ROOT=self.root/"previous_installed_internal"
        self.module.ROOT.mkdir(parents=True)

    def restore(self):
        for k,v in self.existing.items():
            if v is None:os.environ.pop(k,None)
            else:os.environ[k]=v

    def test_upgrade_sqlite_uses_non_destructive_atomic_backup(self):
        legacy=self.module.ROOT/"reviews.db"
        with closing(sqlite3.connect(legacy)) as con:
            con.execute("CREATE TABLE companies (pk TEXT PRIMARY KEY,name TEXT)")
            con.execute("INSERT INTO companies VALUES('qa','Synthetic Branch')")
            con.commit()
        os.environ["LOCALAPPDATA"]=str(self.root/"appdata")
        os.environ.pop("REVIEWS_TRACKER_DATA_DIR",None)
        self.module._initialize_user_data_directory()
        data=Path(os.environ["REVIEWS_TRACKER_DATA_DIR"])
        self.assertTrue(data.is_relative_to(self.root))
        self.assertTrue(legacy.is_file(),"Migration must retain old DB")
        with closing(sqlite3.connect(data/"reviews.db")) as con:
            self.assertEqual(con.execute("SELECT COUNT(*) FROM companies").fetchone()[0],1)
            self.assertEqual(con.execute("PRAGMA quick_check").fetchone()[0],"ok")
        self.module._initialize_user_data_directory()
        with closing(sqlite3.connect(data/"reviews.db")) as con:
            self.assertEqual(con.execute("SELECT COUNT(*) FROM companies").fetchone()[0],1)

    def test_explicit_qa_override_is_not_replaced(self):
        explicit=self.root/"isolated_qa"
        os.environ["REVIEWS_TRACKER_DATA_DIR"]=str(explicit)
        self.module._initialize_user_data_directory()
        self.assertEqual(Path(os.environ["REVIEWS_TRACKER_DATA_DIR"]),explicit.resolve())
        self.assertTrue(explicit.is_dir())

if __name__=="__main__":
    unittest.main(verbosity=2)

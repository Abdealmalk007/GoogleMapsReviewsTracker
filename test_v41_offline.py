"""v41 offline regression tests against an isolated database copy."""
import ast
import contextlib
import datetime
import importlib.util
import json
import os
import pathlib
import sqlite3
import tempfile
import unittest
from pathlib import Path

ROOT=Path(__file__).resolve().parent
SOURCE=(ROOT/"app.py").read_text(encoding="utf-8")
TREE=ast.parse(SOURCE)
DB_CLASS=next(n for n in TREE.body if isinstance(n,ast.ClassDef) and n.name=="DB")
METHOD_NAMES={"_c","_table_exists","_cols","_ensure_search_jobs_compat_v41"}
DB_TEST_CLASS=ast.ClassDef(name="DBTest",bases=[],keywords=[],body=[n for n in DB_CLASS.body if isinstance(n,(ast.FunctionDef,ast.AsyncFunctionDef)) and n.name in METHOD_NAMES],decorator_list=[])
ast.fix_missing_locations(DB_TEST_CLASS)
PROGRAM=ast.Module(body=[DB_TEST_CLASS],type_ignores=[])
ast.fix_missing_locations(PROGRAM)

class RegressionV41(unittest.TestCase):
 def setUp(self):
  self.temp=tempfile.TemporaryDirectory()
  self.db_path=Path(self.temp.name)/"reviews.db"
  with contextlib.closing(sqlite3.connect(self.db_path)) as conn, conn:
   conn.execute("CREATE TABLE search_jobs (id INTEGER PRIMARY KEY, workspace_id INTEGER, query TEXT, status TEXT, started_at TEXT, completed_at TEXT, elapsed_ms REAL, results INTEGER, discovered INTEGER, tiles INTEGER, errors INTEGER, detail TEXT)")
   conn.execute("CREATE TABLE companies (pk TEXT PRIMARY KEY, name TEXT)")
   conn.execute("INSERT INTO companies VALUES (?,?)",("fixture-1","Sample branch"))
  self.ns={"sqlite3":sqlite3,"contextlib":contextlib,"DB_PATH":self.db_path}
  exec(compile(PROGRAM,"<db-v41>","exec"),self.ns)
 def tearDown(self):self.temp.cleanup()
 def test_database_migration_is_idempotent(self):
  test_db=self.ns["DBTest"]()
  test_db._ensure_search_jobs_compat_v41()
  test_db._ensure_search_jobs_compat_v41()
  with contextlib.closing(sqlite3.connect(self.db_path)) as c, c:
   self.assertIn("job_id",[x[1] for x in c.execute("PRAGMA table_info(search_jobs)")])
   self.assertEqual(c.execute("PRAGMA quick_check").fetchone()[0],"ok")
   self.assertEqual(c.execute("SELECT COUNT(*) FROM companies").fetchone()[0],1)
 def test_job_creation_and_deduplication(self):
  test_db=self.ns["DBTest"]()
  test_db._ensure_search_jobs_compat_v41()
  with contextlib.closing(sqlite3.connect(self.db_path)) as c, c:
   c.execute("INSERT INTO search_jobs(job_id,workspace_id,query,status,started_at) VALUES (?,?,?,?,?)",("v41-smoke",1,"test search","running","2020-01-01T00:00:00"))
   self.assertEqual(c.execute("SELECT job_id FROM search_jobs WHERE job_id=?",("v41-smoke",)).fetchone()[0],"v41-smoke")
   with self.assertRaises(sqlite3.IntegrityError):
    c.execute("INSERT INTO search_jobs(job_id,workspace_id) VALUES (?,?)",("v41-smoke",1))
 def test_api_search_jobs_schema_and_auth(self):
  test_db=self.ns["DBTest"]()
  test_db._ensure_search_jobs_compat_v41()
  spec=importlib.util.spec_from_file_location("rt41_api",ROOT/"api.py")
  module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module)
  module.DB_PATH=self.db_path
  client=module.app.test_client()
  self.assertEqual(client.get("/api/v1/search-jobs").status_code,401)
  self.assertEqual(client.get("/api/v1/search-jobs",query_string={"api_key":module.API_KEY}).status_code,401)
  response=client.get("/api/v1/search-jobs",headers={"X-API-Key":module.API_KEY})
  self.assertEqual(response.status_code,200,response.get_json())
 def test_design_and_python_parser(self):
  for filename in ("app.py","api.py","worker.py","daemon.py"):
   ast.parse((ROOT/filename).read_text(encoding="utf-8"))
  self.assertTrue((ROOT/"ui_v41.css").exists())
  self.assertEqual(SOURCE.count("TOP_MENUS=["),0)
  self.assertIn("render_dashboard_v42(",SOURCE)
  self.assertTrue((ROOT/"dashboard_v42.py").is_file())

if __name__=="__main__":
 unittest.main(verbosity=2)

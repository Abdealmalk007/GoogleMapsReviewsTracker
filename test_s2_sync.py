#!/usr/bin/env python3
"""Offline deep-sync orchestration tests on synthetic data only."""
import ast,hashlib,json,sys,tempfile,time,unittest
from pathlib import Path
from unittest.mock import Mock
import pandas as pd
ROOT=Path(__file__).resolve().parent
sys.path.insert(0,str(ROOT))
from worker import review_identity_key

def load_sync_function(tmp, callback):
    tree=ast.parse((ROOT/"app.py").read_text(encoding="utf-8"))
    fn=next(x for x in tree.body if isinstance(x,ast.FunctionDef) and x.name=="deep_sync_reviews_for_branch")
    mod=ast.Module(body=[fn],type_ignores=[])
    ast.fix_missing_locations(mod)
    ns={"pd":pd,"json":json,"JOB_DIR":tmp,"hashlib":hashlib,"time":time,
        "review_identity_key":review_identity_key,"scrape_and_store":callback,
        "safe_num":lambda v,default=0:float(v or default)}
    exec(compile(mod,"v41-deep-sync","exec"),ns)
    return ns["deep_sync_reviews_for_branch"]

class MockDB:
    def __init__(self):
        self.rows=[{"review_id":"existing","author":"A","rating":4,"text":"Good"}]
        self.sync_calls=[]
    def get_reviews(self,pk,workspace):return pd.DataFrame(self.rows)
    def update_deep_review_sync_state(self,pk,workspace,**kwargs):
        self.sync_calls.append((pk,workspace,kwargs))

class SyncCases(unittest.TestCase):
    def test_incremental_add_and_state(self):
        db=MockDB()
        with tempfile.TemporaryDirectory() as td:
            known_files=[]
            def callback(db_obj,target,limit,is_url,ws,user,**kwargs):
                known_path=Path(kwargs["known_reviews_file"])
                known_files.append(known_path)
                known=set(json.loads(known_path.read_text(encoding="utf-8"))["known_review_keys"])
                self.assertIn(review_identity_key(db_obj.rows[0]),known)
                db_obj.rows.append({"review_id":"new","author":"B","rating":1,"text":"Bad"})
                return "pk-1",{"review_count":2},1,1,None
            sync=load_sync_function(Path(td),callback)
            result=sync(db,"pk-1",{"maps_url":"https://maps.google.com/example"},ws_id=1,username="test")
            self.assertTrue(result["ok"])
            self.assertEqual(result["new"],1)
            self.assertEqual(result["known_before"],1)
            self.assertEqual(result["known_after"],2)
            self.assertEqual(db.sync_calls[-1][2]["status"],"ok")
            self.assertFalse(known_files[0].exists())
    def test_error_is_recorded_without_faking_success(self):
        db=MockDB()
        with tempfile.TemporaryDirectory() as td:
            def callback(db_obj,target,limit,is_url,ws,user,**kwargs):
                return "pk-1",{"review_count":1},0,0,"provider_unavailable"
            sync=load_sync_function(Path(td),callback)
            result=sync(db,"pk-1",{"name":"Test"},ws_id=1)
            self.assertFalse(result["ok"])
            self.assertEqual(result["new"],0)
            self.assertEqual(db.sync_calls[-1][2]["status"],"error")
if __name__=="__main__":unittest.main(verbosity=2)

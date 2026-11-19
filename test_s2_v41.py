#!/usr/bin/env python3
"""Stage S2 offline tests; all external APIs mocked, baseline DB read-only."""
from __future__ import annotations
import ast, contextlib, importlib.util, io, json, math, os, sqlite3
import tempfile, unittest
from pathlib import Path
from unittest.mock import patch
import pandas as pd

ROOT=Path(__file__).resolve().parent
def import_file(name):
    spec=importlib.util.spec_from_file_location("rt41_"+name,ROOT/(name+".py"))
    mod=importlib.util.module_from_spec(spec);spec.loader.exec_module(mod)
    return mod

worker=import_file("worker")
quality=import_file("quality_v41")
def extract_app_functions(*names):
    tree=ast.parse((ROOT/"app.py").read_text(encoding="utf-8"))
    selected=[n for n in tree.body if isinstance(n,ast.FunctionDef) and n.name in names]
    if len(selected)!=len(names):
        raise AssertionError(f"Expected {names}, got {[x.name for x in selected]}")
    module=ast.Module(body=selected,type_ignores=[])
    ast.fix_missing_locations(module)
    ns={"pd":pd,"math":math,"RTL":False,
        "safe_num":lambda v,default=0.0:float(v) if v is not None else float(default),
        "current_health_weights":lambda:{"rating":35,"sentiment":25,"response":20,"trend":10,"critical":10}}
    exec(compile(module,"app-functions","exec"),ns)
    return ns

class FakeResponse:
    def __init__(self,payload):self.payload=payload
    def __enter__(self):return self
    def __exit__(self,*args):return False
    def read(self,*args):return json.dumps(self.payload).encode("utf-8")

class StageTwoTests(unittest.TestCase):
    def test_places_pagination_sample_and_duplicate_places(self):
        a={"id":"place-a","displayName":{"text":"فرع أ"},"userRatingCount":200,
           "rating":4.8,"reviews":[{"name":"review-a","rating":4,"text":{"text":"جيد"}}]}
        b={"id":"place-b","displayName":{"text":"فرع ب"},"userRatingCount":110,
           "rating":4.4,"reviews":[{"name":f"review-{i}","rating":5,"text":{"text":"ok"}} for i in range(8)]}
        c={"id":"place-c","displayName":{"text":"فرع ج"},"userRatingCount":88,"rating":4}
        pages=[{"places":[a,b],"nextPageToken":"next-page"},{"places":[b,c]}]
        requests=[]
        def fake_get(request,timeout=12):
            requests.append(json.loads(request.data.decode("utf-8")))
            return FakeResponse(pages[len(requests)-1])
        with patch.dict(os.environ,{"GOOGLE_MAPS_API_KEY":"fixture-only","GOOGLE_PLACES_API_KEY":""}):
            with patch.object(worker.urllib.request,"urlopen",side_effect=fake_get):
                with patch.object(worker,"_emit_live_result"):
                    with patch.object(worker.time,"sleep"):
                        results,meta=worker.google_places_api_search("شركة",max_results=3)
        self.assertEqual(meta["calls"],2)
        self.assertEqual(len(results),3)
        self.assertEqual([x["place_id"] for x in results],["place-a","place-b","place-c"])
        self.assertEqual(requests[1]["pageToken"],"next-page")
        self.assertTrue(all(x["review_sample_only"] for x in results))
        self.assertLessEqual(max(x["preview_review_count"] for x in results),5)

    def test_checkpoint_resume_and_fingerprint(self):
        a=worker._search_resume_fingerprint("  مطعم الرياض ",mode="maps",limit=20)
        b=worker._search_resume_fingerprint("مطعم الرياض",mode="maps",limit=20)
        other=worker._search_resume_fingerprint("مطعم الرياض",mode="maps",limit=30)
        self.assertEqual(a,b);self.assertNotEqual(a,other)
        with tempfile.TemporaryDirectory() as d:
            p=Path(d)/"checkpoint.json"
            state=worker._new_search_checkpoint(a,"مطعم الرياض")
            state["completed_jobs"]=["tile-A"]
            state["results"]=[{"place_id":"place-A"}]
            self.assertTrue(worker._save_search_checkpoint(p,state))
            got=worker._load_search_checkpoint(p,fingerprint=a)
            self.assertEqual(got["completed_jobs"],["tile-A"])
            self.assertEqual(len(got["results"]),1)
            mismatch=worker._load_search_checkpoint(p,fingerprint=other)
            self.assertFalse(mismatch["compatible"])
            self.assertEqual(mismatch["reset_reason"],"fingerprint_mismatch")

    def test_review_idempotency(self):
        items=[{"review_id":"r1","author":"A","rating":5,"text":"Great"},
               {"review_id":"r1","author":"A","rating":5,"text":"Great"},
               {"review_id":"idx-0-1","author":"B","rating":1,"text":"Bad","date":"yesterday"},
               {"review_id":"idx-0-2","author":"B","rating":1,"text":"Bad","date":"yesterday"}]
        result=worker.filter_unseen_reviews(items)
        self.assertEqual(len(result),2)
        stored={x["review_identity_key"] for x in result}
        self.assertEqual(worker.filter_unseen_reviews(items,stored),[])

    def test_review_extractor_scroll_duplicate_without_ids(self):
        class FakeCard:
            def __init__(self,info):self.info=info
            def get_attribute(self,*args,**kwargs):return ""
            def locator(self,*args):return self
            def count(self):return 0
        class FakeCards:
            def __init__(self,page):self.page=page
            def count(self):
                self.page.iteration+=1
                return 1 if self.page.iteration==1 else 2
            def nth(self,i):return self.page.cards[i]
        class FakePage:
            def __init__(self):
                self.iteration=0
                self.cards=[FakeCard({"author":"A","text":"same","date":"now"}),
                            FakeCard({"author":"B","text":"other","date":"yesterday"})]
            def wait_for_selector(self,*args,**kwargs):return True
            def locator(self,*args):return FakeCards(self)
            def evaluate(self,*args):return None
            def wait_for_timeout(self,*args):return None
        def fake_text(card,selectors,timeout=500):
            vals=" ".join(selectors)
            if ".d4r55" in vals:return card.info["author"]
            if ".wiI7pd" in vals:return card.info["text"]
            if ".rsqaWe" in vals:return card.info["date"]
            return ""
        with patch.object(worker,"first_text",side_effect=fake_text):
            with patch.object(worker,"first_attr",return_value="5 stars"):
                with patch.object(worker,"_progress"):
                    result=worker.extract_reviews(FakePage(),max_reviews=10)
        self.assertEqual(len(result),2)
        self.assertEqual(len({x["review_identity_key"] for x in result}),2)

    def test_quality_from_public_synthetic_fixture(self):
        df=pd.DataFrame([
            {"pk":"a","name":"A","place_id":"pa","maps_url":"https://maps.google.com/a","rating":4.7,"phone":"+966500000000"},
            {"pk":"b","name":"B","place_id":"pb","maps_url":"","rating":None,"phone":None},
            {"pk":"c","name":"C","place_id":"","maps_url":"https://maps.google.com/c","rating":4.3,"mobile":"0500000000"}])
        result=quality.branch_quality_summary(df)
        self.assertEqual(result["total_branches"],3)
        self.assertEqual(result["place_ids"],2)
        self.assertEqual(result["maps_urls"],2)
        self.assertEqual(result["contact_numbers"],2)
        self.assertEqual(result["ratings"],2)
        self.assertEqual(len(result["missing_rows"]),2)
        self.assertIn("missing_fields",result["missing_rows"].columns)

    def test_history_uses_only_real_snapshots(self):
        ns=extract_app_functions("aggregate_rating_timeline_v40","rating_trend_summary_v40")
        rows=pd.DataFrame([
            {"pk":"A","day":"2026-01-01","rating":4.1,"total":10,"neg":1,"pos":9},
            {"pk":"A","day":"2026-01-30","rating":4.3,"total":13,"neg":1,"pos":12},
            {"pk":"A","day":"2026-03-20","rating":4.0,"total":20,"neg":3,"pos":17},
        ])
        actual=ns["aggregate_rating_timeline_v40"](rows)
        self.assertEqual(actual["period"].tolist(),["2026-01","2026-03"])
        self.assertAlmostEqual(float(actual.iloc[-1]["rating_delta"]),-.3)
        summary=ns["rating_trend_summary_v40"](actual)
        self.assertEqual(summary["points"],2)
        self.assertEqual(summary["current_total"],20)
        self.assertEqual(summary["direction"],"declined")

    def test_public_rating_priority_over_partial_sample(self):
        ns=extract_app_functions("health_grade_v3","health_score_v3","kpis")
        reviews=pd.DataFrame([{"rating":1,"sentiment":"negative","responded":0},
                              {"rating":2,"sentiment":"negative","responded":0}])
        score,detail=ns["health_score_v3"](reviews,{"rating":4.6})
        self.assertAlmostEqual(detail["raw"]["avg_rating"],4.6)
        self.assertEqual(detail["raw"]["rating_source"],"branch_aggregate")
        self.assertAlmostEqual(ns["kpis"](reviews,{"rating":4.6})["avg"],4.6)
        self.assertLess(detail["raw"]["sample_rating_avg"],4.6)
    def test_full_export_filters_scope_and_escapes_excel_formulas(self):
        from datetime import datetime
        from openpyxl import load_workbook
        ns=extract_app_functions(
            "aggregate_rating_timeline_v40",
            "rating_trend_summary_v40",
            "complete_export_datasets_v40",
            "build_complete_export_excel_v40",
            "_sanitize_export_dataframe_v41",
            "to_csv")
        ns.update({"VERSION":"41.0.0-dev","datetime":datetime,"io":io})
        branches=pd.DataFrame([
            {"pk":"A","name":"=SUM(2,2)","rating":4.5,"review_count":50,
             "phone":"+966555555555","maps_url":"https://maps.google.com/a"},
            {"pk":"B","name":"PRIVATE_OTHER_BRANCH","rating":2.0,"review_count":5}
        ])
        class Database:
            def snapshots(self,workspace,pk,limit):
                return pd.DataFrame([
                    {"pk":"A","day":"2026-01-10","rating":4.3,"total":30,"neg":1,"pos":29},
                    {"pk":"B","day":"2026-01-10","rating":2.5,"total":5,"neg":3,"pos":2}])
            def all_reviews(self,workspace):
                return pd.DataFrame([
                    {"pk":"A","rating":4,"text":"=HYPERLINK(\"https://example.test\",\"x\")",
                     "sentiment":"positive","responded":1},
                    {"pk":"B","rating":1,"text":"PRIVATE_REVIEW",
                     "sentiment":"negative","responded":0}])
        d=ns["complete_export_datasets_v40"](Database(),1,branches,pks=["A"])
        self.assertEqual(len(d["branches"]),1)
        self.assertEqual(len(d["reviews"]),1)
        self.assertEqual(len(d["rating_history"]),1)
        self.assertEqual(d["summary"].iloc[0]["version"],"41.0.0-dev")
        excel=ns["build_complete_export_excel_v40"](d)
        wb=load_workbook(io.BytesIO(excel))
        self.assertEqual(len(wb.sheetnames),6)
        self.assertEqual(wb["Branches"]["B2"].data_type,"s")
        self.assertTrue(str(wb["Branches"]["B2"].value).startswith("'="))
        self.assertFalse(any("PRIVATE_OTHER_BRANCH" in str(c.value) for sh in wb for row in sh for c in row))
        csv=ns["to_csv"](pd.DataFrame([{"name":"=SUM(2,2)"}])).decode("utf-8-sig")
        self.assertIn("'=SUM(2,2)",csv)

if __name__=="__main__":unittest.main(verbosity=2)

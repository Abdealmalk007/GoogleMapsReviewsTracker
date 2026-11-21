"""Regression tests for the user-approved v42 dashboard. Synthetic data only."""
import importlib.util
import math
import pathlib
import unittest
import pandas as pd

ROOT = pathlib.Path(__file__).resolve().parent
spec=importlib.util.spec_from_file_location("rt_dashboard_v42",ROOT/"dashboard_v42.py")
dashboard=importlib.util.module_from_spec(spec)
spec.loader.exec_module(dashboard)

class DashboardV42Tests(unittest.TestCase):
    def test_empty_data_cannot_fabricate_kpis(self):
        result=dashboard.build_dashboard_data(pd.DataFrame(),pd.DataFrame())
        self.assertIsNone(result["average"])
        self.assertIsNone(result["response_rate"])
        self.assertEqual(result["public_reviews"],0)
        self.assertEqual(result["stored_reviews"],0)
        self.assertTrue(result["trend"].empty)
        self.assertEqual(result["open_alerts"],0)

    def test_weighted_real_branch_rating_not_review_sample(self):
        branches=pd.DataFrame([
            {"pk":"one","name":"A","rating":5.0,"review_count":100},
            {"pk":"two","name":"B","rating":3.0,"review_count":300}
        ])
        reviews=pd.DataFrame([
            {"pk":"one","rating":1,"sentiment":"negative","responded":0,"text":"sample"},
            {"pk":"two","rating":2,"sentiment":"neutral","responded":1,"text":"sample"}
        ])
        result=dashboard.build_dashboard_data(branches,reviews)
        self.assertAlmostEqual(result["average"],3.5,5)
        self.assertEqual(result["average_source"],"weighted_complete")
        self.assertEqual(result["public_reviews"],400)
        self.assertEqual(result["stored_reviews"],2)
        self.assertEqual(result["response_rate"],50)
        self.assertEqual(result["sentiments"]["negative"],1)
        self.assertEqual(result["sentiments"]["neutral"],1)

    def test_branch_filter_applies_everywhere(self):
        companies=pd.DataFrame([
            {"pk":"a","name":"Riyadh","rating":4.8,"review_count":90},
            {"pk":"b","name":"Jeddah","rating":3.2,"review_count":50}
        ])
        reviews=pd.DataFrame([
            {"pk":"a","rating":5,"sentiment":"positive","responded":True,
             "text":"Good","fetched_at":"2026-11-19"},
            {"pk":"b","rating":1,"sentiment":"negative","responded":False,
             "text":"Bad","fetched_at":"2026-11-18"}
        ])
        snapshots=pd.DataFrame([
            {"pk":"a","day":"2026-11-10","rating":4.5,"total":85},
            {"pk":"b","day":"2026-11-10","rating":3.0,"total":48},
            {"pk":"a","day":"2026-11-18","rating":4.8,"total":90}
        ])
        alerts=pd.DataFrame([{"pk":"b","title":"Delayed response"}])
        result=dashboard.build_dashboard_data(companies,reviews,snapshots,alerts,"a")
        self.assertEqual(result["branches"],1)
        self.assertEqual(result["public_reviews"],90)
        self.assertEqual(result["stored_reviews"],1)
        self.assertEqual(result["open_alerts"],0)
        self.assertEqual(len(result["trend"]),2)
        self.assertTrue((result["recent_reviews"]["pk"]=="a").all())

    def test_unknown_sentiment_is_not_called_neutral(self):
        companies=pd.DataFrame([{"pk":"a","name":"A"}])
        reviews=pd.DataFrame([{"pk":"a","sentiment":"","text":"x"},
                              {"pk":"a","sentiment":"positive","text":"y"}])
        result=dashboard.build_dashboard_data(companies,reviews)
        self.assertEqual(result["sentiments"]["unknown"],1)
        self.assertEqual(result["sentiments"]["neutral"],0)
        self.assertEqual(result["sentiments"]["positive"],1)

    def test_real_snapshot_weighting(self):
        snapshots=pd.DataFrame([
            {"pk":"one","day":"2026-11-18","rating":5,"total":100},
            {"pk":"two","day":"2026-11-18","rating":3,"total":300}
        ])
        result=dashboard.build_dashboard_data(pd.DataFrame(),pd.DataFrame(),snapshots)
        self.assertAlmostEqual(result["trend"].iloc[0]["rating"],3.5)

    def test_user_input_html_escaping(self):
        from html import escape
        self.assertEqual(escape('<img src=x onerror=alert(1)>'),
                         '&lt;img src=x onerror=alert(1)&gt;')
        self.assertEqual(dashboard._format_value(None),"—")

if __name__=="__main__":
    unittest.main(verbosity=2)

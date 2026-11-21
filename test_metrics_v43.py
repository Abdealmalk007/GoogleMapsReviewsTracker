"""Reviews Tracker v43 data-integrity regression tests (synthetic fixtures only)."""
from __future__ import annotations
import unittest
import pandas as pd
from metrics_v43 import build_dashboard_data

NOW="2026-11-19T12:00:00+00:00"


def branches(*rows):
    return pd.DataFrame(list(rows))


class V43AccuracyTests(unittest.TestCase):
    def verify(self,companies=None,reviews=None,snapshots=None,alerts=None,**kwargs):
        return build_dashboard_data(
            companies if companies is not None else pd.DataFrame(),
            reviews if reviews is not None else pd.DataFrame(),
            snapshots,alerts,as_of=NOW,**kwargs
        )

    def test_empty_values_are_unknown_not_zero(self):
        x=self.verify()
        self.assertIsNone(x["average"])
        self.assertIsNone(x["response_rate"])
        self.assertIsNone(x["data_age_days"])
        self.assertTrue(x["stale_data"])

    def test_sparse_review_counts_do_not_create_biased_weighted_rating(self):
        x=self.verify(branches(
            {"pk":"a","name":"A","rating":5.0,"review_count":100},
            {"pk":"b","name":"B","rating":1.0,"review_count":None},
            {"pk":"c","name":"C","rating":3.0,"review_count":0}))
        self.assertEqual(x["average_source"],"mean_known_branch_ratings")
        self.assertAlmostEqual(x["average"],3.0)
        self.assertEqual(x["rating_coverage"],3)
        self.assertEqual(x["count_coverage"],2)
        self.assertEqual(x["missing_review_counts"],1)
        self.assertFalse(x["public_reviews_complete"])

    def test_full_review_count_coverage_allows_weighted_mean(self):
        x=self.verify(branches(
            {"pk":"a","name":"A","rating":5.0,"review_count":2},
            {"pk":"b","name":"B","rating":1.0,"review_count":6}))
        self.assertEqual(x["average_source"],"weighted_complete")
        self.assertAlmostEqual(x["average"],2.0)

    def test_invalid_values_never_pollute_counts(self):
        x=self.verify(branches(
            {"pk":"a","name":"A","rating":8.0,"review_count":-3},
            {"pk":"b","name":"B","rating":"NaN","review_count":"??"},
            {"pk":"c","name":"C","rating":4.2,"review_count":"3.5"}))
        self.assertEqual(x["rating_coverage"],1)
        self.assertEqual(x["count_coverage"],0)
        self.assertEqual(x["public_reviews"],0)
        self.assertEqual(x["missing_review_counts"],3)

    def test_exact_duplicate_review_id_deduped_only_within_branch(self):
        rows=branches(
            {"id":1,"pk":"a","review_id":"r100","responded":0,"sentiment":"positive"},
            {"id":2,"pk":"a","review_id":"r100","responded":1,"sentiment":"negative"},
            {"id":3,"pk":"b","review_id":"r100","responded":0,"sentiment":"neutral"})
        x=self.verify(reviews=rows)
        self.assertEqual(x["duplicate_review_records"],1)
        self.assertEqual(x["stored_reviews"],2)
        self.assertEqual(x["sentiments"]["negative"],1)
        self.assertEqual(x["sentiments"]["neutral"],1)

    def test_unidentified_similar_reviews_are_not_deleted(self):
        x=self.verify(reviews=branches(
            {"id":1,"pk":"a","author":"X","text":"Great","rating":5},
            {"id":2,"pk":"a","author":"X","text":"Great","rating":5}))
        self.assertEqual(x["stored_reviews"],2)
        self.assertEqual(x["duplicate_review_records"],0)

    def test_response_rate_uses_known_denominator_only(self):
        x=self.verify(reviews=branches(
            {"id":1,"pk":"a","responded":1,"response_text":""},
            {"id":2,"pk":"a","responded":0,"response_text":""},
            {"id":3,"pk":"a","responded":None,"response_text":None},
            {"id":4,"pk":"a","responded":None,"response_text":"Owner reply"}))
        self.assertEqual(x["response_known"],3)
        self.assertEqual(x["response_unknown"],1)
        self.assertEqual(x["response_responded"],2)
        self.assertEqual(x["response_rate"],66.7)

    def test_arabic_english_unknown_sentiment_labels(self):
        x=self.verify(reviews=branches(
            {"id":1,"pk":"a","sentiment":"إيجابية"},
            {"id":2,"pk":"a","sentiment":"NEGATIVE"},
            {"id":3,"pk":"a","sentiment":"محايدة"},
            {"id":4,"pk":"a","sentiment":"not_set"}))
        self.assertEqual(x["sentiments"],{
            "positive":1,"neutral":1,"negative":1,"unknown":1
        })

    def test_relative_review_post_date_not_used_for_sorting(self):
        x=self.verify(reviews=branches(
            {"id":1,"pk":"a","review_date":"قبل شهر","fetched_at":"2026-09-15T12:00:00"},
            {"id":2,"pk":"a","review_date":"قبل سنتين","fetched_at":"2026-10-10T12:00:00"}))
        r=x["recent_reviews"]
        self.assertEqual(int(r.iloc[0]["id"]),2)
        self.assertTrue(bool(r.iloc[0]["source_date_unverified"]))

    def test_period_applies_to_import_date_and_snapshots_not_latest_rating(self):
        comp=branches({"pk":"a","name":"A","rating":4.5,"review_count":20})
        rev=branches(
            {"id":1,"pk":"a","fetched_at":"2026-10-01T12:00:00"},
            {"id":2,"pk":"a","fetched_at":"2026-11-18T12:00:00"})
        sn=branches(
            {"pk":"a","day":"2026-10-01","rating":3.8,"total":19},
            {"pk":"a","day":"2026-11-18","rating":4.5,"total":20})
        x=self.verify(comp,rev,sn,period_start="2026-11-01T00:00:00+00:00")
        self.assertEqual(x["stored_reviews"],1)
        self.assertEqual(x["snapshot_dates"],1)
        self.assertEqual(x["average"],4.5)
        self.assertEqual(x["public_reviews"],20)

    def test_snapshot_requires_real_dates(self):
        x=self.verify(snapshots=branches(
            {"pk":"a","day":"قبل شهر","rating":4.0,"total":1},
            {"pk":"b","day":"2027-01-01","rating":5.0,"total":10},
            {"pk":"c","day":"2026-09-14","rating":3.0,"total":0}))
        self.assertEqual(x["snapshot_dates"],1)
        self.assertAlmostEqual(x["trend"].iloc[0]["rating"],3.0)

    def test_same_branch_day_snapshot_not_counted_twice(self):
        x=self.verify(snapshots=branches(
            {"pk":"a","day":"2026-09-14","rating":4.0,"total":10},
            {"pk":"a","day":"2026-09-14","rating":2.0,"total":10},
            {"pk":"b","day":"2026-09-14","rating":5.0,"total":10}))
        self.assertEqual(x["snapshot_dates"],1)
        self.assertEqual(x["trend"].iloc[0]["branch_count"],2)

    def test_freshness_from_verified_source_not_edit_time(self):
        x=self.verify(branches(
            {"pk":"a","rating":4.0,"last_updated":"2026-11-19T00:00:00",
             "source_fetched_at":"2026-09-16T00:00:00"}))
        self.assertTrue(x["stale_data"])
        self.assertGreaterEqual(x["data_age_days"],60)
        self.assertEqual(x["source_coverage"],1)

    def test_open_alert_count_is_not_limited_by_preview(self):
        a=branches({"pk":"a","status":"new","severity":"low","created_at":"2026-11-01"},
                   {"pk":"a","status":"resolved","severity":"high","created_at":"2026-11-01"},
                   {"pk":"a","status":"new","severity":"critical","created_at":"2026-11-03"})
        x=self.verify(alerts=a,full_alert_count=900)
        self.assertEqual(x["open_alerts"],900)
        self.assertEqual(len(x["alerts"]),2)
        self.assertEqual(x["alerts"].iloc[0]["severity"],"critical")

    def test_branch_scope_isolated_across_reviews_snapshots_alerts(self):
        co=branches({"pk":"a","rating":4.2},{"pk":"b","rating":2.2})
        re=branches({"id":1,"pk":"a"},{"id":2,"pk":"b"})
        sn=branches({"pk":"a","day":"2026-09-14","rating":4.2},
                    {"pk":"b","day":"2026-09-14","rating":2.2})
        al=branches({"pk":"a","status":"new"},{"pk":"b","status":"new"})
        x=self.verify(co,re,sn,al,selected_pk="a",full_alert_count=999)
        self.assertEqual(x["branches"],1)
        self.assertEqual(x["stored_reviews"],1)
        self.assertEqual(x["open_alerts"],1)
        self.assertEqual(x["snapshot_dates"],1)

    def test_stable_ranking_and_full_untruncated_table(self):
        c=branches(*[
            {"pk":str(i),"name":"Branch "+str(i),"rating":4.0,"review_count":i}
            for i in range(89)])
        x=self.verify(c)
        self.assertEqual(len(x["ranked_branches"]),5)
        self.assertEqual(len(x["all_branches"]),89)
        self.assertEqual(x["ranked_branches"].iloc[0]["pk"],"88")

    def test_duplicate_branch_key_is_detected_not_mutated(self):
        c=branches({"pk":"same","name":"Old","rating":2.0,
                    "source_fetched_at":"2026-09-01"},
                   {"pk":"same","name":"New","rating":4.0,
                    "source_fetched_at":"2026-09-16"})
        x=self.verify(c)
        self.assertEqual(x["raw_branches"],2)
        self.assertEqual(x["branches"],1)
        self.assertEqual(x["duplicate_company_keys"],1)
        self.assertEqual(x["all_branches"].iloc[0]["name"],"New")

if __name__=="__main__":
    unittest.main(verbosity=2)

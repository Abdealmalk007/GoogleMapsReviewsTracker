"""Offline v46 review querying, filters, paging, data safety and CSV tests."""
from __future__ import annotations
from datetime import datetime
from contextlib import closing
from pathlib import Path
import sqlite3
import tempfile
import unittest
import pandas as pd
from reviews_v46 import (
    ReviewFilters, build_review_query, fetch_review_page, reply_state,
    export_review_csv, _render_card
)
from unittest.mock import patch


class DemoDB:
    def __init__(self, path: Path):
        self.path = path

    def _c(self):
        return sqlite3.connect(str(self.path))


class TestReviewV46(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix="rt46_reviews_")
        self.addCleanup(self.temp.cleanup)
        self.db = DemoDB(Path(self.temp.name)/"synthetic.db")
        with closing(self.db._c()) as c, c:
            c.executescript("""
                CREATE TABLE reviews (
                  id INTEGER PRIMARY KEY,workspace_id INTEGER,pk TEXT,author TEXT,
                  rating INTEGER,text TEXT,review_date TEXT,fetched_at TEXT,
                  responded INTEGER,response_text TEXT,sentiment TEXT,topic TEXT,
                  workflow_status TEXT,priority TEXT,language TEXT
                );
                CREATE TABLE companies(
                  id INTEGER PRIMARY KEY,workspace_id INTEGER,pk TEXT,name TEXT,address TEXT
                );
                INSERT INTO companies VALUES (1,1,'a','Branch A','City A');
                INSERT INTO companies VALUES (2,1,'a','Branch A newest','City B');
                INSERT INTO companies VALUES (3,1,'b','Branch B','City C');
                INSERT INTO reviews VALUES
                 (1,1,'a','Ali',5,'Very good','قبل سنتين','2026-09-15T10:00:00',0,NULL,'positive','service','new','normal','ar'),
                 (2,1,'a','Mona',1,'Bad service','قبل شهر','2026-11-19T10:00:00',NULL,NULL,'negative','service','new','critical','ar'),
                 (3,1,'b','Noor',4,'Excellent','قبل 5 سنوات','2026-11-18T10:00:00',1,'Thank you','positive','quality','responded','normal','en'),
                 (4,1,'b','Tester',NULL,NULL,NULL,'2026-11-17T10:00:00',NULL,NULL,NULL,NULL,'new','low','en'),
                 (5,1,'b','Omar',3,'=1+2 formula','قبل سنة','2026-10-18T10:00:00',0,'Stored owner reply','neutral','delivery','new','high','ar'),
                 (6,2,'a','Other workspace',5,'Hidden','قبل يوم','2026-11-19T10:00:00',0,NULL,'positive','service','new','normal','en');
            """)

    def fetch(self, **kwargs):
        return fetch_review_page(self.db, ReviewFilters(**kwargs), 1, 20)

    def test_default_all_saved_not_30_days(self):
        total,count,frame=self.fetch()
        self.assertEqual((total,count,len(frame)),(5,5,5))
        self.assertIn(1,frame["id"].tolist())

    def test_newest_order_is_import_time_not_relative_review_date(self):
        _,_,frame=self.fetch()
        self.assertEqual(frame["id"].tolist(),[2,3,4,5,1])

    def test_oldest_order(self):
        _,_,frame=self.fetch(sort="oldest")
        self.assertEqual(frame["id"].tolist()[0],1)

    def test_workspace_isolation(self):
        _,count,frame=self.fetch()
        self.assertEqual(count,5)
        self.assertNotIn("Other workspace",frame["author"].tolist())

    def test_branch_filter_without_duplicate_rows_from_companies(self):
        _,count,frame=self.fetch(branch="a")
        self.assertEqual(count,2)
        self.assertEqual(len(frame),2)
        self.assertTrue((frame["company_name"]=="Branch A newest").all())

    def test_exact_star_rating(self):
        _,count,frame=self.fetch(rating=1)
        self.assertEqual((count,frame.iloc[0]["id"]),(1,2))

    def test_sentiment_negative(self):
        _,count,frame=self.fetch(sentiment="negative")
        self.assertEqual((count,frame.iloc[0]["author"]),(1,"Mona"))

    def test_unknown_sentiment(self):
        _,count,frame=self.fetch(sentiment="unknown")
        self.assertEqual((count,frame.iloc[0]["id"]),(1,4))

    def test_reply_answered_when_written_reply_present_even_if_responded_zero(self):
        _,count,frame=self.fetch(reply="answered")
        self.assertEqual(set(frame["id"]),{3,5})
        self.assertEqual(count,2)

    def test_reply_unanswered_does_not_include_null(self):
        _,count,frame=self.fetch(reply="unanswered")
        self.assertEqual((count,frame.iloc[0]["id"]),(1,1))

    def test_reply_unknown(self):
        _,count,frame=self.fetch(reply="unknown")
        self.assertEqual(set(frame["id"]),{2,4})

    def test_reply_state_tri_state(self):
        self.assertEqual(reply_state({"responded":None,"response_text":None}),"unknown")
        self.assertEqual(reply_state({"responded":0,"response_text":None}),"unanswered")
        self.assertEqual(reply_state({"responded":0,"response_text":"Thanks"}),"answered")

    def test_period_filter_uses_fetched_date(self):
        where,params=build_review_query(ReviewFilters(period="30d"),now=datetime(2026,11,21))
        self.assertIn("fetched_at",where)
        self.assertIn("2026-10-22",params[1])
        self.assertNotIn("review_date",where)
        self.assertEqual(self.fetch(period="30d")[1],3)

    def test_with_and_without_written_text(self):
        self.assertEqual(self.fetch(has_text="with_text")[1],4)
        self.assertEqual(self.fetch(has_text="without_text")[1],1)

    def test_word_search_escapes_like_wildcards(self):
        self.assertEqual(self.fetch(keyword="service")[1],2)
        self.assertEqual(self.fetch(keyword="%")[1],0)
        self.assertEqual(self.fetch(keyword="' OR 1=1 --")[1],0)

    def test_word_search_in_branch_name(self):
        self.assertEqual(self.fetch(keyword="Branch B")[1],3)

    def test_pagination_is_stable(self):
        f=ReviewFilters()
        total,count,first=fetch_review_page(self.db,f,1,10)
        self.assertEqual((total,count),(5,5))
        self.assertEqual(len(first),5)
        with self.assertRaises(ValueError):
            fetch_review_page(self.db,f,0,10)

    def test_topic_priority_workflow(self):
        self.assertEqual(self.fetch(topic="delivery")[1],1)
        self.assertEqual(self.fetch(priority="critical")[1],1)
        self.assertEqual(self.fetch(workflow="responded")[1],1)

    def test_csv_has_all_expected_headers_and_bom(self):
        _,_,df=self.fetch()
        raw=export_review_csv(df,rtl=True)
        self.assertTrue(raw.startswith(b"\xef\xbb\xbf"))
        parsed=raw.decode("utf-8-sig")
        self.assertIn("التاريخ كما ورد",parsed)
        self.assertIn("تاريخ الاستيراد",parsed)
        self.assertIn("'=1+2 formula",parsed)

    def test_escaping_untrusted_html(self):
        record={"company_name":"<script>alert(1)</script>","author":"<b>bad</b>",
                "rating":5,"text":"<img src=x onerror=alert(1)>",
                "review_date":"قبل شهر","fetched_at":"2026-11-01T12:00:00",
                "responded":0,"response_text":""}
        with patch("reviews_v46.st.markdown") as mark:
            _render_card(record,rtl=True)
        html=mark.call_args.args[0]
        self.assertNotIn("<script>",html)
        self.assertNotIn("<img src=x",html)
        self.assertIn("&lt;script&gt;",html)

    def test_invalid_rating_and_sort_are_rejected(self):
        with self.assertRaises(ValueError):
            build_review_query(ReviewFilters(rating=6))
        with self.assertRaises(ValueError):
            self.fetch(sort=";DROP TABLE reviews")

if __name__=="__main__":
    unittest.main(verbosity=2)

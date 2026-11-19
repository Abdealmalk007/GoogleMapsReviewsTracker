#!/usr/bin/env python3
"""Offline PDF regression for real Reviews Tracker export function.

Only synthetic test records, never a production database.
Python 3.12+, pandas, reportlab, arabic-reshaper, python-bidi.
"""
from __future__ import annotations
import ast,html,io,os,re,unittest
from pathlib import Path
import pandas as pd

ROOT=Path(__file__).resolve().parent

def load_export():
    source=(ROOT/"app.py").read_text(encoding="utf-8")
    module=ast.parse(source)
    names={"_report_font","_pdf_text","_fallback_bidi_display","dataframe_to_pdf_v39"}
    functions=[node for node in module.body
               if isinstance(node,ast.FunctionDef) and node.name in names]
    assert any(node.name=="dataframe_to_pdf_v39" for node in functions)
    isolated=ast.Module(body=functions,type_ignores=[])
    ast.fix_missing_locations(isolated)
    env={"pd":pd,"io":io,"os":os,"re":re,"html":html,
         "RTL":True,"VERSION":"42.0.0-beta.1"}
    exec(compile(isolated,str(ROOT/"app.py"),"exec"),env)
    return env["dataframe_to_pdf_v39"]

class PDFExportTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.export=staticmethod(load_export())

    def test_synthetic_pdf_is_a_real_document(self):
        frame=pd.DataFrame([
          {"name":"Synthetic Riyadh QA Branch","rating":4.4,
           "review_count":20,"address":"Synthetic data only"},
          {"name":"Synthetic Jeddah QA Branch","rating":3.7,
           "review_count":10,"address":"Synthetic data only"}
        ])
        raw=self.export(frame,title="Reviews Tracker QA Report",max_rows=100)
        self.assertTrue(raw.startswith(b"%PDF-"))
        self.assertGreater(len(raw),1000)
        self.assertIn(b"%%EOF",raw[-100:])

    def test_empty_dataframe_still_produces_valid_pdf(self):
        raw=self.export(pd.DataFrame(columns=["name","rating"]),
                        title="Empty report")
        self.assertTrue(raw.startswith(b"%PDF-"))
        self.assertGreater(len(raw),500)

if __name__=="__main__":
    unittest.main(verbosity=2)

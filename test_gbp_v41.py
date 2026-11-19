#!/usr/bin/env python3
"""Contract tests for the official owned-location review provider."""
import importlib.util,json
import unittest
from pathlib import Path
from unittest.mock import patch
ROOT=Path(__file__).resolve().parent
spec=importlib.util.spec_from_file_location("business_profile_v41",ROOT/"business_profile_v41.py")
bp=importlib.util.module_from_spec(spec);spec.loader.exec_module(bp)
class FakeResponse:
    def __init__(self,payload):self.payload=payload
    def __enter__(self):return self
    def __exit__(self,*args):return False
    def read(self,*args):return json.dumps(self.payload).encode("utf-8")
class ProviderTests(unittest.TestCase):
    def test_requires_verified_location_and_oauth_token(self):
        with self.assertRaises(ValueError):
            bp.list_owned_location_reviews("https://evil.test/reviews","token")
        with self.assertRaises(ValueError):
            bp.list_owned_location_reviews("accounts/123/locations/456","")
    def test_two_pages_deduplicate_and_normalize(self):
        responses=[
            {"reviews":[{"reviewId":"review-1","starRating":"FIVE","comment":"Good","reviewer":{"displayName":"A"}},
                        {"reviewId":"review-2","starRating":"ONE","comment":"Poor",
                         "reviewReply":{"comment":"Thanks","updateTime":"2026-11-01T00:00:00Z"}}],
             "averageRating":4.1,"totalReviewCount":3,"nextPageToken":"next"},
            {"reviews":[{"reviewId":"review-2","starRating":"ONE","comment":"Poor"},
                        {"reviewId":"review-3","starRating":"FOUR","comment":"Nice"}],
             "averageRating":4.1,"totalReviewCount":3}]
        requests=[]
        def responder(request,timeout):
            requests.append(request)
            return FakeResponse(responses[len(requests)-1])
        with patch.object(bp.urllib.request,"urlopen",side_effect=responder):
            reviews,meta=bp.list_owned_location_reviews("accounts/123/locations/456","TEST_OAUTH",page_size=50)
        self.assertEqual(meta["pages"],2)
        self.assertEqual(meta["returned_reviews"],3)
        self.assertTrue(meta["complete"])
        self.assertEqual(meta["total_review_count"],3)
        self.assertEqual(reviews[0]["rating"],5)
        self.assertEqual(reviews[1]["owner_response"],"Thanks")
        self.assertIn("pageToken=next",requests[1].full_url)
        self.assertNotIn("TEST_OAUTH",requests[0].full_url)
        self.assertIn("Bearer TEST_OAUTH",requests[0].get_header("Authorization"))
    def test_partial_pages_are_not_claimed_complete(self):
        page={"reviews":[{"reviewId":"review-1","starRating":"FIVE"}],"nextPageToken":"more",
              "totalReviewCount":40}
        with patch.object(bp.urllib.request,"urlopen",return_value=FakeResponse(page)):
            result,meta=bp.list_owned_location_reviews("accounts/123/locations/456","TOKEN",max_pages=1)
        self.assertEqual(len(result),1)
        self.assertFalse(meta["complete"])
        self.assertEqual(meta["next_page_token"],"more")
if __name__=="__main__":unittest.main(verbosity=2)

#!/usr/bin/env python3
"""Google Business Profile v4: read-only reviews for an authorized verified location.

OAuth token is injected at call time and never stored or logged by this module.
This adapter makes no requests without an explicitly provided access token.
"""
from __future__ import annotations
import json,re,urllib.parse,urllib.request
from typing import Any

PARENT_PATTERN=re.compile(r"^accounts/[A-Za-z0-9_-]+/locations/[A-Za-z0-9_-]+$")
STAR_RATINGS={"ONE":1,"TWO":2,"THREE":3,"FOUR":4,"FIVE":5}

class BusinessProfileError(RuntimeError):
    pass

def normalize_review(review:dict[str,Any])->dict[str,Any]:
    reviewer=review.get("reviewer") or {}
    reply=review.get("reviewReply") or {}
    stars=review.get("starRating")
    if isinstance(stars,str):
        stars=STAR_RATINGS.get(stars.upper())
    if not isinstance(stars,(int,float)) or not 1<=stars<=5:
        stars=None
    return {
        "review_id":str(review.get("reviewId") or "").strip(),
        "author":str(reviewer.get("displayName") or "").strip(),
        "rating":stars,
        "text":str(review.get("comment") or "").strip(),
        "date":str(review.get("createTime") or "").strip(),
        "updated_at":str(review.get("updateTime") or "").strip(),
        "owner_response":str(reply.get("comment") or "").strip(),
        "owner_response_updated_at":str(reply.get("updateTime") or "").strip(),
        "source":"google_business_profile",
    }

def list_owned_location_reviews(parent:str,access_token:str,*,page_size:int=50,
                                max_pages:int=20,timeout:float=15.0)->tuple[list[dict[str,Any]],dict[str,Any]]:
    """Fetch actual paginated reviews only for a location authorized by OAuth."""
    if not isinstance(parent,str) or not PARENT_PATTERN.fullmatch(parent.strip()):
        raise ValueError("parent must be accounts/{accountId}/locations/{locationId}")
    if not access_token or not access_token.strip():
        raise ValueError("An authorized Google Business Profile OAuth access token is required")
    size=max(1,min(50,int(page_size)))
    limit=max(1,min(200,int(max_pages)))
    endpoint="https://mybusiness.googleapis.com/v4/"+parent+"/reviews"
    rows:list[dict[str,Any]]=[]
    seen:set[str]=set()
    next_token=None
    pages=0
    total=None
    avg=None
    while pages<limit:
        params={"pageSize":size,"orderBy":"updateTime desc"}
        if next_token:params["pageToken"]=next_token
        url=endpoint+"?"+urllib.parse.urlencode(params)
        req=urllib.request.Request(url,method="GET",headers={
            "Authorization":"Bearer "+access_token,
            "Accept":"application/json",
            "User-Agent":"ReviewsTracker/41.0"
        })
        try:
            with urllib.request.urlopen(req,timeout=float(timeout)) as res:
                response=json.loads(res.read(5_000_000).decode("utf-8"))
        except Exception as exc:
            # Avoid logging exception details; intermediaries could include credential-bearing URLs.
            raise BusinessProfileError("Google Business Profile request failed; check OAuth scope, verified location, quota and connectivity") from None
        if not isinstance(response,dict):
            raise BusinessProfileError("Unexpected Google Business Profile response")
        pages+=1
        if response.get("totalReviewCount") is not None:total=response["totalReviewCount"]
        if response.get("averageRating") is not None:avg=response["averageRating"]
        for item in response.get("reviews") or []:
            review=normalize_review(item)
            rid=review["review_id"]
            if rid and rid in seen:continue
            if rid:seen.add(rid)
            rows.append(review)
        candidate=response.get("nextPageToken") or None
        if not candidate or candidate==next_token:next_token=None;break
        next_token=candidate
    return rows,{
        "source":"google_business_profile",
        "location":parent,
        "pages":pages,
        "returned_reviews":len(rows),
        "total_review_count":total,
        "average_rating":avg,
        "complete":next_token is None,
        "next_page_token":next_token,
        "verified_live":True
    }

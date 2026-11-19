#!/usr/bin/env python3
"""v41 branch data-quality summary. No network or database writes."""
from __future__ import annotations
import pandas as pd

def _field(frame:pd.DataFrame, name:str)->pd.Series:
    if name not in frame:
        return pd.Series([""]*len(frame),index=frame.index,dtype="string")
    return frame[name].fillna("").astype("string").str.strip()

def _present(frame:pd.DataFrame, name:str)->pd.Series:
    x=_field(frame,name)
    return x.ne("") & x.str.lower().ne("nan") & x.str.lower().ne("none")

def branch_quality_summary(branches:pd.DataFrame, missing_limit:int=200)->dict:
    """Count complete records and list incomplete branches without changing source rows."""
    frame=branches.copy() if isinstance(branches,pd.DataFrame) else pd.DataFrame(branches or [])
    n=len(frame)
    if n==0:
        return {"total_branches":0,"place_ids":0,"maps_urls":0,"contact_numbers":0,
                "ratings":0,"missing_rows":pd.DataFrame(),
                "duplicate_place_ids":0,"place_id_coverage_pct":0.0}
    place=_present(frame,"place_id")
    maps=_present(frame,"maps_url")
    phones=(_present(frame,"phone")|_present(frame,"mobile"))
    ratings=pd.to_numeric(frame["rating"],errors="coerce").between(1,5) if "rating" in frame else pd.Series(False,index=frame.index)
    valid_ids=_field(frame,"place_id")[place]
    duplicate_ids=int(valid_ids.duplicated(keep=False).sum())
    missing=[("place_id",place),("maps_url",maps),
             ("rating",ratings),("phone/mobile",phones)]
    details=pd.DataFrame({
        "name":_field(frame,"name"),
        "pk":_field(frame,"pk"),
        "missing_fields":["، ".join(key for key,series in missing if not bool(series.loc[idx]))
                          for idx in frame.index]
    },index=frame.index)
    details=details[details["missing_fields"].ne("")].head(max(1,int(missing_limit))).reset_index(drop=True)
    return {
        "total_branches":n,
        "place_ids":int(place.sum()),
        "maps_urls":int(maps.sum()),
        "contact_numbers":int(phones.sum()),
        "ratings":int(ratings.sum()),
        "duplicate_place_ids":duplicate_ids,
        "place_id_coverage_pct":round(float(place.mean()*100),1),
        "contact_coverage_pct":round(float(phones.mean()*100),1),
        "missing_rows":details
    }

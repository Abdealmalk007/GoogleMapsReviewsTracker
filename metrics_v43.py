"""Evidence-first, read-only analytics for Reviews Tracker v43.

No network calls, mutations, imputation of missing values, or fabricated history.
Dates of imported reviews are not necessarily dates the customer posted them.
"""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any
import re

import pandas as pd


def _frame(value: Any) -> pd.DataFrame:
    return value.copy() if isinstance(value, pd.DataFrame) else pd.DataFrame()


def _col(frame: pd.DataFrame, name: str, default: Any = "") -> pd.Series:
    if name in frame.columns:
        return frame[name]
    return pd.Series([default] * len(frame), index=frame.index)


def _text(frame: pd.DataFrame, name: str) -> pd.Series:
    return _col(frame, name).fillna("").astype(str).str.strip()


def _num(frame: pd.DataFrame, name: str) -> pd.Series:
    return pd.to_numeric(_col(frame, name, float("nan")), errors="coerce")


def _times(frame: pd.DataFrame, name: str) -> pd.Series:
    # Timestamp parsing is intentional: use ISO-like source timestamps only,
    # not relative strings such as "قبل شهرين" / "2 months ago".
    raw = _text(frame, name)
    iso = raw.str.match(r"^\d{4}-\d{2}-\d{2}(?:[T ]\d{2}:\d{2}(?::\d{2})?)?")
    return pd.to_datetime(raw.where(iso), utc=True, errors="coerce")


def _date_limit(value: Any):
    if value is None or value == "":
        return None
    t = pd.to_datetime(value, utc=True, errors="coerce")
    return None if pd.isna(t) else t


def _dedupe_reviews(frame: pd.DataFrame) -> tuple[pd.DataFrame, int]:
    if frame.empty:
        return frame.copy(), 0
    out = frame.copy()
    out["_import_ts"] = _times(out, "fetched_at")
    if "id" in out.columns:
        out["_row_id"] = _num(out, "id")
    else:
        out["_row_id"] = pd.Series(range(len(out)), index=out.index)
    out = out.sort_values(["_import_ts", "_row_id"], ascending=[False, False],
                          na_position="last", kind="stable")
    seen: set[tuple[str, str, str]] = set()
    keep: list[bool] = []
    for index, row in out.iterrows():
        pk = str(row.get("pk") or "")
        ws = str(row.get("workspace_id") or "")
        source_id = ""
        for key in ("review_id", "review_hash", "content_fingerprint"):
            raw = row.get(key)
            if raw is None or pd.isna(raw):
                continue
            candidate = str(raw).strip()
            if candidate and candidate.lower() not in {"none", "nan", "null"}:
                source_id = key + ":" + candidate
                break
        # Do not deduplicate unidentified reviews based on author/text alone.
        identity = (ws, pk, source_id)
        if source_id and identity in seen:
            keep.append(False)
        else:
            keep.append(True)
            if source_id:
                seen.add(identity)
    de_duped = out.loc[keep].copy()
    return de_duped, int(len(out) - len(de_duped))


def _clean_companies(frame: pd.DataFrame) -> tuple[pd.DataFrame, int]:
    if frame.empty:
        return frame.copy(), 0
    out = frame.copy()
    out["_source_ts"] = _times(out, "source_fetched_at")
    out["_edit_ts"] = _times(out, "last_updated")
    out = out.sort_values(["_source_ts", "_edit_ts"], ascending=[False, False],
                          na_position="last", kind="stable")
    if "pk" not in out.columns:
        return out, 0
    identity = _text(out, "pk")
    nonempty = identity.ne("")
    duplicate_mask = nonempty & identity.duplicated(keep="first")
    duplicates = int(duplicate_mask.sum())
    return out.loc[~duplicate_mask].copy(), duplicates


def _status_label(alerts: pd.DataFrame) -> pd.DataFrame:
    if alerts.empty or "status" not in alerts.columns:
        return alerts.copy()
    statuses = _text(alerts, "status").str.lower()
    return alerts.loc[statuses.isin(("new", "open", "unread", "pending"))].copy()


def _sentiment(series: pd.Series) -> pd.Series:
    labels = {
        "positive": "positive", "pos": "positive", "إيجابي": "positive",
        "إيجابية": "positive", "ايجابي": "positive", "ايجابية": "positive",
        "negative": "negative", "neg": "negative", "سلبي": "negative",
        "سلبية": "negative", "neutral": "neutral", "محايد": "neutral",
        "محايدة": "neutral"
    }
    return series.fillna("").astype(str).str.strip().str.lower().map(labels).fillna("unknown")


def _response_counts(frame: pd.DataFrame) -> tuple[int, int, int]:
    if frame.empty:
        return 0, 0, 0
    responded = _text(frame, "responded").str.lower()
    # SQLite flags become floats (1.0/0.0) when NULL is present in a pandas
    # column. Accept both numeric and textual forms without treating NaN as false.
    numeric = pd.to_numeric(_col(frame, "responded"), errors="coerce")
    yes = responded.isin(("1", "true", "yes", "answered", "responded")) | numeric.eq(1)
    no = responded.isin(("0", "false", "no", "unanswered", "not_responded")) | numeric.eq(0)
    if "response_text" in frame.columns:
        has_written_reply = _text(frame, "response_text").ne("")
        yes = yes | has_written_reply
        no = no & ~has_written_reply
    known = yes | no
    return int(yes.sum()), int(known.sum()), int((~known).sum())


def build_dashboard_data(
    companies: pd.DataFrame,
    reviews: pd.DataFrame,
    snapshots: pd.DataFrame | None = None,
    alerts: pd.DataFrame | None = None,
    selected_pk: str | None = None,
    *,
    period_start: Any = None,
    as_of: Any = None,
    full_alert_count: int | None = None,
) -> dict[str, Any]:
    """Build defensible KPIs from source records without mutating the database."""
    raw_branches = _frame(companies)
    c, duplicate_pk_count = _clean_companies(raw_branches)
    r, review_duplicates = _dedupe_reviews(_frame(reviews))
    snaps = _frame(snapshots)
    a = _status_label(_frame(alerts))
    selected = str(selected_pk) if selected_pk and selected_pk != "__ALL__" else None

    if selected:
        c = c.loc[_text(c, "pk") == selected].copy()
        r = r.loc[_text(r, "pk") == selected].copy()
        if "pk" in snaps.columns:
            snaps = snaps.loc[_text(snaps, "pk") == selected].copy()
        if "pk" in a.columns:
            a = a.loc[_text(a, "pk") == selected].copy()

    now = _date_limit(as_of)
    if now is None:
        now = pd.Timestamp.now(tz="UTC")
    start = _date_limit(period_start)
    if start is not None:
        # An explicit fetched-at period, never incorrectly called "review posted in period".
        r = r.loc[_times(r, "fetched_at") >= start].copy()
        snaps = snaps.loc[_times(snaps, "day") >= start].copy()

    ratings = _num(c, "rating")
    counts = _num(c, "review_count")
    valid_rating = ratings.between(1, 5) & ratings.notna()
    valid_counts = counts.ge(0) & counts.notna() & (counts % 1 == 0)
    positive_counts = valid_counts & counts.gt(0)
    weighted_coverage = valid_rating & positive_counts
    rated_total = int(valid_rating.sum())
    # Weighted mean over just a tiny sub-set is MISLEADING. Use it only with
    # complete positive count coverage among all branches with a known rating.
    if rated_total and int(weighted_coverage.sum()) == rated_total:
        avg = float((ratings[weighted_coverage] * counts[weighted_coverage]).sum()
                    / counts[weighted_coverage].sum())
        avg_method = "weighted_complete"
    elif rated_total:
        avg = float(ratings[valid_rating].mean())
        avg_method = "mean_known_branch_ratings"
    else:
        avg = None
        avg_method = "unavailable"

    published_counts = int(counts.where(valid_counts).fillna(0).sum())
    n_missing_count = int((~valid_counts).sum())
    n_positive = int(positive_counts.sum())
    n_with_count = int(valid_counts.sum())
    n_with_source = int(c["_source_ts"].notna().sum()) if "_source_ts" in c else 0
    sources = c["_source_ts"].dropna() if "_source_ts" in c else pd.Series(dtype="datetime64[ns, UTC]")
    source_at = sources.max() if len(sources) else None
    age_days = max(0, int((now - source_at).total_seconds() // 86400)) if source_at is not None else None

    yes, response_known, response_unknown = _response_counts(r)
    response_rate = round((100.0 * yes / response_known), 1) if response_known else None
    labels = _sentiment(_col(r, "sentiment"))
    sentiments = {s: int((labels == s).sum()) for s in
                  ("positive", "neutral", "negative", "unknown")}

    ranked = pd.DataFrame({"name": _text(c, "name"), "pk": _text(c, "pk"),
                           "rating": ratings, "review_count": counts})
    ranked = ranked.loc[valid_rating].sort_values(
        ["rating", "review_count", "name", "pk"],
        ascending=[False, False, True, True], na_position="last", kind="stable")

    all_branches = pd.DataFrame({
        "name": _text(c, "name"), "pk": _text(c, "pk"),
        "rating": ratings.where(valid_rating),
        "review_count": counts.where(valid_counts),
        "source_fetched_at": _text(c, "source_fetched_at"),
        "address": _text(c, "address")
    }).sort_values(["rating", "name", "pk"], ascending=[False, True, True],
                   na_position="last", kind="stable").reset_index(drop=True)

    # Same-day copies are not separate historical points: keep the newest row
    # for each branch/day, then aggregate branch ratings by day.
    trend = pd.DataFrame(columns=["day", "rating", "branch_count", "method"])
    if not snaps.empty and {"day", "rating"}.issubset(snaps.columns):
        work = snaps.copy()
        work["_day"] = _times(work, "day").dt.normalize()
        work["_rating"] = _num(work, "rating")
        work["_count"] = _num(work, "total")
        work["_pk"] = _text(work, "pk") if "pk" in work.columns else work.index.astype(str)
        work = work.loc[
            work["_day"].notna() &
            (work["_day"] <= now.normalize()) &
            work["_rating"].between(1, 5)
        ].copy()
        if not work.empty:
            work = work.drop_duplicates(subset=["_day", "_pk"], keep="last")
            data = []
            for day, group in work.groupby("_day", sort=True):
                ok = group["_count"].gt(0) & group["_count"].notna()
                if ok.all():
                    value = (group["_rating"] * group["_count"]).sum() / group["_count"].sum()
                    method = "weighted_complete"
                else:
                    value = group["_rating"].mean()
                    method = "unweighted_due_to_missing_counts"
                data.append((day, float(value), len(group), method))
            trend = pd.DataFrame(data, columns=["day", "rating", "branch_count", "method"]).tail(60)

    if not r.empty:
        r = r.copy()
        r["_time"] = _times(r, "fetched_at")
        r["_row_id"] = _num(r, "id")
        r = r.sort_values(["_time", "_row_id"], ascending=[False, False],
                          na_position="last", kind="stable")
        recent = r.head(6).copy()
    else:
        recent = pd.DataFrame()
    name_map = dict(zip(_text(c, "pk"), _text(c, "name")))
    if not recent.empty:
        recent["branch_name"] = _text(recent, "pk").map(name_map).fillna("—")
        # Keep the original customer's relative review_date, but never parse it
        # into a made-up calendar date. Display the fetch timestamp separately.
        recent["source_date_unverified"] = _text(recent, "review_date").str.contains(
            r"قبل|ago|yesterday|today", case=False, regex=True)

    severity_rank = {"critical": 0, "high": 1, "urgent": 1, "medium": 2,
                     "warning": 2, "low": 3, "info": 4}
    if not a.empty:
        a = a.copy()
        a["_rank"] = _text(a, "severity").str.lower().map(severity_rank).fillna(5)
        a["_created"] = _times(a, "created_at")
        a = a.sort_values(["_rank", "_created"], ascending=[True, False],
                          na_position="last", kind="stable")

    if selected:
        # Branch-scoped count must derive from matching rows, not the unfiltered
        # global SQL COUNT(*) passed to the display.
        alert_count = len(a)
    elif full_alert_count is not None and full_alert_count >= len(a):
        alert_count = int(full_alert_count)
    else:
        alert_count = len(a)
    places = _text(c, "place_id")
    duplicate_place_ids = int(places[places.ne("")].duplicated(keep=False).sum())

    return {
        "branches": len(c), "raw_branches": len(raw_branches),
        "average": avg, "average_source": avg_method,
        "rating_coverage": rated_total, "count_coverage": n_with_count,
        "positive_count_branches": n_positive,
        "missing_review_counts": n_missing_count,
        "public_reviews": published_counts,
        "public_reviews_complete": n_missing_count == 0 and age_days is not None and age_days <= 7,
        "stored_reviews": len(r), "response_rate": response_rate,
        "response_responded": yes, "response_known": response_known,
        "response_unknown": response_unknown,
        "open_alerts": alert_count, "alerts": a.head(5),
        "sentiments": sentiments, "ranked_branches": ranked.head(5),
        "all_branches": all_branches,
        "trend": trend, "snapshot_dates": len(trend),
        "recent_reviews": recent,
        "latest_source_at": source_at, "data_age_days": age_days,
        "source_coverage": n_with_source,
        "stale_data": age_days is None or age_days > 7,
        "duplicate_review_records": review_duplicates,
        "duplicate_company_keys": duplicate_pk_count,
        "duplicate_place_ids": duplicate_place_ids,
        "period_start": start,
        "scope": "single_branch" if selected else "all_branches",
        "date_scope": "review_fetched_at_and_snapshots_only"
    }

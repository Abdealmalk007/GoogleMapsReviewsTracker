"""Reviews Tracker v42 — approved RTL dashboard. All indicators use stored data only."""
from __future__ import annotations

from html import escape
from typing import Callable

import pandas as pd
import plotly.graph_objects as go
import streamlit as st

BLUE = "#1559CB"
INK = "#17304F"
GRID = "#E7EDF6"


def _frame(value) -> pd.DataFrame:
    return value.copy() if isinstance(value, pd.DataFrame) else pd.DataFrame()


def _number(frame: pd.DataFrame, key: str) -> pd.Series:
    if key not in frame:
        return pd.Series(float("nan"), index=frame.index, dtype=float)
    return pd.to_numeric(frame[key], errors="coerce")


def _string(frame: pd.DataFrame, key: str) -> pd.Series:
    if key not in frame:
        return pd.Series("", index=frame.index, dtype="string")
    return frame[key].fillna("").astype(str)


def _yes(series: pd.Series) -> pd.Series:
    return series.astype(str).str.strip().str.lower().isin(
        {"1", "true", "yes", "responded", "answered"}
    )


def _format_value(value, digits=0) -> str:
    if value is None or pd.isna(value):
        return "—"
    if digits:
        return f"{float(value):,.{digits}f}"
    return f"{int(value):,}"


def build_dashboard_data(
    companies: pd.DataFrame,
    reviews: pd.DataFrame,
    snapshots: pd.DataFrame | None = None,
    alerts: pd.DataFrame | None = None,
    selected_pk: str | None = None,
) -> dict:
    """Pure, deterministic dashboard aggregation; no data fetching or fabricated points."""
    c, r, snaps, a = map(_frame, (companies, reviews, snapshots, alerts))
    if selected_pk and selected_pk != "__ALL__":
        for name, frame in (("c", c), ("r", r), ("snaps", snaps), ("a", a)):
            if "pk" in frame.columns:
                filtered = frame.loc[frame["pk"].astype(str) == str(selected_pk)].copy()
                if name == "c": c = filtered
                elif name == "r": r = filtered
                elif name == "snaps": snaps = filtered
                else: a = filtered

    # The published branch rating is distinct from the average of collected review samples.
    branch_ratings = _number(c, "rating")
    branch_totals = _number(c, "review_count")
    valid_ratings = branch_ratings.between(1, 5)
    weights = branch_totals.where(branch_totals > 0, 0).fillna(0)
    weighted = valid_ratings & (weights > 0)
    if weighted.any():
        avg = float((branch_ratings[weighted] * weights[weighted]).sum() / weights[weighted].sum())
        average_source = "weighted_published_branch_ratings"
    elif valid_ratings.any():
        avg = float(branch_ratings[valid_ratings].mean())
        average_source = "unweighted_published_branch_ratings"
    else:
        avg = None
        average_source = "unavailable"

    public_reviews = int(branch_totals.clip(lower=0).fillna(0).sum()) if not c.empty else 0
    if "responded" in r and not r.empty:
        answered = _yes(r["responded"].fillna(False))
        response_rate = round(float(answered.mean() * 100), 1)
    else:
        response_rate = None

    sent = _string(r, "sentiment").str.lower()
    aliases = {"positive": "positive", "negative": "negative", "neutral": "neutral",
               "إيجابي": "positive", "سلبي": "negative", "محايد": "neutral"}
    mapped = sent.map(aliases).fillna("unknown")
    sentiments = {key: int((mapped == key).sum()) for key in ("positive", "neutral", "negative", "unknown")}

    score = pd.DataFrame({"name": _string(c, "name"), "rating": branch_ratings})
    score = score.loc[valid_ratings].sort_values(["rating", "name"], ascending=[False, True]).head(5)

    trend = pd.DataFrame(columns=["day", "rating"])
    if not snaps.empty and {"day", "rating"}.issubset(snaps.columns):
        work = snaps.copy()
        work["day"] = pd.to_datetime(work["day"], errors="coerce").dt.normalize()
        work["rating"] = _number(work, "rating")
        work["_weight"] = _number(work, "total").where(lambda x: x > 0, 1).fillna(1)
        work = work.loc[work["day"].notna() & work["rating"].between(1, 5)]
        if not work.empty:
            work["_weighted"] = work["rating"] * work["_weight"]
            grouped = work.groupby("day", as_index=False)[["_weighted", "_weight"]].sum()
            grouped["rating"] = grouped["_weighted"] / grouped["_weight"]
            trend = grouped[["day", "rating"]].sort_values("day").tail(60)

    if not r.empty:
        sorted_reviews = r.copy()
        date_key = next(
            (col for col in ("date", "review_date", "created_at", "fetched_at")
             if col in sorted_reviews.columns), None
        )
        if date_key:
            sorted_reviews["_sort"] = pd.to_datetime(sorted_reviews[date_key], errors="coerce", utc=True)
            sorted_reviews = sorted_reviews.sort_values("_sort", ascending=False, na_position="last")
        sorted_reviews = sorted_reviews.head(6).copy()
    else:
        sorted_reviews = pd.DataFrame()
    name_by_pk = dict(zip(_string(c, "pk"), _string(c, "name")))
    if not sorted_reviews.empty:
        sorted_reviews["branch_name"] = _string(sorted_reviews, "pk").map(name_by_pk).fillna("—")

    alerts_to_show = a.head(5).copy()
    return {
        "branches": int(len(c)),
        "average": avg,
        "average_source": average_source,
        "public_reviews": public_reviews,
        "stored_reviews": int(len(r)),
        "response_rate": response_rate,
        "open_alerts": int(len(a)),
        "sentiments": sentiments,
        "ranked_branches": score,
        "trend": trend,
        "recent_reviews": sorted_reviews,
        "alerts": alerts_to_show,
    }


def _card(title: str, value: str, caption: str, icon: str, tone: str) -> None:
    st.markdown(
        '<div class="rt42-kpi rt42-{}">'
        '<div class="rt42-kpi-top"><span class="rt42-kpi-icon">{}</span>'
        '<span class="rt42-kpi-title">{}</span></div>'
        '<div class="rt42-kpi-value">{}</div>'
        '<div class="rt42-kpi-note">{}</div></div>'.format(
            tone, escape(icon), escape(title), escape(value), escape(caption)
        ), unsafe_allow_html=True
    )


def _chart_base(fig: go.Figure, height: int = 252) -> go.Figure:
    fig.update_layout(
        height=height, margin=dict(l=16, r=12, t=10, b=18),
        paper_bgcolor="#FFFFFF", plot_bgcolor="#FFFFFF",
        showlegend=False, font=dict(family="Segoe UI, Tahoma", size=12, color=INK),
        hovermode="closest",
    )
    return fig


def _panel_heading(label: str) -> None:
    st.markdown('<div class="rt42-panel-heading">{}</div>'.format(escape(label)),
                unsafe_allow_html=True)


def _empty(label: str) -> None:
    st.markdown('<div class="rt42-empty">{}</div>'.format(escape(label)),
                unsafe_allow_html=True)


def _recent_rows(frame: pd.DataFrame, rtl: bool) -> pd.DataFrame:
    if frame.empty:
        return pd.DataFrame()
    cols = pd.DataFrame(index=frame.index)
    cols["المراجعة" if rtl else "Review"] = _string(frame, "text").str.slice(0, 135)
    cols["التقييم" if rtl else "Rating"] = _number(frame, "rating").map(
        lambda v: "★ " + f"{float(v):.1f}" if pd.notna(v) else "—"
    )
    cols["الفرع" if rtl else "Branch"] = _string(frame, "branch_name")
    field = next((k for k in ("date", "review_date", "created_at", "fetched_at")
                  if k in frame.columns), None)
    cols["التاريخ" if rtl else "Date"] = _string(frame, field) if field else "—"
    return cols.reset_index(drop=True)


def render_dashboard_v42(
    companies: pd.DataFrame,
    reviews: pd.DataFrame,
    snapshots: pd.DataFrame,
    alerts: pd.DataFrame,
    *,
    rtl: bool,
    selected_pk: str | None,
    navigate: Callable[[str], None],
) -> dict:
    """Render the approved dashboard with Streamlit widgets and actual stored data."""
    data = build_dashboard_data(companies, reviews, snapshots, alerts, selected_pk)
    st.markdown(
        '<div class="rt42-welcome">'
        '<h1>{}</h1><p>{}</p></div>'.format(
            "👋 مرحبًا بك في Reviews Tracker" if rtl else "👋 Welcome to Reviews Tracker",
            "نظرة شاملة على أداء التقييمات والمراجعات المحفوظة، دون بيانات تجريبية."
            if rtl else "Ratings and review insights from saved data only. No fabricated metrics."
        ), unsafe_allow_html=True
    )

    cols = st.columns(4, gap="small")
    items = [
        ("متوسط تقييم الفروع" if rtl else "Average branch rating",
         _format_value(data["average"], 2),
         "من بيانات الفروع المحفوظة" if rtl else "From saved branch ratings", "☆", "gold"),
        ("إجمالي مراجعات الفروع" if rtl else "Published review count",
         _format_value(data["public_reviews"]),
         "الإجمالي المُعلن للفروع المحفوظة" if rtl else "Published counts for saved branches", "▤", "blue"),
        ("معدل الاستجابة" if rtl else "Response rate",
         _format_value(data["response_rate"], 1) + "%" if data["response_rate"] is not None else "—",
         "من المراجعات المخزنة فقط" if rtl else "From stored reviews only", "◉", "green"),
        ("تنبيهات مفتوحة" if rtl else "Open alerts",
         _format_value(data["open_alerts"]),
         "المعروض حتى 300 تنبيه" if rtl else "Based on up to 300 alerts", "♢", "red"),
    ]
    for col, item in zip(cols, items):
        with col:
            _card(*item)

    trend_col, branch_col, sentiment_col = st.columns([1.25, 1.25, 1], gap="medium")
    with trend_col:
        with st.container(border=True, key="rt42-chart-trend"):
            _panel_heading("اتجاه التقييم عبر اللقطات الزمنية" if rtl else "Recorded rating trend")
            trend = data["trend"]
            if trend.empty:
                _empty("لا توجد لقطات تقييم تاريخية كافية" if rtl else "No historical rating snapshots")
            else:
                fig = go.Figure(go.Scatter(
                    x=trend["day"], y=trend["rating"], mode="lines+markers",
                    line=dict(color=BLUE, width=3), marker=dict(size=7, color="#FFFFFF", line=dict(width=2, color=BLUE)),
                    fill="tozeroy", fillcolor="rgba(30,104,208,.08)", hovertemplate="%{x|%Y-%m-%d}<br>%{y:.2f}<extra></extra>"
                ))
                fig.update_yaxes(range=[0, 5.05], gridcolor=GRID, tickvals=[1, 2, 3, 4, 5])
                fig.update_xaxes(showgrid=False)
                st.plotly_chart(_chart_base(fig), use_container_width=True, key="rt42-trend")
    with branch_col:
        with st.container(border=True, key="rt42-chart-branches"):
            _panel_heading("مقارنة الفروع حسب التقييم" if rtl else "Top rated branches")
            rows = data["ranked_branches"]
            if rows.empty:
                _empty("لا توجد تقييمات فروع محفوظة" if rtl else "No saved branch ratings")
            else:
                fig = go.Figure(go.Bar(
                    x=rows["name"], y=rows["rating"], marker_color=["#1764d5", "#3887ec", "#61a1ed", "#8abcf6", "#b3d4f7"][:len(rows)],
                    text=rows["rating"].map(lambda x: f"{float(x):.2f}"), textposition="outside"
                ))
                fig.update_yaxes(range=[0, 5.4], gridcolor=GRID, showline=False)
                fig.update_xaxes(showgrid=False, tickangle=-12)
                st.plotly_chart(_chart_base(fig), use_container_width=True, key="rt42-branches")
    with sentiment_col:
        with st.container(border=True, key="rt42-chart-sentiment"):
            _panel_heading("توزيع المشاعر" if rtl else "Sentiment breakdown")
            sent = data["sentiments"]
            labels = {
                "positive": "إيجابية" if rtl else "Positive",
                "neutral": "محايدة" if rtl else "Neutral",
                "negative": "سلبية" if rtl else "Negative",
                "unknown": "غير مصنّفة" if rtl else "Unclassified"
            }
            values = [(labels[k], sent[k], color) for k, color in
                      (("positive", "#18a56b"), ("neutral", "#aebdd0"),
                       ("negative", "#ef525d"), ("unknown", "#e2e8f0"))
                      if sent[k] > 0]
            if not values:
                _empty("لا توجد مراجعات مصنّفة في الفترة" if rtl else "No classified reviews")
            else:
                fig = go.Figure(go.Pie(
                    labels=[x[0] for x in values], values=[x[1] for x in values], hole=.65,
                    marker=dict(colors=[x[2] for x in values]), textinfo="none",
                    hovertemplate="%{label}: %{value}<extra></extra>",
                ))
                fig.add_annotation(text=f"<b>{data['stored_reviews']:,}</b><br>" +
                                   ("مراجعة مخزنة" if rtl else "Stored reviews"),
                                   x=.5, y=.5, showarrow=False, font=dict(size=15, color=INK))
                st.plotly_chart(_chart_base(fig), use_container_width=True, key="rt42-sentiment")
                st.caption("  ·  ".join(f"{label}: {count}" for label, count, _ in values))

    reviews_col, alert_col = st.columns([1.9, 1], gap="medium")
    with reviews_col:
        with st.container(border=True, key="rt42-recent-reviews"):
            _panel_heading("أحدث المراجعات المحفوظة" if rtl else "Recent saved reviews")
            df = _recent_rows(data["recent_reviews"], rtl)
            if df.empty:
                _empty("لا توجد مراجعات محفوظة ضمن الفترة المختارة" if rtl else "No stored reviews in the selected period")
            else:
                st.dataframe(df, use_container_width=True, hide_index=True, height=242)
            if st.button("عرض جميع المراجعات ←" if rtl else "View all reviews →",
                         key="rt42-view-reviews", use_container_width=True):
                navigate("review_all")
    with alert_col:
        with st.container(border=True, key="rt42-open-alerts"):
            _panel_heading("التنبيهات التي تحتاج متابعة" if rtl else "Alerts needing attention")
            if data["alerts"].empty:
                _empty("لا توجد تنبيهات مفتوحة" if rtl else "No open alerts")
            else:
                for _, item in data["alerts"].iterrows():
                    title = item.get("title") or item.get("company_name") or item.get("type") or ("تنبيه" if rtl else "Alert")
                    message = item.get("message") or item.get("detail") or item.get("description") or ""
                    st.markdown(
                        '<div class="rt42-alert-item"><b>{}</b><small>{}</small></div>'.format(
                            escape(str(title)[:110]), escape(str(message)[:175])
                        ), unsafe_allow_html=True
                    )
            if st.button("عرض جميع التنبيهات ←" if rtl else "View alerts →",
                         key="rt42-view-alerts", use_container_width=True):
                navigate("alerts")

    st.caption(
        ("المؤشرات مبنية على البيانات المحفوظة. إجمالي مراجعات الفروع المعلن قد يختلف عن عدد المراجعات المخزنة."
         if rtl else "Metrics use saved records. Published review counts may differ from locally stored reviews.")
    )
    return data

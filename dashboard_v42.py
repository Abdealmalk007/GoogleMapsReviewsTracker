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


from metrics_v43 import build_dashboard_data


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
    cols["تاريخ المراجعة كما ورد" if rtl else "Review date (source)"] = (
        _string(frame, "review_date") if "review_date" in frame.columns else "—"
    )
    cols["تاريخ الاستيراد" if rtl else "Imported at"] = (
        _string(frame, "fetched_at").str.slice(0, 19)
        if "fetched_at" in frame.columns else "—"
    )
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
    period_start=None,
    full_alert_count: int | None = None,
) -> dict:
    """Render approved dashboard with transparent source coverage and freshness."""
    data = build_dashboard_data(
        companies, reviews, snapshots, alerts, selected_pk,
        period_start=period_start, full_alert_count=full_alert_count
    )
    st.markdown(
        '<div class="rt42-welcome">'
        '<h1>{}</h1><p>{}</p></div>'.format(
            "👋 مرحبًا بك في Reviews Tracker" if rtl else "👋 Welcome to Reviews Tracker",
            "نظرة شاملة على أداء التقييمات والمراجعات المحفوظة، دون بيانات تجريبية."
            if rtl else "Ratings and review insights from saved data only. No fabricated metrics."
        ), unsafe_allow_html=True
    )

    last_source = data.get("latest_source_at")
    source_stamp = last_source.strftime("%Y-%m-%d") if last_source is not None else ("غير معروف" if rtl else "Unknown")
    if data["stale_data"]:
        st.warning(
            ("آخر جلب موثق للبيانات: {}. البيانات قديمة أو غير مكتملة، ولا تمثل تقييمات Google الحية. "
             "حدّث الفروع المصرّح بها للتحقق قبل اتخاذ قرار.").format(source_stamp)
            if rtl else
            ("Last verified source fetch: {}. Stored records are stale or incomplete; "
             "these are not live Google ratings. Refresh authorized branches before making decisions.").format(source_stamp)
        )
    else:
        st.caption(("آخر جلب موثق: " if rtl else "Last verified fetch: ")+source_stamp)
    st.caption(
        "الفترة المختارة تخص المراجعات المستوردة ولقطات التقييم فقط؛ آخر تقييم محفوظ وعدد المراجعات المعلن لا يتغيران بالفترة."
        if rtl else
        "The selected period filters imported reviews and snapshots only. Latest saved branch ratings and published counts are not period totals."
    )
    cols = st.columns(4, gap="small")
    items = [
        ("متوسط تقييم الفروع" if rtl else "Average branch rating",
         _format_value(data["average"], 2),
         ("متوسط {} من {} فرع بتقييم معروف؛ لا تقدير للفروع الناقصة".format(data["rating_coverage"], data["branches"])
          if rtl else "Mean of {} / {} known branch ratings; missing excluded".format(data["rating_coverage"],data["branches"])),
         "☆", "gold"),
        ("إجمالي مراجعات الفروع" if rtl else "Published review count",
         _format_value(data["public_reviews"]),
         ("جزئي: {} فرع بعدد معروف؛ {} بلا قيمة".format(data["count_coverage"], data["missing_review_counts"])
          if rtl else "Partial: {} known counts; {} missing".format(data["count_coverage"],data["missing_review_counts"])),
         "▤", "blue"),
        ("معدل الاستجابة" if rtl else "Response rate",
         _format_value(data["response_rate"], 1) + "%" if data["response_rate"] is not None else "—",
         ("{} ردود من {} حالة معروفة؛ {} غير محددة".format(data["response_responded"],data["response_known"],data["response_unknown"])
          if rtl else "{} responses / {} known; {} unknown".format(data["response_responded"],data["response_known"],data["response_unknown"])),
         "◉", "green"),
        ("تنبيهات مفتوحة" if rtl else "Open alerts",
         _format_value(data["open_alerts"]),
         ("إجمالي التنبيهات المفتوحة" if rtl else "Exact count of open alerts"), "♢", "red"),
    ]
    for col, item in zip(cols, items):
        with col:
            _card(*item)

    st.markdown('<div class="rt45-section-title">{}</div>'.format(
        "أدوات إدارة مراجعاتك" if rtl else "Review management tools"
    ), unsafe_allow_html=True)
    _feature_items = [
        ("★", "تتبع المراجعات" if rtl else "Track reviews",
         "عرض المراجعات المخزنة والفرز حسب التقييم والفرع." if rtl else
         "Inspect stored reviews and filter by branch and rating.",
         "عرض المراجعات" if rtl else "Open reviews", "review_all", "blue"),
        ("⌖", "إدارة الفروع" if rtl else "Manage branches",
         "البحث عن الفروع ومراجعة تفاصيلها وبيانات الاتصال." if rtl else
         "Discover branches and inspect saved contact details.",
         "فتح الفروع" if rtl else "Open branches", "branch_workspace", "indigo"),
        ("▥", "تحليل الأداء" if rtl else "Performance insights",
         "مقارنة التقييمات ونسب الاستجابة حسب البيانات المتاحة." if rtl else
         "Compare ratings and response coverage from known records.",
         "عرض التحليل" if rtl else "View analytics", "performance", "violet"),
        ("♢", "التنبيهات الذكية" if rtl else "Smart alerts",
         "متابعة التنبيهات المسجلة والتحقق من مصدرها." if rtl else
         "Review saved alerts and their source evidence.",
         "فتح التنبيهات" if rtl else "Open alerts", "alerts", "rose"),
    ]
    for _col, _item in zip(st.columns(4, gap="small"), _feature_items):
        _icon, _title, _description, _cta, _destination, _tone = _item
        with _col:
            st.markdown(
                '<div class="rt45-feature rt45-feature-{}">'
                '<div class="rt45-feature-symbol">{}</div>'
                '<strong>{}</strong><p>{}</p></div>'.format(
                    _tone, escape(_icon), escape(_title), escape(_description)
                ), unsafe_allow_html=True
            )
            if st.button(_cta, key="rt45-feature-"+_destination,
                         use_container_width=True):
                navigate(_destination)

    trend_col, branch_col, sentiment_col = st.columns([1.25, 1.25, 1], gap="medium")
    with trend_col:
        with st.container(border=True, key="rt42-chart-trend"):
            _panel_heading("لقطات التقييم ضمن الفترة" if rtl else "Rating snapshots in selected period")
            trend = data["trend"]
            if trend.empty:
                _empty("لا توجد لقطات صحيحة خلال الفترة المختارة" if rtl else "No valid snapshots in the selected period")
            else:
                fig = go.Figure(go.Scatter(
                    x=trend["day"], y=trend["rating"],
                    mode="lines+markers" if data["snapshot_dates"]>=2 else "markers",
                    line=dict(color=BLUE, width=3), marker=dict(size=7, color="#FFFFFF", line=dict(width=2, color=BLUE)),
                    fill="tozeroy", fillcolor="rgba(30,104,208,.08)", hovertemplate="%{x|%Y-%m-%d}<br>%{y:.2f}<extra></extra>"
                ))
                fig.update_yaxes(range=[0, 5.05], gridcolor=GRID, tickvals=[1, 2, 3, 4, 5])
                fig.update_xaxes(showgrid=False)
                st.plotly_chart(_chart_base(fig), use_container_width=True, key="rt42-trend")
                if data["snapshot_dates"]<2:
                    st.caption("توجد لقطة واحدة فقط؛ لا يمكن استنتاج اتجاه زمني."
                               if rtl else "Only one snapshot date; no trend can be inferred.")
    with branch_col:
        with st.container(border=True, key="rt42-chart-branches"):
            _panel_heading("أعلى 5 فروع بتقييم معروف" if rtl else "Top 5 branches with known ratings")
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
            _panel_heading("أحدث المراجعات حسب تاريخ الاستيراد" if rtl else "Recent reviews by import date")
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

    with st.expander("عرض جميع الفروع ({}) وترتيبها".format(data["branches"]) if rtl
                     else "View and sort all {} branches".format(data["branches"]), expanded=False):
        full = data["all_branches"].copy()
        full.columns = (["الفرع","معرّف الفرع","التقييم","المراجعات المعلنة",
                         "آخر جلب موثق","العنوان"] if rtl else
                        ["Branch","Branch ID","Rating","Published reviews",
                         "Source fetched at","Address"])
        st.dataframe(full, use_container_width=True, hide_index=True, height=360)
        # Spreadsheet formula injection guard. Text values starting with formula
        # operators are not allowed to become executable Excel expressions.
        safe = full.copy()
        for col in safe.columns:
            if not pd.api.types.is_numeric_dtype(safe[col]):
                safe[col] = safe[col].fillna("").astype(str).map(
                    lambda v: "'" + v if v.lstrip().startswith(("=", "+", "-", "@")) else v
                )
        st.download_button(
            "تنزيل جميع الفروع CSV" if rtl else "Download all branches CSV",
            data=safe.to_csv(index=False).encode("utf-8-sig"),
            file_name="reviews_tracker_branches.csv",
            mime="text/csv",use_container_width=True,key="rt43_all_branches_csv"
        )
    if data["duplicate_review_records"] or data["duplicate_place_ids"] or data["duplicate_company_keys"]:
        st.warning(
            ("فحص التكرار: {} مراجعة مكررة؛ {} معرف فرع مكرر؛ {} Place ID مكرر. لم تُحذف سجلات المصدر."
             if rtl else "Duplicate audit: {} repeated reviews, {} duplicate branch keys, {} duplicate Place IDs. Source rows were not deleted.").format(
                data["duplicate_review_records"],data["duplicate_company_keys"],data["duplicate_place_ids"]
            )
        )
    st.caption(
        ("المصدر: قاعدة البيانات المحلية. التواريخ النسبية للمراجعات غير محسومة؛ "
         "لا تتوفر مزامنة حية دون OAuth مصرح به. لا توجد بيانات تقديرية."
         if rtl else "Source: local database. Relative review dates remain unverified; "
         "live synchronization requires authorized OAuth. No fabricated data.")
    )
    return data

"""Reviews Tracker v46: precise paginated comment browsing and RTL review cards.

Uses existing local SQLite records only. The source's relative review_date is
never silently converted into a calendar date or used to order the results.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
from contextlib import closing
import html
import math
import sqlite3
from typing import Any

import pandas as pd
import streamlit as st


@dataclass(frozen=True)
class ReviewFilters:
    workspace_id: int = 1
    branch: str = "all"
    keyword: str = ""
    sentiment: str = "all"
    rating: int = 0
    reply: str = "all"
    period: str = "all"
    has_text: str = "all"
    workflow: str = "all"
    topic: str = "all"
    priority: str = "all"
    sort: str = "newest"


def build_review_query(filters: ReviewFilters, *, now: datetime | None = None) -> tuple[str, list[Any]]:
    """Parameterized SQLite predicate; never interpolates user input into SQL."""
    clauses = ["r.workspace_id=?"]
    params: list[Any] = [int(filters.workspace_id)]
    if filters.branch != "all":
        clauses.append("r.pk=?")
        params.append(filters.branch)
    if filters.keyword.strip():
        key = "%" + filters.keyword.strip().replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_") + "%"
        clauses.append(
            "(r.text LIKE ? ESCAPE '\\' OR r.author LIKE ? ESCAPE '\\' "
            "OR r.topic LIKE ? ESCAPE '\\' OR EXISTS("
            "SELECT 1 FROM companies cb WHERE cb.workspace_id=r.workspace_id "
            "AND cb.pk=r.pk AND cb.name LIKE ? ESCAPE '\\'))"
        )
        params.extend([key] * 4)
    if filters.sentiment != "all":
        if filters.sentiment == "unknown":
            clauses.append("(r.sentiment IS NULL OR TRIM(r.sentiment)='')")
        else:
            clauses.append("LOWER(TRIM(COALESCE(r.sentiment,'')))=?")
            params.append(filters.sentiment)
    if filters.rating:
        if not 1 <= filters.rating <= 5:
            raise ValueError("review rating filter must be 1 through 5")
        clauses.append("r.rating=?")
        params.append(int(filters.rating))
    answered = "(COALESCE(r.responded,0)=1 OR LENGTH(TRIM(COALESCE(r.response_text,'')))>0)"
    no_written_reply = "(LENGTH(TRIM(COALESCE(r.response_text,'')))=0)"
    if filters.reply == "answered":
        clauses.append(answered)
    elif filters.reply == "unanswered":
        clauses.append("(r.responded=0 AND " + no_written_reply + ")")
    elif filters.reply == "unknown":
        clauses.append("(r.responded IS NULL AND " + no_written_reply + ")")
    if filters.period != "all":
        days = {"7d": 7, "30d": 30, "90d": 90, "365d": 365}.get(filters.period)
        if days is None:
            raise ValueError("Unsupported import period")
        date = (now or datetime.now()) - timedelta(days=days)
        clauses.append("r.fetched_at >= ?")
        params.append(date.isoformat(timespec="seconds"))
    if filters.has_text == "with_text":
        clauses.append("LENGTH(TRIM(COALESCE(r.text,''))) > 0")
    elif filters.has_text == "without_text":
        clauses.append("LENGTH(TRIM(COALESCE(r.text,''))) = 0")
    if filters.workflow != "all":
        clauses.append("COALESCE(r.workflow_status,'new')=?")
        params.append(filters.workflow)
    if filters.topic != "all":
        clauses.append("r.topic=?")
        params.append(filters.topic)
    if filters.priority != "all":
        clauses.append("COALESCE(r.priority,'normal')=?")
        params.append(filters.priority)
    return " AND ".join(clauses), params


_ORDER_BY = {
    "newest": "r.fetched_at DESC, r.id DESC",
    "oldest": "r.fetched_at ASC, r.id ASC",
    "rating_high": "r.rating DESC, r.fetched_at DESC, r.id DESC",
    "rating_low": "r.rating ASC, r.fetched_at DESC, r.id DESC",
    "author": "LOWER(COALESCE(r.author,'')) ASC, r.id DESC",
    "unanswered": "CASE WHEN COALESCE(r.responded,0)=1 OR LENGTH(TRIM(COALESCE(r.response_text,'')))>0 THEN 1 ELSE 0 END ASC, r.fetched_at DESC, r.id DESC",
}


def fetch_review_page(db: Any, filters: ReviewFilters, page: int = 1, size: int = 20
                      ) -> tuple[int, int, pd.DataFrame]:
    if size not in (10, 20, 50):
        raise ValueError("Invalid page size")
    if page < 1:
        raise ValueError("Invalid page")
    predicate, params = build_review_query(filters)
    ordering = _ORDER_BY.get(filters.sort)
    if ordering is None:
        raise ValueError("Invalid sort")
    # Deterministic one-to-one branch join even for legacy duplicate company rows.
    statement = (
        "SELECT r.*,c.name AS company_name,c.address AS company_address "
        "FROM reviews r LEFT JOIN companies c ON c.rowid=("
        " SELECT rowid FROM companies WHERE workspace_id=r.workspace_id "
        " AND pk=r.pk ORDER BY rowid DESC LIMIT 1)"
    )
    with closing(db._c()) as connection:
        count = int(connection.execute(
            "SELECT COUNT(*) FROM reviews r WHERE " + predicate, params).fetchone()[0])
        total = int(connection.execute(
            "SELECT COUNT(*) FROM reviews WHERE workspace_id=?", (filters.workspace_id,)
        ).fetchone()[0])
        offset = (page - 1) * size
        frame = pd.read_sql_query(
            statement + " WHERE " + predicate + " ORDER BY " + ordering +
            " LIMIT ? OFFSET ?", connection, params=[*params, size, offset]
        )
    return total, count, frame


def reply_state(row: dict[str, Any]) -> str:
    response = str(row.get("response_text") or "").strip()
    raw = row.get("responded")
    if response or str(raw).strip().lower() in ("1", "1.0", "true"):
        return "answered"
    if raw is None or (isinstance(raw, float) and math.isnan(raw)):
        return "unknown"
    if str(raw).strip().lower() in ("0", "0.0", "false"):
        return "unanswered"
    return "unknown"


def safe_value(value: Any, empty: str = "—") -> str:
    if value is None or (isinstance(value, float) and math.isnan(value)):
        return empty
    text = str(value).strip()
    return text if text else empty


def export_review_csv(frame: pd.DataFrame, *, rtl: bool) -> bytes:
    columns = [
        ("company_name", "الفرع", "Branch"),
        ("author", "الكاتب", "Author"),
        ("rating", "التقييم", "Rating"),
        ("text", "نص التعليق", "Review"),
        ("review_date", "التاريخ كما ورد", "Source date as received"),
        ("fetched_at", "تاريخ الاستيراد", "Imported at"),
        ("sentiment", "المشاعر", "Sentiment"),
        ("topic", "الموضوع", "Topic"),
        ("response_text", "رد المالك المحفوظ", "Stored owner reply"),
        ("workflow_status", "حالة المتابعة", "Workflow"),
    ]
    out = pd.DataFrame(index=frame.index)
    for source, ar, en in columns:
        if source == "company_name" and source not in frame:
            source = "pk"
        data = frame[source] if source in frame else pd.Series([""] * len(frame), index=frame.index)
        if source != "rating":
            data = data.fillna("").astype(str).map(
                lambda v: "'" + v if v.lstrip().startswith(("=", "+", "-", "@", "\t", "\r")) else v
            )
        out[ar if rtl else en] = data
    return out.to_csv(index=False).encode("utf-8-sig")


def _escape(value: Any, empty: str = "—") -> str:
    return html.escape(safe_value(value, empty), quote=True)


def _render_card(record: dict[str, Any], *, rtl: bool) -> None:
    name = _escape(record.get("company_name") or record.get("pk"))
    author = _escape(record.get("author"))
    raw_rating = pd.to_numeric(record.get("rating"), errors="coerce")
    rating = int(raw_rating) if pd.notna(raw_rating) and 1 <= raw_rating <= 5 else None
    stars = ("★" * rating + "☆" * (5 - rating)) if rating is not None else "—"
    badge = reply_state(record)
    reply_labels = {
        "answered": "تم الرد" if rtl else "Answered",
        "unanswered": "بدون رد" if rtl else "No reply",
        "unknown": "غير معروف" if rtl else "Unknown",
    }
    sent = safe_value(record.get("sentiment"), "unknown").lower()
    sent_label = {
        "positive": "إيجابي" if rtl else "Positive",
        "negative": "سلبي" if rtl else "Negative",
        "neutral": "محايد" if rtl else "Neutral",
    }.get(sent, "غير مصنف" if rtl else "Unclassified")
    text = _escape(record.get("text"), "لا يوجد نص محفوظ" if rtl else "No stored text")
    posted = _escape(record.get("review_date"), "غير محدد" if rtl else "Unspecified")
    imported = _escape(str(record.get("fetched_at") or "")[:19])
    owner = safe_value(record.get("response_text"), "")
    owner_html = (
        '<div class="rt46-owner"><strong>' +
        ("رد المالك المحفوظ" if rtl else "Stored owner reply") +
        '</strong><p dir="auto">' + _escape(owner) + '</p></div>'
        if owner else ""
    )
    st.markdown(
        '<article class="rt46-review-card">'
        '<div class="rt46-review-header">'
        '<div><span class="rt46-branch">' + name + '</span>'
        '<div class="rt46-author">' + author + '</div></div>'
        '<div class="rt46-rating">' + html.escape(stars) + '</div></div>'
        '<div class="rt46-review-text" dir="auto">' + text + '</div>'
        '<div class="rt46-review-meta">'
        '<span class="rt46-chip rt46-sent-' + html.escape(sent) + '">' + html.escape(sent_label) + '</span>'
        '<span class="rt46-chip rt46-reply-' + badge + '">' + html.escape(reply_labels[badge]) + '</span>'
        '<span class="rt46-chip">' + ("الموضوع: " if rtl else "Topic: ") + _escape(record.get("topic")) + '</span></div>'
        + owner_html +
        '<div class="rt46-dates"><span>' +
        ("تاريخ التعليق كما ورد: " if rtl else "Original review date: ") +
        posted + '</span><span>' +
        ("تاريخ الاستيراد: " if rtl else "Imported at: ") +
        imported + '</span></div></article>',
        unsafe_allow_html=True
    )


def _reset_review_filters(page: str) -> None:
    defaults = {
        "rt46_text": "", "rt46_branch": "all",
        "rt46_sentiment": "negative" if page == "review_negative" else "all",
        "rt46_rating": 0,
        "rt46_reply": "unanswered" if page == "review_unanswered" else "all",
        "rt46_period": "all", "rt46_content": "all", "rt46_sort": "newest",
        "rt46_workflow": "all", "rt46_topic": "all", "rt46_priority": "all",
        "rt46_page_number": 1,
    }
    for key, value in defaults.items():
        st.session_state[key] = value
    st.session_state["rt46_page_context"] = page


def render_review_workspace(
    db: Any, workspace_id: int, page: str,
    company_options: dict[str, str], companies: pd.DataFrame,
    rtl: bool, navigate: Any, invalidate_cache: Any, user: dict[str, Any],
) -> None:
    title = "المراجعات والتعليقات" if rtl else "Reviews and comments"
    st.markdown(
        '<div class="rt46-review-hero"><div class="rt46-review-eyebrow">REVIEWS TRACKER</div>'
        '<h2>' + title + '</h2><p>' +
        ("تصفّح تعليقات الفروع المحفوظة، وفلترها حسب النص والتقييم والردود والتاريخ، مع فصل البيانات الناقصة عن الصفر."
         if rtl else "Browse stored branch comments; filter by text, rating, replies and import date without inventing missing values.")
        + '</p></div>', unsafe_allow_html=True
    )
    previous = st.session_state.get("rt46_page_context")
    if previous != page:
        _reset_review_filters(page)

    branches = ["all"]
    if isinstance(companies, pd.DataFrame) and "pk" in companies:
        branches += [str(k) for k in companies["pk"].dropna().unique().tolist()]
    if st.session_state.get("rt46_branch", "all") not in branches:
        st.session_state["rt46_branch"] = "all"
    topics = ["all"] + [str(x) for x in db.review_topics(workspace_id)]
    if st.session_state.get("rt46_topic", "all") not in topics:
        st.session_state["rt46_topic"] = "all"

    with st.form("rt46_filter_form", border=False):
        a, b = st.columns([2.4, 1.4])
        a.text_input("ابحث في التعليقات أو الكاتب أو الموضوع" if rtl else "Search reviews, author or topic",
                     key="rt46_text", placeholder="اكتب كلمة البحث..." if rtl else "Search stored comments...")
        b.selectbox("الفرع" if rtl else "Branch", branches, key="rt46_branch",
                    format_func=lambda x: ("جميع الفروع" if rtl else "All branches") if x == "all" else company_options.get(x, x))
        c1, c2, c3, c4 = st.columns(4)
        c1.selectbox("التقييم" if rtl else "Rating", [0, 1, 2, 3, 4, 5], key="rt46_rating",
                     format_func=lambda x: ("كل التقييمات" if rtl else "All ratings") if x == 0 else f"{x} ★")
        c2.selectbox("المشاعر" if rtl else "Sentiment", ["all", "positive", "neutral", "negative", "unknown"],
                     key="rt46_sentiment",
                     format_func=lambda x: {
                         "all": "الكل", "positive": "إيجابي", "neutral": "محايد",
                         "negative": "سلبي", "unknown": "غير مصنف",
                     }.get(x, x) if rtl else x.title())
        c3.selectbox("الرد" if rtl else "Reply", ["all", "answered", "unanswered", "unknown"],
                     key="rt46_reply",
                     format_func=lambda x: {
                         "all": "الكل", "answered": "تم الرد", "unanswered": "بدون رد", "unknown": "غير معروف",
                     }.get(x, x) if rtl else x.title())
        c4.selectbox("فترة الاستيراد" if rtl else "Import period", ["all", "7d", "30d", "90d", "365d"],
                     key="rt46_period",
                     format_func=lambda x: {"all": "جميع السجلات", "7d": "7 أيام", "30d": "30 يومًا",
                                            "90d": "90 يومًا", "365d": "سنة"}.get(x, x) if rtl else x.upper())
        with st.expander("فلاتر وترتيب إضافية" if rtl else "More filters and sorting", expanded=False):
            d1, d2, d3 = st.columns(3)
            d1.selectbox("وجود نص" if rtl else "Review text", ["all", "with_text", "without_text"],
                         key="rt46_content", format_func=lambda x: {
                             "all": "الكل", "with_text": "تعليقات مكتوبة", "without_text": "تقييم دون نص"
                         }.get(x, x) if rtl else x.replace("_", " ").title())
            d2.selectbox("حالة المتابعة" if rtl else "Workflow", [
                "all", "new", "needs_response", "in_progress", "responded", "resolved", "ignored"],
                key="rt46_workflow", format_func=lambda x: {
                    "all": "الكل", "new": "جديد", "needs_response": "يحتاج رد",
                    "in_progress": "قيد المتابعة", "responded": "تم الرد",
                    "resolved": "مغلق", "ignored": "متجاهل",
                }.get(x, x) if rtl else x.replace("_", " ").title())
            d3.selectbox("الموضوع" if rtl else "Topic", topics, key="rt46_topic",
                         format_func=lambda x: ("الكل" if rtl else "All") if x == "all" else x)
            e1, e2 = st.columns(2)
            e1.selectbox("الأولوية" if rtl else "Priority", ["all", "critical", "high", "normal", "low"],
                         key="rt46_priority", format_func=lambda x: {
                             "all": "الكل", "critical": "حرجة", "high": "عالية",
                             "normal": "عادية", "low": "منخفضة",
                         }.get(x, x) if rtl else x.title())
            e2.selectbox("ترتيب" if rtl else "Sort", list(_ORDER_BY), key="rt46_sort",
                         format_func=lambda x: {
                             "newest": "آخر المستورد أولًا", "oldest": "الأقدم استيرادًا",
                             "rating_high": "أعلى تقييم", "rating_low": "أقل تقييم",
                             "author": "اسم المراجع", "unanswered": "بدون رد أولًا",
                         }.get(x, x) if rtl else x.replace("_", " ").title())
        st.form_submit_button("🔎 تطبيق الفلاتر" if rtl else "🔎 Apply filters",
                              use_container_width=True, type="primary")

    reset, mode_col, size_col = st.columns([1.15, 2, 1])
    reset.button("مسح الفلاتر" if rtl else "Clear filters", key="rt46_clear",
                 use_container_width=True, on_click=_reset_review_filters, args=(page,))
    mode = mode_col.radio("طريقة العرض" if rtl else "View",
                          ["cards", "table"], horizontal=True, key="rt46_view_mode",
                          format_func=lambda x: ("بطاقات" if x == "cards" else "جدول") if rtl else x.title())
    size = size_col.selectbox("لكل صفحة" if rtl else "Per page", [10, 20, 50], index=1, key="rt46_page_size")

    filters = ReviewFilters(
        workspace_id=workspace_id,
        branch=st.session_state.get("rt46_branch", "all"),
        keyword=st.session_state.get("rt46_text", ""),
        sentiment=st.session_state.get("rt46_sentiment", "all"),
        rating=st.session_state.get("rt46_rating", 0),
        reply=st.session_state.get("rt46_reply", "all"),
        period=st.session_state.get("rt46_period", "all"),
        has_text=st.session_state.get("rt46_content", "all"),
        workflow=st.session_state.get("rt46_workflow", "all"),
        topic=st.session_state.get("rt46_topic", "all"),
        priority=st.session_state.get("rt46_priority", "all"),
        sort=st.session_state.get("rt46_sort", "newest"),
    )
    # Read count and page in one database connection, with indexed SQL paging.
    page_current = int(st.session_state.get("rt46_page_number") or 1)
    total, count, frame = fetch_review_page(db, filters, max(1, page_current), int(size))
    pages = max(1, (count + int(size) - 1) // int(size))
    if page_current > pages:
        page_current = pages
        st.session_state["rt46_page_number"] = pages
        total, count, frame = fetch_review_page(db, filters, page_current, int(size))
    summary_1, summary_2, summary_3 = st.columns(3)
    summary_1.metric("جميع المحفوظ" if rtl else "All stored", total)
    summary_2.metric("مطابق للفلاتر" if rtl else "Matching", count)
    summary_3.metric("المعروض في الصفحة" if rtl else "On this page", len(frame))
    st.caption("الفترة الزمنية تخص **تاريخ الاستيراد** وليس تاريخ نشر التعليق النسبي في Google. آخر 30 يومًا غير مفعل افتراضيًا."
               if rtl else "Date filters use the import timestamp—not Google's relative review-posting dates. All saved reviews are shown by default.")
    if count == 0:
        st.info("لا توجد تعليقات تطابق هذه الفلاتر. جرّب «مسح الفلاتر» أو اختر جميع السجلات."
                if rtl else "No stored reviews match. Clear filters or select all saved records.")
        return
    if pages > 1:
        st.number_input("الصفحة" if rtl else "Page", min_value=1, max_value=pages,
                        value=page_current, step=1, key="rt46_page_number")
    if mode == "cards":
        for record in frame.to_dict("records"):
            _render_card(record, rtl=rtl)
    else:
        table = pd.DataFrame(index=frame.index)
        for source, ar, en in (
            ("company_name", "الفرع", "Branch"),
            ("author", "الكاتب", "Author"),
            ("rating", "التقييم", "Rating"),
            ("text", "التعليق", "Review"),
            ("review_date", "التاريخ كما ورد", "Original date"),
            ("fetched_at", "تاريخ الاستيراد", "Imported at"),
            ("sentiment", "المشاعر", "Sentiment"),
        ):
            table[ar if rtl else en] = frame[source] if source in frame else "—"
        table["الرد" if rtl else "Reply"] = [reply_state(x) for x in frame.to_dict("records")]
        st.dataframe(table, use_container_width=True, hide_index=True, height=440)

    st.download_button("⬇ تنزيل تعليقات الصفحة CSV" if rtl else "⬇ Download page reviews CSV",
                       data=export_review_csv(frame, rtl=rtl),
                       file_name="reviews_filtered_page.csv", mime="text/csv",
                       use_container_width=True, key="rt46_export_page")
    st.caption("التصدير خاص بالصفحة المعروضة بعد الفلاتر، ولا يشمل مراجعات غير مخزنة."
               if rtl else "Export contains the filtered current page only; it cannot include reviews not stored locally.")
    with st.expander("إجراءات التعليقات والمتابعة" if rtl else "Review workflow actions", expanded=False):
        options = [int(v) for v in frame["id"].tolist()]
        selected = st.selectbox("اختر تعليقًا" if rtl else "Select review", options,
                                format_func=lambda rid: next(
                                    ("#{} · {} · {}".format(rid, safe_value(row.get("author")),
                                    safe_value(row.get("company_name"))) for row in frame.to_dict("records")
                                     if int(row["id"]) == rid), str(rid)),
                                key="rt46_action_review")
        a, b, c, d = st.columns(4)
        actions = [
            (a, "needs_response", "يحتاج رد" if rtl else "Needs reply"),
            (b, "in_progress", "قيد المتابعة" if rtl else "In progress"),
            (c, "resolved", "تم الحل" if rtl else "Resolved"),
        ]
        for col, action, label in actions:
            if col.button(label, key="rt46_action_" + action, use_container_width=True):
                db.review_action(selected, action)
                invalidate_cache()
                st.rerun()
        if d.button("فتح الفرع" if rtl else "Open branch", key="rt46_open_branch", use_container_width=True):
            match = frame.loc[frame["id"] == selected]
            if not match.empty:
                navigate("branch_workspace", company=str(match.iloc[0]["pk"]))
        pcol, ncol = st.columns([1, 2])
        current = frame.loc[frame["id"] == selected]
        current_priority = safe_value(current.iloc[0].get("priority"), "normal") if not current.empty else "normal"
        priorities = ["low", "normal", "high", "critical"]
        if current_priority not in priorities:
            current_priority = "normal"
        level = pcol.selectbox("الأولوية" if rtl else "Priority", priorities,
                               index=priorities.index(current_priority), key="rt46_action_priority")
        if pcol.button("حفظ الأولوية" if rtl else "Save priority", key="rt46_save_priority"):
            db.set_review_priority(selected, level)
            invalidate_cache()
            st.rerun()
        note = ncol.text_input("ملاحظة داخلية" if rtl else "Internal note", key="rt46_action_note")
        if ncol.button("إضافة ملاحظة" if rtl else "Add note", key="rt46_add_note", disabled=not note.strip()):
            db.add_note(selected, note, workspace_id, user.get("username") or "local")
            st.rerun()
        saved = db.notes(selected, workspace_id)
        if not saved.empty:
            cols = [x for x in ("note", "author", "created_at") if x in saved]
            st.dataframe(saved[cols], use_container_width=True, hide_index=True)
        st.divider()
        bulk_ids = st.multiselect(
            "إجراءات جماعية للتعليقات المعروضة" if rtl else "Bulk actions on displayed reviews",
            options, format_func=lambda rid: f"#{rid}", key="rt46_bulk_ids"
        )
        bc1, bc2, bc3 = st.columns(3)
        for column, action, label in (
            (bc1, "needs_response", "تحتاج رد" if rtl else "Needs reply"),
            (bc2, "in_progress", "قيد المتابعة" if rtl else "In progress"),
            (bc3, "resolved", "تم الحل" if rtl else "Resolved"),
        ):
            if column.button(label, disabled=not bulk_ids,
                             key="rt46_bulk_" + action, use_container_width=True):
                db.bulk_review_action(bulk_ids, action)
                invalidate_cache()
                st.rerun()

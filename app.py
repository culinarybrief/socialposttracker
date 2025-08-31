import sqlite3
from pathlib import Path
from datetime import datetime, date, time, timedelta
import pandas as pd
import streamlit as st
import yaml

APP_TITLE = "Social Post Tracker — Lean"
DB_PATH = Path("data/social_tracker.db")
TAXO_PATH = Path("config/taxonomies.yml")

PLATFORMS = ["instagram","tiktok","facebook","youtube","pinterest","email"]

DEFAULT_TAXO = {
    "campaign": ["BTS","Storytelling","Growth","Launch","Evergreen"],
    "caption_style": ["Short hook","Story paragraph","How-to/Recipe","Question/Poll","CTA"]
}

# ---------- setup ----------
def ensure_dirs():
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    TAXO_PATH.parent.mkdir(parents=True, exist_ok=True)

def get_conn():
    ensure_dirs()
    conn = sqlite3.connect(DB_PATH)
    conn.execute("PRAGMA journal_mode=WAL;")
    return conn

def init_db():
    conn = get_conn()
    conn.executescript("""
    CREATE TABLE IF NOT EXISTS posts(
      id INTEGER PRIMARY KEY,
      platform TEXT NOT NULL,
      post_datetime TEXT NOT NULL,
      campaign TEXT,
      caption_style TEXT,
      reach INTEGER DEFAULT 0,
      likes INTEGER DEFAULT 0,
      follows_gained INTEGER DEFAULT 0,
      email_captures INTEGER DEFAULT 0,
      notes TEXT,
      created_at TEXT DEFAULT (datetime('now'))
    );
    CREATE INDEX IF NOT EXISTS idx_posts_platform_date ON posts(platform, post_datetime);
    CREATE INDEX IF NOT EXISTS idx_posts_campaign ON posts(campaign);
    CREATE INDEX IF NOT EXISTS idx_posts_caption ON posts(caption_style);
    """)
    conn.commit()
    conn.close()

def load_taxonomies():
    ensure_dirs()
    if not TAXO_PATH.exists():
        TAXO_PATH.write_text(yaml.safe_dump(DEFAULT_TAXO, sort_keys=True))
    return yaml.safe_load(TAXO_PATH.read_text())

def save_taxonomies(data):
    TAXO_PATH.write_text(yaml.safe_dump(data, sort_keys=True))

def quick_add(label, key, group, options):
    opts = options + ["➕ Add new…"]
    choice = st.selectbox(label, opts, key=f"sel_{key}")
    if choice == "➕ Add new…":
        new_val = st.text_input(f"Add new {group}", key=f"add_{key}")
        if new_val:
            norm = new_val.strip().title()
            taxo = load_taxonomies()
            lst = list(taxo.get(group, []))
            if norm not in lst:
                lst.append(norm)
                taxo[group] = lst
                save_taxonomies(taxo)
            st.caption("Added ✔")
            return norm
        else:
            st.stop()
    return choice

# ---------- helpers ----------
def sdiv(a, b):
    try:
        a = float(a); b = float(b)
        return (a / b) if b else 0.0
    except Exception:
        return 0.0

def monday_of(d: date) -> date:
    return d - timedelta(days=d.weekday())

def last_full_week(today: datetime):
    lm = monday_of(today.date())
    return (lm - timedelta(days=7), lm - timedelta(days=1))

# ---------- Scorecard ----------
def scorecard(filters):
    st.subheader("Scorecard (Lean)")
    conn = get_conn()
    q = "SELECT platform, post_datetime, campaign, caption_style, reach, likes, follows_gained, email_captures FROM posts WHERE date(post_datetime) BETWEEN ? AND ?"
    params = [filters["start"].isoformat(), filters["end"].isoformat()]
    if filters["platforms"]:
        q += " AND platform IN (%s)" % ",".join(["?"] * len(filters["platforms"]))
        params += filters["platforms"]
    if filters["campaigns"]:
        q += " AND campaign IN (%s)" % ",".join(["?"] * len(filters["campaigns"]))
        params += filters["campaigns"]
    if filters["caption_styles"]:
        q += " AND caption_style IN (%s)" % ",".join(["?"] * len(filters["caption_styles"]))
        params += filters["caption_styles"]
    df = pd.read_sql_query(q, conn, params=params, parse_dates=["post_datetime"]) if DB_PATH.exists() else pd.DataFrame()
    conn.close()

    if df.empty:
        st.info("No data in this window. Add posts in Data Entry.")
        return

    totals = df[["reach","likes","follows_gained","email_captures"]].fillna(0).sum()
    like_rate    = sdiv(totals["likes"], totals["reach"])
    follow_rate  = sdiv(totals["follows_gained"], totals["reach"])
    capture_rate = sdiv(totals["email_captures"], totals["reach"])

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Reach", int(totals["reach"]))
    c2.metric("Likes", int(totals["likes"]), f"{like_rate*100:.1f}% rate")
    c3.metric("Follows", int(totals["follows_gained"]), f"{follow_rate*100:.1f}% rate")
    c4.metric("Email Captures", int(totals["email_captures"]), f"{capture_rate*100:.1f}% rate")

    week = df.set_index("post_datetime").resample("W-MON").sum(numeric_only=True).reset_index().rename(columns={"post_datetime": "week"})
    st.line_chart(week.set_index("week")[["reach","likes","follows_gained","email_captures"]])

# ---------- Insights (Caption / Platform / Campaign) ----------
DIMENSIONS = {
    "Caption Style": "caption_style",
    "Platform": "platform",
    "Campaign/Theme": "campaign",
}

def insights(filters):
    st.subheader("Insights")

    # Persist rank-by within the session (default composite)
    options_rank = ["success_score (composite)","follow_rate","capture_rate","like_rate","follows_gained","email_captures","likes","reach"]
    if "insights_rank_by" not in st.session_state:
        st.session_state["insights_rank_by"] = "success_score (composite)"

    dim = st.selectbox("Dimension", list(DIMENSIONS.keys()), index=0)

    # Pull filtered rows
    conn = get_conn()
    q = "SELECT platform, campaign, caption_style, reach, likes, follows_gained, email_captures FROM posts WHERE date(post_datetime) BETWEEN ? AND ?"
    params = [filters["start"].isoformat(), filters["end"].isoformat()]
    if filters["platforms"]:
        q += " AND platform IN (%s)" % ",".join(["?"] * len(filters["platforms"]))
        params += filters["platforms"]
    if filters["campaigns"]:
        q += " AND campaign IN (%s)" % ",".join(["?"] * len(filters["campaigns"]))
        params += filters["campaigns"]
    if filters["caption_styles"]:
        q += " AND caption_style IN (%s)" % ",".join(["?"] * len(filters["caption_styles"]))
        params += filters["caption_styles"]
    df = pd.read_sql_query(q, conn, params=params) if DB_PATH.exists() else pd.DataFrame()
    conn.close()

    if df.empty:
        st.info("No posts for this window/filters yet.")
        return

    col = DIMENSIONS[dim]
    label = {"caption_style": "style", "platform": "platform", "campaign": "campaign"}[col]
    df[col] = df[col].fillna("Unlabeled").replace("", "Unlabeled")
    g = df.groupby(col, dropna=False)[["reach","likes","follows_gained","email_captures"]].sum().reset_index().rename(columns={col: label})

    # Derived rates
    g["like_rate"]    = g.apply(lambda r: sdiv(r["likes"], r["reach"]), axis=1)
    g["follow_rate"]  = g.apply(lambda r: sdiv(r["follows_gained"], r["reach"]), axis=1)
    g["capture_rate"] = g.apply(lambda r: sdiv(r["email_captures"], r["reach"]), axis=1)

    # Controls
    c1, c2, c3 = st.columns(3)
    with c1:
        rank_by = st.selectbox("Rank by", options_rank, index=options_rank.index(st.session_state["insights_rank_by"]), key="insights_rank_by")
    with c2:
        min_reach = st.number_input("Min reach per group (exclude tiny samples)", min_value=0, value=100, step=50)
    with c3:
        top_n = st.slider("Show top N", min_value=3, max_value=20, value=10)

    # Composite weights if selected
    if st.session_state["insights_rank_by"] == "success_score (composite)":
        w1, w2, w3 = st.columns(3)
        with w1: wf = st.slider("Weight: follow_rate", 0.0, 1.0, 0.6, 0.05, key="wf")
        with w2: wc = st.slider("Weight: capture_rate", 0.0, 1.0, 0.3, 0.05, key="wc")
        with w3: wl = st.slider("Weight: like_rate",   0.0, 1.0, 0.1, 0.05, key="wl")
        total = max(wf + wc + wl, 1e-9)
        wf, wc, wl = wf/total, wc/total, wl/total
        g["success_score (composite)"] = (wf*g["follow_rate"] + wc*g["capture_rate"] + wl*g["like_rate"])

    g_f = g[g["reach"] >= min_reach].copy()
    if g_f.empty:
        st.warning("All groups were filtered out by the min reach threshold.")
        return

    g_sorted = g_f.sort_values(st.session_state["insights_rank_by"], ascending=False).head(top_n)
    st.dataframe(g_sorted, use_container_width=True)
    try:
        st.bar_chart(g_sorted.set_index(label)[st.session_state["insights_rank_by"]])
    except Exception:
        pass

    st.download_button("Download insights (CSV)", g_sorted.to_csv(index=False).encode("utf-8"), "insights.csv", "text/csv")

# ---------- Weekly Review ----------
GROUPS = {"Platform": "platform", "Campaign/Theme": "campaign", "Caption Style": "caption_style"}

def weekly_review():
    st.subheader("Weekly Review")
    today = datetime.now()
    default_start, _ = last_full_week(today)

    mondays = sorted({monday_of((today - timedelta(weeks=w)).date()) for w in range(12)})
    start = st.selectbox("Week starting (Monday)", mondays, index=mondays.index(default_start))
    end = start + timedelta(days=6)
    st.caption(f"Window: {start} → {end}")

    # Sync to Scorecard button (use 'pending_' keys so we don't set live widget state)
    if st.button("Use this week in Scorecard"):
        st.session_state["pending_filters_date_range"] = (start, end)
        # also hand off current campaigns to sidebar
        st.session_state["pending_filter_campaigns"] = st.session_state.get("wr_campaigns", [])
        st.session_state["page_selector"] = "Scorecard"
        st.rerun()

    # local filters (unique keys to avoid collisions with sidebar)
    col1, col2, col3 = st.columns(3)
    with col1:
        platforms = st.multiselect("Filter: Platforms", PLATFORMS, default=[], key="wr_platforms")
    with col2:
        taxo = load_taxonomies()
        campaigns = st.multiselect("Filter: Campaign/Theme", taxo.get("campaign", []), default=[], key="wr_campaigns")
    with col3:
        caption_styles = st.multiselect("Filter: Caption Style", taxo.get("caption_style", []), default=[], key="wr_caption_styles")

    # query
    conn = get_conn()
    q = "SELECT platform, post_datetime, campaign, caption_style, reach, likes, follows_gained, email_captures FROM posts WHERE date(post_datetime) BETWEEN ? AND ?"
    params = [start.isoformat(), end.isoformat()]
    if platforms:
        q += " AND platform IN (%s)" % ",".join(["?"] * len(platforms)); params += platforms
    if campaigns:
        q += " AND campaign IN (%s)" % ",".join(["?"] * len(campaigns)); params += campaigns
    if caption_styles:
        q += " AND caption_style IN (%s)" % ",".join(["?"] * len(caption_styles)); params += caption_styles
    df = pd.read_sql_query(q, conn, params=params) if DB_PATH.exists() else pd.DataFrame()
    conn.close()

    if df.empty:
        st.info("No posts in this week (with current filters).")
        return

    gcol1, gcol2 = st.columns(2)
    with gcol1:
        group_by = st.selectbox("Group by", list(GROUPS.keys()), index=0, key="wr_groupby")
        gcol = GROUPS[group_by]
    with gcol2:
        metric = st.selectbox("Sort by", ["reach","likes","follows_gained","email_captures","like_rate","follow_rate","capture_rate"], index=0, key="wr_metric")

    agg_cols = ["reach","likes","follows_gained","email_captures"]
    g = df.groupby(gcol, dropna=False)[agg_cols].sum().reset_index().rename(columns={gcol: "group"})
    g["like_rate"]    = g.apply(lambda r: sdiv(r["likes"], r["reach"]), axis=1)
    g["follow_rate"]  = g.apply(lambda r: sdiv(r["follows_gained"], r["reach"]), axis=1)
    g["capture_rate"] = g.apply(lambda r: sdiv(r["email_captures"], r["reach"]), axis=1)
    g = g.sort_values(metric, ascending=False)

    st.dataframe(g, use_container_width=True)
    try:
        st.bar_chart(g.set_index("group")[metric].head(10))
    except Exception:
        pass

    st.download_button("Download grouped summary (CSV)", g.to_csv(index=False).encode("utf-8"),
                       file_name=f"weekly_{group_by.lower()}_{start}.csv", mime="text/csv")

# ---------- Data Entry ----------
def data_entry():
    st.subheader("Data Entry (Lean)")
    taxo = load_taxonomies()
    with st.form("post_form", clear_on_submit=True):
        col1, col2, col3 = st.columns(3)
        with col1:
            platform = st.selectbox("Platform", PLATFORMS)
            d = st.date_input("Date", value=date.today())
            t = st.time_input("Time", value=time(9, 0))
        with col2:
            campaign = quick_add("Campaign/Theme", "campaign", "campaign", taxo.get("campaign", []))
            caption_style = quick_add("Caption Style", "caption", "caption_style", taxo.get("caption_style", []))
        with col3:
            notes = st.text_area("Notes (optional)", height=100)

        st.markdown("**Metrics**")
        m1, m2, m3, m4 = st.columns(4)
        with m1:
            reach = st.number_input("Reach", min_value=0, step=1)
        with m2:
            likes = st.number_input("Likes", min_value=0, step=1)
        with m3:
            follows = st.number_input("Follows", min_value=0, step=1)
        with m4:
            captures = st.number_input("Email Captures", min_value=0, step=1)

        submit = st.form_submit_button("Save Post")
        if submit:
            if (reach or likes or follows or captures):
                conn = get_conn()
                conn.execute(
                    "INSERT INTO posts (platform, post_datetime, campaign, caption_style, reach, likes, follows_gained, email_captures, notes) VALUES (?,?,?,?,?,?,?,?,?)",
                    (platform, datetime.combine(d, t).isoformat(timespec="minutes"), campaign, caption_style,
                     int(reach or 0), int(likes or 0), int(follows or 0), int(captures or 0), notes)
                )
                conn.commit()
                conn.close()
                st.success("Saved ✔")
            else:
                st.error("Enter at least one metric (Reach, Likes, Follows, or Email Captures).")

# ---------- Sidebar ----------
def sidebar():
    st.sidebar.header("Filters")
    default_start, default_end = last_full_week(datetime.now())

    # allow Weekly Review to push date range/campaigns safely
    if "pending_filters_date_range" in st.session_state:
        st.session_state["filters_date_range"] = st.session_state.pop("pending_filters_date_range")
    value = st.session_state.get("filters_date_range", (default_start, default_end))
    date_range = st.sidebar.date_input("Date range", value=value, key="filters_date_range")
    if isinstance(date_range, tuple):
        start, end = date_range
    else:
        start, end = date_range, date_range

    platforms = st.sidebar.multiselect("Platforms", PLATFORMS, default=[])
    taxo = load_taxonomies()
    if "pending_filter_campaigns" in st.session_state:
        st.session_state["filter_campaigns"] = st.session_state.pop("pending_filter_campaigns")
    campaigns = st.sidebar.multiselect("Campaigns/Themes", taxo.get("campaign", []),
                                       key="filter_campaigns",
                                       default=st.session_state.get("filter_campaigns", []))
    caption_styles = st.sidebar.multiselect("Caption Styles", taxo.get("caption_style", []), default=[])

    st.sidebar.markdown("---")
    page = st.sidebar.radio("Go to",
                            ["Insights", "Weekly Review", "Scorecard", "Data Entry"],
                            index=["Insights","Weekly Review","Scorecard","Data Entry"].index(st.session_state.get("page_selector", "Insights")),
                            key="page_selector")

    return {"start": start, "end": end, "platforms": platforms, "campaigns": campaigns,
            "caption_styles": caption_styles, "page": page}

def main():
    st.set_page_config(page_title=APP_TITLE, layout="wide")
    st.title(APP_TITLE)
    init_db()
    filters = sidebar()
    if filters["page"] == "Insights":
        insights(filters)
    elif filters["page"] == "Weekly Review":
        weekly_review()
    elif filters["page"] == "Scorecard":
        scorecard(filters)
    else:
        data_entry()

if __name__ == "__main__":
    main()

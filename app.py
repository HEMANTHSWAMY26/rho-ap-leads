"""
app.py
------
Main entry point for the Job Leads Dashboard.

Layout (per spec):
    1. Header
    2. Metrics Row  (Total Leads | Leads on Selected Date | Available Data Dates)
    3. Date Selector  (single date — st.date_input)
    4. Download Buttons  (CSV + Excel, left-aligned, below date selector)
    5. Results Table  (st.dataframe, filtered by selected date)

Run locally:
    streamlit run app.py

Deploy on Streamlit Cloud by pointing to this file.
"""

from google_sheets_writer import (
    append_new_leads,
    read_existing_leads,
    patch_missing_fields,
)
from deduplicator import remove_duplicates_against_existing
from data_loader import load_all_leads
from data_cleaner import clean_data, normalize_columns
from config import get_config
import io
import logging
from datetime import datetime, timezone

import pandas as pd
import streamlit as st

# -----------------------------------------------------------------------
# Logging
# -----------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(name)s — %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

# -----------------------------------------------------------------------
# Page config — MUST be the first Streamlit call
# -----------------------------------------------------------------------
st.set_page_config(
    page_title="Job Leads Dashboard",
    page_icon="💼",
    layout="wide",
    initial_sidebar_state="expanded",
)

# -----------------------------------------------------------------------
# Constants
# -----------------------------------------------------------------------
TODAY = datetime.now(timezone.utc).date()

# Columns shown in the results table (includes newly cleaned columns)
TABLE_COLUMNS = [
    "Job Title",
    "Company",
    "Location",
    "ERP",
    "Intensity",
    "FilterState",
    "Experience",
    "Employment type",
    "source",
    "first_seen_date",
    "Job url",
]


# -----------------------------------------------------------------------
# Helpers — export
# -----------------------------------------------------------------------
def _to_excel_bytes(df: pd.DataFrame) -> bytes:
    """Serialize dataframe to Excel bytes in memory."""
    buffer = io.BytesIO()
    with pd.ExcelWriter(buffer, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name="Job Leads")
    return buffer.getvalue()


def _to_csv_bytes(df: pd.DataFrame) -> bytes:
    """Serialize dataframe to CSV bytes."""
    return df.to_csv(index=False).encode("utf-8")


# -----------------------------------------------------------------------
# Cached loaders
# -----------------------------------------------------------------------
@st.cache_data(ttl=3600, show_spinner=False)
def load_config_cached() -> dict:
    """Load and cache app configuration."""
    return get_config()


@st.cache_data(ttl=3600, show_spinner=False)
def load_leads_from_sheets(_config: dict) -> pd.DataFrame:
    """
    Read all stored leads from Google Sheets.
    Cached for 1 hour to minimise API calls.
    """
    return read_existing_leads(_config)


# -----------------------------------------------------------------------
# Data pipeline — Apify → clean → deduplicate → append to Sheets
# -----------------------------------------------------------------------
def run_pipeline(config: dict) -> tuple[pd.DataFrame, str]:
    """
    Fetch the latest Apify data, clean it, deduplicate against what already
    exists in Google Sheets, and append only genuinely new leads.

    Returns (new_leads_df, status_message).
    """
@st.cache_data(ttl=3600, show_spinner=False)
def load_all_leads_cached(_config: dict) -> pd.DataFrame:
    """
    Fetch all leads from Apify.
    Cached for 1 hour to prevent redundant API calls on UI interaction.
    """
    return load_all_leads(_config)


# -----------------------------------------------------------------------
# Data pipeline — Apify → clean → deduplicate → append to Sheets
# -----------------------------------------------------------------------
def run_pipeline(config: dict) -> tuple[pd.DataFrame, str]:
    """
    Fetch the latest Apify data, clean it, deduplicate against what already
    exists in Google Sheets, and append only genuinely new leads.

    Returns (new_leads_df, status_message).
    """
    # Clear Apify cache before starting sync so we get fresh data
    load_all_leads_cached.clear()

    with st.spinner("Fetching latest leads from Apify…"):
        raw = load_all_leads_cached(config)

    if raw.empty:
        return pd.DataFrame(), "⚠️ No data returned from Apify. Scrapers may not have run yet."

    normalized = normalize_columns(raw)
    cleaned = clean_data(normalized)

    if cleaned.empty:
        return pd.DataFrame(), "⚠️ All fetched rows were invalid after cleaning."

    existing = read_existing_leads(config)
    truly_new = remove_duplicates_against_existing(cleaned, existing)

    # Patch existing rows that have empty Job url / Experience / Employment type
    # using values from the freshly-scraped Apify data
    with st.spinner("Patching missing fields in existing leads…"):
        patched = patch_missing_fields(cleaned, config)
    if patched:
        logger.info("Existing leads patched with missing fields from Apify.")

    if truly_new.empty:
        patch_note = " Existing leads updated with missing fields." if patched else ""
        load_leads_from_sheets.clear()
        return pd.DataFrame(), f"✅ No new leads found — all fetched records already exist.{patch_note}"

    success = append_new_leads(truly_new, config)
    if success:
        msg = f"✅ Successfully added **{len(truly_new)}** new leads to Google Sheets."
    else:
        msg = "❌ Failed to write new leads to Google Sheets. Check logs for details."

    # Bust cache so the table picks up the new rows on next load
    load_leads_from_sheets.clear()

    return truly_new, msg


# -----------------------------------------------------------------------
# UI — Section renderers
# -----------------------------------------------------------------------

def render_header() -> None:
    """Render the page title and subtitle."""
    st.title("💼 Job Leads Dashboard")
    st.caption("Daily Job Leads from Apify Tasks")
    st.divider()


def render_metrics(df: pd.DataFrame, filtered_df: pd.DataFrame) -> None:
    """
    Render three KPI tiles:
      - Total Leads
      - Leads on Selected Date
      - Available Data Dates
    """
    total_leads = len(df)
    selected_leads = len(filtered_df)

    try:
        unique_dates = df["first_seen_date"].nunique() if not df.empty else 0
    except Exception:
        unique_dates = 0

    col1, col2, col3 = st.columns(3)
    col1.metric("📋 Total Leads", f"{total_leads:,}")
    col2.metric("📅 Leads on Selected Date", f"{selected_leads:,}")
    col3.metric("🗓️ Available Data Dates", f"{unique_dates:,}")


def render_date_selector(df: pd.DataFrame):
    """
    Render a single-date calendar selector.
    Defaults to the most recent date that has data (or today).

    Returns the selected date as a datetime.date.
    """
    st.subheader("📆 Select Lead Date")

    # Determine the default date — most recent date with data
    default_date = TODAY
    if not df.empty:
        try:
            dates_with_data = pd.to_datetime(
                df["first_seen_date"], errors="coerce"
            ).dropna()
            if not dates_with_data.empty:
                default_date = dates_with_data.max().date()
        except Exception:
            pass

    selected_date = st.date_input(
        "Select Lead Date",
        value=default_date,
        key="selected_date",
        label_visibility="collapsed",
    )
    return selected_date


def render_download_buttons(filtered_df: pd.DataFrame, selected_date) -> None:
    """
    Render CSV and Excel download buttons left-aligned, below the date selector.
    Buttons are only shown when there is data to download.
    """
    if filtered_df.empty:
        return

    date_str = selected_date.strftime("%Y-%m-%d")
    col1, col2, col3 = st.columns([1, 1, 4])  # left-aligned in first two cols

    with col1:
        csv_bytes = _to_csv_bytes(filtered_df)
        st.download_button(
            label="⬇️ Download CSV",
            data=csv_bytes,
            file_name=f"job_leads_{date_str}.csv",
            mime="text/csv",
            use_container_width=True,
        )

    with col2:
        excel_bytes = _to_excel_bytes(filtered_df)
        st.download_button(
            label="⬇️ Download Excel",
            data=excel_bytes,
            file_name=f"job_leads_{date_str}.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            use_container_width=True,
        )


def render_table(filtered_df: pd.DataFrame, selected_date) -> None:
    """
    Display the filtered leads in a scrollable, responsive st.dataframe.
    Shows a clear empty-state message when no data exists for the selected date.
    """
    date_label = selected_date.strftime("%B %d, %Y")
    st.subheader(
        f"📊 Job Leads for {date_label} ({len(filtered_df):,} results)"
    )

    if filtered_df.empty:
        st.info(f"No data available for the selected date ({date_label}).")
        return

    # Only show columns that exist in the dataframe
    display_cols = [c for c in TABLE_COLUMNS if c in filtered_df.columns]
    display_df = filtered_df[display_cols].reset_index(drop=True)

    # Ensure all columns are string dtype so PyArrow does not mis-infer
    # mixed columns like Experience ("3", "5+", "Unknown") as int64.
    display_df = display_df.astype(str).replace({"nan": "", "None": "", "NaN": ""})

    st.dataframe(
        display_df,
        use_container_width=True,
        height=600,
        column_config={
            "Job url": st.column_config.LinkColumn(
                "Job url",
                display_text="View Job",
            ),
            "Experience": st.column_config.TextColumn("Experience"),
            "ERP": st.column_config.TextColumn("ERP"),
            "Intensity": st.column_config.TextColumn("Intensity"),
            "FilterState": st.column_config.TextColumn("FilterState"),
            "Employment type": st.column_config.TextColumn("Employment type"),
        },
    )


# -----------------------------------------------------------------------
# Main application
# -----------------------------------------------------------------------
def main() -> None:

    # ── Header ────────────────────────────────────────────────────────
    render_header()

    # ── Configuration ─────────────────────────────────────────────────
    try:
        config = load_config_cached()
    except EnvironmentError as exc:
        st.error(f"⚙️ Configuration error: {exc}")
        st.stop()

    # ── Sidebar: Sync button ──────────────────────────────────────────
    st.sidebar.header("🔄 Data Sync")
    sync_clicked = st.sidebar.button(
        "Fetch & Sync Latest Leads",
        help="Reads today's Apify results and appends new leads to Google Sheets.",
    )

    if sync_clicked:
        new_leads, status = run_pipeline(config)
        if new_leads is not None and not new_leads.empty:
            st.sidebar.success(status)
        else:
            st.sidebar.info(status)

    # ── Load all leads from Google Sheets ─────────────────────────────
    with st.spinner("Loading leads from Google Sheets…"):
        try:
            all_leads = load_leads_from_sheets(config)
        except Exception as exc:
            logger.error("Failed to load leads: %s", exc)
            st.error("Unable to load job leads. Please try again later.")
            st.stop()

    # ── Layout order per spec:
    #    Header → Metrics → Date Selector → Downloads → Table
    #
    #    Because Metrics depend on the selected date (which comes from the
    #    date selector widget), we use st.container() as a placeholder so
    #    metrics render visually ABOVE the date selector while still
    #    being computed with the correct filtered data.
    # ─────────────────────────────────────────────────────────────────

    # Reserve metrics slot (visual position 1 — above date selector)
    metrics_placeholder = st.container()
    st.divider()

    # ── Date Selector (visual position 2) ─────────────────────────────
    selected_date = render_date_selector(all_leads)

    # ── Filter by selected date ────────────────────────────────────────
    selected_str = selected_date.strftime("%Y-%m-%d")

    if not all_leads.empty:
        filtered_leads = all_leads[
            all_leads["first_seen_date"] == selected_str
        ].reset_index(drop=True)
    else:
        filtered_leads = pd.DataFrame(
            columns=list(all_leads.columns) if not all_leads.empty else []
        )

    # ── Fill Metrics placeholder (now we have filtered_leads) ─────────
    with metrics_placeholder:
        render_metrics(all_leads, filtered_leads)

    st.divider()

    # ── Download Buttons (left-aligned, below date selector) ──────────
    render_download_buttons(filtered_leads, selected_date)

    st.divider()

    # ── Results Table ─────────────────────────────────────────────────
    if all_leads.empty:
        st.warning(
            "No leads found in Google Sheets yet. "
            "Click **Fetch & Sync Latest Leads** in the sidebar to import today's data."
        )
    else:
        render_table(filtered_leads, selected_date)

    # ── Footer ────────────────────────────────────────────────────────
    st.markdown(
        "<br><p style='text-align:center;color:#aaa;font-size:0.8em;'>"
        "Job Leads Dashboard · Apify → Google Sheets · Streamlit"
        "</p>",
        unsafe_allow_html=True,
    )


if __name__ == "__main__":
    main()

"""
google_sheets_writer.py
-----------------------
Handles all interactions with Google Sheets.

Responsibilities:
- Authenticate via service account credentials (google-auth).
- Read existing leads from the master sheet.
- Append only new (deduplicated) leads.
- Maintain consistent column ordering.

Uses google-auth (compatible with gspread >= 6.x).
"""

import json
import logging
import os

import gspread
import pandas as pd
from google.oauth2.service_account import Credentials

from data_cleaner import (
    REQUIRED_COLUMNS,
    normalize_erp,
    normalize_intensity,
    normalize_filter_state,
    normalize_source,
    normalize_experience,
    normalize_employment_type,
    normalize_job_url,
)

logger = logging.getLogger(__name__)

SCOPES = [
    "https://spreadsheets.google.com/feeds",
    "https://www.googleapis.com/auth/drive",
    "https://www.googleapis.com/auth/spreadsheets",
]

SHEET_TAB_NAME = "job_leads_master"


def _get_credentials(service_account_json: str) -> Credentials:
    """
    Build Google credentials from a service account JSON file path
    or from a raw JSON string (used on Streamlit Cloud).

    Parameters
    ----------
    service_account_json : str
        Either a file path to the JSON key file, or the JSON string itself.

    Returns
    -------
    google.oauth2.service_account.Credentials
    """
    # If the value looks like a JSON string, parse it directly
    stripped = service_account_json.strip()
    if stripped.startswith("{"):
        info = json.loads(stripped)
        return Credentials.from_service_account_info(info, scopes=SCOPES)

    # Otherwise treat it as a file path
    if not os.path.exists(service_account_json):
        raise FileNotFoundError(
            f"Service account file not found: {service_account_json}"
        )
    return Credentials.from_service_account_file(service_account_json, scopes=SCOPES)


def _open_or_create_sheet(client: gspread.Client, sheet_id: str) -> gspread.Worksheet:
    """
    Open the target worksheet by sheet ID.
    Creates the 'job_leads_master' tab if it does not yet exist.

    Parameters
    ----------
    client : gspread.Client
    sheet_id : str
        Google Sheets document ID from environment variables.

    Returns
    -------
    gspread.Worksheet
    """
    spreadsheet = client.open_by_key(sheet_id)

    try:
        worksheet = spreadsheet.worksheet(SHEET_TAB_NAME)
    except gspread.exceptions.WorksheetNotFound:
        logger.info("Worksheet '%s' not found — creating it.", SHEET_TAB_NAME)
        worksheet = spreadsheet.add_worksheet(
            title=SHEET_TAB_NAME,
            rows=10000,
            cols=len(REQUIRED_COLUMNS),
        )
        # Write header row immediately
        worksheet.append_row(REQUIRED_COLUMNS, value_input_option="RAW")
        logger.info("Header row written to new worksheet.")

    return worksheet


def _normalize_sheet_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Map actual Google Sheet column headers to REQUIRED_COLUMNS using
    case-insensitive matching so that minor capitalization differences
    (e.g. 'Employment Type' vs 'Employment type', 'Source' vs 'source',
    'Job URL' vs 'Job url') do not produce empty columns.

    Parameters
    ----------
    df : pd.DataFrame
        Raw dataframe loaded from gspread get_all_records().

    Returns
    -------
    pd.DataFrame
        Dataframe with columns renamed to match REQUIRED_COLUMNS exactly.
    """
    # Build a lookup: lowercase(required_col) → required_col
    lower_to_required = {col.lower(): col for col in REQUIRED_COLUMNS}

    rename_map = {}
    for actual_col in df.columns:
        target = lower_to_required.get(actual_col.lower())
        if target and target != actual_col:
            rename_map[actual_col] = target

    if rename_map:
        logger.info("Column name normalization applied: %s", rename_map)
        df = df.rename(columns=rename_map)

    return df


def _backfill_derived_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    For rows that have empty derived columns but have source data available,
    re-apply the normalizers so columns like ERP, Intensity, FilterState,
    source, Experience, Employment type, and Job url are populated.

    This handles existing sheet rows that were stored before cleaning was
    applied, or when the column was previously misnamed (now renamed by
    _normalize_sheet_columns).

    Parameters
    ----------
    df : pd.DataFrame
        Dataframe with REQUIRED_COLUMNS already present.

    Returns
    -------
    pd.DataFrame
        Dataframe with derived columns filled where possible.
    """
    if df.empty:
        return df

    # Helper: treat a column value as "empty" if it's blank, nan, None, or Unknown
    def _is_empty(series: pd.Series) -> pd.Series:
        return series.astype(str).str.strip().isin(["", "nan", "None", "NaN", "Unknown"])

    desc_col = df.get("Job Description", pd.Series([""] * len(df), index=df.index))
    title_col = df.get("Job Title", pd.Series([""] * len(df), index=df.index))

    # Re-apply Job url extraction for rows where it looks like a nested object
    if "Job url" in df.columns:
        needs_url = df["Job url"].astype(str).str.strip().str.startswith("[") | \
                    df["Job url"].astype(str).str.strip().str.startswith("{") | \
                    _is_empty(df["Job url"])
        if needs_url.any():
            df.loc[needs_url, "Job url"] = normalize_job_url(df.loc[needs_url, "Job url"])

    # Re-apply ERP normalization for rows where ERP is empty/Unknown
    if "ERP" in df.columns:
        mask = _is_empty(df["ERP"])
        if mask.any():
            df.loc[mask, "ERP"] = normalize_erp(
                df.loc[mask, "ERP"], desc_col[mask]
            ).values

    # Re-apply Intensity normalization for rows where Intensity is empty/Unknown
    if "Intensity" in df.columns:
        mask = _is_empty(df["Intensity"])
        if mask.any():
            df.loc[mask, "Intensity"] = normalize_intensity(
                df.loc[mask, "Intensity"], desc_col[mask], title_col[mask]
            ).values

    # Re-apply FilterState for rows where it is empty — extract from Location
    if "FilterState" in df.columns and "Location" in df.columns:
        mask = _is_empty(df["FilterState"])
        if mask.any():
            df.loc[mask, "FilterState"] = normalize_filter_state(
                df.loc[mask, "Location"]
            ).values

    # Re-apply source normalization for rows where source is empty
    if "source" in df.columns:
        mask = _is_empty(df["source"])
        if mask.any():
            df.loc[mask, "source"] = normalize_source(df.loc[mask, "source"]).values

    # Re-apply Experience normalization for rows where it is empty/Unknown
    # Pass Job Description so years can be extracted from description text
    if "Experience" in df.columns:
        mask = _is_empty(df["Experience"])
        if mask.any():
            df.loc[mask, "Experience"] = normalize_experience(
                df.loc[mask, "Experience"],
                desc_col[mask],
            ).values

    # Re-apply Employment type normalization for rows where it is empty/Unknown
    # Pass Job Description so type can be extracted from description text
    if "Employment type" in df.columns:
        mask = _is_empty(df["Employment type"])
        if mask.any():
            df.loc[mask, "Employment type"] = normalize_employment_type(
                df.loc[mask, "Employment type"],
                desc_col[mask],
            ).values

    logger.info("Backfill of derived columns complete.")
    return df


def read_existing_leads(config: dict) -> pd.DataFrame:
    """
    Read all existing leads from the Google Sheets master tab.

    Parameters
    ----------
    config : dict
        Application configuration dictionary.

    Returns
    -------
    pd.DataFrame
        Existing leads, or an empty DataFrame if the sheet is empty or unavailable.
    """
    try:
        creds = _get_credentials(config["GOOGLE_SERVICE_ACCOUNT_JSON"])
        client = gspread.authorize(creds)
        worksheet = _open_or_create_sheet(client, config["GOOGLE_SHEET_ID"])

        records = worksheet.get_all_records()
        if not records:
            logger.info("Google Sheets tab is empty.")
            return pd.DataFrame(columns=REQUIRED_COLUMNS)

        df = pd.DataFrame(records)

        # Normalize column names — handles case mismatches from the sheet header row
        df = _normalize_sheet_columns(df)

        # Ensure all required columns exist (sheet may have extra/missing cols)
        for col in REQUIRED_COLUMNS:
            if col not in df.columns:
                df[col] = ""

        df = df[REQUIRED_COLUMNS]

        # Backfill any derived columns that are empty (handles legacy rows)
        df = _backfill_derived_columns(df)

        logger.info("Read %d existing leads from Google Sheets.", len(df))
        return df

    except Exception as exc:
        logger.error("Failed to read from Google Sheets: %s", exc)
        return pd.DataFrame(columns=REQUIRED_COLUMNS)


def patch_missing_fields(fresh_df: pd.DataFrame, config: dict) -> bool:
    """
    For existing rows in Google Sheets that have empty Job url, Experience,
    or Employment type — fill them in using the matching row from fresh_df
    (freshly scraped + cleaned Apify data).

    Matching is done on the same composite key: Job Title + Company + Location.
    Only columns that are currently empty in the sheet are overwritten.
    If any updates are made, the entire sheet is rewritten (overwrite_all_leads).

    Parameters
    ----------
    fresh_df : pd.DataFrame
        Freshly cleaned Apify data (output of clean_data).
    config : dict
        Application configuration dictionary.

    Returns
    -------
    bool
        True if patches were applied (sheet was updated), False if no patch needed.
    """
    if fresh_df.empty:
        return False

    try:
        existing = read_existing_leads(config)
        if existing.empty:
            return False

        # Columns that may need patching from fresh data
        PATCH_COLS = ["Job url", "Experience", "Employment type", "ERP", "FilterState"]

        def _make_key(df: pd.DataFrame) -> pd.Series:
            return (
                df["Job Title"].astype(str).str.lower().str.strip()
                + "|"
                + df["Company"].astype(str).str.lower().str.strip()
                + "|"
                + df["Location"].astype(str).str.lower().str.strip()
            )

        fresh_df = fresh_df.copy()
        fresh_df["_key"] = _make_key(fresh_df)
        fresh_lookup = fresh_df.drop_duplicates("_key").set_index("_key")

        existing = existing.copy()
        existing["_key"] = _make_key(existing)

        def _is_empty_val(v) -> bool:
            return str(v).strip() in ("", "nan", "None", "NaN", "Unknown")

        patches_applied = 0
        for idx, row in existing.iterrows():
            key = row["_key"]
            if key not in fresh_lookup.index:
                continue
            fresh_row = fresh_lookup.loc[key]
            for col in PATCH_COLS:
                if col in existing.columns and col in fresh_lookup.columns:
                    if _is_empty_val(row[col]) and not _is_empty_val(fresh_row[col]):
                        existing.at[idx, col] = fresh_row[col]
                        patches_applied += 1

        existing = existing.drop(columns=["_key"])

        if patches_applied > 0:
            logger.info("Patching %d empty field(s) in existing leads.", patches_applied)
            return overwrite_all_leads(existing, config)
        else:
            logger.info("No empty fields to patch in existing leads.")
            return False

    except Exception as exc:
        logger.error("Failed to patch missing fields: %s", exc)
        return False


def append_new_leads(new_leads: pd.DataFrame, config: dict) -> bool:
    """
    Append new job leads to the Google Sheets master tab.

    Parameters
    ----------
    new_leads : pd.DataFrame
        Leads to append (already cleaned and deduplicated).
    config : dict
        Application configuration dictionary.

    Returns
    -------
    bool
        True if successful, False otherwise.
    """
    if new_leads.empty:
        logger.info("No new leads to append.")
        return True

    try:
        creds = _get_credentials(config["GOOGLE_SERVICE_ACCOUNT_JSON"])
        client = gspread.authorize(creds)
        worksheet = _open_or_create_sheet(client, config["GOOGLE_SHEET_ID"])

        # Enforce column order and fill blanks
        new_leads = new_leads[REQUIRED_COLUMNS].fillna("").astype(str)

        # Convert to list-of-lists for batch append
        rows = new_leads.values.tolist()
        worksheet.append_rows(rows, value_input_option="RAW")

        logger.info("Appended %d new leads to Google Sheets.", len(rows))
        return True

    except Exception as exc:
        logger.error("Failed to write to Google Sheets: %s", exc)
        return False


def overwrite_all_leads(all_leads: pd.DataFrame, config: dict) -> bool:
    """
    Clear the sheet and write the entire dataset from scratch.

    Use only for full refreshes (e.g. re-sync after a schema change).

    Parameters
    ----------
    all_leads : pd.DataFrame
        Complete deduplicated dataset.
    config : dict
        Application configuration dictionary.

    Returns
    -------
    bool
        True if successful, False otherwise.
    """
    if all_leads.empty:
        logger.warning("overwrite_all_leads called with empty dataframe.")
        return False

    try:
        creds = _get_credentials(config["GOOGLE_SERVICE_ACCOUNT_JSON"])
        client = gspread.authorize(creds)
        worksheet = _open_or_create_sheet(client, config["GOOGLE_SHEET_ID"])

        all_leads = all_leads[REQUIRED_COLUMNS].fillna("").astype(str)

        # Header + data rows
        data = [REQUIRED_COLUMNS] + all_leads.values.tolist()

        worksheet.clear()
        worksheet.update(data, value_input_option="RAW")

        logger.info("Overwrote Google Sheets with %d leads.", len(all_leads))
        return True

    except Exception as exc:
        logger.error("Failed to overwrite Google Sheets: %s", exc)
        return False

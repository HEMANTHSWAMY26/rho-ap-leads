"""
deduplicator.py
---------------
Removes duplicate job leads from the dataset.

Duplicate definition:
    Job Title + Company + Location  (case-insensitive)
"""

import logging

import pandas as pd

logger = logging.getLogger(__name__)


def remove_duplicates(df: pd.DataFrame) -> pd.DataFrame:
    """
    Remove duplicate rows based on a composite key of
    Job Title + Company + Location (case-insensitive).

    Parameters
    ----------
    df : pd.DataFrame
        Cleaned dataframe.

    Returns
    -------
    pd.DataFrame
        Dataframe with duplicates removed.
    """
    if df.empty:
        return df

    before = len(df)

    # Build lowercase composite key for comparison
    df["_dedup_key"] = (
        df["Job Title"].str.lower().str.strip()
        + "|"
        + df["Company"].str.lower().str.strip()
        + "|"
        + df["Location"].str.lower().str.strip()
    )

    # Keep the first occurrence of each unique key
    df = df.drop_duplicates(subset=["_dedup_key"], keep="first")
    df = df.drop(columns=["_dedup_key"])

    after = len(df)
    logger.info("Deduplication: removed %d duplicate rows (%d → %d)",
                before - after, before, after)

    df = df.reset_index(drop=True)
    return df


def remove_duplicates_against_existing(
    new_df: pd.DataFrame, existing_df: pd.DataFrame
) -> pd.DataFrame:
    """
    Remove rows from new_df that already exist in existing_df,
    based on the same composite key (case-insensitive).

    Use this when appending only new leads to Google Sheets.

    Parameters
    ----------
    new_df : pd.DataFrame
        Freshly loaded and cleaned leads.
    existing_df : pd.DataFrame
        Leads already stored in Google Sheets.

    Returns
    -------
    pd.DataFrame
        Only the truly new leads not yet in existing_df.
    """
    if new_df.empty:
        return new_df

    if existing_df.empty:
        return remove_duplicates(new_df)

    def _make_key(df: pd.DataFrame) -> pd.Series:
        return (
            df["Job Title"].astype(str).str.lower().str.strip()
            + "|"
            + df["Company"].astype(str).str.lower().str.strip()
            + "|"
            + df["Location"].astype(str).str.lower().str.strip()
        )

    existing_keys = set(_make_key(existing_df))
    new_df = new_df.copy()
    new_df["_dedup_key"] = _make_key(new_df)

    before = len(new_df)
    new_df = new_df[~new_df["_dedup_key"].isin(existing_keys)]
    new_df = new_df.drop(columns=["_dedup_key"])
    after = len(new_df)

    logger.info(
        "Cross-deduplication: %d already exist, %d new leads to append",
        before - after,
        after,
    )

    # Also deduplicate within the new batch itself
    new_df = remove_duplicates(new_df)
    return new_df

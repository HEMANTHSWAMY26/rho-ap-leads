"""
data_loader.py
--------------
Fetches job lead datasets from the latest run of each Apify task.
NEVER triggers or starts any Apify scraper — read-only access.
"""

import logging
from datetime import datetime, timezone
from typing import Optional

import pandas as pd
import requests

logger = logging.getLogger(__name__)

# Apify dataset endpoint — reads only the most recent completed run
APIFY_ENDPOINT = (
    "https://api.apify.com/v2/actor-tasks/{task_id}/runs/last/dataset/items"
)

# Always request a clean dataset with a generous row limit so results are
# never silently truncated by the Apify default page size.
APIFY_PARAMS = {
    "clean": "true",
    "limit": 10000,
}


def _fetch_task_dataset(task_id: str, api_token: str, source_label: str) -> pd.DataFrame:
    """
    Fetch dataset items from the latest run of a single Apify task.

    Parameters
    ----------
    task_id : str
        The Apify task ID.
    api_token : str
        The Apify API token loaded from environment variables.
    source_label : str
        Human-readable label (e.g. 'US', 'Canada') added to every row.

    Returns
    -------
    pd.DataFrame
        Raw dataframe (may be empty if the run had no items).
    """
    url = APIFY_ENDPOINT.format(task_id=task_id)
    headers = {"Authorization": f"Bearer {api_token}"}

    try:
        response = requests.get(url, headers=headers, params=APIFY_PARAMS, timeout=30)
        response.raise_for_status()
    except requests.exceptions.Timeout:
        logger.error("Request timed out for task %s", task_id)
        return pd.DataFrame()
    except requests.exceptions.HTTPError as exc:
        logger.error("HTTP error for task %s: %s", task_id, exc)
        return pd.DataFrame()
    except requests.exceptions.RequestException as exc:
        logger.error("Network error for task %s: %s", task_id, exc)
        return pd.DataFrame()

    try:
        data = response.json()
    except ValueError:
        logger.error("Invalid JSON response for task %s", task_id)
        return pd.DataFrame()

    if not data:
        logger.warning("Empty dataset returned for task %s", task_id)
        return pd.DataFrame()

    df = pd.DataFrame(data)
    df["source"] = source_label
    df["run_id"] = task_id

    # Tag every record with today's date as first_seen_date if not already present
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    if "first_seen_date" not in df.columns:
        df["first_seen_date"] = today

    logger.info("Fetched %d records from task %s (%s)",
                len(df), task_id, source_label)
    return df


def load_all_leads(config: dict) -> pd.DataFrame:
    """
    Fetch leads from both Apify tasks and return a single merged dataframe.

    Parameters
    ----------
    config : dict
        Configuration dictionary from config.get_config().

    Returns
    -------
    pd.DataFrame
        Combined raw dataframe from both tasks.
    """
    api_token = config["APIFY_API_TOKEN"]

    df_us = _fetch_task_dataset(config["TASK_US"], api_token, "US")
    df_canada = _fetch_task_dataset(config["TASK_CANADA"], api_token, "Canada")

    frames = [df for df in [df_us, df_canada] if not df.empty]

    if not frames:
        logger.warning("No data retrieved from any Apify task.")
        return pd.DataFrame()

    combined = pd.concat(frames, ignore_index=True)
    logger.info("Total combined records: %d", len(combined))
    return combined

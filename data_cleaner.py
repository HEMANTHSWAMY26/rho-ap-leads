"""
data_cleaner.py
---------------
Normalizes raw Apify data into the required schema,
strips invalid rows, and applies all column-level cleaning rules.

Columns cleaned / normalized:
  - ERP          : extract known ERP names, deduplicate, standardize
  - Intensity    : High / Medium / Low based on keywords
  - FilterState  : full state/province name → abbreviation
  - source       : normalize platform name capitalization
  - Job url      : extract primary URL from nested list/dict objects
  - Experience   : extract years from description text when field is empty
  - Employment type : handle LinkedIn enum values + extract from description
"""

import ast
import json
import logging
import re
from datetime import datetime, timezone

import pandas as pd

logger = logging.getLogger(__name__)

# ------------------------------------------------------------------
# Target schema column order
# ------------------------------------------------------------------
REQUIRED_COLUMNS = [
    "Job Title",
    "Company",
    "Location",
    "Job Description",
    "Job url",
    "source",
    "first_seen_date",
    "run_id",
    "ERP",
    "Intensity",
    "FilterState",
    "Experience",
    "Employment type",
]

# ------------------------------------------------------------------
# Field mapping — maps common Apify field names → schema column names
# ------------------------------------------------------------------
FIELD_MAP = {
    # Job Title variants
    "title": "Job Title",
    "job_title": "Job Title",
    "jobTitle": "Job Title",
    "position": "Job Title",
    "positionName": "Job Title",
    "name": "Job Title",

    # Company variants
    "company": "Company",
    "companyName": "Company",
    "employer": "Company",
    "organization": "Company",

    # Location variants
    "location": "Location",
    "jobLocation": "Location",
    "city": "Location",
    "place": "Location",

    # Job Description variants
    "description": "Job Description",
    "jobDescription": "Job Description",
    "job_description": "Job Description",
    "details": "Job Description",
    "summary": "Job Description",
    "descriptionHtml": "Job Description",
    "descriptionText": "Job Description",

    # URL variants — LinkedIn, Indeed, ZipRecruiter, Glassdoor, Monster all covered
    "url": "Job url",
    "job_url": "Job url",
    "jobUrl": "Job url",
    "link": "Job url",
    "applyUrl": "Job url",
    "externalApplyUrl": "Job url",
    "applyLink": "Job url",
    "applyLinks": "Job url",
    "jobUrls": "Job url",
    "links": "Job url",
    "pageUrl": "Job url",
    "canonicalUrl": "Job url",
    "jobLink": "Job url",
    "href": "Job url",
    "apply_url": "Job url",
    "apply_link": "Job url",
    "jobPageUrl": "Job url",
    "detailsPageUrl": "Job url",
    "externalUrl": "Job url",
    "redirectedUrl": "Job url",

    # Passthrough fields
    "source": "source",
    "first_seen_date": "first_seen_date",
    "run_id": "run_id",
    "ERP": "ERP",
    "Intensity": "Intensity",
    "FilterState": "FilterState",

    # Experience variants
    "experience": "Experience",
    "yearsExperience": "Experience",
    "years_experience": "Experience",
    "experienceRequired": "Experience",
    "experienceLevel": "Experience",
    "seniority": "Experience",
    "seniorityLevel": "Experience",

    # Employment type variants
    "employmentType": "Employment type",
    "employment_type": "Employment type",
    "jobType": "Employment type",
    "job_type": "Employment type",
    "contractType": "Employment type",
    "workType": "Employment type",
    "scheduleType": "Employment type",
    "schedule_type": "Employment type",
    "type": "Employment type",
}

# ------------------------------------------------------------------
# ERP normalization data
# ------------------------------------------------------------------
_VALID_ERPS = [
    "SAP",
    "Oracle",
    "NetSuite",
    "QuickBooks",
    "Microsoft Dynamics",
    "Workday",
    "Yardi",
    "Sage",
    "Concur",
    "Coupa",
    "Xero",
]

# Aliases that map raw values → canonical name
_ERP_ALIASES = {
    "ms dynamics": "Microsoft Dynamics",
    "ms-dynamics": "Microsoft Dynamics",
    "msdynamics": "Microsoft Dynamics",
    "dynamics": "Microsoft Dynamics",
    "dynamics 365": "Microsoft Dynamics",
    "dynamics365": "Microsoft Dynamics",
    "netsuite": "NetSuite",
    "net suite": "NetSuite",
    "quickbooks": "QuickBooks",
    "quick books": "QuickBooks",
    "sap": "SAP",
    "oracle": "Oracle",
    "workday": "Workday",
    "yardi": "Yardi",
    "sage": "Sage",
    "concur": "Concur",
    "coupa": "Coupa",
    "xero": "Xero",
}

# ------------------------------------------------------------------
# FilterState — US states + Canadian provinces
# ------------------------------------------------------------------
_STATE_MAP = {
    # US States
    "alabama": "AL", "alaska": "AK", "arizona": "AZ", "arkansas": "AR",
    "california": "CA", "colorado": "CO", "connecticut": "CT", "delaware": "DE",
    "florida": "FL", "georgia": "GA", "hawaii": "HI", "idaho": "ID",
    "illinois": "IL", "indiana": "IN", "iowa": "IA", "kansas": "KS",
    "kentucky": "KY", "louisiana": "LA", "maine": "ME", "maryland": "MD",
    "massachusetts": "MA", "michigan": "MI", "minnesota": "MN", "mississippi": "MS",
    "missouri": "MO", "montana": "MT", "nebraska": "NE", "nevada": "NV",
    "new hampshire": "NH", "new jersey": "NJ", "new mexico": "NM", "new york": "NY",
    "north carolina": "NC", "north dakota": "ND", "ohio": "OH", "oklahoma": "OK",
    "oregon": "OR", "pennsylvania": "PA", "rhode island": "RI", "south carolina": "SC",
    "south dakota": "SD", "tennessee": "TN", "texas": "TX", "utah": "UT",
    "vermont": "VT", "virginia": "VA", "washington": "WA", "west virginia": "WV",
    "wisconsin": "WI", "wyoming": "WY", "district of columbia": "DC",
    # Canadian Provinces / Territories
    "ontario": "ON", "quebec": "QC", "british columbia": "BC", "alberta": "AB",
    "manitoba": "MB", "saskatchewan": "SK", "nova scotia": "NS",
    "new brunswick": "NB", "newfoundland and labrador": "NL",
    "newfoundland": "NL", "labrador": "NL",
    "prince edward island": "PE", "northwest territories": "NT",
    "nunavut": "NU", "yukon": "YT",
}

# ------------------------------------------------------------------
# Source normalization
# ------------------------------------------------------------------
_SOURCE_CANONICAL = {
    "indeed": "Indeed",
    "linkedin": "LinkedIn",
    "linked in": "LinkedIn",
    "ziprecruiter": "ZipRecruiter",
    "zip recruiter": "ZipRecruiter",
    "glassdoor": "Glassdoor",
    "glass door": "Glassdoor",
    "monster": "Monster",
    "recruit.net": "Recruit.net",
    "recruitnet": "Recruit.net",
    "company website": "Company Website",
    "companywebsite": "Company Website",
    "unknown": "Unknown",
}

# ------------------------------------------------------------------
# Intensity keywords
# ------------------------------------------------------------------
_HIGH_INTENSITY_KEYWORDS = [
    r"\burgent\b",
    r"\bimmediately\b",
    r"\bimmediate hire\b",
    r"\bhiring now\b",
    r"\bactively hiring\b",
    r"\basap\b",
    r"\bstart immediately\b",
    r"\bquick hire\b",
]

# ------------------------------------------------------------------
# Employment type normalization
# LinkedIn enum values: FULL_TIME, PART_TIME, CONTRACT, TEMPORARY, INTERNSHIP, OTHER
# ------------------------------------------------------------------
_EMPLOYMENT_TYPE_MAP = {
    # LinkedIn-style enum values (uppercase with underscores)
    "full_time": "Full Time",
    "part_time": "Part Time",
    "contract": "Contract",
    "temporary": "Temporary",
    "internship": "Internship",
    "other": "Unknown",
    # Standard human-readable values
    "full time": "Full Time",
    "full-time": "Full Time",
    "fulltime": "Full Time",
    "ft": "Full Time",
    "permanent": "Full Time",
    "permanent full-time": "Full Time",
    "permanent full time": "Full Time",
    "part time": "Part Time",
    "part-time": "Part Time",
    "parttime": "Part Time",
    "pt": "Part Time",
    "contractor": "Contract",
    "freelance": "Contract",
    "temp": "Temporary",
    "temp to perm": "Temporary",
    "temp-to-perm": "Temporary",
    "temporary contract": "Temporary",
    "intern": "Internship",
    "co-op": "Internship",
    "coop": "Internship",
    "co op": "Internship",
}

_ALLOWED_EMPLOYMENT_TYPES = {
    "Full Time", "Part Time", "Contract", "Temporary", "Internship", "Unknown"
}

# Keywords to scan in Job Description for employment type detection
_EMPLOYMENT_TYPE_DESC_PATTERNS = [
    (re.compile(r"\bfull[\s\-]?time\b", re.IGNORECASE), "Full Time"),
    (re.compile(r"\bpart[\s\-]?time\b", re.IGNORECASE), "Part Time"),
    (re.compile(r"\bcontract(or|ing)?\b", re.IGNORECASE), "Contract"),
    (re.compile(r"\bfreelance\b", re.IGNORECASE), "Contract"),
    (re.compile(r"\btemporar(y|ily)\b", re.IGNORECASE), "Temporary"),
    (re.compile(r"\bintern(ship)?\b", re.IGNORECASE), "Internship"),
    (re.compile(r"\bco[\s\-]?op\b", re.IGNORECASE), "Internship"),
    (re.compile(r"\bpermanent\b", re.IGNORECASE), "Full Time"),
]

# ------------------------------------------------------------------
# Experience extraction patterns
# Used to extract years of experience from Job Description text
# ------------------------------------------------------------------
_EXPERIENCE_PATTERNS = [
    # "5+ years", "5 or more years", "five years"
    re.compile(r"(\d+)\s*\+\s*(?:years?|yrs?)\s*(?:of\s+)?(?:experience|exp)?", re.IGNORECASE),
    # "minimum 5 years", "at least 3 years", "minimum of 5 years"
    re.compile(r"(?:minimum|min|at\s+least|minimum\s+of)\s+(\d+)\s*(?:\+)?\s*(?:years?|yrs?)", re.IGNORECASE),
    # "3-5 years", "3 to 5 years" → take the lower bound
    re.compile(r"(\d+)\s*[-–to]+\s*\d+\s*(?:years?|yrs?)\s*(?:of\s+)?(?:experience|exp)?", re.IGNORECASE),
    # "5 years of experience", "5 years experience"
    re.compile(r"(\d+)\s*(?:years?|yrs?)\s*(?:of\s+)?(?:experience|exp)", re.IGNORECASE),
    # "experience of 5 years"
    re.compile(r"experience\s+of\s+(\d+)\s*(?:\+)?\s*(?:years?|yrs?)", re.IGNORECASE),
]


# ==================================================================
# Individual column normalizers
# ==================================================================

def _extract_url_from_value(value) -> str:
    """
    Extract a plain URL string from a value that might be:
    - already a plain string URL
    - a JSON / Python-literal list of {'title': ..., 'link': ...} dicts
    - a single dict with url/link/href keys
    Returns the first valid http/https URL found, or empty string.
    """
    if not value or (isinstance(value, str) and value.strip() in ("", "nan", "None")):
        return ""

    # Already a valid plain URL
    if isinstance(value, str) and value.strip().startswith("http"):
        return value.strip()

    # Try to parse as a Python literal or JSON
    parsed = None
    if isinstance(value, str):
        raw = value.strip()
        try:
            parsed = ast.literal_eval(raw)
        except Exception:
            try:
                parsed = json.loads(raw)
            except Exception:
                pass

    if parsed is None and not isinstance(value, (list, dict)):
        return str(value).strip() if value else ""

    if parsed is not None:
        value = parsed

    # value is now a list or dict
    if isinstance(value, dict):
        # Check common URL key names inside a dict
        for key in ("url", "link", "href", "applyUrl", "applyLink", "jobUrl", "pageUrl"):
            link = value.get(key, "")
            if link and str(link).startswith("http"):
                return str(link).strip()
        value = [value]

    if isinstance(value, list):
        for item in value:
            if isinstance(item, dict):
                for key in ("url", "link", "href", "applyUrl", "applyLink", "jobUrl", "pageUrl"):
                    link = item.get(key, "")
                    if link and str(link).startswith("http"):
                        return str(link).strip()
            elif isinstance(item, str) and item.startswith("http"):
                return item.strip()

    return ""


def normalize_job_url(series: pd.Series) -> pd.Series:
    """Extract primary URL from potentially nested list/dict structures."""
    return series.apply(_extract_url_from_value)


def normalize_erp(erp_series: pd.Series, desc_series: pd.Series) -> pd.Series:
    """
    Normalize the ERP column.

    1. Map aliases to canonical names.
    2. Remove duplicates within a row (comma-separated).
    3. If ERP is empty/unknown, scan Job Description for ERP keywords.
    4. Default to 'Unknown' if still nothing found.
    """
    def _clean_erp_value(erp_val: str, description: str) -> str:
        erp_val = str(erp_val).strip()

        # Start with what's in the ERP field
        raw_parts = [p.strip()
                     for p in re.split(r"[,;/|]+", erp_val) if p.strip()]

        found = []
        for part in raw_parts:
            lower = part.lower()
            canonical = _ERP_ALIASES.get(lower)
            if canonical:
                found.append(canonical)
            elif part in _VALID_ERPS:
                found.append(part)
            # else: skip unknown/junk values

        # If nothing found in ERP field, scan the Job Description
        if not found and description:
            desc_lower = str(description).lower()
            for alias, canonical in _ERP_ALIASES.items():
                if alias in desc_lower and canonical not in found:
                    found.append(canonical)

        # Deduplicate while preserving order
        seen = set()
        deduped = []
        for item in found:
            if item not in seen:
                seen.add(item)
                deduped.append(item)

        return ", ".join(deduped) if deduped else "Unknown"

    return pd.Series(
        [_clean_erp_value(e, d) for e, d in zip(erp_series, desc_series)],
        index=erp_series.index,
    )


def normalize_intensity(
    intensity_series: pd.Series,
    desc_series: pd.Series,
    title_series: pd.Series,
) -> pd.Series:
    """
    Normalize Intensity to High / Medium / Low.

    High  : contains urgent keywords in title or description.
    Medium: standard job posting (default when no urgency detected).
    Low   : explicitly marked as old/unclear, or intensity field says low.
    """
    urgent_pattern = re.compile(
        "|".join(_HIGH_INTENSITY_KEYWORDS), re.IGNORECASE
    )

    def _classify(intensity: str, description: str, title: str) -> str:
        intensity_lower = str(intensity).strip().lower()

        # Honour existing valid values
        if intensity_lower in ("high", "medium", "low"):
            return intensity_lower.capitalize()

        # Scan title + description for urgency keywords
        combined_text = f"{title} {description}"
        if urgent_pattern.search(combined_text):
            return "High"

        if intensity_lower in ("low", "old", "stale", "unclear"):
            return "Low"

        return "Medium"

    return pd.Series(
        [_classify(i, d, t) for i, d, t in zip(
            intensity_series, desc_series, title_series
        )],
        index=intensity_series.index,
    )


def normalize_filter_state(series: pd.Series) -> pd.Series:
    """
    Normalize state / province values to standard abbreviations.
    Full names (e.g. 'Florida') are converted to abbreviations (e.g. 'FL').
    Already-abbreviated values are left as-is if valid.
    """
    def _normalize_state(val: str) -> str:
        val = str(val).strip()
        if not val or val.lower() in ("", "nan", "none", "unknown"):
            return val

        # Already an abbreviation (1–3 uppercase letters)?
        if re.match(r"^[A-Z]{1,3}$", val):
            return val  # trust it

        lower = val.lower()
        # Direct full-name lookup
        if lower in _STATE_MAP:
            return _STATE_MAP[lower]

        # Try to find a state name anywhere in the string
        for name, abbr in sorted(_STATE_MAP.items(), key=lambda x: -len(x[0])):
            if name in lower:
                return abbr

        return val  # return as-is if no match

    return series.apply(_normalize_state)


def normalize_source(series: pd.Series) -> pd.Series:
    """Normalize source/platform names to canonical capitalization."""
    def _normalize(val: str) -> str:
        val = str(val).strip()
        lower = val.lower()
        return _SOURCE_CANONICAL.get(lower, val)

    return series.apply(_normalize)


def normalize_experience(
    series: pd.Series,
    desc_series: pd.Series | None = None,
) -> pd.Series:
    """
    Normalize experience values to clean format.
    When the experience field is empty/Unknown, scan the Job Description
    for patterns like "5+ years of experience", "3-5 years", etc.

    Examples:
        '5+ yrs'           → '5+'
        '3 yrs'            → '3'
        '10+ years'        → '10+'
        '' (desc has "minimum 3 years experience") → '3'
        ''                 → 'Unknown'
    """
    if desc_series is None:
        desc_series = pd.Series([""] * len(series), index=series.index)

    def _extract_from_desc(text: str) -> str:
        """Try to extract years of experience from description text."""
        if not text or str(text).strip() in ("", "nan", "None"):
            return "Unknown"
        for pattern in _EXPERIENCE_PATTERNS:
            match = pattern.search(str(text))
            if match:
                num = match.group(1)
                # Check if there's a '+' after the number in the original text
                end_pos = match.end()
                remaining = str(text)[end_pos:end_pos + 5].strip()
                has_plus = "+" in str(text)[match.start():end_pos + 2]
                return f"{num}+" if has_plus else num
        return "Unknown"

    def _normalize(val: str, desc: str) -> str:
        val = str(val).strip()

        # Already a valid value
        if val and val.lower() not in ("nan", "none", "", "unknown", "n/a"):
            # Match patterns like "5+", "5+ years", "3 yrs", "10+ years"
            match = re.search(r"(\d+)(\+)?", val)
            if match:
                num = match.group(1)
                plus = match.group(2) or ""
                return f"{num}{plus}"

        # Field is empty/Unknown — try to extract from description
        return _extract_from_desc(desc)

    return pd.Series(
        [_normalize(v, d) for v, d in zip(series, desc_series)],
        index=series.index,
    )


def normalize_employment_type(
    series: pd.Series,
    desc_series: pd.Series | None = None,
) -> pd.Series:
    """
    Normalize employment type to allowed values:
    Full Time | Part Time | Contract | Temporary | Internship | Unknown

    Handles:
    - LinkedIn API enum values: FULL_TIME, PART_TIME, CONTRACT, etc.
    - Human-readable strings: "Full-time", "part time", etc.
    - Extracts from Job Description when field is empty/Unknown.
    """
    if desc_series is None:
        desc_series = pd.Series([""] * len(series), index=series.index)

    def _extract_from_desc(text: str) -> str:
        """Scan description text for employment type clues."""
        if not text or str(text).strip() in ("", "nan", "None"):
            return "Unknown"
        for pattern, result in _EMPLOYMENT_TYPE_DESC_PATTERNS:
            if pattern.search(str(text)):
                return result
        return "Unknown"

    def _normalize(val: str, desc: str) -> str:
        val = str(val).strip()
        lower = val.lower()

        # Empty or unknown — try description
        if lower in ("", "nan", "none", "unknown"):
            return _extract_from_desc(desc)

        # Direct exact match
        canonical = _EMPLOYMENT_TYPE_MAP.get(lower)
        if canonical:
            return canonical

        # Partial match
        for key, mapped in _EMPLOYMENT_TYPE_MAP.items():
            if key in lower:
                return mapped

        # Already one of the allowed values (case-insensitive)
        title_val = val.title()
        if title_val in _ALLOWED_EMPLOYMENT_TYPES:
            return title_val

        # Last resort — scan description
        result = _extract_from_desc(desc)
        if result != "Unknown":
            return result

        return "Unknown"

    return pd.Series(
        [_normalize(v, d) for v, d in zip(series, desc_series)],
        index=series.index,
    )


# ------------------------------------------------------------------
# URL validation
# ------------------------------------------------------------------
def _is_valid_url(value) -> bool:
    """Return True if value looks like a valid http/https URL."""
    if not isinstance(value, str) or not value.strip():
        return False
    pattern = re.compile(
        r"^https?://"
        r"[^\s/$.?#].[^\s]*$",
        re.IGNORECASE,
    )
    return bool(pattern.match(value.strip()))


# ==================================================================
# Main pipeline functions
# ==================================================================

def normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Rename incoming columns to match the target schema using FIELD_MAP.
    Columns not in the map are dropped (except those already in schema).

    Priority: if multiple raw columns map to the same target, use the first
    non-empty value found.
    """
    # Build reverse map: target → list of source cols present in df
    target_to_sources: dict[str, list[str]] = {}
    for raw_col, target_col in FIELD_MAP.items():
        if raw_col in df.columns:
            target_to_sources.setdefault(target_col, []).append(raw_col)

    # For each target that has multiple source candidates, coalesce them
    for target_col, source_cols in target_to_sources.items():
        if len(source_cols) == 1:
            df = df.rename(columns={source_cols[0]: target_col})
        else:
            # Coalesce: use first non-empty value across candidate columns
            coalesced = df[source_cols[0]].astype(str).str.strip()
            for src in source_cols[1:]:
                alt = df[src].astype(str).str.strip()
                coalesced = coalesced.where(
                    coalesced.isin(["", "nan", "None", "NaN"]) == False,
                    alt,
                )
            df[target_col] = coalesced
            # Drop the original source columns
            drop_cols = [c for c in source_cols if c != target_col]
            df = df.drop(columns=drop_cols, errors="ignore")

    # Add any missing required columns as empty strings
    for col in REQUIRED_COLUMNS:
        if col not in df.columns:
            df[col] = ""

    # Keep only required columns in canonical order
    df = df[REQUIRED_COLUMNS]
    return df


def clean_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Clean and validate the normalized dataframe.

    Steps applied in order:
    1.  Extract URLs from nested structures.
    2.  Strip whitespace from all string columns.
    3.  Remove rows with missing Job Title.
    4.  Remove rows with missing Company.
    5.  Remove rows with invalid Job url (keep empty URLs).
    6.  Normalise first_seen_date → YYYY-MM-DD.
    7.  Normalize ERP values.
    8.  Normalize Intensity → High / Medium / Low.
    9.  Normalize FilterState → state/province abbreviations.
    10. Normalize source names.
    11. Normalize Experience values (with description fallback).
    12. Normalize Employment type values (with description fallback).
    13. Fill remaining NaN with empty string.
    """
    if df.empty:
        return df

    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    # 1. Extract URLs BEFORE stripping (nested objects need to be parsed first)
    if "Job url" in df.columns:
        df["Job url"] = normalize_job_url(df["Job url"])

    # 2. Strip whitespace from all string columns
    str_cols = df.select_dtypes(include="object").columns
    for col in str_cols:
        df[col] = df[col].astype(str).str.strip()
        df[col] = df[col].replace({"nan": "", "None": "", "NaN": ""})

    # 3. Remove rows with missing Job Title
    before = len(df)
    df = df[df["Job Title"].str.len() > 0]
    logger.info("Removed %d rows with missing Job Title", before - len(df))

    # 4. Remove rows with missing Company
    before = len(df)
    df = df[df["Company"].str.len() > 0]
    logger.info("Removed %d rows with missing Company", before - len(df))

    # 5. Filter invalid URLs (keep rows with valid URL or empty URL)
    before = len(df)
    url_mask = df["Job url"].apply(_is_valid_url) | (df["Job url"] == "")
    df = df[url_mask]
    logger.info("Removed %d rows with invalid Job url", before - len(df))

    # 6. Normalise first_seen_date → YYYY-MM-DD
    def _parse_date(val: str) -> str:
        if not val or val == "":
            return today
        try:
            return pd.to_datetime(val).strftime("%Y-%m-%d")
        except Exception:
            return today

    df["first_seen_date"] = df["first_seen_date"].apply(_parse_date)

    # Common helper series
    desc_col = df["Job Description"] if "Job Description" in df.columns else pd.Series(
        [""] * len(df), index=df.index
    )
    title_col = df["Job Title"] if "Job Title" in df.columns else pd.Series(
        [""] * len(df), index=df.index
    )

    # 7. Normalize ERP
    df["ERP"] = normalize_erp(df["ERP"], desc_col)

    # 8. Normalize Intensity
    df["Intensity"] = normalize_intensity(df["Intensity"], desc_col, title_col)

    # 9. Normalize FilterState
    df["FilterState"] = normalize_filter_state(df["FilterState"])

    # 10. Normalize source
    df["source"] = normalize_source(df["source"])

    # 11. Normalize Experience — falls back to Job Description scan
    if "Experience" in df.columns:
        df["Experience"] = normalize_experience(df["Experience"], desc_col)

    # 12. Normalize Employment type — falls back to Job Description scan
    if "Employment type" in df.columns:
        df["Employment type"] = normalize_employment_type(
            df["Employment type"], desc_col
        )

    # 13. Fill remaining NaN with empty string
    df = df.fillna("")
    df = df.reset_index(drop=True)
    logger.info("Clean dataset size after cleaning: %d rows", len(df))
    return df

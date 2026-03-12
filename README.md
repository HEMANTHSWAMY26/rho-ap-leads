# 💼 Job Leads Dashboard

A production-ready Streamlit dashboard that reads job lead data from Apify task runs, cleans and deduplicates the data, stores it in Google Sheets, and allows the client to filter and download leads.

---

## 🏗️ Project Structure

```
rho-ap-leads/
│
├── app.py                    # Main Streamlit dashboard (single entry point)
├── config.py                 # Environment variable loader
├── data_loader.py            # Fetches data from Apify API (read-only)
├── data_cleaner.py           # Normalises and cleans raw data
├── deduplicator.py           # Removes duplicate leads
├── google_sheets_writer.py   # Reads/writes to Google Sheets
│
├── requirements.txt          # Python dependencies
├── .env                      # Local environment variables (not committed)
├── service_account.json      # Google service account key (not committed)
│
└── .streamlit/
    ├── config.toml           # Streamlit server/theme settings
    └── secrets.toml.example  # Template for Streamlit Cloud secrets
```

---

## ⚙️ Environment Variables

| Variable                      | Description                                                  |
| ----------------------------- | ------------------------------------------------------------ |
| `APIFY_API_TOKEN`             | Apify API authentication token                               |
| `TASK_US`                     | Apify task ID for US job scraper                             |
| `TASK_CANADA`                 | Apify task ID for Canada job scraper                         |
| `GOOGLE_SHEET_ID`             | Google Sheets document ID                                    |
| `GOOGLE_SERVICE_ACCOUNT_JSON` | Path to service account JSON file, or the JSON string itself |

---

## 🚀 Local Development

### 1. Install dependencies

```bash
pip install -r requirements.txt
```

### 2. Configure environment

Ensure `.env` exists in the project root with all required variables (see above).

Ensure `service_account.json` is present.

### 3. Run the dashboard

```bash
streamlit run app.py
```

---

## ☁️ Streamlit Cloud Deployment

1. Push the repository to GitHub (exclude `.env` and `service_account.json` via `.gitignore`).
2. Connect the repository in [Streamlit Cloud](https://streamlit.io/cloud).
3. Set **Main file path** to `app.py`.
4. Add all environment variables under **App Settings → Secrets** using the format shown in `.streamlit/secrets.toml.example`.
5. For `GOOGLE_SERVICE_ACCOUNT_JSON`, paste the entire contents of `service_account.json` as a multi-line string.

---

## 📊 Data Pipeline

```
Apify Tasks (daily, 10:30 PM)
        ↓
Apify Dataset API (read-only)
        ↓
data_loader.py  — fetch JSON (up to 10,000 rows per task)
        ↓
data_cleaner.py — normalise + clean
        ↓
deduplicator.py — remove duplicates
        ↓
google_sheets_writer.py — append new leads
        ↓
Streamlit Dashboard — display + filter + download
```

---

## 📋 Target Data Schema

| Column            | Description                                      |
| ----------------- | ------------------------------------------------ |
| `Job Title`       | Position title                                   |
| `Company`         | Employer name                                    |
| `Location`        | City / region                                    |
| `Job Description` | Full description text                            |
| `Job url`         | Link to original job posting                     |
| `source`          | Platform source (e.g. LinkedIn, Indeed)          |
| `first_seen_date` | Date lead was first collected (YYYY-MM-DD)       |
| `run_id`          | Apify task ID                                    |
| `ERP`             | ERP system detected (e.g. SAP, Oracle, NetSuite) |
| `Intensity`       | Lead urgency: High / Medium / Low                |
| `FilterState`     | US state or Canadian province abbreviation       |
| `Experience`      | Years of experience required (e.g. 3, 5+)        |
| `Employment type` | Full Time / Part Time / Contract / Temporary     |

---

## 🔒 Security

- Dashboard is **read-only** — no scraping is triggered from the UI.
- No user authentication required (internal client dashboard).
- All secrets stored in environment variables or Streamlit Cloud secrets.
- `.env` and `service_account.json` are excluded from version control via `.gitignore`.

---

## 📈 Performance

- Supports **10,000+ leads per month** (per Apify task).
- Data cached for **1 hour** using `st.cache_data`.
- Batch append to Google Sheets minimises API calls.
- All date filtering happens in-memory — no repeated API calls on user interaction.

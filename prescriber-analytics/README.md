# 💊 Prescriber Analytics and Customer Segmentation

Commercial analytics on CMS Medicare Part D 2023 data, segmenting 20,935 Medicare prescribers into behavioral clusters using K-means, performing Pareto and cohort analysis, and surfacing high-value target insights via an interactive Streamlit dashboard.

---

## Key Findings

| Insight | Value |
|---|---|
| Total prescribers profiled | 20,935 |
| Optimal segments (K-means, silhouette 0.448) | 5 |
| Top 20% prescribers drive | 88.7% of total spend |
| Drugs driving 80% of spend | 126 of 1,306 (9.6%) |
| High Value Writers avg spend | $2.96M per prescriber |
| Hematology-Oncology | 143 prescribers, $318M spend |
| California leads state spend | $391M (9.6% of total) |

---

## Prescriber Segments

| Segment | Count | Avg Spend |
|---|---|---|
| High Value Writers | 254 | $2,961,013 |
| Specialty High Cost | 3 | $1,633,944 |
| Brand Preferrers | 5,958 | $519,355 |
| Low Engagement | 14,720 | $16,216 |

---

## Business Questions Answered

1. What distinct prescriber segments exist in Medicare data?
2. Which segments represent the highest value and engagement priority?
3. Which specialties drive the highest spend per prescriber?
4. Which states are under-penetrated vs high volume?
5. Which drugs have the highest prescriber concentration risk?
6. How many prescribers drive 80 percent of total Medicare drug spend?

---

## Project Structure

```
prescriber-analytics/
├── 01_prescriber_segmentation.py   # K-means segmentation + cohort analysis
├── 02_dashboard.py                  # Streamlit interactive dashboard
├── data/
│   └── processed/
│       ├── prescribers_segmented.csv
│       ├── specialty_cohort.csv
│       ├── state_cohort.csv
│       ├── drug_concentration.csv
│       └── analysis_summary.json
└── README.md
```

---

## Methodology

**Step 1: Prescriber Feature Engineering**

Aggregated 500k prescriber-drug records into 20,935 prescriber-level profiles with features including total spend, total claims, average cost per claim, unique drug count, brand preference ratio, and spend per drug.

**Step 2: K-means Segmentation**

Tested k=3 to k=7 using silhouette scoring on a 10,000 record sample. Selected k=5 (silhouette score 0.448). Applied StandardScaler normalization and PCA (2 components) for visualization.

**Step 3: Cohort Analysis**

Analyzed prescriber behavior across three dimensions: specialty cohorts (spend per prescriber, drug mix), state cohorts (volume vs penetration), and drug concentration (Pareto analysis identifying which drugs drive 80 percent of spend).

**Step 4: Dashboard**

Built interactive Streamlit dashboard with 5 tabs: customer segments with PCA scatter and segment profiles, specialty cohort analysis, geographic choropleth maps, drug concentration Pareto chart, and searchable prescriber detail table.

---

## Tech Stack

| Layer | Technology |
|---|---|
| Data Processing | Python, pandas, numpy |
| Machine Learning | scikit-learn (K-means, PCA, silhouette scoring) |
| Visualization | Plotly, Streamlit |
| Data Source | CMS Medicare Part D 2023 (500k records) |

---

## Run Locally

```bash
git clone https://github.com/mhh388/prescriber-analytics.git
cd prescriber-analytics
python3 -m venv venv
source venv/bin/activate
pip install pandas numpy scikit-learn plotly streamlit loguru

# Add your CMS data file
cp /path/to/cms_medicare_clean.csv .

# Run segmentation
python 01_prescriber_segmentation.py

# Launch dashboard
python -m streamlit run 02_dashboard.py
```

---

## Companion Projects

| Project | Description |
|---|---|
| [healthcare-etl-pipeline](https://github.com/mhh388/healthcare-etl-pipeline) | AWS PySpark ETL, 26.7M row CMS Medicare dataset |
| [healthcare-azure-pipeline](https://github.com/mhh388/healthcare-azure-pipeline) | Azure Databricks + ADF pipeline |
| [healthcare-ml-api](https://github.com/mhh388/healthcare-ml-api) | Drug cost prediction ML API on Kubernetes |
| [proteomics-hcp-pipeline](https://github.com/mhh388/proteomics-hcp-pipeline) | DDA + DIA proteomics pipeline |
| [agentic-biopharma-assistant](https://github.com/mhh388/agentic-biopharma-assistant) | Agentic AI suite with RAG and AWS monitoring |
| [healthcare-bi-dashboard](https://github.com/mhh388/healthcare-bi-dashboard) | Tableau + Power BI analytics dashboards |

---

Built as part of a healthcare data engineering and analytics portfolio.

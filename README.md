# GastroSense 🍽️

> 台灣壽星優惠餐廳資料蒐集與智慧分類系統

A data pipeline that crawls birthday-deal restaurant articles across Taiwan cities, enriches them with location data, stores them in BigQuery, and classifies them using Gemini AI.

---

## 📌 Project Overview

This project was built as a data engineering final project. It constructs an end-to-end pipeline that:

1. **Crawls** Pixnet blog articles about birthday restaurant deals across 19 Taiwan cities via Google Custom Search API
2. **Scores** each article using a composite SEO + popularity (hot rank) scoring algorithm
3. **Enriches** results with real-world location data (coordinates, ratings, address) via Google Places API
4. **Loads** raw data into Google BigQuery via an Apache Beam / Dataflow pipeline
5. **Classifies** restaurant data (cuisine type, dining style, promo type) and rewrites descriptions using **Gemini 2.0 Flash** on Vertex AI

---

## 🗂️ Project Structure

```
CODE/
├── pixnet_code/
│   ├── search_pixnet_final.py       # Step 1: Crawl & score Pixnet articles by city
│   ├── fast_pixnet_enrich_step2.py  # Step 2: Enrich with Google Places API → BigQuery
│   └── dataflow_pixnet_to_bq.py     # Step 3: Apache Beam pipeline for bulk BQ load
│
└── classfy/
    ├── fast_classify_with_gemini.py # Step 4: Classify restaurants via Gemini 2.0 Flash
    ├── food_type_groups.json        # Cuisine category config
    └── offer_categories.json        # Promo type config
```

---

## 🛠️ Tech Stack

| Layer | Technology |
|---|---|
| Data Collection | Google Custom Search API, BeautifulSoup |
| Location Enrichment | Google Places API (New) |
| Data Warehouse | Google BigQuery |
| Batch Pipeline | Apache Beam / Google Cloud Dataflow |
| AI Classification | Gemini 2.0 Flash (Vertex AI) |
| Language | Python 3 |

---

## ⚙️ Pipeline Flow

```
Pixnet Blog Articles
        │
        ▼
search_pixnet_final.py
  ├─ Google Custom Search API (19 cities × keyword patterns)
  ├─ SEO Score (URL length, content length, domain authority)
  ├─ Hot Score (rank-based popularity weight)
  └─ Output: NDJSON file
        │
        ▼
dataflow_pixnet_to_bq.py
  └─ Apache Beam → BigQuery (pixnet_deals_raw)
        │
        ▼
fast_pixnet_enrich_step2.py
  ├─ Google Places API (address, lat/lng, rating, review count)
  ├─ City-match validation (prevents incorrect location mapping)
  └─ BigQuery (pixnet_deals_step2), batch write with ThreadPoolExecutor
        │
        ▼
fast_classify_with_gemini.py
  ├─ Gemini 2.0 Flash structured output (JSON schema enforced)
  ├─ cuisine_category (15 options, single-select)
  ├─ restaurant_category (25 options, multi-select)
  ├─ promo_type (7 categories)
  ├─ description rewrite (50–100 words)
  └─ BigQuery (final_table), parallel with ThreadPoolExecutor (4 workers)
```

---

## 🔑 Environment Variables

This project requires the following environment variable:

```bash
GOOGLE_API_KEY=your_google_api_key_here
```

Google Cloud credentials (for BigQuery / Vertex AI) are handled via Application Default Credentials (ADC).

---

## 📊 Coverage

- **19 cities** covered (6 major municipalities · 11 counties · 3 outlying islands)
- Articles filtered to **2025 content only** (title must contain "2025")
- Scoring: `final_score = hot_score × 0.6 + seo_score × 0.4`

# GastroSense 🍽️

> 台灣壽星優惠餐廳資料蒐集與智慧分類系統

---

## 📌 專案簡介

本專案為資料模式課程期末專案，建構一條端對端的資料管道：

1. **爬取** Pixnet 部落格文章：透過 Google Custom Search API 搜尋台灣各縣市的壽星優惠餐廳文章
2. **評分** 每篇文章：以 SEO 分數與熱門度（Hot Rank）組合成複合評分
3. **地點豐富化**：透過 Google Places API 補充真實地點資料（座標、評分、地址）
4. **寫入 BigQuery**：使用 Apache Beam / Dataflow Pipeline 批次載入原始資料
5. **智慧分類**：使用 **Gemini 2.0 Flash**（Vertex AI）對餐廳進行菜系、用餐形式、優惠類型分類，並自動改寫描述

---

## 🗂️ 專案結構

```
CODE/
├── pixnet_code/
│   ├── search_pixnet_final.py       # Step 1：爬取並評分 Pixnet 文章
│   ├── fast_pixnet_enrich_step2.py  # Step 2：Google Places API 豐富化 → BigQuery
│   └── dataflow_pixnet_to_bq.py     # Step 3：Apache Beam 批次寫入 BigQuery
│
└── classfy/
    ├── fast_classify_with_gemini.py # Step 4：Gemini 2.0 Flash 分類與描述改寫
    ├── food_type_groups.json        # 菜系分類設定
    └── offer_categories.json        # 優惠類型設定
```

---

## 🛠️ 技術棧

| 層級 | 技術 |
|---|---|
| 資料蒐集 | Google Custom Search API、BeautifulSoup |
| 地點豐富化 | Google Places API (New) |
| 資料倉儲 | Google BigQuery |
| 批次管道 | Apache Beam / Google Cloud Dataflow |
| AI 分類 | Gemini 2.0 Flash（Vertex AI） |
| 程式語言 | Python 3 |

---

## ⚙️ Pipeline 流程

```
Pixnet 部落格文章
        │
        ▼
search_pixnet_final.py
  ├─ Google Custom Search API（19 縣市 × 多組關鍵字）
  ├─ SEO 評分（URL 長度、內文長度、網域權重）
  ├─ 熱門度評分（基於排名的權重計算）
  └─ 輸出：NDJSON 檔案
        │
        ▼
dataflow_pixnet_to_bq.py
  └─ Apache Beam → BigQuery（pixnet_deals_raw）
        │
        ▼
fast_pixnet_enrich_step2.py
  ├─ Google Places API（地址、經緯度、評分、評論數）
  ├─ 城市比對驗證（防止地點資料錯誤匹配）
  └─ BigQuery（pixnet_deals_step2），ThreadPoolExecutor 批次寫入
        │
        ▼
fast_classify_with_gemini.py
  ├─ Gemini 2.0 Flash 結構化輸出（強制 JSON Schema）
  ├─ cuisine_category（15 個菜系，單選）
  ├─ restaurant_category（25 種用餐形式，多選）
  ├─ promo_type（7 種優惠類型）
  ├─ description 改寫（50–100 字）
  └─ BigQuery（final_table），ThreadPoolExecutor 4 執行緒平行處理
```

---

## 🔑 環境變數

```bash
GOOGLE_API_KEY=你的_google_api_key
```

BigQuery / Vertex AI 的 GCP 憑證透過 Application Default Credentials（ADC）處理。

---

## 📊 資料涵蓋範圍

- 涵蓋 **19 個縣市**（6 直轄市・11 縣市・3 離島）
- 僅保留標題含「**2025**」的最新文章
- 複合評分公式：`final_score = hot_score × 0.6 + seo_score × 0.4`

import os
import time
import requests
import json
import re
from datetime import date, datetime
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor
from google.cloud import bigquery
from google.api_core.exceptions import NotFound
from tqdm import tqdm

# --- 設定區 ---
API_KEY = os.environ.get("GOOGLE_API_KEY")
PROJECT = "hbdproject-479215"
DATASET = "pixnet"
INPUT_TABLE = "pixnet_deals_step1"
OUTPUT_TABLE = "pixnet_deals_step2"

client = bigquery.Client(project=PROJECT)
session = requests.Session()

# ======================================================
# 輔助工具：處理 台/臺 與 地址比對
# ======================================================

def normalize(text):
    return str(text).replace("臺", "台").strip() if text else ""

def is_city_match(address, target_city):
    """檢查地址是否包含目標城市（解決張冠李戴問題）"""
    if not address or not target_city: return False
    return normalize(target_city) in normalize(address)

def json_fix(obj):
    """處理日期格式，避免寫入 BigQuery 時報錯"""
    if isinstance(obj, dict):
        return {k: json_fix(v) for k, v in obj.items()}
    elif isinstance(obj, (date, datetime)):
        return obj.isoformat()
    return obj

# ======================================================
# 核心邏輯：API 呼叫與校驗
# ======================================================

def call_places_api(brand, city, title):
    # 頻率控制：併發 5 下，延遲確保不超過 600 req/min
    time.sleep(0.15)
    
    # 如果品牌是 null，改用標題的前幾個字搜尋
    search_brand = brand if brand and brand.lower() != 'null' else title[:10]
    query = f"{search_brand} {city}".strip()
    
    url = "https://places.googleapis.com/v1/places:searchText"
    headers = {
        "Content-Type": "application/json",
        "X-Goog-Api-Key": API_KEY,
        "X-Goog-FieldMask": "places.id,places.formattedAddress,places.location,places.rating,places.userRatingCount"
    }
    
    try:
        resp = session.post(url, json={"textQuery": query, "pageSize": 1, "languageCode": "zh-TW"}, headers=headers, timeout=10)
        resp.raise_for_status()
        places = resp.json().get("places", [])
        
        # 預設空的結果
        res = {k: None for k in ["place_id", "rating", "user_ratings_total", "formatted_address", "lat", "lng"]}
        
        if places:
            p = places[0]
            addr = p.get("formattedAddress", "")
            # 【關鍵校驗】：如果地址不包含該城市，則視為沒找到（避免南港店地址污染全台）
            if is_city_match(addr, city):
                loc = p.get("location", {})
                res.update({
                    "place_id": p.get("id"),
                    "rating": p.get("rating"),
                    "user_ratings_total": p.get("userRatingCount"),
                    "formatted_address": addr,
                    "lat": loc.get("latitude"),
                    "lng": loc.get("longitude")
                })
        return res
    except:
        return {k: None for k in ["place_id", "rating", "user_ratings_total", "formatted_address", "lat", "lng"]}

def process_row(row):
    row_dict = dict(row)
    enrich = call_places_api(row_dict.get('brand'), row_dict.get('city_name'), row_dict.get('title', ''))
    row_dict.update(enrich)
    row_dict["cleaned_at"] = datetime.now(timezone.utc).isoformat()
    return json_fix(row_dict)

# ======================================================
# 主程式
# ======================================================

def main():
    table_id = f"{PROJECT}.{DATASET}.{OUTPUT_TABLE}"
    
    # 1. 取得增量 ID
    try:
        query_max = f"SELECT MAX(id) FROM `{table_id}`"
        max_id = next(client.query(query_max).result())[0] or 0
    except:
        max_id = 0
    
    print(f"🚀 起點 ID > {max_id}")

    # 2. 讀取 Step 1 資料
    query_in = f"SELECT * FROM `{PROJECT}.{DATASET}.{INPUT_TABLE}` WHERE id > {max_id} ORDER BY id ASC"
    rows = [row for row in client.query(query_in).result()]
    
    if not rows:
        print("✅ 無新資料需處理。")
        return

    print(f"📦 準備處理 {len(rows)} 筆資料...")

    # 3. 分批處理 (每 50 筆處理並寫入一次)
    batch_size = 50
    for i in range(0, len(rows), batch_size):
        batch = rows[i : i + batch_size]
        print(f"\n⏳ 正在處理批次 {i//batch_size + 1}...")
        
        with ThreadPoolExecutor(max_workers=5) as executor:
            batch_results = list(tqdm(executor.map(process_row, batch), total=len(batch)))
        
        # 寫入 BigQuery
        errors = client.insert_rows_json(table_id, batch_results)
        if not errors:
            print(f"💾 批次 {i//batch_size + 1} 成功存入。")
        else:
            print(f"❌ 寫入出錯：{errors[:1]}")

    print("\n🏁 全部任務完成。")

if __name__ == "__main__":
    main()
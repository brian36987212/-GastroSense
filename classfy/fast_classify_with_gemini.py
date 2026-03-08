import vertexai
from vertexai.generative_models import GenerativeModel, GenerationConfig, SafetySetting
from google.cloud import bigquery
import pandas as pd
import json
import time
import random
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading

# --- 設定區 ---
PROJECT_ID = "hbdproject-479215"
LOCATION = "us-central1"
SOURCE_TABLE = "hbdproject-479215.final_table.initial_restaurant_list_for_test"
DEST_TABLE = "hbdproject-479215.final_table.initial_restaurant_list_for_test_labeled"
MAX_WORKERS = 4  # 🔥 同時處理 4 筆，速度約快 4 倍

vertexai.init(project=PROJECT_ID, location=LOCATION)
bq_client = bigquery.Client(project=PROJECT_ID)

# --- 模型設定 ---
SYSTEM_INSTRUCTION = """
你是一個專業的餐飲資料分析師。請根據輸入的餐廳資訊，進行分類並潤飾描述。

請嚴格遵守以下分類規則，絕對不要輸出選項以外的內容：

【分類規則 1：菜系/風格 (cuisine_category)】
- 規則：單選。
- 限制：必須完全屬於以下選項之一，若無法歸類請選 "不限制"。
- 選項：["不限制", "中式", "台式", "港式", "日式", "韓式", "泰式", "越式", "東南亞", "義式", "法式", "美式", "歐式", "墨西哥", "素食"]

【分類規則 2：餐點形式 (restaurant_category)】
- 限制：必須從清單中選擇 (可多選)。
- 選項定義補充：
  1. "合菜"：適用於泰式料理、中式熱炒、烤鴨三吃、川菜等，以「單點多道菜共享」為主的餐廳。
  2. "特色餐廳"：適用於專賣特定料理且無法歸類者，如：酸菜魚專賣、牛肉麵、鰻魚飯、片皮鴨。
  3. "飯店餐廳"：適用於位於大飯店內的餐廳或 Buffet。
  4. "簡餐"：適用於一人一份的定食、丼飯、韓式套餐 (如偷飯賊)。

【分類規則 3：優惠分類 (promo_type)】
- 規則：請根據描述判斷屬於哪一類，回傳對應的中文名稱。
- 參考範例 (請依此邏輯判斷)：
  1. 免費贈送：送肉盤、送小菜、幾歲送幾隻蝦、壽星送蛋糕、升級湯底。
  2. 同行優惠：四人同行一人免費、第二位半價、兩人同行送好禮。
  3. 帳單折扣：滿千折百、整單 85 折、壽星五折、現金折抵、壽星本人免費。
  4. 服務優惠：免服務費、優先訂位、專屬包廂、免費停車。
  5. 活動優惠：身分證對對碰、轉盤遊戲、抽獎活動、變裝派對、打卡送禮。
  6. 贈送點數：點數兩倍送、加贈會員點數、集點卡活動、註冊禮。
  7. 買幾送幾：買一送一、買二送一 (強調數量的直接贈送)。

【任務 4：描述潤飾 (description)】
- 請將原始描述改寫，使其通順、吸引人且簡潔 (約 50-100 字)。
- 去除亂碼與無關的 HTML 標籤。
"""

model = GenerativeModel(
    "gemini-2.0-flash",
    system_instruction=SYSTEM_INSTRUCTION
)

generation_config = GenerationConfig(
    temperature=0.1, # 溫度調低，降低隨機性
    response_mime_type="application/json",
    response_schema={
        "type": "OBJECT",
        "properties": {
            # 1. 強制限制 cuisine_category 只能選這 15 個
            "cuisine_category": {
                "type": "STRING",
                "enum": [
                    "不限制", "中式", "台式", "港式", "日式",
                    "韓式", "泰式", "越式", "東南亞", "義式",
                    "法式", "美式", "歐式", "墨西哥", "素食"
                ]
            },
            # 2. 強制限制 restaurant_category 只能包含這 21 個
            # 這樣它就絕對不可能輸出 "中式" 或 "甜點" 在這裡面
            "restaurant_category": {
                "type": "ARRAY",
                "items": {
                    "type": "STRING",
                    "enum": [
                        "火鍋", "牛排", "烤肉", "鐵板燒", "吃到飽", 
                        "自助餐", "速食", "簡餐", "早午餐", "輕食", 
                        "壽司", "生魚片", "串燒", "下午茶", "甜點", 
                        "烘焙", "咖啡館", "飲品", "酒吧", "餐酒館", 
                        "景觀餐廳",

                        # --- 🔥 必加的新選項 (針對您提供的資料) ---
                        "合菜",       # 救星：解決泰式、中式、桌菜
                        "特色餐廳",   # 救星：解決酸菜魚、烤鴨
                        "飯店餐廳",   # 救星：解決大飯店
                        "咖哩"        # 救星：解決咖哩專賣
                    ]
                }
            },
            # 3. 優惠分類
            "promo_type": {
                "type": "STRING",
                "enum": [
                    "免費贈送", "同行優惠", "帳單折扣", "服務優惠", 
                    "活動優惠", "贈送點數", "買幾送幾"
                ]
            },
            "description": {"type": "STRING"}
        },
        "required": ["cuisine_category", "restaurant_category", "promo_type", "description"]
    }
)

safety_settings = [
    SafetySetting(category=cat, threshold=SafetySetting.HarmBlockThreshold.BLOCK_NONE)
    for cat in [
        SafetySetting.HarmCategory.HARM_CATEGORY_HATE_SPEECH,
        SafetySetting.HarmCategory.HARM_CATEGORY_DANGEROUS_CONTENT,
        SafetySetting.HarmCategory.HARM_CATEGORY_SEXUALLY_EXPLICIT,
        SafetySetting.HarmCategory.HARM_CATEGORY_HARASSMENT
    ]
]

# --- 核心處理函式 ---
def process_row(row):
    prompt = f"""
    【待處理資料】
    - Brand: {row.get('brand')}
    - Title: {row.get('title')}
    - Original Description: {row.get('description')}
    - Original Promo Type Hint: {row.get('promo_type')}
    """
    attempt = 0
    base_delay = 2
    max_delay = 60

    while True:
        attempt += 1
        try:
            # 不主動 sleep，全速執行
            response = model.generate_content(
                prompt,
                generation_config=generation_config,
                safety_settings=safety_settings
            )
            result = json.loads(response.text)
            
            return {
                "brand_id": row['brand_id'],
                "cuisine_category": result.get('cuisine_category'),
                "restaurant_category": result.get('restaurant_category'),
                "promo_type": result.get('promo_type'),
                "description": result.get('description'),
            }

        except Exception as e:
            error_msg = str(e)
            # 針對限流或網路問題進行重試
            if any(code in error_msg for code in ["429", "503", "500", "504", "Resource exhausted", "Service Unavailable"]):
                wait_time = min(max_delay, base_delay * (2 ** (attempt - 1))) + random.uniform(0, 1)
                if attempt > 1:
                    print(f"⚠️ Row {row.get('brand_id')} 重試中 ({attempt}次), 等待 {wait_time:.1f}s...")
                time.sleep(wait_time)
            else:
                print(f"❌ Row {row.get('brand_id')} 發生無法處理的錯誤: {e}")
                return None

# --- 主程式 ---

#1. 先清空舊資料 (重跑的關鍵)
#print(f"🗑️ 正在清空目標表格 `{DEST_TABLE}` ...")
#try:
   #bq_client.query(f"TRUNCATE TABLE `{DEST_TABLE}`").result()
   #print("✨ 表格已清空，準備重新寫入。")
#except Exception as e:
   #print(f"⚠️ 清空表格失敗 (可能是表格不存在): {e}，將嘗試直接建立。")

# 2. 讀取全部資料
print("1. 正在讀取 BigQuery 全部原始資料...")
query = f"SELECT * FROM `{SOURCE_TABLE}`"
df = bq_client.query(query).to_dataframe()

print(f"📋 共 {len(df)} 筆資料，啟動 {MAX_WORKERS} 個執行緒極速處理中...🚀")

results_buffer = []
BATCH_SIZE = 50 # 每 50 筆存一次
lock = threading.Lock() 

# 因為前面已經 TRUNCATE 過了，這裡統一用 APPEND
job_config = bigquery.LoadJobConfig(
    schema=[
        bigquery.SchemaField("brand_id", "INTEGER"),
        bigquery.SchemaField("cuisine_category", "STRING"),
        bigquery.SchemaField("restaurant_category", "STRING", mode="REPEATED"),
        bigquery.SchemaField("promo_type", "STRING"),
        bigquery.SchemaField("description", "STRING"),
    ],
    write_disposition="WRITE_APPEND", 
)

def save_batch(data_to_save):
    if not data_to_save: return
    try:
        print(f"💾 正在寫入 {len(data_to_save)} 筆資料...")
        temp_df = pd.DataFrame(data_to_save)
        job = bq_client.load_table_from_dataframe(
            temp_df, DEST_TABLE, job_config=job_config
        )
        job.result()
        print(f"🎉 批次寫入成功！")
    except Exception as e:
        print(f"❌ 寫入失敗: {e}")

# 平行處理
with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
    future_to_row = {executor.submit(process_row, row): row for index, row in df.iterrows()}
    
    completed_count = 0
    
    for future in as_completed(future_to_row):
        data = future.result()
        if data:
            with lock:
                results_buffer.append(data)
                completed_count += 1
                
                # 簡單的進度條
                if completed_count % 10 == 0:
                    print(f"⚡ 進度: {completed_count}/{len(df)} ({(completed_count/len(df))*100:.1f}%)")

                if len(results_buffer) >= BATCH_SIZE:
                    save_batch(results_buffer)
                    results_buffer = [] 

# 存入剩下的
if results_buffer:
    save_batch(results_buffer)

print("🏆 563 筆資料急速處完成！")
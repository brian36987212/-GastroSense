# zh-TW
import os
import requests
import json
import time
from datetime import datetime, timezone
from bs4 import BeautifulSoup

API_KEY = os.environ.get("GOOGLE_API_KEY")
CX = "a2b983a3599664a27"

MAX_HOT_SCORE = 40  # 熱門度最大分數

# ---------------------------------------------------------
# 城市設定（直轄市 40、本島縣市 12、離島 4）
# ---------------------------------------------------------
CITY_CONFIG = {
    # ⭐ 直轄市 → 40 篇
    "台北": {"city_key": "taipei", "requests": 40},
    "新北": {"city_key": "newtaipei", "requests": 40},
    "桃園": {"city_key": "taoyuan", "requests": 40},
    "台中": {"city_key": "taichung", "requests": 40},
    "台南": {"city_key": "tainan", "requests": 40},
    "高雄": {"city_key": "kaohsiung", "requests": 40},

    # ⭐ 本島縣市 → 12 篇
    "基隆": {"city_key": "keelung", "requests": 12},
    "新竹": {"city_key": "hsinchu", "requests": 12},
    "苗栗": {"city_key": "miaoli", "requests": 12},
    "彰化": {"city_key": "changhua", "requests": 12},
    "南投": {"city_key": "nantou", "requests": 12},
    "雲林": {"city_key": "yunlin", "requests": 12},
    "嘉義": {"city_key": "chiayi", "requests": 12},
    "屏東": {"city_key": "pingtung", "requests": 12},
    "宜蘭": {"city_key": "yilan", "requests": 12},
    "花蓮": {"city_key": "hualien", "requests": 12},
    "台東": {"city_key": "taitung", "requests": 12},

    # ⭐ 離島 → 4 篇
    "澎湖": {"city_key": "penghu", "requests": 4},
    "金門": {"city_key": "kinmen", "requests": 4},
    "連江": {"city_key": "lienchiang", "requests": 4},
}

CITIES = list(CITY_CONFIG.keys())

# ---------------------------------------------------------
# 抓 Pixnet 文章（不抓 views）
# ---------------------------------------------------------
def fetch_pixnet_article(url):
    try:
        r = requests.get(url, timeout=10)
        r.encoding = r.apparent_encoding
    except Exception:
        return None

    soup = BeautifulSoup(r.text, "html.parser")
    title_tag = soup.find("title")
    title = title_tag.get_text(strip=True) if title_tag else ""
    content = soup.get_text(" ", strip=True)
    return title, content

# ---------------------------------------------------------
# SEO Score（維持你原本結構：URL長度 + 內容長度 + 網域）
# 注意：你主程式沒有傳 main_keyword/sub_keywords，所以 title_score 會是 0
# ---------------------------------------------------------
def compute_seo_score(url, title, content, main_keyword=None, sub_keywords=None):
    url_score = 10 if len(url) < 60 else 0
    if main_keyword and main_keyword.lower() in url.lower():
        url_score += 10

    length = len(content)
    if length > 2500:
        length_score = 30
    elif length > 1500:
        length_score = 22
    elif length > 800:
        length_score = 15
    else:
        length_score = 8

    title_score = 0
    if main_keyword and main_keyword.lower() in title.lower():
        title_score += 20
    if sub_keywords:
        hits = sum(1 for k in sub_keywords if k.lower() in title.lower())
        title_score += min(hits * 3.3, 10)

    trusted_domains = ["pixnet.net", "medium.com", "blogspot.com", "wordpress.com"]
    domain_score = 20 if any(d in url for d in trusted_domains) else 0

    final = min(url_score + length_score + title_score + domain_score, 100)
    return round(final, 2)

# ---------------------------------------------------------
# Hot Score
# ---------------------------------------------------------
def compute_hot_score(hot_rank):
    return MAX_HOT_SCORE - (hot_rank - 1)

# ---------------------------------------------------------
# Google API 呼叫（加：節流 + 429 退避重試）
# ---------------------------------------------------------
GOOGLE_REQ_MIN_INTERVAL_SEC = 0.8  # 每次 Google API 呼叫至少間隔 0.8 秒（降低撞到每分鐘上限）
_last_google_call_ts = 0.0

def _throttle_google_calls():
    global _last_google_call_ts
    now = time.time()
    wait = GOOGLE_REQ_MIN_INTERVAL_SEC - (now - _last_google_call_ts)
    if wait > 0:
        time.sleep(wait)
    _last_google_call_ts = time.time()

def google_cse_request(params, max_retries=6):
    """
    回傳 (data_dict, error_text_or_none)
    遇到 429 會自動退避重試（exponential backoff）。
    """
    url = "https://www.googleapis.com/customsearch/v1"
    backoff = 2  # seconds

    for attempt in range(1, max_retries + 1):
        _throttle_google_calls()

        try:
            r = requests.get(url, params=params, timeout=10)

            # 429: rate limit
            if r.status_code == 429:
                # 若有 Retry-After 就用它，否則用 backoff
                ra = r.headers.get("Retry-After")
                sleep_s = int(ra) if ra and ra.isdigit() else backoff
                print(f"[429] Rate limit exceeded. Sleep {sleep_s}s (attempt {attempt}/{max_retries})")
                time.sleep(sleep_s)
                backoff = min(backoff * 2, 60)
                continue

            r.raise_for_status()
            return r.json(), None

        except Exception as e:
            # 其他錯誤：同樣退避，但不無限重試
            print(f"[Google API ERROR] {e} (attempt {attempt}/{max_retries})")
            time.sleep(backoff)
            backoff = min(backoff * 2, 60)

    return None, "Google API request failed after retries"

# ---------------------------------------------------------
# 搜尋：收集候選 URL（不強制 query 含 2025，最後靠 title 篩）
# ---------------------------------------------------------
def run_google_patterns(keywords, target_count, seen, max_pages=3):
    """
    max_pages：每個 keyword 最多翻幾頁（每頁 10 筆）
    調小它可以顯著降低 API 請求量 → 不易 429
    """
    collected_local = []

    for keyword in keywords:
        page = 1
        while len(collected_local) < target_count and page <= max_pages:
            start = 1 + (page - 1) * 10
            params = {
                "key": API_KEY,
                "cx": CX,
                "q": keyword,
                "start": start,
                "filter": 0,
            }

            data, err = google_cse_request(params)
            if err or not data:
                break

            items = data.get("items", [])
            if not items:
                break

            for item in items:
                link = item.get("link", "")
                if "pixnet.net" in link and link not in seen:
                    seen.add(link)
                    collected_local.append(link)
                    if len(collected_local) >= target_count:
                        return collected_local

            page += 1

    return collected_local

def google_search_city_pixnet(city_name, target_count, candidate_multiplier=3):
    """
    取得候選 URL（不保證 target_count）
    之後在 main() 用「標題包含 2025」篩選，最多取 target_count。
    """
    seen = set()
    candidate_target = target_count * candidate_multiplier

    key_patterns_city = [
        f"{city_name} 壽星優惠 site:pixnet.net",
        f"{city_name} 生日優惠 site:pixnet.net",
        f"{city_name} 壽星 免費 site:pixnet.net",
        f"{city_name} 生日 免費 site:pixnet.net",
    ]
    collected = run_google_patterns(key_patterns_city, candidate_target, seen, max_pages=3)

    if len(collected) < candidate_target:
        key_patterns_fallback = [
            "壽星優惠 site:pixnet.net",
            "生日優惠 site:pixnet.net",
            "2025 壽星優惠 site:pixnet.net",
            "2025 生日優惠 site:pixnet.net",
        ]
        collected.extend(
            run_google_patterns(
                key_patterns_fallback,
                candidate_target - len(collected),
                seen,
                max_pages=3
            )
        )

    return collected

# ---------------------------------------------------------
# 主程式：標題必含 2025；有就取前 N，沒有就取多少算多少
# ---------------------------------------------------------
def main():
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_name = f"pixnet_weighted_{ts}.ndjson"

    with open(output_name, "w", encoding="utf-8") as f:
        for city in CITIES:
            cfg = CITY_CONFIG[city]
            city_key = cfg["city_key"]
            target_count = cfg["requests"]

            print(f"\n📌搜尋：{city}（目標最多 {target_count} 篇；每篇標題需包含 2025）")
            urls = google_search_city_pixnet(city, target_count)
            print(f"➡ 候選 URL 共 {len(urls)} 篇（將篩選 title 含 2025）")

            kept = 0
            hot_rank = 0  # 只對「符合條件且寫入」的文章排序

            for url in urls:
                if kept >= target_count:
                    break

                article = fetch_pixnet_article(url)
                if not article:
                    continue

                title, content = article

                # 條件：標題必須包含 2025
                if "2025" not in title:
                    continue

                kept += 1
                hot_rank += 1

                hot_score = compute_hot_score(hot_rank)
                seo_score = compute_seo_score(url, title, content)
                final_score = round(hot_score * 0.6 + seo_score * 0.4, 2)

                record = {
                    "city_key": city_key,
                    "city_name": city,
                    "brand": None,
                    "title": title,
                    "promo_type": None,
                    "description": content,
                    "expire_date": None,
                    "source": "pixnet_crawler",
                    "source_url": url,
                    "fetched_at": datetime.now(timezone.utc).isoformat(),
                    "hot_rank": hot_rank,
                    "hot_score": hot_score,
                    "seo_score": seo_score,
                    "final_score": final_score,
                }

                f.write(json.dumps(record, ensure_ascii=False) + "\n")

            print(f"✅ {city} 最終寫入（title 含 2025）共 {kept} 篇")

    print(f"\n🎉 NDJSON 已建立：{output_name}")

if __name__ == "__main__":
    main()

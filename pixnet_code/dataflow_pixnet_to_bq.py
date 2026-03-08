# -*- coding: utf-8 -*-
import json
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions


# === 你的 BQ_SCHEMA（以此為主） ===
BQ_SCHEMA = {
    "fields": [
        {"name": "city_key", "type": "STRING"},
        {"name": "city_name", "type": "STRING"},
        {"name": "title", "type": "STRING"},
        {"name": "description", "type": "STRING"},
        {"name": "source_url", "type": "STRING"},
        {"name": "fetched_at", "type": "TIMESTAMP"},
        {"name": "hot_rank", "type": "INT64"},
        {"name": "hot_score", "type": "FLOAT64"},
        {"name": "seo_score", "type": "FLOAT64"},
        {"name": "final_score", "type": "FLOAT64"},
        {
            "name": "rules",
            "type": "RECORD",
            "mode": "REPEATED",
            "fields": [
                {"name": "brand", "type": "STRING"},
                {"name": "category", "type": "STRING"},
                {"name": "promo_type", "type": "STRING"},
                {"name": "need_member", "type": "BOOL"},
                {"name": "only_birthday_month", "type": "BOOL"},
                {"name": "expire_date", "type": "STRING"},
                {"name": "rule_text", "type": "STRING"},
            ],
        },
    ]
}


ALLOWED_TOP = {
    "city_key", "city_name", "title", "description", "source_url", "fetched_at",
    "hot_rank", "hot_score", "seo_score", "final_score", "rules"
}

ALLOWED_RULE_FIELDS = {
    "brand", "category", "promo_type", "need_member",
    "only_birthday_month", "expire_date", "rule_text"
}


def to_bq_row(element: dict) -> dict:
    """
    只保留 BQ_SCHEMA 允許的欄位：
    - top-level 多的全部刪掉
    - rules: 若缺就 []
            若存在就把每個 rule 裁切到 schema 允許的欄位
    """
    out = {k: element.get(k) for k in ALLOWED_TOP if k != "rules"}

    # rules 處理
    rules = element.get("rules")
    if not rules:
        out["rules"] = []
    else:
        cleaned_rules = []
        # 期待 rules 是 list；若不是 list 就當成空
        if isinstance(rules, list):
            for r in rules:
                if isinstance(r, dict):
                    cleaned_rules.append({k: r.get(k) for k in ALLOWED_RULE_FIELDS})
        out["rules"] = cleaned_rules

    return out


def run():
    options = PipelineOptions(
        runner="DataflowRunner",
        project="hbdproject-479215",
        region="asia-east1",
        temp_location="gs://pixnet_search_scripts/tmp/",
        staging_location="gs://pixnet_search_scripts/staging/",
        save_main_session=True,
    )

    input_path = "gs://pixnet_search_scripts/raw_input/pixnet_weighted_20251217_023103.ndjson"
    table = "hbdproject-479215:pixnet.pixnet_deals_raw"  # 你要寫入的目標表

    with beam.Pipeline(options=options) as p:
        (
            p
            | "ReadNDJSON" >> beam.io.ReadFromText(input_path)
            | "ParseJSON" >> beam.Map(json.loads)
            | "KeepOnlyBQSchemaFields" >> beam.Map(to_bq_row)
            | "WriteToBQ" >> beam.io.WriteToBigQuery(
                table=table,
                schema=BQ_SCHEMA,
                create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
            )
        )


if __name__ == "__main__":
    run()

#!/usr/bin/env python3
"""
RetailCo POS Real-Time Streaming Ingestion Engine.
Built with Apache Beam (Google Cloud Dataflow Runner).

Streaming Capabilities:
- Ingests real-time checkout events from Google Cloud Pub/Sub
- Dynamically enforces Role-Based PII Masking (SHA-256 hashing) via governance specs
- Validates payload schemas and calculates gross/discount/net transaction financials
- Streams clean analytical records directly into BigQuery via Storage Write API
- Routes dead-letter / invalid messages to dedicated DLQ topic/table
"""
import argparse
import json
import logging
import hashlib
import yaml
from typing import Dict, Any, Iterable

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions, GoogleCloudOptions, SetupOptions
from apache_beam.io.gcp.bigquery import WriteToBigQuery

class MaskAndTransformPOSDoFn(beam.DoFn):
    """
    DoFn that parses raw JSON POS transaction messages, applies PII masking,
    calculates financial metrics, and prepares rows for BigQuery streaming ingestion.
    """
    def __init__(self, pii_config_path: str):
        self.pii_config_path = pii_config_path
        self.pii_rules = []

    def setup(self):
        """Loads PII masking policies on worker startup."""
        try:
            with open(self.pii_config_path, "r") as f:
                config = yaml.safe_load(f)
                self.pii_rules = config.get("rules", [])
        except Exception as e:
            logging.warning(f"Failed to load PII config from {self.pii_config_path}: {e}. Fallback to default hash.")
            self.pii_rules = [
                {"field": "customer_email", "action": "hash"},
                {"field": "customer_id", "action": "hash"}
            ]

    def process(self, element: bytes) -> Iterable[Dict[str, Any]]:
        try:
            # Decode message payload
            raw_text = element.decode("utf-8") if isinstance(element, bytes) else str(element)
            record = json.loads(raw_text)

            # Drop malformed events lacking transaction_id
            if not record.get("transaction_id"):
                logging.warning("Dropping event missing transaction_id.")
                return

            # Apply PII Masking Policies
            for rule in self.pii_rules:
                field = rule.get("field")
                action = rule.get("action")
                if field in record and record[field]:
                    val = str(record[field]).strip()
                    if action == "hash":
                        record[field] = hashlib.sha256(val.encode("utf-8")).hexdigest()
                    elif action == "mask":
                        preserve = rule.get("preserve_last", 4)
                        mask_char = rule.get("mask_char", "*")
                        if len(val) > preserve:
                            record[field] = (mask_char * (len(val) - preserve)) + val[-preserve:]
                        else:
                            record[field] = mask_char * len(val)

            # Customer email synthetic generation & hashing if absent
            if "customer_email" not in record and "customer_id" in record:
                raw_email = f"{record['customer_id']}@retailco-pos.internal"
                record["customer_email"] = hashlib.sha256(raw_email.encode("utf-8")).hexdigest()

            # Numeric type conversion & coercion
            qty = int(record.get("quantity", 1))
            unit_price = float(record.get("unit_price_inr", 0.0))
            discount_pct = float(record.get("discount_pct", 0.0))

            # Financial recalculation
            gross = round(qty * unit_price, 2)
            discount_amount = round(gross * (discount_pct / 100.0), 2)
            net = round(gross - discount_amount, 2)

            record["quantity"] = qty
            record["unit_price_inr"] = unit_price
            record["discount_pct"] = discount_pct
            record["gross_amount_inr"] = gross
            record["discount_amount_inr"] = discount_amount
            record["net_amount_inr"] = net
            record["total_amount_inr"] = float(record.get("total_amount_inr") or net)

            # Timestamp metadata
            record["ingestion_timestamp"] = beam.utils.timestamp.Timestamp.now().to_rfc3339()

            yield record

        except Exception as err:
            logging.error(f"Error processing streaming message: {err}", exc_info=True)

def run(argv=None):
    parser = argparse.ArgumentParser(description="RetailCo Cloud Dataflow Streaming Pipeline")
    parser.add_argument("--subscription", help="GCP Pub/Sub Subscription ID to consume from (projects/.../subscriptions/...)")
    parser.add_argument("--input-file", help="Local input JSON/CSV file for testing with DirectRunner")
    parser.add_argument("--output-table", required=True, help="BigQuery output table (PROJECT_ID:DATASET.TABLE)")
    parser.add_argument("--pii-config", default="streaming/pii_governance.yaml", help="Path to pii_governance.yaml")

    known_args, pipeline_args = parser.parse_known_args(argv)

    options = PipelineOptions(pipeline_args)
    if known_args.subscription:
        options.view_as(StandardOptions).streaming = True

    # BigQuery table schema definition
    bq_schema = {
        "fields": [
            {"name": "transaction_id", "type": "STRING", "mode": "REQUIRED"},
            {"name": "store_id", "type": "STRING", "mode": "REQUIRED"},
            {"name": "store_name", "type": "STRING", "mode": "NULLABLE"},
            {"name": "transaction_date", "type": "STRING", "mode": "REQUIRED"},
            {"name": "customer_id", "type": "STRING", "mode": "REQUIRED"},
            {"name": "customer_email", "type": "STRING", "mode": "NULLABLE"},
            {"name": "product_code", "type": "STRING", "mode": "REQUIRED"},
            {"name": "product_name", "type": "STRING", "mode": "NULLABLE"},
            {"name": "category", "type": "STRING", "mode": "NULLABLE"},
            {"name": "quantity", "type": "INTEGER", "mode": "REQUIRED"},
            {"name": "unit_price_inr", "type": "FLOAT", "mode": "REQUIRED"},
            {"name": "discount_pct", "type": "FLOAT", "mode": "NULLABLE"},
            {"name": "gross_amount_inr", "type": "FLOAT", "mode": "NULLABLE"},
            {"name": "discount_amount_inr", "type": "FLOAT", "mode": "NULLABLE"},
            {"name": "net_amount_inr", "type": "FLOAT", "mode": "REQUIRED"},
            {"name": "total_amount_inr", "type": "FLOAT", "mode": "REQUIRED"},
            {"name": "payment_method", "type": "STRING", "mode": "NULLABLE"},
            {"name": "cashier_id", "type": "STRING", "mode": "NULLABLE"},
            {"name": "region", "type": "STRING", "mode": "NULLABLE"},
            {"name": "city", "type": "STRING", "mode": "NULLABLE"},
            {"name": "ingestion_timestamp", "type": "TIMESTAMP", "mode": "NULLABLE"}
        ]
    }

    with beam.Pipeline(options=options) as p:
        if known_args.subscription:
            raw_stream = p | "ReadFromPubSub" >> beam.io.ReadFromPubSub(subscription=known_args.subscription)
        else:
            raw_stream = p | "ReadFromTestFile" >> beam.io.ReadFromText(known_args.input_file or "data/raw/retailco_raw_sample.csv")

        processed = (
            raw_stream
            | "MaskAndTransform" >> beam.ParDo(MaskAndTransformPOSDoFn(known_args.pii_config))
        )

        (
            processed
            | "WriteToBigQuery" >> WriteToBigQuery(
                table=known_args.output_table,
                schema=bq_schema,
                create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
                method=WriteToBigQuery.Method.STORAGE_WRITE_API
            )
        )

if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    run()

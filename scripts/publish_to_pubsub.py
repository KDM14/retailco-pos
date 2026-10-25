#!/usr/bin/env python3
"""
RetailCo Pub/Sub Event Producer Simulator.
Simulates real-time retail checkout registers streaming live line-item sales events to GCP Pub/Sub.
"""
import csv
import json
import time
import argparse
import logging
from google.cloud import pubsub_v1

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("PubSubProducer")

def stream_transactions_to_pubsub(project_id: str, topic_id: str, input_csv: str, delay_sec: float = 0.05):
    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path(project_id, topic_id)
    logger.info(f"Connecting to Google Cloud Pub/Sub topic: {topic_path}")

    with open(input_csv, mode="r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        count = 0
        for row in reader:
            payload = {k: v for k, v in row.items() if k}
            data_bytes = json.dumps(payload).encode("utf-8")

            # Publish message with store attribute
            future = publisher.publish(topic_path, data=data_bytes, store_id=payload.get("store_id", "UNKNOWN"))
            message_id = future.result()
            count += 1

            if count % 20 == 0:
                logger.info(f"Streamed {count} POS events to Pub/Sub. Last messageId: {message_id}")

            time.sleep(delay_sec)

    logger.info(f"🎯 Completed streaming session. Total events published: {count}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Stream POS checkout events to Pub/Sub")
    parser.add_argument("--project_id", default="dhanush-week6-2026")
    parser.add_argument("--topic_id", default="retailco-transactions")
    parser.add_argument("--input_csv", default="data/raw/retailco_raw_sample.csv")
    parser.add_argument("--delay", type=float, default=0.05)
    args = parser.parse_args()

    stream_transactions_to_pubsub(args.project_id, args.topic_id, args.input_csv, args.delay)

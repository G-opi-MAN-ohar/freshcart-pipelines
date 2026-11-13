#!/usr/bin/env python3
"""
Routing key format: <city>.<zone>.<type>
Payload format:     {"id": ..., "city": ..., "zone": ..., "type": ..., "ts": ...}
"""

import json
import os
import random
import sys
import time
import uuid
from datetime import datetime, timezone

import pika

EXCHANGE_NAME = "rapido-dispatch"
EXCHANGE_TYPE = "topic"
PUBLISH_INTERVAL_SEC = 2.0

CITY_ZONES = {
    "bangalore": ["koramangala", "indiranagar", "whitefield", "hsr"],
    "chennai": ["tnagar", "adyar", "velachery", "annanagar"],
}

REQUEST_TYPES = ["ride", "food"]


def get_connection() -> pika.BlockingConnection:
    host = os.getenv("RABBITMQ_HOST", "localhost")
    port = int(os.getenv("RABBITMQ_PORT", "5672"))
    user = os.getenv("RABBITMQ_USER", "guest")
    password = os.getenv("RABBITMQ_PASS", "guest")

    credentials = pika.PlainCredentials(user, password)
    params = pika.ConnectionParameters(
        host=host,
        port=port,
        credentials=credentials,
        heartbeat=60,
    )
    return pika.BlockingConnection(params)


def main() -> None:
    connection = get_connection()
    channel = connection.channel()

    # Declare topic exchange (idempotent)
    channel.exchange_declare(
        exchange=EXCHANGE_NAME,
        exchange_type=EXCHANGE_TYPE,
        durable=True,
    )

    print(
        f"[DISPATCHER] Connected to RabbitMQ. Publishing to topic exchange "
        f"'{EXCHANGE_NAME}' every {PUBLISH_INTERVAL_SEC:.0f}s (Ctrl+C to stop)..."
    )

    seq = 1
    try:
        while True:
            city = random.choice(list(CITY_ZONES.keys()))
            zone = random.choice(CITY_ZONES[city])
            req_type = random.choice(REQUEST_TYPES)

            routing_key = f"{city}.{zone}.{req_type}"
            message = {
                "id": f"req-{seq:04d}-{uuid.uuid4().hex[:6]}",
                "city": city,
                "zone": zone,
                "type": req_type,
                "ts": datetime.now(timezone.utc).isoformat(timespec="seconds"),
            }
            payload = json.dumps(message)

            channel.basic_publish(
                exchange=EXCHANGE_NAME,
                routing_key=routing_key,
                body=payload.encode("utf-8"),
                properties=pika.BasicProperties(
                    content_type="application/json",
                    delivery_mode=pika.DeliveryMode.Persistent,
                ),
            )

            print(f"[DISPATCHER] Sent [{routing_key}] -> {payload}", flush=True)
            seq += 1
            time.sleep(PUBLISH_INTERVAL_SEC)

    except KeyboardInterrupt:
        print("\n[DISPATCHER] Shutting down gracefully...")
    finally:
        if connection.is_open:
            connection.close()


if __name__ == "__main__":
    try:
        main()
    except pika.exceptions.AMQPConnectionError as exc:
        print(f"[DISPATCHER] Could not connect to RabbitMQ: {exc}", file=sys.stderr)
        sys.exit(1)
#!/usr/bin/env python3
"""
stream/dispatcher.py

Publishes simulated Rapido ride and food dispatch requests every 2 seconds
to the 'rapido-dispatch' RabbitMQ topic exchange.
Every 10th message is intentionally malformed (missing the "zone" field)
to test consumer basic_nack(requeue=False) rejection handling.

Routing key format: <city>.<zone>.<type>
Good payload:       {"id": ..., "city": ..., "zone": ..., "type": ..., "ts": ...}
Bad payload (1/10): {"id": ..., "city": ..., "type": ..., "ts": ...}
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
        f"'{EXCHANGE_NAME}' every {PUBLISH_INTERVAL_SEC:.0f}s "
        f"(sending 1 bad message missing 'zone' every 10 messages)..."
    )

    seq = 1
    try:
        while True:
            city = random.choice(list(CITY_ZONES.keys()))
            zone = random.choice(CITY_ZONES[city])
            req_type = random.choice(REQUEST_TYPES)

            routing_key = f"{city}.{zone}.{req_type}"
            msg_id = f"req-{seq:04d}-{uuid.uuid4().hex[:6]}"
            ts = datetime.now(timezone.utc).isoformat(timespec="seconds")

            if seq % 10 == 0:
                # Every 10th message omits the 'zone' field
                message = {
                    "id": msg_id,
                    "city": city,
                    "type": req_type,
                    "ts": ts,
                }
                status_label = "BAD (missing 'zone')"
            else:
                message = {
                    "id": msg_id,
                    "city": city,
                    "zone": zone,
                    "type": req_type,
                    "ts": ts,
                }
                status_label = "GOOD"

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

            print(
                f"[DISPATCHER] #{seq:02d} [{status_label}] Sent [{routing_key}] -> {payload}",
                flush=True,
            )
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
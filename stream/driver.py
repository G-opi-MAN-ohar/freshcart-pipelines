#!/usr/bin/env python3
"""
stream/driver.py

Usage:
    python3 stream/driver.py NAME PATTERN

Declares an exclusive private queue, binds it to the 'rapido-dispatch' topic
exchange with PATTERN, inspects each incoming JSON request:
- If the 'zone' field is missing/empty, rejects with basic_nack(requeue=False).
- Otherwise, processes the request and acknowledges with basic_ack.
"""

import json
import os
import sys

import pika

EXCHANGE_NAME = "rapido-dispatch"
EXCHANGE_TYPE = "topic"


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
    if len(sys.argv) < 3:
        print("Usage: python3 stream/driver.py NAME PATTERN", file=sys.stderr)
        sys.exit(1)

    driver_name = sys.argv[1]
    binding_patterns = sys.argv[2:]

    connection = get_connection()
    channel = connection.channel()

    # Ensure the topic exchange exists
    channel.exchange_declare(
        exchange=EXCHANGE_NAME,
        exchange_type=EXCHANGE_TYPE,
        durable=True,
    )

    # Declare a server-named exclusive private queue that auto-deletes on disconnect
    result = channel.queue_declare(queue="", exclusive=True)
    queue_name = result.method.queue

    # Bind the private queue with each supplied pattern
    for pattern in binding_patterns:
        channel.queue_bind(
            exchange=EXCHANGE_NAME,
            queue=queue_name,
            routing_key=pattern,
        )

    patterns_str = ", ".join(binding_patterns)
    print(
        f"[DRIVER:{driver_name}] Private queue '{queue_name}' bound to "
        f"'{EXCHANGE_NAME}' with pattern(s): {patterns_str}",
        flush=True,
    )
    print(f"[DRIVER:{driver_name}] Waiting for requests (Ctrl+C to stop)...", flush=True)

    def on_message(ch, method, properties, body: bytes) -> None:
        try:
            data = json.loads(body.decode("utf-8"))
        except json.JSONDecodeError:
            data = None

        # Reject messages missing the 'zone' field without requeueing
        if not isinstance(data, dict) or not data.get("zone"):
            print(
                f"[DRIVER:{driver_name}] REJECTED (missing 'zone') "
                f"key='{method.routing_key}' | payload={body.decode('utf-8', errors='replace')} | "
                f"Sending basic_nack(requeue=False) (tag={method.delivery_tag})",
                flush=True,
            )
            ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
            return

        print(
            f"[DRIVER:{driver_name}] PROCESSED key='{method.routing_key}' | "
            f"payload={json.dumps(data)} | "
            f"Sending basic_ack (tag={method.delivery_tag})",
            flush=True,
        )
        ch.basic_ack(delivery_tag=method.delivery_tag)

    channel.basic_qos(prefetch_count=1)
    channel.basic_consume(
        queue=queue_name,
        on_message_callback=on_message,
        auto_ack=False,
    )

    try:
        channel.start_consuming()
    except KeyboardInterrupt:
        print(f"\n[DRIVER:{driver_name}] Disconnecting...")
    finally:
        if connection.is_open:
            connection.close()


if __name__ == "__main__":
    try:
        main()
    except pika.exceptions.AMQPConnectionError as exc:
        print(f"[DRIVER] Could not connect to RabbitMQ: {exc}", file=sys.stderr)
        sys.exit(1)
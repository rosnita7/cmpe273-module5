#!/usr/bin/env python3
"""
Order Producer
==============
Publishes order messages to a RabbitMQ queue.
Each message contains:
  - event_id: Unique event identifier
  - order_id: Identifier for the order
  - item_id: Identifier for the item purchased
  - quantity: Number of items ordered
"""

import argparse
import json
import os
import sys
import time
import uuid
import pika

DEFAULT_QUEUE = "order_queue"
DEFAULT_HOST = os.getenv("RABBITMQ_HOST", "localhost")
DEFAULT_PORT = int(os.getenv("RABBITMQ_PORT", "5672"))


def get_connection(host=DEFAULT_HOST, port=DEFAULT_PORT):
    """Establish and return a blocking connection to RabbitMQ."""
    credentials = pika.PlainCredentials("guest", "guest")
    parameters = pika.ConnectionParameters(
        host=host,
        port=port,
        credentials=credentials,
        connection_attempts=5,
        retry_delay=2,
    )
    return pika.BlockingConnection(parameters)


def publish_order(channel, queue_name, order_message):
    """
    Publish an order message to the specified queue as persistent JSON.
    """
    body = json.dumps(order_message)
    channel.basic_publish(
        exchange="",
        routing_key=queue_name,
        body=body.encode("utf-8"),
        properties=pika.BasicProperties(
            delivery_mode=pika.DeliveryMode.Persistent,  # make message persistent
            content_type="application/json",
        ),
    )
    timestamp = time.strftime("%Y-%m-%d %H:%M:%S")
    print(
        f"[{timestamp}] [PRODUCER] Published -> "
        f"event_id={order_message['event_id']}, "
        f"order_id={order_message['order_id']}, "
        f"item_id={order_message['item_id']}, "
        f"quantity={order_message['quantity']}"
    )


def main():
    parser = argparse.ArgumentParser(description="Order Producer for RabbitMQ")
    parser.add_argument(
        "--count",
        type=int,
        default=5,
        help="Number of order messages to publish (default: 5)",
    )
    parser.add_argument(
        "--queue",
        type=str,
        default=DEFAULT_QUEUE,
        help=f"RabbitMQ queue name (default: {DEFAULT_QUEUE})",
    )
    parser.add_argument(
        "--host",
        type=str,
        default=DEFAULT_HOST,
        help=f"RabbitMQ host (default: {DEFAULT_HOST})",
    )
    parser.add_argument(
        "--port",
        type=int,
        default=DEFAULT_PORT,
        help=f"RabbitMQ port (default: {DEFAULT_PORT})",
    )
    parser.add_argument(
        "--delay",
        type=float,
        default=0.2,
        help="Delay in seconds between publishing messages (default: 0.2)",
    )
    parser.add_argument(
        "--event-id",
        type=str,
        default=None,
        help="Publish a single message with this specific event_id",
    )
    parser.add_argument(
        "--order-id",
        type=str,
        default=None,
        help="Specify order_id for single message publish",
    )
    parser.add_argument(
        "--item-id",
        type=str,
        default=None,
        help="Specify item_id for single message publish",
    )
    parser.add_argument(
        "--quantity",
        type=int,
        default=1,
        help="Specify quantity for single message publish (default: 1)",
    )
    parser.add_argument(
        "--send-duplicates",
        action="store_true",
        help="Publish a batch of messages including intentional duplicate event_ids",
    )

    args = parser.parse_args()

    print(f"Connecting to RabbitMQ at {args.host}:{args.port}...")
    try:
        connection = get_connection(args.host, args.port)
        channel = connection.channel()
    except Exception as e:
        print(f"Failed to connect to RabbitMQ: {e}", file=sys.stderr)
        sys.exit(1)

    # Declare durable queue
    channel.queue_declare(queue=args.queue, durable=True)
    print(f"Connected. Target queue: '{args.queue}' (durable=True)\n")

    # Mode 1: Single custom message
    if args.event_id:
        message = {
            "event_id": args.event_id,
            "order_id": args.order_id or f"ord-{args.event_id[-4:]}",
            "item_id": args.item_id or "item-keyboard-pro",
            "quantity": args.quantity,
        }
        publish_order(channel, args.queue, message)
        connection.close()
        return

    # Mode 2: Send duplicate demonstration batch
    if args.send_duplicates:
        fixed_event_id = "evt-dup-9001"
        messages = [
            {
                "event_id": fixed_event_id,
                "order_id": "ord-7001",
                "item_id": "item-wireless-mouse",
                "quantity": 2,
            },
            {
                "event_id": f"evt-{uuid.uuid4().hex[:6]}",
                "order_id": "ord-7002",
                "item_id": "item-usb-c-cable",
                "quantity": 3,
            },
            # Duplicate of the first message
            {
                "event_id": fixed_event_id,
                "order_id": "ord-7001",
                "item_id": "item-wireless-mouse",
                "quantity": 2,
            },
        ]
        print("Sending batch with intentional duplicate event_id...")
        for msg in messages:
            publish_order(channel, args.queue, msg)
            time.sleep(args.delay)
        connection.close()
        print(f"\nFinished publishing {len(messages)} messages (including duplicate {fixed_event_id}).")
        return

    # Mode 3: Default batch publish of N orders
    catalog = [
        ("item-laptop-m3", 1),
        ("item-keyboard-pro", 2),
        ("item-wireless-mouse", 1),
        ("item-4k-monitor", 1),
        ("item-usb-c-dock", 3),
        ("item-noise-cancelling-headphones", 1),
    ]

    print(f"Publishing {args.count} order messages...")
    for i in range(1, args.count + 1):
        item_id, default_qty = catalog[(i - 1) % len(catalog)]
        message = {
            "event_id": f"evt-{1000 + i}",
            "order_id": f"ord-{5000 + i}",
            "item_id": item_id,
            "quantity": default_qty,
        }
        publish_order(channel, args.queue, message)
        if i < args.count:
            time.sleep(args.delay)

    connection.close()
    print(f"\nSuccessfully published {args.count} orders.")


if __name__ == "__main__":
    main()

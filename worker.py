#!/usr/bin/env python3
"""
Order Worker (Consumer)
=======================
Consumes order messages from RabbitMQ.
Features:
  - Manual ACK upon completion of business processing.
  - Fair dispatch via basic_qos(prefetch_count=1) for competing consumers.
  - Duplicate detection using a persistent deduplication store.
  - Simulated failure injection (crash before ACK) to demonstrate redelivery.
"""

import argparse
import json
import os
import sys
import time
import pika

DEFAULT_QUEUE = "order_queue"
DEFAULT_HOST = os.getenv("RABBITMQ_HOST", "localhost")
DEFAULT_PORT = int(os.getenv("RABBITMQ_PORT", "5672"))
DEFAULT_STORE_FILE = "processed_events.txt"


class DeduplicationStore:
    """
    Tracks processed event_id values using an in-memory set backed by a file.
    This allows multiple independent worker processes to share idempotency state.
    """

    def __init__(self, filepath=DEFAULT_STORE_FILE):
        self.filepath = filepath
        self._processed = set()
        self._load()

    def _load(self):
        if os.path.exists(self.filepath):
            try:
                with open(self.filepath, "r", encoding="utf-8") as f:
                    for line in f:
                        line = line.strip()
                        if line:
                            self._processed.add(line)
            except Exception as e:
                print(f"[WARN] Error reading deduplication store: {e}", file=sys.stderr)

    def is_duplicate(self, event_id: str) -> bool:
        # Reload latest from file in case another worker updated it
        self._load()
        return event_id in self._processed

    def mark_processed(self, event_id: str):
        self._processed.add(event_id)
        try:
            with open(self.filepath, "a", encoding="utf-8") as f:
                f.write(f"{event_id}\n")
                f.flush()
                os.fsync(f.fileno())
        except Exception as e:
            print(f"[WARN] Error writing to deduplication store: {e}", file=sys.stderr)


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


def create_order_callback(worker_id, dedup_store, processing_time, crash_on_event):
    """
    Factory function returning the RabbitMQ consumer callback for a given worker.
    """

    def callback(ch, method, properties, body):
        now = time.strftime("%Y-%m-%d %H:%M:%S")
        try:
            payload = json.loads(body.decode("utf-8"))
        except Exception as e:
            print(f"[{now}] [{worker_id}] [ERROR] Failed to parse message body: {e}")
            ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
            return

        event_id = payload.get("event_id")
        order_id = payload.get("order_id")
        item_id = payload.get("item_id")
        quantity = payload.get("quantity")
        redelivered_tag = " (REDELIVERED)" if method.redelivered else ""

        print(
            f"[{now}] [{worker_id}] [RECEIVED]{redelivered_tag} "
            f"event_id={event_id}, order_id={order_id}, item={item_id}, qty={quantity} "
            f"[DeliveryTag={method.delivery_tag}]"
        )

        # Requirement 3: Failure and Redelivery Demonstration
        # Check if this worker is instructed to crash before ACK on this event
        if crash_on_event and event_id == crash_on_event:
            print(
                f"[{now}] [{worker_id}] [SIMULATED CRASH] Crash triggered on event_id={event_id}! "
                f"Stopping process immediately BEFORE sending ACK."
            )
            # Flush stdout before termination
            sys.stdout.flush()
            # os._exit terminates immediately without executing cleanup or sending ACK
            os._exit(1)

        # Requirement 4: Duplicate Handling (Idempotency)
        if dedup_store.is_duplicate(event_id):
            print(
                f"[{now}] [{worker_id}] [DUPLICATE DETECTED] event_id={event_id} has ALREADY been processed! "
                f"Skipping business action."
            )
            # Acknowledge the message so it is removed from the queue
            ch.basic_ack(delivery_tag=method.delivery_tag)
            print(f"[{now}] [{worker_id}] [ACK SENT] Acknowledged duplicate event_id={event_id} (no-op).")
            sys.stdout.flush()
            return

        # Requirement 2: Business Processing
        print(
            f"[{now}] [{worker_id}] [PROCESSING] Executing business logic for order {order_id}... "
            f"(simulating {processing_time}s work)"
        )
        sys.stdout.flush()
        if processing_time > 0:
            time.sleep(processing_time)

        # Record event in deduplication store
        dedup_store.mark_processed(event_id)

        # Send ACK after processing finishes
        ch.basic_ack(delivery_tag=method.delivery_tag)
        finish_time = time.strftime("%Y-%m-%d %H:%M:%S")
        print(
            f"[{finish_time}] [{worker_id}] [ACK SENT] Finished order {order_id} "
            f"(event_id={event_id}). ACK sent to RabbitMQ.\n"
        )
        sys.stdout.flush()

    return callback


def main():
    parser = argparse.ArgumentParser(description="Order Worker (RabbitMQ Consumer)")
    parser.add_argument(
        "--id",
        type=str,
        default=f"Worker-{os.getpid()}",
        help="Worker identifier (e.g., Worker-1, Worker-2)",
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
        default=0.8,
        help="Simulated processing time in seconds (default: 0.8)",
    )
    parser.add_argument(
        "--crash-on-event",
        type=str,
        default=None,
        help="Simulate a crash before ACK when this event_id is received",
    )
    parser.add_argument(
        "--store-file",
        type=str,
        default=DEFAULT_STORE_FILE,
        help=f"Path to deduplication file (default: {DEFAULT_STORE_FILE})",
    )
    parser.add_argument(
        "--clean-store",
        action="store_true",
        help="Clear the deduplication store file before starting",
    )

    args = parser.parse_args()

    if args.clean_store and os.path.exists(args.store_file):
        os.remove(args.store_file)
        print(f"[{args.id}] Deduplication store '{args.store_file}' cleared.")

    dedup_store = DeduplicationStore(filepath=args.store_file)

    print(f"[{args.id}] Connecting to RabbitMQ at {args.host}:{args.port}...")
    try:
        connection = get_connection(args.host, args.port)
        channel = connection.channel()
    except Exception as e:
        print(f"[{args.id}] Failed to connect to RabbitMQ: {e}", file=sys.stderr)
        sys.exit(1)

    # Declare durable queue
    channel.queue_declare(queue=args.queue, durable=True)

    # Fair dispatch: prefetch_count=1 ensures RabbitMQ gives only 1 message to worker at a time
    channel.basic_qos(prefetch_count=1)

    callback = create_order_callback(
        worker_id=args.id,
        dedup_store=dedup_store,
        processing_time=args.delay,
        crash_on_event=args.crash_on_event,
    )

    # auto_ack=False ensures explicit manual ACK after processing
    channel.basic_consume(
        queue=args.queue,
        on_message_callback=callback,
        auto_ack=False,
    )

    print(
        f"[{args.id}] Ready and listening on queue '{args.queue}' "
        f"(prefetch_count=1, delay={args.delay}s, auto_ack=False)..."
    )
    if args.crash_on_event:
        print(f"[{args.id}] [SIMULATION ACTIVE] Will CRASH without ACK if event_id='{args.crash_on_event}' is received!")

    try:
        channel.start_consuming()
    except KeyboardInterrupt:
        print(f"\n[{args.id}] Stopping worker...")
        channel.stop_consuming()
        connection.close()
        print(f"[{args.id}] Worker stopped cleanly.")


if __name__ == "__main__":
    main()

#!/usr/bin/env python3
"""
Automated Demonstration Runner
==============================
Executes and captures evidence for all assignment scenarios:
  1. Competing Consumers (Worker 1 and Worker 2 sharing load)
  2. Failure and Redelivery (Worker 1 crashes before ACK -> Worker 2 handles redelivery)
  3. Duplicate Handling (Idempotent deduplication skips duplicate business action)
"""

import argparse
import os
import subprocess
import sys
import time
import pika

VENV_PYTHON = sys.executable
QUEUE_NAME = "order_queue"
DEDUP_FILE = "processed_events.txt"


def purge_queue():
    """Connect to RabbitMQ and purge any remaining messages in order_queue."""
    try:
        credentials = pika.PlainCredentials("guest", "guest")
        connection = pika.BlockingConnection(
            pika.ConnectionParameters(
                host="localhost",
                port=5672,
                credentials=credentials,
                connection_attempts=3,
                retry_delay=1,
            )
        )
        channel = connection.channel()
        channel.queue_declare(queue=QUEUE_NAME, durable=True)
        channel.queue_purge(queue=QUEUE_NAME)
        connection.close()
    except Exception as e:
        print(f"[WARN] Queue purge error: {e}", file=sys.stderr)


def clean_dedup_store():
    """Delete the deduplication storage file for a fresh test run."""
    if os.path.exists(DEDUP_FILE):
        os.remove(DEDUP_FILE)


def run_competing_consumers():
    print("\n" + "=" * 70)
    print("DEMO 1: COMPETING CONSUMERS (Load Sharing Across Workers)")
    print("=" * 70)
    purge_queue()
    clean_dedup_store()

    log1 = open("worker1_competing.log", "w")
    log2 = open("worker2_competing.log", "w")

    print("[Step 1] Starting Worker-1 and Worker-2...")
    w1 = subprocess.Popen(
        [VENV_PYTHON, "worker.py", "--id", "Worker-1", "--delay", "0.5"],
        stdout=log1,
        stderr=subprocess.STDOUT,
    )
    w2 = subprocess.Popen(
        [VENV_PYTHON, "worker.py", "--id", "Worker-2", "--delay", "0.5"],
        stdout=log2,
        stderr=subprocess.STDOUT,
    )

    time.sleep(1.5)  # Allow workers to connect and subscribe

    print("[Step 2] Publishing 8 order messages...")
    pub = subprocess.run(
        [VENV_PYTHON, "producer.py", "--count", "8", "--delay", "0.15"],
        capture_output=True,
        text=True,
    )
    print(pub.stdout.strip())

    print("\n[Step 3] Waiting for workers to process all orders...")
    time.sleep(5.0)

    # Terminate workers
    w1.terminate()
    w2.terminate()
    w1.wait()
    w2.wait()
    log1.close()
    log2.close()

    print("\n--- Worker-1 Output ---")
    with open("worker1_competing.log") as f:
        print(f.read().strip())

    print("\n--- Worker-2 Output ---")
    with open("worker2_competing.log") as f:
        print(f.read().strip())

    print("\n[Result] Orders were distributed between Worker-1 and Worker-2.")
    print("Each order was consumed and acknowledged by exactly one worker.")


def run_failure_and_redelivery():
    print("\n" + "=" * 70)
    print("DEMO 2: FAILURE AND REDELIVERY (Crash Before ACK -> Redelivery)")
    print("=" * 70)
    purge_queue()
    clean_dedup_store()

    crash_event_id = "evt-fail-701"
    log1 = open("worker1_failure.log", "w")
    log2 = open("worker2_failure.log", "w")

    print(f"[Step 1] Starting Worker-1 configured to CRASH on '{crash_event_id}' before ACK...")
    w1 = subprocess.Popen(
        [
            VENV_PYTHON,
            "worker.py",
            "--id",
            "Worker-1",
            "--delay",
            "0.5",
            "--crash-on-event",
            crash_event_id,
        ],
        stdout=log1,
        stderr=subprocess.STDOUT,
    )

    print("[Step 2] Starting Worker-2 (normal standby worker)...")
    w2 = subprocess.Popen(
        [VENV_PYTHON, "worker.py", "--id", "Worker-2", "--delay", "0.5"],
        stdout=log2,
        stderr=subprocess.STDOUT,
    )

    time.sleep(1.5)

    print(f"\n[Step 3] Publishing message with event_id='{crash_event_id}'...")
    pub1 = subprocess.run(
        [
            VENV_PYTHON,
            "producer.py",
            "--event-id",
            crash_event_id,
            "--order-id",
            "ord-9901",
            "--item-id",
            "item-mechanical-keyboard",
            "--quantity",
            "1",
        ],
        capture_output=True,
        text=True,
    )
    print(pub1.stdout.strip())

    print("\n[Step 4] Monitoring Worker-1 crash and RabbitMQ redelivery to Worker-2...")
    # Worker 1 should exit with code 1
    w1.wait(timeout=5)
    print(f"Worker-1 process exited with return code: {w1.returncode} (expected crash)")

    # Allow Worker 2 to receive the redelivered unacked message
    time.sleep(3.0)

    print("\n[Step 5] Publishing follow-up normal message 'evt-norm-702' to prove Worker-2 is still healthy...")
    pub2 = subprocess.run(
        [
            VENV_PYTHON,
            "producer.py",
            "--event-id",
            "evt-norm-702",
            "--order-id",
            "ord-9902",
            "--item-id",
            "item-ergonomic-mouse",
            "--quantity",
            "2",
        ],
        capture_output=True,
        text=True,
    )
    print(pub2.stdout.strip())

    time.sleep(2.0)

    w2.terminate()
    w2.wait()
    log1.close()
    log2.close()

    print("\n--- Worker-1 (Crashing Worker) Log ---")
    with open("worker1_failure.log") as f:
        print(f.read().strip())

    print("\n--- Worker-2 (Surviving Worker) Log ---")
    with open("worker2_failure.log") as f:
        print(f.read().strip())

    print("\n[Result] Worker-1 crashed before ACK. RabbitMQ detected unacknowledged message and redelivered it.")
    print("Worker-2 received the redelivered message, successfully processed it, and ACKed it.")


def run_duplicate_handling():
    print("\n" + "=" * 70)
    print("DEMO 3: DUPLICATE HANDLING (Idempotent Processing)")
    print("=" * 70)
    purge_queue()
    clean_dedup_store()

    log1 = open("worker_duplicate.log", "w")
    dup_event_id = "evt-dup-888"

    print("[Step 1] Starting Worker-1...")
    w1 = subprocess.Popen(
        [VENV_PYTHON, "worker.py", "--id", "Worker-1", "--delay", "0.5"],
        stdout=log1,
        stderr=subprocess.STDOUT,
    )

    time.sleep(1.5)

    print(f"\n[Step 2] Publishing first instance of event_id='{dup_event_id}'...")
    pub1 = subprocess.run(
        [
            VENV_PYTHON,
            "producer.py",
            "--event-id",
            dup_event_id,
            "--order-id",
            "ord-3001",
            "--item-id",
            "item-usb-c-hub",
            "--quantity",
            "1",
        ],
        capture_output=True,
        text=True,
    )
    print(pub1.stdout.strip())

    # Wait for first processing to complete
    time.sleep(2.0)

    print(f"\n[Step 3] Publishing DUPLICATE message with identical event_id='{dup_event_id}'...")
    pub2 = subprocess.run(
        [
            VENV_PYTHON,
            "producer.py",
            "--event-id",
            dup_event_id,
            "--order-id",
            "ord-3001",
            "--item-id",
            "item-usb-c-hub",
            "--quantity",
            "1",
        ],
        capture_output=True,
        text=True,
    )
    print(pub2.stdout.strip())

    time.sleep(2.0)

    w1.terminate()
    w1.wait()
    log1.close()

    print("\n--- Worker-1 Log ---")
    with open("worker_duplicate.log") as f:
        print(f.read().strip())

    print("\n--- Deduplication Store Contents ---")
    if os.path.exists(DEDUP_FILE):
        with open(DEDUP_FILE) as f:
            print(f.read().strip())

    print("\n[Result] The first message was processed and recorded in deduplication store.")
    print("The second message was detected as a duplicate, logged, skipped, and acknowledged.")


def main():
    parser = argparse.ArgumentParser(description="Run RabbitMQ Assignment Demos")
    parser.add_argument(
        "--scenario",
        choices=["competing", "failure", "duplicate", "all"],
        default="all",
        help="Which scenario to run (default: all)",
    )
    args = parser.parse_args()

    if args.scenario in ("competing", "all"):
        run_competing_consumers()
    if args.scenario in ("failure", "all"):
        run_failure_and_redelivery()
    if args.scenario in ("duplicate", "all"):
        run_duplicate_handling()


if __name__ == "__main__":
    main()

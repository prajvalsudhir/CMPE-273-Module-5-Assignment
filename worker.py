"""Order Worker: consumes and processes order messages from RabbitMQ.

Each worker is an independent process. Two workers on the same queue
are competing consumers: RabbitMQ delivers each message to only one
of them. prefetch_count=1 gives fair dispatch (one unacked message
at a time per worker).

Usage:
    python worker.py --worker-id worker-1
    python worker.py --worker-id worker-2
    python worker.py --worker-id worker-1 --crash-on-event evt-001

Duplicate handling: processed event_ids are kept in a shared record,
processed_events.txt, in the same folder as this file. Every worker
reads that file on every message, under a file lock, so a duplicate
is detected even when a different worker processed the first copy.
A repeat is ACKed but not processed again. Delete
processed_events.txt before a fresh demo run, or earlier event_ids
will (correctly) be reported as duplicates.
Failure handling: if the worker stops before ACKing (see
--crash-on-event), RabbitMQ requeues the message and delivers it
to another worker. The simulated crash happens before the event_id
is recorded, so the redelivered message is processed, not skipped.
"""

import argparse
import json
import os
import sys
import time
from contextlib import contextmanager

import pika

try:
    import fcntl
except ImportError:  # non-Unix platforms: lock is skipped, file is still shared
    fcntl = None

QUEUE = "orders"
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
PROCESSED_FILE = os.path.join(BASE_DIR, "processed_events.txt")
LOCK_FILE = os.path.join(BASE_DIR, "processed_events.lock")


@contextmanager
def shared_lock():
    """Hold an exclusive lock on the shared record, if fcntl is available."""
    if fcntl is None:
        yield
        return
    with open(LOCK_FILE, "a") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX)
        try:
            yield
        finally:
            fcntl.flock(lock, fcntl.LOCK_UN)


def is_processed(event_id):
    if not os.path.exists(PROCESSED_FILE):
        return False
    with open(PROCESSED_FILE) as f:
        return event_id in {line.strip() for line in f if line.strip()}


def mark_processed(event_id):
    with open(PROCESSED_FILE, "a") as f:
        f.write(event_id + "\n")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--worker-id", default="worker-1")
    parser.add_argument("--crash-on-event", default=None,
                        help="Simulate a crash on this event_id: exit "
                             "without sending an ACK.")
    parser.add_argument("--priority", type=int, default=0,
                        help="Consumer priority (x-priority): RabbitMQ "
                             "delivers to the highest-priority idle "
                             "worker first. Used in the failure demo so "
                             "the crashing worker receives the message "
                             "even though both workers are running.")
    args = parser.parse_args()

    connection = pika.BlockingConnection(
        pika.ConnectionParameters(host="localhost"))
    channel = connection.channel()
    channel.queue_declare(queue=QUEUE)
    channel.basic_qos(prefetch_count=1)

    def callback(ch, method, properties, body):
        order = json.loads(body)
        event_id = order["event_id"]
        print(f"[{args.worker_id}] Received: {order}", flush=True)

        # Simulated crash: before the event_id is recorded, so the
        # redelivered message is processed by the next worker.
        if args.crash_on_event and event_id == args.crash_on_event:
            print(f"[{args.worker_id}] Simulating crash on "
                  f"event_id={event_id} (no ACK sent).", flush=True)
            # Exit without ACKing: RabbitMQ requeues the message.
            os._exit(1)

        # Shared record, checked and reserved under one lock, so two
        # workers cannot both decide a duplicate is new.
        with shared_lock():
            if is_processed(event_id):
                print(f"[{args.worker_id}] Duplicate detected: "
                      f"event_id={event_id} already processed "
                      f"(shared record), skipping business action.",
                      flush=True)
                ch.basic_ack(delivery_tag=method.delivery_tag)
                return
            mark_processed(event_id)

        print(f"[{args.worker_id}] Processing order "
              f"{order['order_id']}: {order['quantity']} x "
              f"{order['item_id']}", flush=True)
        time.sleep(0.5)  # simulate work; keeps the demo distribution visible
        ch.basic_ack(delivery_tag=method.delivery_tag)
        print(f"[{args.worker_id}] Done: order {order['order_id']} "
              f"(event_id={event_id}) ACK sent.", flush=True)

    channel.basic_consume(queue=QUEUE, on_message_callback=callback,
                          arguments={"x-priority": args.priority})
    print(f"[{args.worker_id}] Waiting for messages. "
          f"To exit press CTRL+C", flush=True)
    try:
        channel.start_consuming()
    except KeyboardInterrupt:
        channel.stop_consuming()
        connection.close()
        sys.exit(0)


if __name__ == "__main__":
    main()

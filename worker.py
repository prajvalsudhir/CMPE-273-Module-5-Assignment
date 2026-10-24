"""Order Worker: consumes and processes order messages from RabbitMQ.

Each worker is an independent process. Two workers on the same queue
are competing consumers: RabbitMQ delivers each message to only one
of them. prefetch_count=1 gives fair dispatch (one unacked message
at a time per worker).

Usage:
    python worker.py --worker-id worker-1
    python worker.py --worker-id worker-2
    python worker.py --worker-id worker-1 --crash-on-event evt-001

Duplicate handling: event_ids already processed by this worker are
kept in an in-memory set. A repeat is ACKed but not processed again.
Failure handling: if the worker stops before ACKing (see
--crash-on-event), RabbitMQ requeues the message and delivers it
to another worker.
"""

import argparse
import json
import os
import sys
import time

import pika

QUEUE = "orders"


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--worker-id", default="worker-1")
    parser.add_argument("--crash-on-event", default=None,
                        help="Simulate a crash on this event_id: exit "
                             "without sending an ACK.")
    args = parser.parse_args()

    processed = set()  # in-memory store of processed event_ids

    connection = pika.BlockingConnection(
        pika.ConnectionParameters(host="localhost"))
    channel = connection.channel()
    channel.queue_declare(queue=QUEUE)
    channel.basic_qos(prefetch_count=1)

    def callback(ch, method, properties, body):
        order = json.loads(body)
        event_id = order["event_id"]
        print(f"[{args.worker_id}] Received: {order}", flush=True)

        if event_id in processed:
            print(f"[{args.worker_id}] Duplicate detected: "
                  f"event_id={event_id} already processed, "
                  f"skipping business action.", flush=True)
            ch.basic_ack(delivery_tag=method.delivery_tag)
            return

        if args.crash_on_event and event_id == args.crash_on_event:
            print(f"[{args.worker_id}] Simulating crash on "
                  f"event_id={event_id} (no ACK sent).", flush=True)
            # Exit without ACKing: RabbitMQ requeues the message.
            os._exit(1)

        print(f"[{args.worker_id}] Processing order "
              f"{order['order_id']}: {order['quantity']} x "
              f"{order['item_id']}", flush=True)
        time.sleep(0.5)  # simulate work; keeps the demo distribution visible
        processed.add(event_id)
        ch.basic_ack(delivery_tag=method.delivery_tag)
        print(f"[{args.worker_id}] Done: order {order['order_id']} "
              f"(event_id={event_id}) ACK sent.", flush=True)

    channel.basic_consume(queue=QUEUE, on_message_callback=callback)
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

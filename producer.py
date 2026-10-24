"""Order Producer: publishes order messages to a RabbitMQ queue.

Usage:
    python producer.py                  # publish 5 orders
    python producer.py --count 10       # publish 10 orders
    python producer.py --count 1 --event-id evt-001 --order-id ORD-001
"""

import argparse
import json
import uuid

import pika

QUEUE = "orders"


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--count", type=int, default=5)
    parser.add_argument("--event-id", default=None,
                        help="Reuse one event_id (for the duplicate demo).")
    parser.add_argument("--order-id", default=None)
    parser.add_argument("--item-id", default="ITEM-101")
    parser.add_argument("--quantity", type=int, default=2)
    args = parser.parse_args()

    connection = pika.BlockingConnection(
        pika.ConnectionParameters(host="localhost"))
    channel = connection.channel()
    channel.queue_declare(queue=QUEUE)

    for i in range(args.count):
        message = {
            "event_id": args.event_id or str(uuid.uuid4()),
            "order_id": args.order_id or f"ORD-{1001 + i}",
            "item_id": args.item_id,
            "quantity": args.quantity,
        }
        channel.basic_publish(
            exchange="",
            routing_key=QUEUE,
            body=json.dumps(message),
            properties=pika.BasicProperties(delivery_mode=2),
        )
        print(f"Published: {message}")

    connection.close()


if __name__ == "__main__":
    main()

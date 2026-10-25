# Order Producer and Worker (RabbitMQ)

Two independent Python processes communicating through a RabbitMQ queue.

## Architecture

![Architecture block diagram](diagrams/architecture.svg)

The producer publishes JSON order messages. Each worker is a separate
process on the same queue, so they are competing consumers: RabbitMQ
delivers each message to only one worker (`prefetch_count=1`).
A worker sends an ACK only after processing finishes.

## Running

Start RabbitMQ (local install, or Docker):

```bash
rabbitmq-server -detached
# or: docker run -d -p 5672:5672 rabbitmq:3
```

Install dependencies and run the workers in separate terminals:

```bash
pip install -r requirements.txt

python worker.py --worker-id worker-1
python worker.py --worker-id worker-2
```

Publish orders from another terminal:

```bash
python producer.py --count 6
```

Each message contains `event_id`, `order_id`, `item_id`, `quantity`:

```json
{"event_id": "37858e99-...", "order_id": "ORD-1001",
 "item_id": "ITEM-101", "quantity": 2}
```

## Successful message processing

```
[worker-1] Received: {'event_id': '...', 'order_id': 'ORD-1002',
                      'item_id': 'ITEM-101', 'quantity': 2}
[worker-1] Processing order ORD-1002: 2 x ITEM-101
[worker-1] Done: order ORD-1002 (event_id=...) ACK sent.
```

## Evidence: two competing consumers

6 orders published; worker-1 handled ORD-1002, ORD-1004, ORD-1005 and
worker-2 handled ORD-1001, ORD-1003, ORD-1006. No order appears in both
logs, and the queue was empty afterwards.

![Competing consumers](screenshots/competing-consumers.png)

## Evidence: failure and redelivery

Worker-1 was started with `--crash-on-event evt-crash-001`, so it
received ORD-9001 and exited without sending an ACK. RabbitMQ
redelivered the message to worker-2, which processed and ACKed it.
A later order (ORD-9002) was then processed normally, so the system
remained running.

```
[worker-1] Received: {'event_id': 'evt-crash-001', 'order_id': 'ORD-9001', ...}
[worker-1] Simulating crash on event_id=evt-crash-001 (no ACK sent).
[worker-2] Received: {'event_id': 'evt-crash-001', 'order_id': 'ORD-9001', ...}
[worker-2] Processing order ORD-9001: 2 x ITEM-101
[worker-2] Done: order ORD-9001 (event_id=evt-crash-001) ACK sent.
```

![Failure and redelivery](screenshots/failure-redelivery.png)

Reproduce it:

```bash
python worker.py --worker-id worker-1 --crash-on-event evt-crash-001
python worker.py --worker-id worker-2
python producer.py --count 1 --event-id evt-crash-001 --order-id ORD-9001
```

## Evidence: duplicate handling

The worker keeps processed `event_id` values in an in-memory set.
Publishing the same event twice (one worker running) processes it
once and skips the second copy:

```
[worker-1] Processing order ORD-8001: 2 x ITEM-101
[worker-1] Done: order ORD-8001 (event_id=evt-dup-001) ACK sent.
[worker-1] Received: {'event_id': 'evt-dup-001', 'order_id': 'ORD-8001', ...}
[worker-1] Duplicate detected: event_id=evt-dup-001 already processed,
           skipping business action.
```

The duplicate is still ACKed, so it is removed from the queue
without being processed again.

![Duplicate handling](screenshots/duplicate-handling.png)

Reproduce it:

```bash
python worker.py --worker-id worker-1
python producer.py --count 2 --event-id evt-dup-001 --order-id ORD-8001
```

## Reflection

A message can be delivered more than once because RabbitMQ only
removes a message when the consumer ACKs it, so if a worker crashes
or its connection drops before the ACK, the broker must redeliver
the message to keep it from being lost. That at-least-once delivery
means the same order can legitimately arrive twice, even though
nothing went wrong with the queue itself. Idempotent handling
solves this by keying the business action on `event_id`: the first
delivery is processed and recorded, and any repeat is recognised
and skipped. The result is that redelivery keeps the system
reliable without causing the order to be processed twice.

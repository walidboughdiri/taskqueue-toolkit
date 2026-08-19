# taskqueue-toolkit

Broker-agnostic async task queue for Python — RabbitMQ, SQS, SNS, Redis Streams, and Google Pub/Sub behind one `TaskQueue[T]` interface, plus an optional Postgres-backed outbox pattern.

## Why

Publishing/consuming a task shouldn't couple your code to one broker's client library. This package gives you:

- **One interface**: `publish(task)`, `consume() -> AsyncIterator[QueuedTask[T]]`, `ack()`/`nack(requeue=...)` — nothing broker-specific (no routing keys, topics, partitions, or consumer groups baked into the contract).
- **One connection string**: a single DSN's scheme picks the broker; everything else (queue/topic name, region, credentials, emulator endpoint, consumer group) travels as query params on the same string.
- **Generic over your payload**: `TaskQueue[T]` — bring your own task type and an `encode`/`decode` pair (`T -> bytes` / `bytes -> T`). This package has no opinion on serialization.
- **An optional outbox pattern**: write a task to Postgres in the same transaction as the state change that produced it, then let a background relay deliver it to the broker — no dual-write race between your database and your broker.

## Install

```bash
pip install taskqueue-toolkit[rabbitmq]      # or: redis-streams, aws, pubsub, outbox, all
```

## Usage

```python
from taskqueue_toolkit.queue.factory import create_task_queue

queue = create_task_queue(
    "amqp://guest:guest@localhost:5672/?queue=my.tasks",
    encode=lambda task: task.encode(),
    decode=lambda body: body.decode(),
)

await queue.publish("hello")

async for queued in queue.consume():
    print(queued.task)
    await queued.ack()
```

### DSN schemes

| Scheme | Broker | Example |
|---|---|---|
| `amqp` / `amqps` | RabbitMQ | `amqp://guest:guest@host:5672/?queue=my.tasks` |
| `redis` / `rediss` | Redis Streams | `redis://:password@host:6379/0?stream=my.tasks&group=workers&consumer=worker-1` |
| `sqs` | Amazon SQS | `sqs://eu-west-3/my.tasks?endpoint_url=...&access_key_id=...&secret_access_key=...` |
| `sns` | Amazon SNS (fan-out) | `sns://eu-west-3/my-tasks?endpoint_url=...&access_key_id=...&secret_access_key=...` |
| `pubsub` | Google Pub/Sub | `pubsub://project-id/my-tasks?subscription=my-tasks-subscriber&emulator_host=localhost:8085` |

### Adding a broker this package doesn't ship

The five schemes above are built in and checked for exhaustiveness at type-check time — `create_task_queue()` can't silently forget one. For a broker outside that list (Kafka, NATS, ...), register a handler once, at import/startup time, instead of forking the package:

```python
from taskqueue_toolkit.queue.registry import register_scheme


def build_kafka_queue(dsn, encode, decode):
    return MyKafkaTaskQueue(dsn=dsn, encode=encode, decode=decode)


register_scheme("kafka", build_kafka_queue)

queue = create_task_queue("kafka://localhost:9092/my-tasks", encode=..., decode=...)
```

A handler receives the raw DSN string plus the `encode`/`decode` pair and returns a `TaskQueue[T]` — parsing and construction happen together, so there's a single call to keep in sync, not a parser and a constructor spread across two files.

### Outbox pattern

```python
from taskqueue_toolkit.outbox.repository import OutboxRepository
from taskqueue_toolkit.outbox.relay import OutboxRelay

outbox = OutboxRepository(session=session, encode=encode, decode=decode)

# Inside the same transaction as your domain state change:
await outbox.enqueue(task)
await session.commit()

# In a background worker/process:
relay = OutboxRelay(outbox=outbox, task_queue=queue)
await relay.run_forever()
```

`OutboxRepository`/`OutboxRelay` are generic over the same `T` as `TaskQueue[T]`. Create the `outbox` table (`taskqueue_toolkit.outbox.orm.Base.metadata`) via your own migration tooling (Alembic, etc.).

**Database support**: only **Postgres** is tested and guaranteed — `claim_pending()`'s `SELECT ... FOR UPDATE SKIP LOCKED` concurrency guarantee is verified against a real Postgres in this package's own test suite (multiple relay instances racing to claim the same batch). MySQL 8.0+/MariaDB 10.6+ support the same `SKIP LOCKED` syntax through SQLAlchemy and may well work, but that combination isn't exercised by any test here — treat it as unverified, not supported, until you've validated the concurrent-claim behavior yourself against your actual engine/version. Older MySQL/MariaDB versions don't support `SKIP LOCKED` at all. SQLite works for single-writer tests (no real concurrent locking) but never for this guarantee under load.

**No NoSQL support (yet)**: the outbox is built directly on SQLAlchemy (`DeclarativeBase`, `select(...).with_for_update(skip_locked=True)`, `AsyncSession`) — this isn't a dialect difference to configure around, it's a relational, transactional row-locking primitive that document/key-value/wide-column stores (MongoDB, DynamoDB, Redis, Cassandra, ...) don't expose in an equivalent form. Adding NoSQL support is tracked as a future feature — it would need a separate `OutboxRepository` implementation per store family, each with its own concurrency mechanism in place of `SKIP LOCKED` (e.g. MongoDB's atomic `findOneAndUpdate`, DynamoDB's conditional `UpdateItem`), and would only preserve the outbox pattern's core guarantee (the task write and the domain state write land in the same transaction) when the outbox table lives in the same transactional store as the domain data it follows from — an outbox in a different store than your domain data reintroduces the dual-write race this pattern exists to avoid.

### Testing your own code

`taskqueue_toolkit.testing` ships an in-memory double for each broker adapter — a real, working implementation of the relevant slice of its SDK (FIFO ordering, ack/nack, consumer groups, fan-out, ...), not a call-recording mock — so you can test code that publishes/consumes tasks without a real RabbitMQ/SQS/SNS/Redis/Pub-Sub:

```python
from taskqueue_toolkit.queue.rabbitmq import RabbitMqTaskQueue
from taskqueue_toolkit.testing import FakeRabbitMqBroker

broker = FakeRabbitMqBroker()
queue = RabbitMqTaskQueue(dsn=dsn, encode=encode, decode=decode, connect=broker.connect)

await queue.publish("hello")
async for queued in queue.consume():
    assert queued.task == "hello"
    await queued.ack()
    break
```

Each adapter constructor takes an injectable client/connection factory for exactly this (`connect=` for RabbitMQ, `client_factory=` for SQS/SNS/Redis Streams, `publisher_factory=`/`subscriber_factory=` for Pub/Sub) — defaulting to the real SDK, so passing nothing behaves exactly as before.

| Fake | Inject into | Requires |
|---|---|---|
| `FakeRabbitMqBroker` | `RabbitMqTaskQueue(connect=broker.connect)` | nothing extra |
| `FakeSqsBroker` | `SqsTaskQueue(client_factory=broker.client)` | `[aws]` |
| `FakeSnsBroker` | `SnsTaskQueue(client_factory=broker.client)` | `[aws]` |
| `FakeRedisStreamsBroker` | `RedisStreamsTaskQueue(client_factory=broker.client)` | `[redis-streams]` |
| `FakePubsubBroker` | `PubsubTaskQueue(publisher_factory=broker.publisher, subscriber_factory=broker.subscriber)` | `[pubsub]` |

Create one broker instance per test — each owns its own isolated in-memory state, so there's nothing to reset between tests and no risk of one test's queue leaking into another's.

## Security

`Decoder[T]` runs on bytes received from a broker — in most deployments, a source outside this process's control. Using an unsafe deserializer (`pickle.loads`, `yaml.load` without `SafeLoader`, `eval`, ...) as your `decode` callback means whoever can publish to your queue/topic can run arbitrary code in your consumer. Use a safe format (JSON, msgpack, protobuf, ...) unless every publisher is fully trusted.

This package's own code is checked on every push/PR and before every release via [bandit](https://bandit.readthedocs.io/) (unsafe code patterns) and [pip-audit](https://github.com/pypa/pip-audit) (known CVEs in resolved dependencies), alongside ruff, mypy, and the test suite. Found a vulnerability? Please open an issue.

## Development

```bash
uv sync --all-extras
uv run pytest
uv run ruff check .
uv run ruff format .
uv run mypy .
uvx bandit -r src/
uvx pip-audit --path .venv
```

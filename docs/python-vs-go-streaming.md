# Python vs Go for Streaming Data Services

## The Core Problem: Concurrency Models

### Python's Split World

Python has two incompatible concurrency models living side by side:

| Model | Mechanism | Parallel? | Limitation |
|-------|-----------|-----------|------------|
| `threading` | Real OS threads | No (GIL) | Only one thread executes Python bytecode at a time |
| `asyncio` | Single-threaded event loop with coroutines | No | Must explicitly `await` to yield; synchronous calls freeze everything |

Both are concurrency without parallelism. `threading` gives you concurrent I/O (GIL releases during I/O), `asyncio` gives you cooperative multitasking on one thread. Neither gives you true parallel execution of Python code.

### Function Coloring Problem

```python
# Sync — blocks the thread, cannot be awaited
def sync_read():
    data = socket.recv(4096)  # OS thread blocks here

# Async — must be awaited, cannot call sync blocking code
async def async_read():
    data = await socket.recv(4096)  # Event loop switches to other work
```

Mixing these requires bridges (`run_in_executor`, `janus` queues, thread pools). Every library in the ecosystem picks a color, and you the developer must thread the needle between them.

### Go's Unified Model

```go
// One kind of function. Always concurrent-capable.
func read() {
    data, _ := socket.Read(buf) // Goroutine parks, runtime handles the rest
}
```

No `async`. No `await`. No function coloring. Every function can be run concurrently by prefixing `go`:

```go
go read()  // Runs concurrently in a new goroutine
```

---

## Goroutines: The Key Abstraction

A goroutine is a **userspace thread** — lightweight, scheduled by the Go runtime, not the OS kernel.

| | OS Thread (Python `threading`) | Goroutine |
|---|---|---|
| Created by | Kernel | Go runtime |
| Initial stack | ~1 MB | ~2 KB |
| Stack growth | Slow, fixed increments | Dynamic, grows/shrinks |
| Context switch | Kernel syscall (~1-10µs) | Userspace function call (~200ns) |
| Memory ceiling | Thousands exhaust RAM | Millions fit in one process |
| Scheduling | Preemptive (OS kernel) | Cooperative + preemptive (Go runtime since 1.14) |

---

## How Go Makes Blocking Code Non-Blocking

When a goroutine calls a blocking operation like `conn.Read()`:

1. Go registers the file descriptor with `epoll` (Linux) or `kqueue` (macOS).
2. The goroutine is **parked** — its state saved, removed from the OS thread.
3. The OS thread picks up another goroutine from the runnable queue.
4. When data arrives on the socket, epoll notifies Go.
5. Go marks the goroutine as runnable and places it back in the queue.
6. An OS thread picks it up and resumes right after `conn.Read()`.

From the goroutine's perspective: it called `Read()`, waited, and got data. Sequential, top-to-bottom code. The concurrency is invisible.

---

## The Scheduler

```
                    Go Runtime Scheduler
                    ┌─────────────────┐
                    │  Logical CPUs   │
                    │  (GOMAXPROCS)   │
                    │                 │
   OS Thread 1 ───→ │  P1  running    │
                    │    goroutine A  │ ← WebSocket read BTCUSDT
   OS Thread 2 ───→ │  P2  running    │
                    │    goroutine B  │ ← WebSocket read ETHUSDT
   OS Thread 3 ───→ │  P3  running    │
                    │    goroutine C  │ ← Kafka produce
                    │                 │
                    │  Parked:        │
                    │  goroutine D    │ ← waiting for epoll
                    │  goroutine E    │ ← waiting for Kafka ack
                    │  goroutine F    │ ← time.Sleep not expired
                    │                 │
                    │  Runnable:      │
                    │  goroutine G    │ ← data arrived, ready
                    └─────────────────┘
```

### GOMAXPROCS

- `GOMAXPROCS` limits OS threads **executing Go code simultaneously** (default: CPU core count).
- The runtime can create **additional** OS threads for blocked syscalls or cgo calls.
- These extra threads are parked in the kernel, not executing Go code, so they don't count against the limit.

---

## Summary Comparison

| | Python `asyncio` | Python `threading` | Go |
|---|---|---|---|
| Concurrency | Yes (cooperative) | Yes (preemptive) | Yes (runtime-managed) |
| Parallelism | No (single thread, GIL) | No (multiple threads, GIL) | Yes (multiple OS threads) |
| Unit of work | Coroutine | OS thread | Goroutine |
| Blocking I/O | Must `await` (freezes if forgotten) | Releases GIL, blocks thread | Runtime parks goroutine automatically |
| Who schedules | Event loop | OS kernel | Go runtime |
| Function coloring | Yes (`async`/`sync` split) | No (all blocking) | No (unified) |
| Memory per unit | ~1 KB (coroutine) | ~1 MB | ~2 KB |
| Scale ceiling | Thousands of coroutines | Hundreds of threads | Millions of goroutines |
| Library ecosystem split | Async vs sync variants everywhere | ⬇ | One standard library, everything works |

---

## Concrete Example: Binance WebSocket → Kafka

### Python (aiokafka — the cleanest approach)

```python
import asyncio
import json
import websockets
from aiokafka import AIOKafkaProducer

WEBSOCKET_URL = "wss://stream.binance.com:9443/ws/btcusdt@trade"

async def consume_trades():
    producer = AIOKafkaProducer(bootstrap_servers="localhost:9092")
    await producer.start()
    try:
        async with websockets.connect(WEBSOCKET_URL) as ws:
            async for message in ws:
                trade = json.loads(message)
                event = {
                    "symbol": trade["s"],
                    "price": float(trade["p"]),
                    "quantity": float(trade["q"]),
                    "trade_time": int(trade["T"]) // 1000,
                }
                await producer.send_and_wait(
                    "crypto_trades",
                    key=event["symbol"].encode(),
                    value=json.dumps(event).encode(),
                )
    finally:
        await producer.stop()

asyncio.run(consume_trades())
```

### Go (sarama/kafka-go — idiomatic)

```go
func streamTrades(symbol string) {
    url := fmt.Sprintf("wss://stream.binance.com:9443/ws/%s@trade", strings.ToLower(symbol))
    conn, _, _ := websocket.DefaultDialer.Dial(url, nil)
    defer conn.Close()

    for {
        _, message, err := conn.ReadMessage()
        if err != nil {
            log.Printf("disconnected: %v — reconnecting...", err)
            time.Sleep(3 * time.Second)
            conn, _, _ = websocket.DefaultDialer.Dial(url, nil)
            continue
        }

        // Goroutine blocks here. Other goroutines keep running.
        // OS thread stays productive with other goroutines.
        kafkaProducer.WriteMessages(context.Background(),
            kafka.Message{
                Topic: "crypto_trades",
                Key:   []byte(symbol),
                Value: message,
            },
        )
    }
}

func main() {
    go streamTrades("BTCUSDT")
    go streamTrades("ETHUSDT")
    go streamTrades("SOLUSDT")
    select {} // block forever
}
```

Note what's absent from the Go code: no `async`, no `await`, no `asyncio.run()`, no event loop, no `run_in_executor`, no thread pools, no `create_task`, no `gather`.

---

## Kafka Client Options in Python

### `confluent-kafka-python` (librdkafka C wrapper)

- Gold standard in industry
- C library underneath — extremely fast
- Has advanced features: idempotent producer, transactions, exactly-once semantics
- **Problem:** Synchronous API. Must thread-bridge into async code via `run_in_executor`, `janus` queues, or dedicated polling threads

### `aiokafka` (pure Python, async-native)

- Built for `asyncio`, no threads, no C dependencies
- Slower but irrelevant at WebSocket-scale rates (10-1000 events/sec)
- Lacks idempotent producer and transactions
- **Fit:** Natural fit for services already using `asyncio`

### Production Patterns (Python)

| Pattern | Description | When used |
|---------|-------------|-----------|
| Multi-process + sync | One process per WebSocket, synchronous Kafka sends, managed by supervisord | Most common at scale; dead simple |
| Async + thread bridge | `asyncio` for WebSocket, `janus` queue → dedicated thread for confluent-kafka | When corporate policy mandates confluent-kafka |
| Pure async (aiokafka) | Everything in `asyncio`, no threads | Growing in adoption; cleanest code |

---

## Decision Framework: Python vs Go for Streaming Services

### Choose Python when:

- The service is thin glue code (connect, normalize, forward)
- Event rate is under a few thousand per second
- Team already knows Python
- The goal is rapid prototyping
- Data science/ML integration is needed downstream

### Choose Go when:

- The service handles thousands+ of concurrent connections
- Per-message latency matters
- The team wants a single static binary with no runtime dependency
- The service is CPU-intensive (compression, serialization at scale)
- You're building core infrastructure that will live for years
- The codebase will grow beyond "thin glue" into complex stateful logic

### Why Go dominates in streaming infrastructure

The CNCF ecosystem is overwhelmingly Go: Kafka (via Sarama), NATS, Redpanda, Prometheus, Thanos, Loki, Grafana, etcd, Kubernetes itself — all Go. The language's concurrency model maps perfectly to streaming workloads (one goroutine per connection, one per partition, one per health check).

---

## Bottom Line

**For a portfolio project at WebSocket scale:** Either works. The architecture (Kafka as buffer, Flink for processing) is what matters, not the language.

**For a career in streaming infrastructure:** Learn Go. The concurrency model is the language, not a library bolted on later. What Java and Go developers get for free (synchronous-looking code that runs concurrently) Python requires ceremony to achieve.

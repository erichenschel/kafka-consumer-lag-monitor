# Kafka Consumer Lag Monitor

A pure-Python library that detects anomalous Kafka consumer lag without requiring a live Kafka cluster. It ingests a stream of per-partition offset snapshots, maintains an explicit OK/DEGRADED state machine per partition, and emits typed `Alert` objects on state transitions.

---

## What it does

Consumer lag is `producer_offset − consumer_offset`. Zero lag means the consumer is caught up. Growing or excessive lag means the consumer is falling behind — and in a pricing or ML pipeline, that means working with stale data.

`LagMonitor` watches two conditions per partition:

1. **Threshold** — lag exceeds a configurable absolute value
2. **Continuously growing** — lag increases strictly for `N` consecutive snapshots

When either condition trips, the partition moves from `OK → DEGRADED` and an `Alert` fires. When **both** conditions clear, the partition moves `DEGRADED → OK` and a recovery `Alert` fires. Alerts only fire on transitions — no duplicate pages during a sustained incident.

---

## How to run

```bash
pip install pytest
pytest -v
```

To see the simulator exercise four deterministic scenarios end-to-end:

```bash
python -m lag_monitor.simulator
```

Example output:
```
t=  19 | P3 |       ok → degraded | threshold_exceeded     | lag=  52 | warning  | streak=5 peak=52
t=  24 | P1 |       ok → degraded | threshold_exceeded     | lag=  52 | warning  | streak=5 peak=52
t=  30 | P2 |       ok → degraded | threshold_exceeded     | lag= 212 | critical | streak=1 peak=212
t=  37 | P2 | degraded → ok       | recovered              | lag=  44 | info     | streak=0 peak=212
```

---

## Design decisions

### Explicit state machine over stateless threshold checks
A naive approach re-evaluates lag against a threshold on every tick and emits an alert whenever the condition holds. That floods on-call with duplicate pages during a sustained incident. An explicit `OK ↔ DEGRADED` state machine fires exactly once per incident onset and once per recovery.

### Two orthogonal detection rules
Threshold and continuous growth catch different failure modes. A large sudden backlog (threshold) needs immediate attention. A consumer slowly falling behind (growth) needs attention before it becomes a backlog. One rule per failure mode, independently configurable.

### Hysteresis via state machine, not debounce timers
Recovery requires both conditions to be false simultaneously — not just a single below-threshold tick. This prevents false recovery alerts on noisy partitions that oscillate around the threshold.

### Bounded `deque` + `itertools.pairwise` for the growth check
Each partition keeps a `deque(maxlen=growth_window + 1)`. N+1 points are needed to check N consecutive increases. The growth check uses `itertools.pairwise` — a single-pass iterator with zero allocation in the hot path. The rule returns `False` until the window is full, so there are no false positives on startup.

### Typed `PartitionRecord` for internal state
Internal per-partition state lives in a `PartitionRecord` dataclass, not a `dict[str, Any]`. Type-safe, self-documenting, and visible to static analysis.

### Typed alert schema
`Alert` is a frozen dataclass — immutable after creation, since alerts are facts about the past. Fields include `topic`, `consumer_group`, and `partition_id` so an on-call engineer knows exactly which service and topic is affected without querying the monitoring system. `context` carries `growth_streak`, `peak_lag`, and `window_size` for triage.

### String enum values
`AlertReason.THRESHOLD_EXCEEDED.value == "threshold_exceeded"` rather than `1`. Self-documenting in logs, JSON serialization, and debugger output.

### Structured logging on transitions
The monitor uses a standard `logging.Logger` and emits a structured log on every state transition (with `partition_id`, `topic`, `consumer_group`, `reason`, `severity`, lag values via `extra`). This integrates cleanly with any downstream log aggregator (ELK, Splunk, Datadog) without a new dependency.

### TDD on the core
The monitor logic was written test-first: behaviors were expressed as failing tests, implementation followed. The simulator and data models were written conventionally — pure shapes and generators don't benefit from TDD.

### Deterministic simulator
The simulator uses pure arithmetic (linear rates, step functions) — no `random` module, no seeding. P2's spike decays linearly over 10 ticks so the recovery is physically plausible rather than an artificial offset jump.

---

## Simplifying assumptions

- **Not thread-safe.** A single monitor instance must not be shared across threads without external synchronization.
- **Single topic and consumer group.** The partition state key is `partition_id` alone; in a production deployment it would be `(topic, consumer_group, partition_id)`.
- **`CONTINUOUSLY_GROWING` always emits `WARNING`.** Threshold-based severity escalates to `CRITICAL` at 2× threshold; growth-based severity does not. A real deployment might escalate based on streak length.
- **`peak_lag` is scoped to the current DEGRADED period** and resets on each `OK → DEGRADED` transition.
- **Inputs are trusted.** `observe()` does not validate snapshots (e.g., negative offsets, `None` partition IDs). The ingestion layer is responsible for ensuring well-formed input; deferred here to avoid duplicating boundary checks.

---

## Production considerations (out of scope)

A full production deployment would need the following. None are implemented here — each is either called out in the spec as out of scope or would meaningfully distort the shape of a two-hour exercise.

- **Consumer group rebalance handling.** When partitions reassign between consumers, per-partition state may need to be invalidated or transferred. Not modeled by the simulator.
- **Time-based staleness detection.** If snapshots stop arriving for a partition, the monitor should eventually flag it. Requires a wall-clock notion; currently snapshot-driven only.
- **Query API.** Dashboards and health endpoints would want `get_partition_state(id)` and `iter_degraded()`. The current monitor exposes observation output only.
- **Metrics emission.** A Prometheus/StatsD exporter for transition counts, lag distribution percentiles, and partition-count gauges. The structured log is the seam for this; a dedicated emitter is not wired up.
- **Input validation.** Reject malformed snapshots at the boundary rather than allowing one poisoned message to corrupt a partition's growth history.
- **Configuration per partition / consumer group.** Different SLAs for different topics. Current design uses a single threshold + growth_window globally.
- **Persistent state.** The monitor is purely in-memory; a restart clears all partition history. A production deployment would either replay recent offsets or accept a cold-start penalty.

---

## AI usage

I used Claude to pressure-test the architecture before writing any code — specifically to enumerate edge cases (negative lag on offset reset, dual-trigger priority, startup window behavior) and to refine the alert schema for on-call utility (adding `topic` and `consumer_group` to the snapshot and alert). I also used it during implementation to double-check that `itertools.pairwise` was the most elegant option for the growth check and to stress-test the test suite against the assignment spec.

All design decisions, the implementation, and the test cases are my own. The code was written by hand. Claude served as a sounding board, not a code generator.

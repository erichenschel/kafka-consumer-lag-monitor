# Kafka Consumer Lag Monitor

## Overview

A pure-Python library that monitors Kafka consumer lag anomalies without requiring a
live cluster. It processes offset snapshots, maintains an explicit state machine per
partition, and emits typed `Alert` objects on state transitions.

## Quick Start

Requires Python 3.11+.

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
python -m pytest -q
python -m lag_monitor.simulator
```

## Sample Output

```text
t=  19 | P3 |       ok → degraded | threshold_exceeded     | lag=  52 | warning  | streak=5 peak=52
t=  24 | P1 |       ok → degraded | continuously_growing   | lag=  12 | warning  | streak=5 peak=12
t=  30 | P2 |       ok → degraded | threshold_exceeded     | lag= 212 | critical | streak=1 peak=212
t=  37 | P2 | degraded → ok       | recovered              | lag=  44 | info     | streak=0 peak=212
```

## Design

Consumer lag is `producer_offset − consumer_offset`. The monitor tracks two independent
conditions:

1. **Threshold** — lag meets or exceeds a configured absolute value
2. **Continuously growing** — lag increases strictly across `N` consecutive snapshots

**State machine over stateless checks.** Partitions move explicitly between `OK` and
`DEGRADED`. Alerts fire only on transitions — once per incident, once per recovery —
eliminating duplicate alerts during sustained degradation.

**Dual orthogonal rules.** Threshold detects sudden backlogs; growth detects slow
degradation. Each addresses a distinct failure mode and configures independently.

**Hysteresis via state, not timers.** Recovery requires both conditions clear
simultaneously. This prevents false recovery signals on oscillating partitions without
introducing time-based complexity.

**Immutable `Alert` schema.** Each alert is a frozen dataclass containing `topic`,
`consumer_group`, `partition_id`, severity, and a frozen context object (growth
streak, peak lag, window size) — enough for on-call triage without a follow-up query.

**Structured logging.** Standard `logging.Logger` emits transition events with partition
metadata attached via `extra`.

**Deterministic simulator.** Scenarios use pure arithmetic with fixed rates — no
randomization — so test runs are fully reproducible and edge cases are predictable.

## Constraints

- Not thread-safe without external synchronization
- Single topic and consumer group (state keyed by `partition_id` only)
- Growth detection is snapshot-count based, not wall-clock — callers control snapshot
  frequency to derive time-window semantics
- `CONTINUOUSLY_GROWING` always emits `WARNING` severity
- `peak_lag` scoped to the current `DEGRADED` period
- Inputs treated as trusted (no validation of negative offsets or malformed data)

## Out of Scope

Production systems typically use [Burrow](https://github.com/linkedin/Burrow) or Kafka
Lag Exporter. This implementation omits:

- Time-based staleness detection
- Consumer group rebalance handling
- Prometheus/StatsD metrics emission
- Per-partition configuration
- Persistent state across restarts

## AI Usage

AI assistance was used during design and polishing to identify edge cases, refine
the alert schema, and review test coverage. The final behavior is captured in the
implementation and pytest suite in this repository.

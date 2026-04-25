"""
Deterministic scenario simulator — no random module used.
All offsets are computed arithmetically (linear rates, step functions).

Partition scenarios:
  P0 — Normal:   consumer keeps pace with producer (lag stays near 0)
  P1 — Growing:  consumer stalls from snapshot 20; lag increases linearly
  P2 — Spike:    sudden lag spike at tick 30, decays gradually to baseline by tick 40
  P3 — Stalled:  consumer offset frozen from tick 15; producer keeps advancing
"""

import logging
from collections.abc import Generator

from lag_monitor.models import Snapshot
from lag_monitor.monitor import LagMonitor

NUM_PARTITIONS = 4
PRODUCER_RATE = 10           # offsets per tick, applied to all partitions
BASELINE_LAG = 2             # steady-state lag for healthy partitions
P2_SPIKE_MAGNITUDE = 210     # extra lag injected into P2 at tick 30
P2_SPIKE_TICK = 30
P2_RECOVERY_RATE = 24        # lag reduction per tick during P2 recovery (> PRODUCER_RATE)
P1_STALL_TICK = 20
P3_STALL_TICK = 15
DEFAULT_TOPIC = "market_data"
DEFAULT_CONSUMER_GROUP = "pricing_engine"


def _p2_extra_lag(tick: int) -> int:
    """Additional lag for P2 beyond baseline: spike at tick 30, linear decay to 0 by tick 40."""
    if tick < P2_SPIKE_TICK:
        return 0
    return max(0, P2_SPIKE_MAGNITUDE - (tick - P2_SPIKE_TICK) * P2_RECOVERY_RATE)


def simulate_stream(num_snapshots: int = 60) -> Generator[Snapshot, None, None]:
    producer = [0] * NUM_PARTITIONS
    consumer = [0] * NUM_PARTITIONS

    for tick in range(num_snapshots):
        t = float(tick)

        for p in range(NUM_PARTITIONS):
            producer[p] += PRODUCER_RATE

        # P0 — always keeps pace
        consumer[0] = producer[0] - BASELINE_LAG

        # P1 — keeps pace until P1_STALL_TICK, then consumer freezes
        if tick < P1_STALL_TICK:
            consumer[1] = producer[1] - BASELINE_LAG
        # else: consumer[1] unchanged from previous tick

        # P2 — baseline with a spike at tick 30 that decays smoothly back to baseline
        consumer[2] = producer[2] - BASELINE_LAG - _p2_extra_lag(tick)

        # P3 — keeps pace until P3_STALL_TICK, then consumer freezes
        if tick < P3_STALL_TICK:
            consumer[3] = producer[3] - BASELINE_LAG
        # else: consumer[3] unchanged

        for p in range(NUM_PARTITIONS):
            yield Snapshot(
                partition_id=p,
                producer_offset=producer[p],
                consumer_offset=consumer[p],
                timestamp=t,
                topic=DEFAULT_TOPIC,
                consumer_group=DEFAULT_CONSUMER_GROUP,
            )


if __name__ == "__main__":
    # Enable the monitor's structured logging for the demo. In production, a JSON
    # formatter would consume the `extra` fields attached to each LogRecord; here
    # we show that the logger fires on every transition.
    logging.basicConfig(
        level=logging.INFO,
        format="[log] %(name)s: %(message)s "
               "pid=%(partition_id)s topic=%(topic)s reason=%(reason)s "
               "severity=%(severity)s current_lag=%(current_lag)s",
    )

    monitor = LagMonitor(threshold=50, growth_window=5)
    for snapshot in simulate_stream(num_snapshots=60):
        for alert in monitor.observe(snapshot):
            print(
                f"t={alert.timestamp:>4.0f} | P{alert.partition_id} | "
                f"{alert.previous_state.value:>8} → {alert.current_state.value:<8} | "
                f"{alert.reason.value:<22} | lag={alert.current_lag:>4} | "
                f"{alert.severity.value:<8} | "
                f"streak={alert.context['growth_streak']} peak={alert.context['peak_lag']}"
            )

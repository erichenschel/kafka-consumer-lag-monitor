"""
LagMonitor — tracks per-partition Kafka consumer lag and emits typed Alert objects
on OK ↔ DEGRADED state transitions.

Not thread-safe: callers must synchronize externally if sharing an instance across threads.
"""

import logging
from collections import deque
from dataclasses import dataclass
from itertools import pairwise

from lag_monitor.models import Alert, AlertContext, AlertReason, PartitionState, Severity, Snapshot

logger = logging.getLogger(__name__)


@dataclass
class PartitionRecord:
    state: PartitionState
    lag_history: deque[int]
    last_lag: int = 0
    peak_lag: int = 0


class LagMonitor:
    def __init__(self, threshold: int, growth_window: int) -> None:
        if threshold <= 0:
            raise ValueError("threshold must be positive")
        if growth_window < 2:
            raise ValueError("growth_window must be >= 2")
        self.threshold = threshold
        self.growth_window = growth_window
        self._state: dict[int, PartitionRecord] = {}

    def observe(self, snapshot: Snapshot) -> list[Alert]:
        """Process a snapshot and return any alerts fired by state transitions."""
        lag = max(0, snapshot.producer_offset - snapshot.consumer_offset)
        pid = snapshot.partition_id
        record = self._get_or_create_record(pid)

        prev_lag = record.last_lag
        prev_state = record.state

        record.lag_history.append(lag)
        record.last_lag = lag

        exceeds = self._exceeds_threshold(lag)
        growing = self._is_continuously_growing(record.lag_history)

        alerts: list[Alert] = []

        if prev_state == PartitionState.OK and (exceeds or growing):
            record.state = PartitionState.DEGRADED
            record.peak_lag = lag
            reason = AlertReason.THRESHOLD_EXCEEDED if exceeds else AlertReason.CONTINUOUSLY_GROWING
            alerts.append(self._make_alert(snapshot, PartitionState.OK, PartitionState.DEGRADED, reason, lag, prev_lag, record))

        elif prev_state == PartitionState.DEGRADED:
            record.peak_lag = max(record.peak_lag, lag)
            if not exceeds and not growing:
                record.state = PartitionState.OK
                alerts.append(self._make_alert(snapshot, PartitionState.DEGRADED, PartitionState.OK, AlertReason.RECOVERED, lag, prev_lag, record))

        for alert in alerts:
            logger.info(
                "partition state transition",
                extra={
                    "partition_id": alert.partition_id,
                    "topic": alert.topic,
                    "consumer_group": alert.consumer_group,
                    "previous_state": alert.previous_state.value,
                    "current_state": alert.current_state.value,
                    "reason": alert.reason.value,
                    "severity": alert.severity.value,
                    "current_lag": alert.current_lag,
                    "previous_lag": alert.previous_lag,
                },
            )
        return alerts

    def _get_or_create_record(self, partition_id: int) -> PartitionRecord:
        record = self._state.get(partition_id)
        if record is None:
            record = PartitionRecord(
                state=PartitionState.OK,
                lag_history=deque(maxlen=self.growth_window + 1),
            )
            self._state[partition_id] = record
        return record

    def _exceeds_threshold(self, lag: int) -> bool:
        return lag >= self.threshold

    def _is_continuously_growing(self, history: deque[int]) -> bool:
        """Return True after growth_window consecutive lag increases.

        N consecutive increases require N + 1 snapshots, because each increase
        is the difference between adjacent lag readings.
        """
        if len(history) < self.growth_window + 1:
            return False
        return all(b > a for a, b in pairwise(history))

    def _make_alert(
        self,
        snapshot: Snapshot,
        previous_state: PartitionState,
        current_state: PartitionState,
        reason: AlertReason,
        current_lag: int,
        previous_lag: int,
        record: PartitionRecord,
    ) -> Alert:
        return Alert(
            timestamp=snapshot.timestamp,
            partition_id=snapshot.partition_id,
            topic=snapshot.topic,
            consumer_group=snapshot.consumer_group,
            previous_state=previous_state,
            current_state=current_state,
            reason=reason,
            current_lag=current_lag,
            previous_lag=previous_lag,
            severity=self._severity(reason, current_lag),
            context=AlertContext(
                growth_streak=self._current_streak(record.lag_history),
                peak_lag=record.peak_lag,
                window_size=self.growth_window,
            ),
        )

    def _severity(self, reason: AlertReason, lag: int) -> Severity:
        if reason == AlertReason.RECOVERED:
            return Severity.INFO
        if reason == AlertReason.THRESHOLD_EXCEEDED:
            return Severity.CRITICAL if lag >= 2 * self.threshold else Severity.WARNING
        return Severity.WARNING  # CONTINUOUSLY_GROWING

    def _current_streak(self, history: deque[int]) -> int:
        streak = 0
        for a, b in pairwise(history):
            if b > a:
                streak += 1
            else:
                streak = 0
        return streak

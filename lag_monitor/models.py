from dataclasses import dataclass, field
from enum import StrEnum


class PartitionState(StrEnum):
    OK = "ok"
    DEGRADED = "degraded"


class AlertReason(StrEnum):
    THRESHOLD_EXCEEDED = "threshold_exceeded"
    CONTINUOUSLY_GROWING = "continuously_growing"
    RECOVERED = "recovered"


class Severity(StrEnum):
    INFO = "info"
    WARNING = "warning"
    CRITICAL = "critical"


@dataclass(frozen=True)
class AlertContext:
    """Structured context attached to every Alert for on-call triage.

    Attributes:
        growth_streak: Number of consecutive strictly-increasing lag values
            ending at the moment of the alert. Ranges 0..growth_window.
        peak_lag: Maximum lag observed during the current DEGRADED period;
            resets on every OK → DEGRADED transition.
        window_size: The configured growth_window at the time of the alert.
    """

    growth_streak: int = 0
    peak_lag: int = 0
    window_size: int = 0


@dataclass(frozen=True)
class Snapshot:
    """Point-in-time offset reading for a single Kafka partition.

    Mirrors the minimal subset of AdminClient.listConsumerGroupOffsets() +
    describeTopics() output that is needed to compute consumer lag. Fields
    log_start_offset, member_id, client_id, and host — which a real Kafka
    AdminClient also returns — are intentionally not modeled here.

    Attributes:
        partition_id: Zero-indexed partition within the topic.
        producer_offset: Log end offset (high-water mark) for the partition.
        consumer_offset: Committed offset for the consumer group on this
            partition — the value persisted to __consumer_offsets, not the
            in-memory current position.
        timestamp: Unix epoch seconds at which the snapshot was taken.
        topic: Kafka topic name.
        consumer_group: Consumer group ID that owns the committed offset.
    """

    partition_id: int
    producer_offset: int
    consumer_offset: int
    timestamp: float
    topic: str
    consumer_group: str


@dataclass(frozen=True)
class Alert:
    """A state-transition event emitted by LagMonitor.

    Alerts fire exactly once per OK → DEGRADED transition and once per
    DEGRADED → OK transition — never while a partition sits in a stable
    state. The `previous_state` / `current_state` pair makes the transition
    explicit; `reason` identifies which rule fired. `context` carries the
    structured triage fields described on AlertContext.

    Because both Alert and AlertContext are frozen dataclasses, callers cannot
    mutate the top-level event fields or its structured context in place.
    """

    timestamp: float
    partition_id: int
    topic: str
    consumer_group: str
    previous_state: PartitionState
    current_state: PartitionState
    reason: AlertReason
    current_lag: int
    previous_lag: int
    severity: Severity
    context: AlertContext = field(default_factory=AlertContext)

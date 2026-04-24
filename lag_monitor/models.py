from dataclasses import dataclass, field
from enum import Enum
from typing import Any


class PartitionState(Enum):
    OK = "ok"
    DEGRADED = "degraded"


class AlertReason(Enum):
    THRESHOLD_EXCEEDED = "threshold_exceeded"
    CONTINUOUSLY_GROWING = "continuously_growing"
    RECOVERED = "recovered"


class Severity(Enum):
    INFO = "info"
    WARNING = "warning"
    CRITICAL = "critical"


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
    # Keys: growth_streak (int), peak_lag (int), window_size (int)
    # frozen=True prevents field reassignment; treat dict contents as read-only
    context: dict[str, Any] = field(default_factory=dict)

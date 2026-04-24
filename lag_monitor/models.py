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
    partition_id: int
    producer_offset: int
    consumer_offset: int
    timestamp: float
    topic: str = "market_data"
    consumer_group: str = "pricing_engine"


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

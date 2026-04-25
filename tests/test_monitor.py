from lag_monitor import LagMonitor, Snapshot, Alert, AlertReason, PartitionState, Severity


def snap(partition_id: int, producer: int, consumer: int, t: float = 0.0) -> Snapshot:
    return Snapshot(
        partition_id=partition_id,
        producer_offset=producer,
        consumer_offset=consumer,
        timestamp=t,
        topic="test_topic",
        consumer_group="test_group",
    )


def growing_snapshots(partition_id: int, start_lag: int, steps: int, step_size: int = 10) -> list[Snapshot]:
    """Return snapshots where lag grows by step_size each tick."""
    snapshots = []
    base_producer = 1000
    for i in range(steps):
        lag = start_lag + i * step_size
        snapshots.append(snap(partition_id, base_producer + lag + i, base_producer + i, float(i)))
    return snapshots


# ── 1. Lag math ──────────────────────────────────────────────────────────────

def test_lag_equals_producer_minus_consumer():
    """Alert emitted at the threshold reports current_lag = producer - consumer."""
    monitor = LagMonitor(threshold=50, growth_window=3)
    alerts = monitor.observe(snap(0, producer=500, consumer=450))  # lag=50, triggers
    assert len(alerts) == 1
    assert alerts[0].current_lag == 50


def test_negative_lag_clamped_to_zero():
    """Consumer offset ahead of producer (e.g. after offset reset) should produce no alert at any positive threshold."""
    monitor = LagMonitor(threshold=1, growth_window=3)
    alerts = monitor.observe(snap(0, producer=100, consumer=200))  # would be lag=-100; clamped to 0
    assert alerts == []


# ── 2. Threshold rule ─────────────────────────────────────────────────────────

def test_first_observation_above_threshold_triggers_immediate_degradation():
    monitor = LagMonitor(threshold=50, growth_window=5)
    alerts = monitor.observe(snap(0, producer=1000, consumer=900))  # lag=100, above 50
    assert len(alerts) == 1
    alert = alerts[0]
    assert alert.reason == AlertReason.THRESHOLD_EXCEEDED
    assert alert.previous_state == PartitionState.OK
    assert alert.current_state == PartitionState.DEGRADED
    assert alert.current_lag == 100
    assert alert.previous_lag == 0


def test_threshold_fires_exactly_once_on_crossing():
    monitor = LagMonitor(threshold=50, growth_window=5)
    monitor.observe(snap(0, producer=1000, consumer=980))  # lag=20, OK
    alerts = monitor.observe(snap(0, producer=1000, consumer=940))  # lag=60, crosses threshold
    assert len(alerts) == 1
    assert alerts[0].reason == AlertReason.THRESHOLD_EXCEEDED
    assert alerts[0].severity == Severity.WARNING


def test_severity_is_critical_at_double_threshold():
    monitor = LagMonitor(threshold=50, growth_window=5)
    alerts = monitor.observe(snap(0, producer=1000, consumer=890))  # lag=110 >= 2*50
    assert len(alerts) == 1
    assert alerts[0].severity == Severity.CRITICAL


def test_no_duplicate_alert_while_degraded():
    monitor = LagMonitor(threshold=50, growth_window=5)
    monitor.observe(snap(0, producer=1000, consumer=900))  # crosses threshold → DEGRADED
    further_alerts = monitor.observe(snap(0, producer=1000, consumer=800))  # still degraded
    assert further_alerts == []


def test_recovery_fires_exactly_once_on_return_to_health():
    monitor = LagMonitor(threshold=50, growth_window=5)
    monitor.observe(snap(0, producer=1000, consumer=900))  # → DEGRADED
    monitor.observe(snap(0, producer=1000, consumer=800))  # still DEGRADED
    alerts = monitor.observe(snap(0, producer=1000, consumer=990))  # lag=10, below threshold → OK
    assert len(alerts) == 1
    alert = alerts[0]
    assert alert.reason == AlertReason.RECOVERED
    assert alert.severity == Severity.INFO
    assert alert.previous_state == PartitionState.DEGRADED
    assert alert.current_state == PartitionState.OK


# ── 3. Growth rule ────────────────────────────────────────────────────────────

def test_growth_rule_fires_after_n_consecutive_increases():
    monitor = LagMonitor(threshold=10_000, growth_window=3)  # threshold far away
    snapshots = growing_snapshots(partition_id=0, start_lag=10, steps=4, step_size=5)
    all_alerts = []
    for s in snapshots:
        all_alerts.extend(monitor.observe(s))
    assert len(all_alerts) == 1
    alert = all_alerts[0]
    assert alert.reason == AlertReason.CONTINUOUSLY_GROWING
    assert alert.context.growth_streak == 3
    assert alert.context.window_size == 3


def test_growth_rule_does_not_fire_with_plateau():
    """A flat tick inside the window breaks strict monotonicity — the rule must not fire prematurely."""
    monitor = LagMonitor(threshold=10_000, growth_window=3)
    # Without plateau: [10, 20, 30, 40] would fire at tick 3 (window full, all increasing)
    # With plateau at tick 3: deque at tick 4 = [20, 30, 30, 40] — 30→30 fails strict check
    lags = [10, 20, 30, 30, 40]
    base = 1000
    alerts = []
    for i, lag in enumerate(lags):
        alerts.extend(monitor.observe(snap(0, producer=base + lag + i, consumer=base + i, t=float(i))))
    assert alerts == [], "Plateau inside the window should prevent growth rule from firing"


def test_stalled_consumer_triggers_via_growth_rule():
    """Producer advances, consumer frozen → lag grows every tick → growth rule fires."""
    monitor = LagMonitor(threshold=10_000, growth_window=3)
    consumer_offset = 500
    alerts = []
    for i in range(5):
        alerts.extend(monitor.observe(snap(0, producer=500 + (i + 1) * 10, consumer=consumer_offset, t=float(i))))
    assert any(a.reason == AlertReason.CONTINUOUSLY_GROWING for a in alerts)


# ── 4. Dual-trigger priority ──────────────────────────────────────────────────

def test_threshold_exceeded_reason_takes_priority_over_growing():
    """When both threshold and growth conditions are true, THRESHOLD_EXCEEDED wins."""
    monitor = LagMonitor(threshold=50, growth_window=3)
    # Fill the growth window with strictly increasing lags that also exceed threshold
    snapshots = growing_snapshots(partition_id=0, start_lag=60, steps=4, step_size=10)
    alerts = []
    for s in snapshots:
        alerts.extend(monitor.observe(s))
    assert len(alerts) == 1
    assert alerts[0].reason == AlertReason.THRESHOLD_EXCEEDED


# ── 5. Independent partition tracking ─────────────────────────────────────────

def test_partitions_tracked_independently():
    """Degradation of one partition must not leak into alerts for another."""
    monitor = LagMonitor(threshold=50, growth_window=5)
    alerts_p0 = monitor.observe(snap(0, producer=100, consumer=98))   # P0 lag=2
    alerts_p1 = monitor.observe(snap(1, producer=100, consumer=30))   # P1 lag=70 → DEGRADED
    alerts_p0_again = monitor.observe(snap(0, producer=120, consumer=118))  # P0 still healthy
    assert alerts_p0 == []
    assert len(alerts_p1) == 1
    assert alerts_p1[0].partition_id == 1
    assert alerts_p0_again == []


# ── 6. Integration ────────────────────────────────────────────────────────────

def test_simulator_integration_all_four_partitions():
    """End-to-end: each simulator scenario produces the exact expected alert sequence."""
    from lag_monitor.simulator import simulate_stream

    monitor = LagMonitor(threshold=50, growth_window=5)
    alerts_by_partition: dict[int, list[Alert]] = {0: [], 1: [], 2: [], 3: []}

    for snapshot in simulate_stream(num_snapshots=60):
        for alert in monitor.observe(snapshot):
            alerts_by_partition[alert.partition_id].append(alert)

    # P0 — normal, never alerts
    assert alerts_by_partition[0] == []

    # P1 — slows at tick 20, degrades via growth before crossing threshold
    assert len(alerts_by_partition[1]) == 1
    assert alerts_by_partition[1][0].reason == AlertReason.CONTINUOUSLY_GROWING
    assert alerts_by_partition[1][0].current_state == PartitionState.DEGRADED
    assert alerts_by_partition[1][0].current_lag < monitor.threshold

    # P2 — spike at tick 30, degrades (CRITICAL, lag >> 2x threshold), recovers gradually
    assert len(alerts_by_partition[2]) == 2
    degraded, recovered = alerts_by_partition[2]
    assert degraded.reason == AlertReason.THRESHOLD_EXCEEDED
    assert degraded.severity == Severity.CRITICAL
    assert degraded.current_state == PartitionState.DEGRADED
    assert recovered.reason == AlertReason.RECOVERED
    assert recovered.current_state == PartitionState.OK

    # P3 — stalls at tick 15, degrades via threshold, never recovers (stalled)
    assert len(alerts_by_partition[3]) == 1
    assert alerts_by_partition[3][0].reason == AlertReason.THRESHOLD_EXCEEDED
    assert alerts_by_partition[3][0].current_state == PartitionState.DEGRADED

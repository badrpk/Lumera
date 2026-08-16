from telemetry import Alert, Reading, TelemetryStream


def test_ingest_and_latest():
    s = TelemetryStream()
    ok, alerts = s.ingest(Reading("m1", 1000, 500.0, 230.0, 50.0))
    assert ok is True
    assert alerts == ()
    assert s.latest("m1").watts == 500.0


def test_duplicate_timestamp_per_meter_rejected():
    s = TelemetryStream()
    s.ingest(Reading("m1", 1000, 500.0))
    ok, alerts = s.ingest(Reading("m1", 1000, 700.0))
    assert ok is False
    assert alerts == ()


def test_out_of_order_readings_are_sorted():
    s = TelemetryStream()
    s.ingest(Reading("m1", 2000, 700.0))
    s.ingest(Reading("m1", 1000, 500.0))
    assert [r.ts_ms for r in s.window("m1", since_ms=0)] == [1000, 2000]


def test_retention_keeps_latest_rows():
    s = TelemetryStream(retention_per_meter=2)
    s.ingest(Reading("m1", 1, 10.0))
    s.ingest(Reading("m1", 2, 20.0))
    s.ingest(Reading("m1", 3, 30.0))
    assert [r.ts_ms for r in s.window("m1", since_ms=0)] == [2, 3]


def test_stale_detection():
    s = TelemetryStream()
    assert s.is_stale("missing", now_ms=1000, max_age_ms=100) is True
    s.ingest(Reading("m1", 900, 10.0))
    assert s.is_stale("m1", now_ms=1000, max_age_ms=100) is False
    assert s.is_stale("m1", now_ms=1001, max_age_ms=100) is True


def test_threshold_alerts():
    s = TelemetryStream()
    s.set_thresholds("m1", high_watts=1000, low_voltage=210, high_voltage=250)
    _, alerts = s.ingest(Reading("m1", 1, 1200.0, 205.0))
    kinds = [a.kind for a in alerts]
    assert kinds == ["high_watts", "low_voltage"]


def test_delta_alert():
    s = TelemetryStream()
    s.set_thresholds("m1", max_delta_watts=100)
    s.ingest(Reading("m1", 1, 100.0))
    _, alerts = s.ingest(Reading("m1", 2, 250.0))
    assert len(alerts) == 1
    assert alerts[0].kind == "delta_watts"
    assert alerts[0].value == 150.0


def test_summary_values():
    s = TelemetryStream()
    s.ingest(Reading("m1", 1, 100.0))
    s.ingest(Reading("m1", 2, 300.0))
    summary = s.summary("m1")
    assert summary["count"] == 2
    assert summary["avg_watts"] == 200.0
    assert summary["min_watts"] == 100.0
    assert summary["max_watts"] == 300.0


def test_snapshot_hash_reproducible():
    one = TelemetryStream()
    two = TelemetryStream()
    for stream in (one, two):
        stream.set_thresholds("m1", high_watts=1000)
        stream.ingest(Reading("m1", 1, 100.0, 230.0))
    assert one.snapshot()["snapshot_hash"] == two.snapshot()["snapshot_hash"]

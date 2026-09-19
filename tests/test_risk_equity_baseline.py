from datetime import date

from ultimate_v1.risk_controller import _continuous_equity_segment


def test_continuous_equity_segment_keeps_normal_drawdown(monkeypatch):
    monkeypatch.setenv("RISK_EQUITY_RESET_RATIO", "0.35")
    rows = [
        (date(2026, 9, 14), 10_000.0),
        (date(2026, 9, 15), 9_500.0),
        (date(2026, 9, 16), 9_100.0),
    ]

    segment, reset = _continuous_equity_segment(rows)

    assert segment == rows
    assert reset is False


def test_continuous_equity_segment_resets_after_capital_change(monkeypatch):
    monkeypatch.setenv("RISK_EQUITY_RESET_RATIO", "0.35")
    rows = [
        (date(2026, 6, 3), 26_120.16),
        (date(2026, 8, 16), 6_141.97),
        (date(2026, 9, 15), 3_292.28),
        (date(2026, 9, 16), 3_161.04),
    ]

    segment, reset = _continuous_equity_segment(rows)

    assert segment == rows[-2:]
    assert reset is True

"""Weekly training/serving skew check."""

import numpy as np
import pandas as pd
import pytest

import drift
import shadow

F = shadow.FEATURE_COLUMNS


def test_psi_is_zero_for_the_same_distribution_and_large_for_a_shift():
    rng = np.random.default_rng(0)
    ref = rng.normal(size=20000)
    assert drift.psi(ref, rng.normal(size=5000)) < 0.01
    assert drift.psi(ref, rng.normal(1.0, 1, size=5000)) > drift.PSI_DRIFT


def test_psi_counts_missing_values_as_their_own_bin():
    ref = pd.Series(np.arange(1000, dtype=float))
    live = ref.copy()
    live[:500] = np.nan                       # half the live values missing
    assert drift.psi(ref, live) > drift.PSI_DRIFT


def test_go_rate_interval():
    r = drift.go_rate([True] * 2 + [False] * 98)
    assert r["rate"] == pytest.approx(0.02) and r["consistent_with_design"]
    r = drift.go_rate([True] * 1 + [False] * 999)
    assert r["ci95"][1] < 0.02 and not r["consistent_with_design"]


class Baseline:
    def __init__(self):
        self.features, self.threshold = F, 0.9
        self.meta, self.trained_through = {"calibration_from": "2026-06-01"}, "2026-08-31"

        class M:
            def predict_proba(_, X):
                p = 1 / (1 + np.exp(-X[F[0]].to_numpy()))
                return np.column_stack([1 - p, p])
        self.model = M()


def frames(sessions, shift=0.0, seed=0):
    rng = np.random.default_rng(seed)
    hist = pd.DataFrame(rng.normal(size=(5000, len(F))), columns=F)
    hist["date"] = np.repeat(pd.date_range("2026-01-01", periods=100).strftime("%Y-%m-%d"), 50)
    live = pd.DataFrame(rng.normal(size=(sessions * 40, len(F))), columns=[f"x_{f}" for f in F])
    live[f"x_{F[3]}"] += shift
    live["date"] = np.repeat([f"2026-10-{d:02d}" for d in range(5, 5 + sessions)], 40)
    live["baseline_score"] = 1 / (1 + np.exp(-live[f"x_{F[0]}"]))
    live["model_go"] = live["baseline_go"] = live["baseline_score"] >= 0.9
    return hist, live


def test_waits_for_a_full_live_week():
    hist, live = frames(sessions=3)
    r = drift.compute(hist, live, Baseline())
    assert r["status"].startswith("waiting") and "features" not in r


def test_flags_the_feature_that_moved():
    hist, live = frames(sessions=5, shift=1.5)
    r = drift.compute(hist, live, Baseline())
    assert r["status"] == "ok" and r["drifted"] == [F[3]]
    assert r["features"][0]["feature"] == F[3]
    assert set(r["go_rate"]) == {"champion", "baseline"}


def test_results_section_is_replaced_not_duplicated(tmp_path):
    hist, live = frames(sessions=5)
    md = drift.markdown(drift.compute(hist, live, Baseline()))
    path = tmp_path / "RESULTS.md"
    path.write_text("# Results\n\nbody\n")
    drift.write_results(md, path)
    drift.write_results(md, path)
    text = path.read_text()
    assert text.count(drift.START) == 1 and text.startswith("# Results\n\nbody")
    assert "Live GO rate" in text


def test_scorecard_shows_the_drift_report(tmp_path, monkeypatch):
    import json
    import webapp
    hist, live = frames(sessions=5, shift=1.5)
    hist["date"] = np.repeat(pd.date_range("2026-06-01", periods=100).strftime("%Y-%m-%d"), 50)
    r = drift.compute(hist, live, Baseline())
    monkeypatch.setattr(shadow, "LIVE_DIR", tmp_path)
    monkeypatch.setattr(shadow, "SIGNALS_FILE", tmp_path / "s.csv")
    monkeypatch.setattr(shadow, "OUTCOMES_FILE", tmp_path / "o.csv")
    (tmp_path / "drift.json").write_text(json.dumps(r, default=str))
    monkeypatch.setattr(webapp, "CHECKER", None)
    html = webapp.app.test_client().get("/scorecard").get_data(as_text=True)
    assert "LIVE GO RATE" in html and "DRIFTED FEATURES" in html and F[3] in html


def test_a_day_level_feature_is_not_flagged_for_being_constant_within_days():
    """A market-wide input is one value per day: a short live window always has a big plain
    PSI, but no bigger than any short stretch of history — so it isn't flagged."""
    rng = np.random.default_rng(3)
    days = pd.date_range("2025-01-01", periods=400).strftime("%Y-%m-%d")
    hist = pd.DataFrame(rng.normal(size=(400 * 30, len(F))), columns=F)
    hist["date"] = np.repeat(days, 30)
    hist[F[9]] = np.repeat(rng.normal(size=400), 30)          # same for every signal of a day
    live = pd.DataFrame(rng.normal(size=(5 * 30, len(F))), columns=[f"x_{f}" for f in F])
    live[f"x_{F[9]}"] = np.repeat(rng.normal(size=5), 30)
    live["date"] = np.repeat([f"2026-10-{d:02d}" for d in range(5, 10)], 30)
    live["baseline_score"] = 0.5
    live["model_go"] = live["baseline_go"] = False
    r = drift.compute(hist, live, Baseline())
    row = next(x for x in r["features"] if x["feature"] == F[9])
    assert row["psi"] >= drift.PSI_DRIFT                     # the naive rule would flag it
    assert F[9] not in r["drifted"]                         # the window-aware rule doesn't

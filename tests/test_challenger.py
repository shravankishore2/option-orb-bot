"""Monthly champion/challenger: leakage checks, the held-out comparison, the
promotion rule, versioning, and that the v2 baseline is never replaced."""

import datetime as dt
import json

import numpy as np
import pandas as pd
import pytest

import challenger as Ch
import registry
import shadow

F = shadow.FEATURE_COLUMNS


def sessions(start, n):
    d, out = start, []
    while len(out) < n:
        if d.weekday() < 5:
            out.append(d.isoformat())
        d += dt.timedelta(days=1)
    return out


def dataset(n_train_days=250, per_day=50, seed=0, start=dt.date(2025, 9, 1)):
    """Signals whose P&L is driven by the first feature (learnable)."""
    rng = np.random.default_rng(seed)
    days = sessions(start, n_train_days) + sessions(dt.date(2026, 9, 1), 22)
    days = sorted({d for d in days if d < "2026-10-01"})
    rows = len(days) * per_day
    df = pd.DataFrame(rng.normal(size=(rows, len(F))), columns=F)
    df["date"] = np.repeat(days, per_day)
    df["time"] = "10:00:00"
    df["symbol"] = [f"S{i % per_day}" for i in range(rows)]
    df["direction"] = "BUY"
    df["pnl_%"] = 0.6 * df[F[0]] + rng.normal(scale=0.3, size=rows)
    df["profit"] = (df["pnl_%"] > 0).astype(int)
    df["source"] = "history"
    return df


class Fixed:
    """A champion that scores at random (no skill)."""

    def __init__(self, trained_through, version="v2", threshold=0.95, seed=1):
        self.version, self.features, self.threshold = version, F, threshold
        self.trained_through = trained_through
        self.rng = np.random.default_rng(seed)

        class M:
            def predict_proba(_, X):
                p = self.rng.uniform(size=len(X))
                return np.column_stack([1 - p, p])
        self.model = M()


@pytest.fixture
def reg(tmp_path, monkeypatch):
    monkeypatch.setattr(registry, "REGISTRY", tmp_path)
    monkeypatch.setattr(registry, "CHAMPION_FILE", tmp_path / "champion.json")
    monkeypatch.setattr(registry, "COMPARISONS", tmp_path / "comparisons.jsonl")
    return tmp_path


def test_leakage_checks():
    df = dataset(n_train_days=5)
    train = df[df["date"] < "2026-09-01"]
    Ch.check_leakage(train, F, "2026-09-01")
    with pytest.raises(AssertionError, match="label"):
        Ch.check_leakage(train, F + ["mfe_pct"], "2026-09-01")
    with pytest.raises(AssertionError, match="held-out"):
        Ch.check_leakage(df, F, "2026-09-01")
    with pytest.raises(AssertionError, match="duplicate"):
        Ch.check_leakage(pd.concat([train, train.head(1)]), F, "2026-09-01")


def test_decide_needs_enough_trades_and_every_cost_level():
    def m(n, mean):
        return {"go_trades": n, "net_per_trade": {str(c): mean - c for c in (0.0, 0.05, 0.10)}}
    assert Ch.decide(m(40, 0.10), m(40, 0.20))[0]
    assert not Ch.decide(m(40, 0.20), m(40, 0.10))[0]
    assert not Ch.decide(m(40, 0.10), m(40, 0.10))[0]                  # a tie keeps the champion
    ok, why = Ch.decide(m(29, 0.10), m(40, 0.50))
    assert not ok and "too few" in why


def test_held_out_month_is_the_last_complete_month():
    assert Ch.held_out_month(dt.date(2026, 11, 7)) == (dt.date(2026, 10, 1), dt.date(2026, 10, 31))
    assert Ch.held_out_month(dt.date(2027, 1, 2)) == (dt.date(2026, 12, 1), dt.date(2026, 12, 31))


def test_better_challenger_is_promoted_and_versioned(reg, monkeypatch):
    champ = Fixed(trained_through="2026-08-31")
    monkeypatch.setattr(registry, "champion", lambda: champ)
    monkeypatch.setattr(registry, "baseline", lambda: Fixed("2026-08-31", threshold=0.9))
    r = Ch.run(dt.date(2026, 10, 3), data=dataset(per_day=100), cv=False)
    assert r["held_out_month"] == "2026-09" and r["promoted"], r["reason"]
    assert r["challenger_metrics"]["go_trades"] >= Ch.MIN_GO_TRADES
    champion_file = json.loads((reg / "champion.json").read_text())
    assert champion_file["version"] == "r2026-10"
    assert (reg / "c2026-10.pkl").exists() and (reg / "r2026-10.pkl").exists()
    meta = json.loads((reg / "r2026-10.json").read_text())
    assert meta["trained_through"] == "2026-09-30" and meta["evaluated_as"] == "c2026-10"
    assert registry.BASELINE_FILE.exists()                              # v2 untouched
    assert len(registry.comparisons()) == 1


def test_comparison_is_logged_when_the_champion_is_kept(reg, monkeypatch):
    data = dataset()
    data["pnl_%"] = np.random.default_rng(5).normal(size=len(data))    # nothing to learn
    data["profit"] = (data["pnl_%"] > 0).astype(int)
    champ = Fixed(trained_through="2026-08-31", threshold=0.5)
    monkeypatch.setattr(registry, "champion", lambda: champ)
    monkeypatch.setattr(registry, "baseline", lambda: champ)
    r = Ch.run(dt.date(2026, 10, 3), data=data, cv=False)
    if r["promoted"]:
        pytest.skip("random data happened to favour the challenger")
    assert not (reg / "champion.json").exists()
    logged = registry.comparisons()
    assert len(logged) == 1 and logged[0]["promoted"] is False and logged[0]["reason"]
    assert Ch.run(dt.date(2026, 10, 4), data=data, cv=False) is None   # month already decided


def test_window_excludes_sessions_the_champion_trained_on(reg, monkeypatch):
    """The champion trained through 22 Sep: only 23-30 Sep is out-of-sample for it, pooling
    can't reach further back, and 6 sessions are too few — so nothing is decided."""
    champ = Fixed(trained_through="2026-09-22")
    monkeypatch.setattr(registry, "champion", lambda: champ)
    monkeypatch.setattr(registry, "baseline", lambda: champ)
    r = Ch.run(dt.date(2026, 10, 3), data=dataset(), cv=False, dry_run=True)
    assert r["window"] is None and not r["would_promote"] and "too few" in r["reason"]
    assert [a["from"] for a in r["pooling"]] == ["2026-09"]           # stopped at the boundary
    assert r["pooling"][0]["sessions"] == 6
    assert not list(reg.iterdir())                                     # dry run writes nothing


def test_months_are_pooled_until_both_models_have_30_go_trades(reg, monkeypatch):
    champ = Fixed(trained_through="2026-02-28", threshold=0.98)
    monkeypatch.setattr(registry, "champion", lambda: champ)
    monkeypatch.setattr(registry, "baseline", lambda: champ)
    data = dataset(n_train_days=520, per_day=30, start=dt.date(2024, 9, 2))
    r = Ch.run(dt.date(2026, 10, 3), data=data, cv=False)
    months = r["pooled_months"]
    assert len(months) > 1 and months[-1] == "2026-09"                 # one month alone was too few
    assert months == sorted(months) and len(r["pooling"]) >= len(months)
    assert r["challenger_metrics"]["go_trades"] >= 30 and r["champion_metrics"]["go_trades"] >= 30
    assert r["train_through"] < r["window"][0]                         # the window was never trained on
    assert r["window"][0][:7] == months[0]
    assert registry.comparisons()[0]["pooled_months"] == months        # logged
    meta = json.loads((reg / "c2026-10.json").read_text())
    assert meta["pooled_months"] == months and meta["trained_through"] < r["window"][0]


def test_pooling_never_reaches_into_the_champions_training(reg, monkeypatch):
    champ = Fixed(trained_through="2026-07-15", threshold=0.98)
    monkeypatch.setattr(registry, "champion", lambda: champ)
    monkeypatch.setattr(registry, "baseline", lambda: champ)
    r = Ch.run(dt.date(2026, 10, 3), data=dataset(n_train_days=520, per_day=30, start=dt.date(2024, 9, 2)), cv=False, dry_run=True)
    tried = [a["from"] for a in r["pooling"]]
    assert "2026-06" not in tried and tried[-1] >= "2026-07"
    if r["window"]:
        assert r["window"][0] > "2026-07-15"


def test_shadow_rows_join_history_with_their_labels(tmp_path):
    hist = dataset(n_train_days=3)
    s = shadow.labelled_rows(tmp_path / "none.csv", tmp_path / "none2.csv")
    df = Ch.labelled_data(history=hist, shadow_rows=s)
    assert len(df) == len(hist) and set(F) <= set(df.columns)


@pytest.mark.skipif(not Ch.HISTORY.exists(), reason="needs data/training/history.csv.gz")
def test_the_recipe_on_the_history_table_reproduces_v2():
    """Same data + same recipe = the registered v2 model: bit for bit on the
    architecture v2 was trained on (Apple arm64), and within floating-point
    drift elsewhere (the VM is x86: XGBoost isn't bitwise reproducible across
    CPU architectures). So a challenger differs from v2 by its data, plus — when
    trained on another architecture — that drift."""
    import platform
    import walk_forward as W
    df = Ch.labelled_data(shadow_rows=pd.DataFrame())
    df = df[df["date"] <= "2026-09-22"]
    model, thr, _ = W.fit_with_threshold(df, F)
    v2 = registry.baseline()
    if platform.machine() == "arm64":
        assert thr == v2.threshold
    else:
        X = df[F].fillna(0)
        same = ((model.predict_proba(X)[:, 1] >= thr) == (v2.model.predict_proba(X)[:, 1] >= v2.threshold)).mean()
        assert abs(thr - v2.threshold) < 0.002 and same > 0.98

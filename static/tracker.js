// Live signal tracker: render the embedded snapshot, then follow /api/tracker/stream.
// Same pattern as QuantRadar's dashboard: one EventSource, the browser reconnects by
// itself, and on an error we ask /api/me whether the session is still valid.
(function () {
  "use strict";
  const $ = (id) => document.getElementById(id);
  let snap = JSON.parse($("trk-data").textContent || "{}");
  let tab = "all";
  try { tab = sessionStorage.getItem("orbital-trk-tab") || "all"; } catch (e) {}

  const esc = (s) => String(s ?? "").replace(/[&<>"']/g, (c) =>
    ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" })[c]);
  const money = (v) => (v == null ? "—" : "₹" + Number(v).toLocaleString("en-IN",
    { minimumFractionDigits: 2, maximumFractionDigits: 2 }));
  const pct = (v) => (v == null ? '<td class="num">—</td>'
    : `<td class="num ${v >= 0 ? "pos" : "neg"}">${v >= 0 ? "+" : ""}${v.toFixed(2)}%</td>`);

  function row(r) {
    const closed = r.status !== "open";
    const skip = r.decision === "SKIP";
    return `<tr class="${closed ? "row-closed" : ""}${skip ? " row-paper" : ""}">
      <td class="symbol ${r.direction === "BUY" ? "buy" : "sell"}-symbol">${esc(r.symbol)}</td>
      <td>${esc(r.time)}</td>
      <td>${r.direction === "BUY" ? "▲ BUY" : "▼ SELL"}</td>
      <td><div class="scorebar" title="score ${r.score.toFixed(3)} vs threshold ${r.threshold.toFixed(3)}">
            <i style="width:${Math.min(100, r.score * 100).toFixed(0)}%"></i><b style="left:${(r.threshold * 100).toFixed(0)}%"></b>
          </div><small>${r.score.toFixed(3)}</small></td>
      <td><span class="decision ${r.go ? "go" : "skip"}">${r.go ? "GO" : "SKIP"}</span>${r.decision === "STALE" ? ' <span class="few" title="first seen late; not sent">stale</span>' : ""}${!r.go ? '<small class="paper-tag" title="paper/shadow position: the model did not take it">tracked, not taken</small>' : ""}</td>
      <td class="num">${money(r.entry)}</td>
      <td class="num">${money(r.price)}</td>
      ${pct(r.pnl_pct)}
      <td class="num">${money(r.stop)}</td>
      <td class="num">${closed ? "—" : money(r.trail_stop)}</td>
      <td class="num">${money(r.target)}</td>
      ${pct(r.best_pct)}
      ${pct(r.worst_pct)}
      <td><span class="trk-status s-${esc(r.status)}">${esc(r.status_label)}</span></td>
      <td>${r.exit_time ? esc(r.exit_time) : ""}</td>
    </tr>`;
  }

  function stat(label, value, cls) {
    return `<div class="stat-card"><span>${label}</span><strong class="${cls || ""}">${value}</strong></div>`;
  }

  const signed = (v) => (v == null ? "—" : `<em class="${v >= 0 ? "pos" : "neg"}">${v >= 0 ? "+" : ""}${v.toFixed(2)}%</em>`);

  function render() {
    const rows = snap.rows || [];
    const go = rows.filter((r) => r.go), nogo = rows.filter((r) => !r.go);
    const shown = tab === "go" ? go : tab === "nogo" ? nogo : rows;
    const open = rows.filter((r) => r.status === "open").length;
    const sm = snap.summary || {};

    $("trk-n-all").textContent = `(${rows.length})`;
    $("trk-n-go").textContent = `(${go.length})`;
    $("trk-n-nogo").textContent = `(${nogo.length})`;
    $("trk-body").innerHTML = shown.map(row).join("");
    const empty = $("trk-empty");
    empty.hidden = shown.length > 0;
    empty.textContent = rows.length ? `No ${tab === "go" ? "GO" : "SKIP"} signals ${snap.day === snap.today ? "today" : "that session"}.`
                                    : "No signals yet today. They appear here from the 09:40 cycle.";
    const asOf = snap.as_of ? new Date(snap.as_of) : null;
    $("trk-asof").textContent = !asOf ? "NO DATA YET"
      : (snap.day === snap.today ? "AS OF " : `LAST SESSION ${snap.day} · AS OF `) +
        asOf.toLocaleTimeString("en-IN", { hour: "2-digit", minute: "2-digit", timeZone: "Asia/Kolkata" });
    $("trk-stats").innerHTML =
      stat("SIGNALS", rows.length, "amber") + stat("GO", sm.go ?? go.length, "green") +
      stat("SKIP (TRACKED, NOT TAKEN)", sm.skip ?? nogo.length, "muted") +
      stat("SKIPS IN PROFIT NOW", sm.skip_priced ? `${sm.skip_in_profit}/${sm.skip_priced}` : "—") +
      stat("SKIP AVG LIVE P&amp;L", signed(sm.skip_avg_pnl)) + stat("OPEN", open);
    document.querySelectorAll('[role="tab"]').forEach((b) => {
      const on = b.dataset.tab === tab;
      b.classList.toggle("active", on);
      b.setAttribute("aria-selected", String(on));
    });
  }

  document.querySelectorAll('[role="tab"]').forEach((b) => b.addEventListener("click", () => {
    tab = b.dataset.tab;
    try { sessionStorage.setItem("orbital-trk-tab", tab); } catch (e) {}
    render();
  }));

  function setConn(state) {
    const pill = $("trk-conn");
    pill.className = "conn-pill " + state;
    pill.lastElementChild.textContent = { live: "live", reconnecting: "reconnecting…", connecting: "connecting…" }[state];
  }

  function connect() {
    const es = new EventSource("/api/tracker/stream");
    es.onopen = () => setConn("live");
    es.addEventListener("tracker", (e) => {
      const next = JSON.parse(e.data);
      next.today = next.today || snap.today;
      snap = next;
      render();
    });
    es.onerror = async () => {
      setConn("reconnecting");
      // EventSource hides the HTTP status; ask the API whether the session is still valid.
      const r = await fetch("/api/me", { credentials: "same-origin" }).catch(() => null);
      if (r && r.status === 401) {
        es.close();
        location.href = "/login?next=/tracker";
        return;
      }
      if (es.readyState === EventSource.CLOSED) {      // refused (e.g. too many tabs): retry later
        setTimeout(connect, 10000);
      }
    };
  }

  render();
  connect();
})();

// Live signals table: filter chips, sortable columns (GO always pinned on top),
// expandable detail rows, and a self-refresh every 30 s that swaps in the new strip
// and table (/live?fragment=1) and re-applies all of the above.
(function () {
  "use strict";
  const REFRESH_MS = 30000;
  const KEY = "orbital-live-view";
  const STATUS_ORDER = { open: 0, target: 1, trailed: 2, stopped: 3, eod: 4, "": 5 };

  let view = { decision: "all", dir: "all", state: "all", sort: null, desc: false, open: [], tickers: [], tickersOpen: false };
  try { Object.assign(view, JSON.parse(sessionStorage.getItem(KEY) || "{}")); } catch (e) {}
  const save = () => { try { sessionStorage.setItem(KEY, JSON.stringify(view)); } catch (e) {} };

  const table = () => document.getElementById("sig-table");
  const groups = () => Array.from(document.querySelectorAll("#sig-table tbody.sig"));

  function value(tb, key) {
    if (key === "time") return tb.dataset.time;
    if (key === "status") return STATUS_ORDER[tb.dataset.status] ?? 9;
    const v = parseFloat(tb.dataset[key]);
    return isNaN(v) ? null : v;
  }

  function applySort() {
    const t = table();
    if (!t) return;
    const rows = groups();
    const k = view.sort;
    rows.sort((a, b) => {
      const ga = a.dataset.decision === "GO" ? 0 : 1, gb = b.dataset.decision === "GO" ? 0 : 1;
      if (ga !== gb) return ga - gb;                                 // GO pinned to the top
      if (!k) return b.dataset.time.localeCompare(a.dataset.time);   // default: newest first
      const va = value(a, k), vb = value(b, k);
      if (va === null && vb === null) return 0;
      if (va === null) return 1;                                     // blanks last either way
      if (vb === null) return -1;
      const c = va < vb ? -1 : va > vb ? 1 : 0;
      return view.desc ? -c : c;
    });
    rows.forEach((r) => t.appendChild(r));
    t.querySelectorAll("th[aria-sort]").forEach((th) => {
      const key = th.querySelector("button.sort")?.dataset.key;
      th.setAttribute("aria-sort", key && key === k ? (view.desc ? "descending" : "ascending") : "none");
    });
  }

  function applyFilters() {
    let shown = 0;
    groups().forEach((tb) => {
      const ok = (view.decision === "all" || tb.dataset.decision === view.decision) &&
                 (view.dir === "all" || tb.dataset.dir === view.dir) &&
                 (view.state === "all" || tb.dataset.state === view.state) &&
                 (!view.tickers.length || view.tickers.includes(tb.dataset.symbol));
      tb.hidden = !ok;
      if (ok) shown += 1;
    });
    // a divider above the first visible SKIP row, under the pinned GO rows
    let seenGo = false, marked = false;
    groups().forEach((tb) => {
      tb.classList.remove("first-skip");
      if (tb.hidden) return;
      if (tb.dataset.decision === "GO") { seenGo = true; return; }
      if (seenGo && !marked) { tb.classList.add("first-skip"); marked = true; }
    });
    document.querySelectorAll(".chip[data-filter]").forEach((c) => {
      const on = c.dataset.filter === "ticker"
        ? (c.dataset.value === "all" ? !view.tickers.length : view.tickers.includes(c.dataset.value))
        : view[c.dataset.filter] === c.dataset.value;
      c.setAttribute("aria-pressed", String(on));
    });
    const bar = document.getElementById("live-tickers");
    if (bar) {                                          // phones: a collapsible row
      bar.classList.toggle("open", view.tickersOpen);
      const t = bar.querySelector(".ticker-toggle");
      t.setAttribute("aria-expanded", String(view.tickersOpen));
      t.querySelector(".ticker-selected").textContent = view.tickers.length ? ` · ${view.tickers.length} selected` : "";
    }
    const total = groups().length;
    const count = document.getElementById("sig-count");
    if (count) count.textContent = total ? `Showing ${shown} of ${total} signals` : "";
    const empty = document.getElementById("sig-filtered-empty");
    if (empty) empty.hidden = !(total && shown === 0);
  }

  function applyOpen() {
    groups().forEach((tb) => {
      const btn = tb.querySelector("button.expand");
      const detail = tb.querySelector("tr.sig-detail");
      const open = view.open.includes(tb.dataset.id);
      btn.setAttribute("aria-expanded", String(open));
      detail.hidden = !open;
      tb.classList.toggle("expanded", open);
    });
  }

  function applyAll() { applySort(); applyFilters(); applyOpen(); }

  // one set of listeners on the document: rows are replaced on every refresh
  document.addEventListener("click", (e) => {
    const chip = e.target.closest(".chip[data-filter]");
    if (chip) {
      const v = chip.dataset.value;
      if (chip.dataset.filter === "ticker") {          // several at once; again (or All) clears
        view.tickers = v === "all" ? [] : view.tickers.includes(v) ? view.tickers.filter((x) => x !== v) : view.tickers.concat(v);
      } else {
        view[chip.dataset.filter] = v;
      }
      save(); applyFilters();
      return;
    }
    if (e.target.closest(".ticker-toggle")) {
      view.tickersOpen = !view.tickersOpen;
      save(); applyFilters();
      return;
    }
    const sort = e.target.closest("button.sort");
    if (sort) {
      const k = sort.dataset.key;
      if (view.sort === k && view.desc) { view.sort = null; view.desc = false; }   // 3rd click: default
      else if (view.sort === k) view.desc = true;
      else { view.sort = k; view.desc = k !== "time" && k !== "status"; }          // numbers: high first
      save(); applySort(); applyFilters();
      return;
    }
    const exp = e.target.closest("button.expand");
    if (exp) {
      const id = exp.closest("tbody.sig").dataset.id;
      view.open = view.open.includes(id) ? view.open.filter((x) => x !== id) : view.open.concat(id);
      save(); applyOpen();
    }
  });

  async function refresh() {
    if (document.hidden) return;
    try {
      const q = new URLSearchParams(location.search);          // keeps time and, for guests, the key
      q.set("fragment", "1");
      const r = await fetch(location.pathname + "?" + q, { credentials: "same-origin", headers: { Accept: "text/html" } });
      if (r.status === 404 && document.body.dataset.guest) { location.reload(); return; }   // key rotated
      if (r.status === 401 || r.redirected) { location.href = "/login?next=" + encodeURIComponent(location.pathname + location.search); return; }
      if (!r.ok) return;
      const doc = new DOMParser().parseFromString(await r.text(), "text/html");
      for (const id of ["live-tickers", "live-strip", "live-table"]) {
        const fresh = doc.getElementById(id), old = document.getElementById(id);
        if (fresh && old) old.replaceWith(fresh);
      }
      applyAll();
    } catch (e) { /* network blip: try again next time */ }
  }

  applyAll();
  setInterval(refresh, REFRESH_MS);
  document.addEventListener("visibilitychange", () => { if (!document.hidden) refresh(); });
})();

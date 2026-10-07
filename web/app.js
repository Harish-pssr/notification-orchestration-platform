"use strict";
/*
 * Dashboard shell. Each section is an object in `sections` with:
 *   title, desc   shown in the page header
 *   mount(root)   called once, builds the static DOM (so form input survives refreshes)
 *   update()      called after every data refresh while the section is visible
 * Later slices fill in the placeholder sections by replacing their mount/update.
 */

// ---------- helpers ----------
const $ = (sel, root = document) => root.querySelector(sel);
const esc = (s) => String(s ?? "").replace(/[&<>"']/g, (c) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[c]));
const rint = (n) => Math.floor(Math.random() * n);
const pick = (a) => a[rint(a.length)];
const CHANNELS = ["email", "push", "webhook"];
const LABEL = { PENDING: "Queued", PROCESSING: "Relay working", RETRYING: "Retrying", PUBLISHED: "Published", DELIVERED: "Delivered", DLQ: "Dead-lettered" };

const chip = (state, text) => `<span class="chip s-${esc(state)}">${esc(text ?? LABEL[state] ?? state)}</span>`;
const neutralChip = (text) => `<span class="chip neutral">${esc(text)}</span>`;
// UUIDv7 ids start with a timestamp, so their first characters look alike; the tail differs.
const shortId = (id) => "…" + String(id).slice(-8);

const pad = (n, w = 2) => String(n).padStart(w, "0");
function fmtTime(iso) {
  if (!iso) return "–";
  const d = new Date(iso);
  return `${pad(d.getHours())}:${pad(d.getMinutes())}:${pad(d.getSeconds())}.${pad(d.getMilliseconds(), 3)}`;
}
function ago(iso) {
  const s = (Date.now() - new Date(iso).getTime()) / 1000;
  if (s < 2) return "just now";
  if (s < 60) return `${Math.floor(s)}s ago`;
  if (s < 3600) return `${Math.floor(s / 60)}m ago`;
  if (s < 86400) return `${Math.floor(s / 3600)}h ago`;
  return `${Math.floor(s / 86400)}d ago`;
}
const fmtMs = (ms) => (ms < 1000 ? `${Math.round(ms)} ms` : `${(ms / 1000).toFixed(2)} s`);
// "96 / 214 ms": one unit for the pair when both are under a second.
const fmtPair = (a, b) => (a < 1000 && b < 1000 ? `${Math.round(a)} / ${Math.round(b)} ms` : `${fmtMs(a)} / ${fmtMs(b)}`);
const num = (n) => Number(n || 0).toLocaleString();

async function api(path, opts) {
  const res = await fetch(path, opts);
  let body = null;
  try { body = await res.json(); } catch { /* not JSON */ }
  return { ok: res.ok, status: res.status, headers: res.headers, body };
}

function toast(msg, kind) {
  const el = document.createElement("div");
  el.className = "toast" + (kind === "bad" ? " bad" : "");
  el.textContent = msg;
  $("#toasts").appendChild(el);
  setTimeout(() => el.remove(), 4000);
}

// ---------- state ----------
const state = { live: true, route: "overview", summary: null, events: [], updated: null, error: null, drawerId: null, filters: { q: "", state: "all", type: "all" }, config: { faults_enabled: false } };

// ---------- admin token and helpers for operator actions (D8) ----------
const tokenEl = $("#admin-token");
try { tokenEl.value = sessionStorage.getItem("adminToken") || ""; } catch { /* storage unavailable */ }
tokenEl.addEventListener("input", () => { try { sessionStorage.setItem("adminToken", tokenEl.value.trim()); } catch { /* ignore */ } });
const getToken = () => tokenEl.value.trim();
const adminApi = (method, path, body) => api(path, {
  method, headers: { "Content-Type": "application/json", Authorization: "Bearer " + getToken() },
  body: body === undefined ? undefined : JSON.stringify(body),
});
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
async function must(promise) {
  const r = await promise;
  if (!r.ok) throw new Error((r.body && r.body.error) || "request failed (" + r.status + ")");
  return r;
}
async function waitUntil(fn, timeoutMs) {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) { if (await fn()) return; await sleep(400); }
  throw new Error("timed out");
}

const attention = (e) => e.status === "RETRYING" || e.status === "DLQ" || Object.values(e.deliveries).some((s) => s === "RETRYING" || s === "DLQ");
const completed = (e) => e.status === "PUBLISHED" && Object.values(e.deliveries).every((s) => s === "DELIVERED");

// ---------- event type definitions (used by the Create section) ----------
const EVENT_TYPES = {
  "payment-succeeded": {
    name: "PaymentSucceeded", blurb: "A payment completed", routes: ["email", "push", "webhook"],
    fields: [
      { key: "account_id", label: "Account ID", type: "text", random: () => "acct-" + rint(1000) },
      { key: "amount_cents", label: "Amount (cents)", type: "number", random: () => 1000 + rint(9000) },
      { key: "currency", label: "Currency", type: "select", options: ["USD", "EUR", "GBP"], random: () => pick(["USD", "EUR", "GBP"]) },
    ],
  },
  "booking-confirmed": {
    name: "BookingConfirmed", blurb: "A booking was confirmed", routes: ["email"],
    fields: [
      { key: "customer_id", label: "Customer ID", type: "text", random: () => "cust-" + rint(1000) },
      { key: "item", label: "Item", type: "text", random: () => "room-" + rint(50) },
    ],
  },
  "password-changed": {
    name: "PasswordChanged", blurb: "A user changed their password", routes: ["email", "push"],
    fields: [{ key: "user_id", label: "User ID", type: "text", random: () => "user-" + rint(1000) }],
  },
};

// ---------- sections ----------
function placeholder({ badge, intro, items }) {
  return (root) => {
    root.innerHTML = `<div class="card placeholder"><span class="badge">${esc(badge)}</span>
      <p>${esc(intro)}</p><ul>${items.map((i) => `<li>${esc(i)}</li>`).join("")}</ul></div>`;
  };
}

const sections = {
  overview: {
    title: "Overview",
    desc: "Live state of the pipeline, counted exactly from the database.",
    mount(root) {
      root.innerHTML = `
        <div class="tiles" id="ov-tiles"></div>
        <div class="card">
          <div class="card-head"><h2>Pipeline</h2><p>Where events are right now, from write to delivery.</p></div>
          <div class="pipeline" id="ov-pipeline"></div>
        </div>
        <div class="card">
          <div class="card-head"><h2>Latest events</h2><a href="#/events">View all events</a></div>
          <div class="table-wrap"><table><thead><tr><th>Event</th><th>Type</th><th>Channels</th><th>Created</th></tr></thead>
          <tbody id="ov-recent"></tbody></table></div>
        </div>`;
      root.addEventListener("click", rowClick);
    },
    update() {
      const s = state.summary;
      if (!s) { $("#ov-tiles").innerHTML = `<div class="empty card">Loading…</div>`; return; }
      const ev = s.events, sum = (o) => Object.values(o || {}).reduce((a, b) => a + b, 0);
      const created = sum(ev);
      const backlog = (ev.PENDING || 0) + (ev.PROCESSING || 0) + (ev.RETRYING || 0);
      let delivered = 0, total = 0, trouble = (ev.DLQ || 0);
      for (const ch of CHANNELS) {
        const c = s.channels[ch] || {};
        delivered += c.DELIVERED || 0; total += sum(c);
        trouble += (c.DLQ || 0) + (c.RETRYING || 0);
      }
      const lat = s.latency;
      $("#ov-tiles").innerHTML = [
        tile("Events created", num(created), "committed to the outbox"),
        tile("Waiting for relay", num(backlog), "outbox backlog"),
        tile("Published to broker", num(ev.PUBLISHED || 0), "relay finished"),
        tile("Deliveries completed", `${num(delivered)}<span class="muted small"> / ${num(total)}</span>`, "all channels"),
        tile("Needs attention", num(trouble), "retrying or dead-lettered", trouble > 0),
        tile("Latency (p50 / p95)", lat.count ? fmtPair(lat.p50_ms, lat.p95_ms) : "–", `last ${num(lat.count)} deliveries`),
      ].join("");

      $("#ov-pipeline").innerHTML = `
        <div class="node"><h3>1 · API</h3><div class="big">${num(created)}</div><div class="sub">events written with their business change in one transaction</div></div>
        <div class="arrow" aria-hidden="true">→</div>
        <div class="node"><h3>2 · Outbox</h3><div class="big">${num(backlog)}</div><div class="sub">waiting for a relay${ev.DLQ ? ` · ${num(ev.DLQ)} dead-lettered` : ""}</div></div>
        <div class="arrow" aria-hidden="true">→</div>
        <div class="node"><h3>3 · Relay → Broker</h3><div class="big">${num(ev.PUBLISHED || 0)}</div><div class="sub">published to NATS JetStream</div></div>
        <div class="arrow" aria-hidden="true">→</div>
        <div class="fan">${CHANNELS.map((ch) => {
          const c = s.channels[ch] || {};
          const inflight = (c.PENDING || 0) + (c.RETRYING || 0);
          return `<div class="channel-node"><strong>${ch}</strong><span class="big">${num(c.DELIVERED || 0)}</span>
            <div class="channel-stats"><span>delivered</span><span>${num(inflight)} in flight</span>${c.DLQ ? `<span style="color:var(--bad)">${num(c.DLQ)} dead-lettered</span>` : ""}</div></div>`;
        }).join("")}</div>`;

      const recent = state.events.slice(0, 8);
      $("#ov-recent").innerHTML = recent.length ? recent.map((e) => eventRow(e, false)).join("")
        : `<tr><td colspan="4" class="empty">No events yet. <a href="#/create">Create one</a> to see it flow through.</td></tr>`;
    },
  },

  create: {
    title: "Create events",
    desc: "Send business events to the API. Each is written with its outbox record in one transaction, then delivered to the channels it routes to.",
    mount(root) {
      root.innerHTML = `
        <div class="grid-2">
          <div class="card">
            <div class="card-head"><h2>New event</h2></div>
            <div class="type-cards" role="radiogroup" aria-label="Event type" id="cr-types"></div>
            <div id="cr-fields"></div>
            <label class="field"><span>Idempotency-Key (generated once per request; reuse it to see replay)</span>
              <input type="text" id="cr-key" class="mono" autocomplete="off"></label>
            <div class="row">
              <label class="field" style="width:110px"><span>How many</span><input type="number" id="cr-count" min="1" max="1000" value="1"></label>
              <button class="btn btn-primary" id="cr-send" style="margin-bottom:10px">Send</button>
              <button class="btn" id="cr-resend" style="margin-bottom:10px" disabled title="Repeat the last request with the same key">Resend last request</button>
            </div>
            <p class="muted small">With more than one, values are randomised and every event gets its own key.</p>
          </div>
          <div class="card">
            <div class="card-head"><h2>Request log</h2><p>This session</p></div>
            <ul class="log" id="cr-log"><li class="muted">Nothing sent yet.</li></ul>
          </div>
        </div>`;
      this.type = "payment-succeeded"; this.last = null; this.logItems = [];
      const cards = $("#cr-types");
      cards.innerHTML = Object.entries(EVENT_TYPES).map(([id, t]) => `
        <button type="button" class="type-card" role="radio" aria-checked="false" data-type="${id}">
          <strong>${t.name}</strong><span class="muted small">${t.blurb}</span>
          <div class="routes">${t.routes.map(neutralChip).join("")}</div></button>`).join("");
      cards.addEventListener("click", (ev) => { const b = ev.target.closest("[data-type]"); if (b) this.setType(b.dataset.type); });
      $("#cr-send").onclick = () => this.send();
      $("#cr-resend").onclick = () => this.resend();
      $("#cr-log").addEventListener("click", rowClick);
      this.setType(this.type);
    },
    setType(id) {
      this.type = id;
      for (const b of document.querySelectorAll("#cr-types [data-type]")) b.setAttribute("aria-checked", String(b.dataset.type === id));
      $("#cr-fields").innerHTML = EVENT_TYPES[id].fields.map((f) => f.type === "select"
        ? `<label class="field"><span>${f.label}</span><select data-key="${f.key}">${f.options.map((o) => `<option>${o}</option>`).join("")}</select></label>`
        : `<label class="field"><span>${f.label}</span><input type="${f.type}" data-key="${f.key}" value="${esc(f.random())}"></label>`).join("");
      $("#cr-key").value = crypto.randomUUID();
    },
    readBody(id, randomise) {
      const body = {};
      for (const f of EVENT_TYPES[id].fields) {
        const el = $(`[data-key="${f.key}"]`);
        const v = randomise ? f.random() : el.value;
        body[f.key] = f.type === "number" ? Number(v) : v;
      }
      return body;
    },
    async post(id, key, body) {
      const r = await api("/api/events/" + id, { method: "POST", headers: { "Content-Type": "application/json", "Idempotency-Key": key }, body: JSON.stringify(body) });
      return { ...r, replayed: r.headers.get("Idempotent-Replayed") === "true" };
    },
    async send() {
      const n = Math.max(1, Math.min(1000, Number($("#cr-count").value) || 1));
      const id = this.type, btn = $("#cr-send");
      btn.disabled = true;
      try {
        if (n === 1) {
          const key = $("#cr-key").value.trim() || crypto.randomUUID(), body = this.readBody(id, false);
          this.last = { id, key, body };
          this.record(await this.post(id, key, body), id, key);
          $("#cr-key").value = crypto.randomUUID(); // a new operation gets a new key
          $("#cr-resend").disabled = false;
        } else {
          await this.burst(id, n);
        }
      } catch (err) { toast("Request failed: " + err.message, "bad"); }
      btn.disabled = false;
      refresh();
    },
    async resend() {
      if (!this.last) return;
      this.record(await this.post(this.last.id, this.last.key, this.last.body), this.last.id, this.last.key);
      refresh();
    },
    async burst(id, n) {
      let ok = 0, failed = 0, next = 0;
      const started = performance.now();
      const worker = async () => {
        while (next < n) {
          next++;
          const r = await this.post(id, crypto.randomUUID(), this.readBody(id, true));
          r.status === 201 ? ok++ : failed++;
        }
      };
      await Promise.all(Array.from({ length: Math.min(8, n) }, worker));
      const secs = (performance.now() - started) / 1000;
      this.logItems.unshift(`<li>${chip(failed ? "RETRYING" : "PUBLISHED", failed ? "Burst with errors" : "Burst sent")}
        <span>${EVENT_TYPES[id].name} × ${num(n)}: ${num(ok)} created, ${num(failed)} failed in ${secs.toFixed(1)}s
        (${num(Math.round(ok / secs))}/s as seen by this browser, not a benchmark)</span></li>`);
      this.renderLog();
    },
    record(r, id, key) {
      const name = EVENT_TYPES[id].name;
      if (r.status === 201) {
        const eid = r.body.event_id;
        this.logItems.unshift(`<li>${chip("PUBLISHED", r.replayed ? "201 replayed" : "201 created")}
          <span>${name} <a href="#" data-event="${esc(eid)}" class="mono">${shortId(eid)}</a></span>
          <span class="muted small">${r.replayed ? "same event returned, nothing new was created" : "→ " + r.body.channels.join(", ")}</span></li>`);
        toast(r.replayed ? "Replayed: same event, nothing new created" : `${name} created`);
      } else {
        const msg = (r.body && r.body.error) || "error";
        this.logItems.unshift(`<li>${chip("DLQ", r.status + " rejected")}<span>${name}</span><span class="muted small">${esc(msg)}</span></li>`);
        toast(`${r.status}: ${msg}`, "bad");
      }
      this.renderLog();
    },
    renderLog() { $("#cr-log").innerHTML = this.logItems.slice(0, 30).join(""); },
    update() {},
  },

  events: {
    title: "Events",
    desc: "Every event with its outbox state and per-channel delivery. Select a row for the full story.",
    mount(root) {
      root.innerHTML = `
        <div class="card">
          <div class="toolbar">
            <input type="search" id="ev-q" placeholder="Search by event id…" aria-label="Search by event id">
            <select id="ev-state" aria-label="Filter by state">
              <option value="all">All states</option><option value="progress">In progress</option>
              <option value="done">Completed</option><option value="attention">Needs attention</option></select>
            <select id="ev-type" aria-label="Filter by type"><option value="all">All types</option>
              ${Object.values(EVENT_TYPES).map((t) => `<option>${t.name}</option>`).join("")}</select>
            <span class="spacer"></span><span class="muted small" id="ev-count"></span>
          </div>
          <div class="table-wrap"><table>
            <thead><tr><th>Event</th><th>Type</th><th>Outbox</th><th>Attempts</th><th>Channels</th><th>Created</th></tr></thead>
            <tbody id="ev-body"></tbody></table></div>
        </div>`;
      $("#ev-q").oninput = (e) => { state.filters.q = e.target.value.trim().toLowerCase(); this.update(); };
      $("#ev-state").onchange = (e) => { state.filters.state = e.target.value; this.update(); };
      $("#ev-type").onchange = (e) => { state.filters.type = e.target.value; this.update(); };
      root.addEventListener("click", rowClick);
    },
    update() {
      const f = state.filters;
      const rows = state.events.filter((e) =>
        (!f.q || e.id.toLowerCase().includes(f.q)) &&
        (f.type === "all" || e.type === f.type) &&
        (f.state === "all" || (f.state === "attention" && attention(e)) || (f.state === "done" && completed(e)) ||
          (f.state === "progress" && !attention(e) && !completed(e))));
      $("#ev-count").textContent = `${rows.length} of ${state.events.length} shown (latest ${state.events.length})`;
      $("#ev-body").innerHTML = rows.length ? rows.map((e) => eventRow(e, true)).join("")
        : `<tr><td colspan="6" class="empty">${state.events.length ? "No events match these filters." : "No events yet."}</td></tr>`;
    },
  },

  health: { title: "Health", desc: "Which relay and consumer instances are alive, and which roles are up, down or paused.",
    mount: placeholder({ badge: "Arrives with failure controls (Slice 5)", intro: "Workers send heartbeats; a role is UP when at least one instance is fresh.",
      items: ["Per-instance heartbeat with last-seen age", "Role status: UP, DOWN or PAUSED for the relay and each consumer", "Backlog depth per channel"] }), update() {} },

  dlq: {
    title: "Dead letters",
    desc: "Deliveries that exhausted their retries or failed permanently. Read the reason, fix the cause, then replay. Replaying is safe: the provider recognises work it already did.",
    selected: new Set(),
    data: { deliveries: [], events: [] },
    mount(root) {
      root.innerHTML = `
        <div class="card">
          <div class="card-head"><h2>Dead-lettered deliveries</h2><p id="dl-count" class="muted small"></p></div>
          <div class="toolbar">
            <button class="btn btn-primary" id="dl-replay-sel" disabled>Replay selected</button>
            <select id="dl-channel" aria-label="Channel to replay"><option value="">All channels</option><option>email</option><option>push</option><option>webhook</option></select>
            <button class="btn" id="dl-replay-all" disabled>Replay all in this channel</button>
            <span class="spacer"></span><span id="dl-needs" class="muted small"></span>
          </div>
          <div class="table-wrap"><table>
            <thead><tr><th></th><th>Event</th><th>Type</th><th>Channel</th><th>Attempts</th><th>Replays</th><th>Reason</th></tr></thead>
            <tbody id="dl-body"></tbody></table></div>
        </div>
        <div class="card" id="dl-events-card" hidden>
          <div class="card-head"><h2>Events the relay gave up on</h2><p>The relay could not publish these to the broker.</p></div>
          <div class="table-wrap"><table>
            <thead><tr><th>Event</th><th>Type</th><th>Attempts</th><th>Reason</th><th></th></tr></thead>
            <tbody id="dl-events"></tbody></table></div>
        </div>`;
      root.addEventListener("click", rowClick);
      root.addEventListener("change", (e) => {
        const cb = e.target.closest("input[data-key]");
        if (!cb) return;
        cb.checked ? this.selected.add(cb.dataset.key) : this.selected.delete(cb.dataset.key);
        this.syncButtons();
      });
      $("#dl-replay-sel").onclick = () => this.replaySelected();
      $("#dl-replay-all").onclick = () => this.replayAll();
      $("#dl-events").addEventListener("click", (e) => {
        const b = e.target.closest("button[data-replay-event]");
        if (b) this.replayEvent(b.dataset.replayEvent);
      });
    },
    async update() {
      const r = await api("/api/dead-letters").catch(() => null);
      if (r && r.ok) { this.data = r.body; this.render(); }
    },
    render() {
      const rows = this.data.deliveries, key = (d) => d.event_id + "|" + d.channel;
      const live = new Set(rows.map(key));
      for (const k of [...this.selected]) if (!live.has(k)) this.selected.delete(k); // it was replayed or removed
      $("#dl-count").textContent = rows.length ? `${rows.length} dead` : "";
      $("#dl-body").innerHTML = rows.length ? rows.map((d) => `<tr>
          <td><input type="checkbox" data-key="${esc(key(d))}" ${this.selected.has(key(d)) ? "checked" : ""} aria-label="Select this delivery"></td>
          <td><a href="#" data-event="${esc(d.event_id)}" class="mono">${shortId(d.event_id)}</a></td>
          <td>${esc(d.event_type)}</td><td>${esc(d.channel)}</td><td>${d.attempts}</td><td>${d.replay_count}</td>
          <td>${esc(d.last_error || "")}</td></tr>`).join("")
        : `<tr><td colspan="7" class="empty">No dead-lettered deliveries. Deliveries land here when their retries run out or the provider rejects them for good.</td></tr>`;
      const ev = this.data.events;
      $("#dl-events-card").hidden = !ev.length;
      $("#dl-events").innerHTML = ev.map((e) => `<tr><td><a href="#" data-event="${esc(e.id)}" class="mono">${shortId(e.id)}</a></td>
        <td>${esc(e.event_type)}</td><td>${e.attempts}</td><td>${esc(e.last_error || "")}</td>
        <td><button class="btn" data-replay-event="${esc(e.id)}">Replay</button></td></tr>`).join("");
      this.syncButtons();
    },
    syncButtons() {
      const token = !!getToken();
      $("#dl-replay-sel").disabled = !token || this.selected.size === 0;
      $("#dl-replay-all").disabled = !token || this.data.deliveries.length === 0;
      $("#dl-needs").textContent = token ? "" : "Enter the admin token (top right) to replay.";
    },
    async call(body) {
      const r = await adminApi("POST", "/api/replay", body);
      if (!r.ok) throw new Error((r.body && r.body.error) || "replay failed (" + r.status + ")");
      return r.body;
    },
    summary(res) {
      return `Replayed ${res.deliveries} deliver${res.deliveries === 1 ? "y" : "ies"}` + (res.skipped ? `; ${res.skipped} skipped (event still being published, try again shortly)` : "");
    },
    async replaySelected() {
      try {
        const total = { deliveries: 0, events: 0, skipped: 0 };
        for (const k of [...this.selected]) {
          const [event_id, channel] = k.split("|");
          const res = await this.call({ event_id, channel });
          for (const f of Object.keys(total)) total[f] += res[f];
        }
        this.selected.clear();
        toast(this.summary(total));
      } catch (e) { toast(e.message, "bad"); }
      this.update(); refresh();
    },
    async replayAll() {
      const channel = $("#dl-channel").value;
      if (!confirm(`Replay ${channel ? "every dead delivery on " + channel : "every dead delivery"}?`)) return;
      try { toast(this.summary(await this.call(channel ? { channel } : { all: true }))); }
      catch (e) { toast(e.message, "bad"); }
      this.update(); refresh();
    },
    async replayEvent(id) {
      try { toast(this.summary(await this.call({ event_id: id }))); }
      catch (e) { toast(e.message, "bad"); }
      this.update(); refresh();
    },
  },

  faults: {
    title: "Failure controls",
    desc: "Break things on purpose and watch the guarantees hold. Controls need the admin token and fault injection enabled on the server.",
    mount(root) {
      root.innerHTML = `
        <div class="grid-2">
          <div class="card">
            <div class="card-head"><h2>Provider (external system)</h2><p>Make a channel's provider misbehave</p></div>
            <div class="row">
              <label class="field"><span>Channel</span><select id="fc-channel"><option>webhook</option><option>email</option><option>push</option></select></label>
              <label class="field"><span>Respond with</span><select id="fc-status">
                <option value="500">500 Server error (transient)</option><option value="503">503 Unavailable (transient)</option>
                <option value="429">429 Too many requests (transient, Retry-After)</option><option value="400">400 Bad request (permanent)</option>
                <option value="0">No error, delay only</option></select></label>
            </div>
            <div class="row">
              <label class="field" style="width:140px"><span>Extra delay (ms)</span><input type="number" id="fc-delay" min="0" max="60000" value="0"></label>
              <label class="field" style="width:150px"><span>Affect next N calls</span><input type="number" id="fc-count" min="1" placeholder="all"></label>
            </div>
            <div class="row"><button class="btn btn-primary" id="fc-apply">Apply failure</button><button class="btn" id="fc-clear">Restore provider</button></div>
            <p class="muted small" style="margin-top:8px">Transient errors are retried with backoff and then dead-lettered. A 4xx goes straight to dead letters. A delay longer than the consumer's call timeout (5 s) counts as a timeout.</p>
          </div>
          <div class="card">
            <div class="card-head"><h2>Relay</h2><p>Faults at the publish step</p></div>
            <div class="row"><button class="btn" id="fc-dup">Duplicate next event</button><button class="btn" id="fc-crash">Crash relay after publish (once)</button></div>
            <p class="muted small" style="margin-top:8px">Crash kills a relay right after the broker accepted the publish and before the database is updated; another relay recovers it once the lease expires. Pause controls arrive with the health panel.</p>
            <hr style="border:none;border-top:1px solid var(--line);margin:14px 0">
            <button class="btn" id="fc-restore-all">Restore everything</button>
          </div>
        </div>
        <div class="card"><div class="card-head"><h2>Currently active</h2></div><div id="fc-active" class="muted">Loading…</div></div>`;
      const act = (fn, ok) => async () => {
        try { await fn(); toast(ok); } catch (e) { toast(e.message, "bad"); }
        this.update();
      };
      $("#fc-apply").onclick = act(async () => {
        const status = Number($("#fc-status").value), delay_ms = Number($("#fc-delay").value) || 0, count = Number($("#fc-count").value);
        if (!status && !delay_ms) throw new Error("Pick an error status or a delay");
        const body = { channel: $("#fc-channel").value, status, delay_ms };
        if (count > 0) body.remaining = count;
        await must(adminApi("POST", "/api/provider/failure", body));
      }, "Provider failure applied");
      $("#fc-clear").onclick = act(() => must(adminApi("POST", "/api/provider/failure/clear", {})), "Provider restored");
      $("#fc-dup").onclick = act(() => must(adminApi("PUT", "/api/faults/relay.after_publish", { action: "duplicate", remaining: 1 })), "The next event will be published twice");
      $("#fc-crash").onclick = act(() => must(adminApi("PUT", "/api/faults/relay.after_publish", { action: "crash", remaining: 1 })), "A relay will crash after its next publish");
      $("#fc-restore-all").onclick = act(async () => {
        await must(adminApi("DELETE", "/api/faults"));
        await must(adminApi("POST", "/api/provider/failure/clear", {}));
        await must(adminApi("POST", "/api/provider/dedupe", { enabled: true }));
      }, "Everything restored");
    },
    async update() {
      const usable = state.config.faults_enabled && getToken();
      for (const id of ["fc-apply", "fc-clear", "fc-dup", "fc-crash", "fc-restore-all"]) $("#" + id).disabled = !usable;
      const [pf, faultsRes] = await Promise.all([
        api("/api/provider/failures").catch(() => null),
        usable ? adminApi("GET", "/api/faults").catch(() => null) : Promise.resolve(null),
      ]);
      const lines = [];
      if (pf && pf.ok) {
        for (const [ch, f] of Object.entries(pf.body || {})) {
          const parts = [];
          if (f.status) parts.push("responds " + f.status);
          if (f.delay_ms) parts.push("adds " + f.delay_ms + " ms delay");
          lines.push(`${chip("RETRYING", "provider · " + ch)} ${parts.join(", ")}${f.remaining ? " for the next " + f.remaining + " call(s)" : ""}`);
        }
      }
      if (faultsRes && faultsRes.ok) {
        for (const f of faultsRes.body.faults || []) {
          if (f.remaining === 0) continue; // used up
          lines.push(`${chip("RETRYING", f.point)} ${esc(f.action)}${f.remaining != null ? " (" + f.remaining + " left)" : ""}`);
        }
      }
      $("#fc-active").innerHTML = lines.length ? lines.map((l) => `<div style="margin:4px 0">${l}</div>`).join("")
        : `<span class="muted">Nothing injected. ${!state.config.faults_enabled ? "Fault injection is disabled on this server." : !getToken() ? "Enter the admin token (top right) to use these controls." : ""}</span>`;
    },
  },

  idempotency: {
    title: "Idempotency",
    desc: "Duplicate deliveries happen by design. This page shows where each one was stopped, and whether any duplicate side effect got through.",
    running: false,
    mount(root) {
      root.innerHTML = `
        <div class="tiles" id="id-tiles"></div>
        <div class="grid-2">
          <div class="card">
            <div class="card-head"><h2>Duplicate-delivery experiment</h2></div>
            <p class="muted small" style="margin-bottom:10px">Creates one PaymentSucceeded event while the relay is told to publish every message twice, bypassing the broker's own de-duplication. Then it shows what each layer did with the copies.</p>
            <label class="field"><span>Protections</span>
              <select id="id-mode">
                <option value="normal">Both on (normal): consumers and provider</option>
                <option value="provider">Consumer shortcut off: the provider alone must stop duplicates</option>
                <option value="control">Negative control: both off, duplicates should get through</option>
              </select></label>
            <div class="row"><button class="btn btn-primary" id="id-run" disabled>Run experiment</button><span id="id-needs" class="muted small"></span></div>
            <p id="id-progress" class="muted small" style="margin-top:8px" aria-live="polite"></p>
          </div>
          <div class="card"><div class="card-head"><h2>Result</h2></div>
            <div id="id-result" class="muted">Run the experiment to see the outcome.</div></div>
        </div>`;
      $("#id-run").onclick = () => this.run();
    },
    async update() {
      const r = await api("/api/idempotency").catch(() => null);
      if (r && r.ok) this.renderTiles(r.body);
      const canRun = state.config.faults_enabled && getToken();
      $("#id-run").disabled = !canRun || this.running;
      $("#id-needs").textContent = !state.config.faults_enabled ? "Fault injection is disabled on this server."
        : !getToken() ? "Enter the admin token (top right)." : "";
    },
    renderTiles(d) {
      const p = d.provider || {};
      $("#id-tiles").innerHTML = [
        tile("Deliveries completed", num(d.deliveries_delivered), "all channels"),
        tile("Ignored by consumers", num(d.duplicates_ignored_by_consumers), "duplicate stopped before the provider"),
        tile("Suppressed by provider", num(d.duplicate_requests_suppressed_by_provider), "duplicate stopped by the key"),
        tile("Side effects executed", num(p.effects), `${num(p.requests)} provider requests`),
        tile("Duplicate side effects", num(p.duplicate_effects), "must stay at 0", (p.duplicate_effects || 0) > 0),
        tile("Provider idempotency", p.dedupe === false ? "OFF" : "On", p.dedupe === false ? "negative control active" : "normal", p.dedupe === false),
      ].join("");
    },
    async run() {
      if (this.running) return;
      this.running = true; $("#id-run").disabled = true;
      const mode = $("#id-mode").value, out = $("#id-result"), say = (t) => { $("#id-progress").textContent = t; };
      let eventId = null;
      out.textContent = "Running…"; // never show the previous run's result while a new one is in progress
      try {
        say("Configuring faults…");
        if (mode !== "normal") await must(adminApi("PUT", "/api/faults/consumer.skip_delivered_check", { action: "disable" }));
        if (mode === "control") await must(adminApi("POST", "/api/provider/dedupe", { enabled: false }));
        await must(adminApi("PUT", "/api/faults/relay.after_publish", { action: "duplicate", remaining: 1 }));
        say("Waiting for every process to pick up the change…");
        await sleep(2500);

        say("Creating the event…");
        const body = {}; for (const f of EVENT_TYPES["payment-succeeded"].fields) body[f.key] = f.random();
        const r = await sections.create.post("payment-succeeded", crypto.randomUUID(), body);
        if (r.status !== 201) throw new Error("create failed: " + r.status);
        eventId = r.body.event_id;

        say("Waiting for delivery…");
        await waitUntil(async () => {
          const e = await api("/api/events/" + eventId);
          return e.ok && e.body.status === "PUBLISHED" && e.body.deliveries.every((d) => d.status === "DELIVERED");
        }, 30000);

        say("Waiting for the duplicate copies to be handled…");
        let data;
        await waitUntil(async () => {
          data = (await api("/api/idempotency?event=" + eventId)).body;
          return mode === "control" ? (data.provider || {}).effects >= 6
            : data.duplicates_ignored_by_consumers + (data.duplicate_requests_suppressed_by_provider || 0) >= 3;
        }, 20000);
        this.showResult(mode, data);
      } catch (e) {
        out.textContent = "Experiment failed: " + e.message;
        toast("Experiment failed: " + e.message, "bad");
      } finally {
        say("Restoring normal settings…");
        await adminApi("DELETE", "/api/faults/relay.after_publish").catch(() => {});
        await adminApi("DELETE", "/api/faults/consumer.skip_delivered_check").catch(() => {});
        await adminApi("POST", "/api/provider/dedupe", { enabled: true }).catch(() => {});
        // The control experiment makes real duplicate effects; remove them so the live counters stay honest.
        if (mode === "control" && eventId) await adminApi("POST", "/api/provider/forget", { prefix: eventId + ":" }).catch(() => {});
        say(""); this.running = false; this.update();
      }
    },
    showResult(mode, d) {
      const p = d.provider || {}, ignored = d.duplicates_ignored_by_consumers, suppressed = d.duplicate_requests_suppressed_by_provider || 0;
      const received = p.effects + ignored + suppressed;
      const dupEffects = p.duplicate_effects;
      const explain = {
        normal: "Each channel's consumer saw the second copy, found the delivery already complete, and ignored it. The provider never saw a duplicate.",
        provider: "The consumer shortcut was off, so the second copies reached the provider. It recognised the idempotency keys and suppressed them: the provider's protection works on its own.",
        control: "Both protections were off, so the duplicates caused duplicate side effects. This proves the counters can detect duplicates, so the zeros in the other runs mean something. These experiment rows were removed afterwards.",
      }[mode];
      $("#id-result").innerHTML = `
        <table><tbody>
          <tr><td>Messages received by consumers</td><td><strong>${num(received)}</strong></td></tr>
          <tr><td>Distinct deliveries</td><td>${num(d.deliveries_delivered)}</td></tr>
          <tr><td>Stopped by consumers (already delivered)</td><td>${num(ignored)}</td></tr>
          <tr><td>Stopped by provider (idempotency key)</td><td>${num(suppressed)}</td></tr>
          <tr><td>Side effects executed</td><td><strong>${num(p.effects)}</strong></td></tr>
          <tr><td>Duplicate side effects</td><td>${dupEffects === 0 ? chip("DELIVERED", "0 · none") : chip("DLQ", num(dupEffects) + " · duplicates got through")}</td></tr>
        </tbody></table>
        <p class="muted small" style="margin-top:10px">${esc(explain)}</p>`;
    },
  },
};

// ---------- shared rendering ----------
const tile = (label, value, note, warn) => `<div class="tile${warn ? " attention" : ""}"><div class="tile-label">${label}</div><div class="tile-value">${value}</div><div class="tile-note">${note}</div></div>`;

function channelChips(deliveries) {
  const entries = CHANNELS.filter((c) => deliveries[c]).map((c) => [c, deliveries[c]]);
  return entries.map(([c, s]) => chip(s, `${c} · ${LABEL[s] || s}`)).join("");
}

function eventRow(e, full) {
  const link = `<a href="#" data-event="${esc(e.id)}" class="mono">${shortId(e.id)}</a>`;
  return full
    ? `<tr class="clickable" data-event="${esc(e.id)}"><td>${link}</td><td>${esc(e.type)}</td><td>${chip(e.status)}</td><td>${e.attempts}</td><td>${channelChips(e.deliveries)}</td><td class="muted" title="${esc(e.created_at)}">${ago(e.created_at)}</td></tr>`
    : `<tr class="clickable" data-event="${esc(e.id)}"><td>${link}</td><td>${esc(e.type)}</td><td>${channelChips(e.deliveries)}</td><td class="muted">${ago(e.created_at)}</td></tr>`;
}

function rowClick(ev) {
  const el = ev.target.closest("[data-event]");
  if (!el) return;
  ev.preventDefault();
  openDrawer(el.dataset.event);
}

// ---------- event detail drawer ----------
async function openDrawer(id) {
  state.drawerId = id;
  history.replaceState(null, "", `#/${state.route}/${id}`); // shareable link; replaceState does not fire hashchange
  $("#drawer-title").textContent = id;
  $("#drawer").hidden = false; $("#drawer-backdrop").hidden = false;
  $("#drawer-body").innerHTML = `<p class="muted">Loading…</p>`;
  await renderDrawer();
  $("#drawer-close").focus();
}
function closeDrawer() { state.drawerId = null; history.replaceState(null, "", `#/${state.route}`); $("#drawer").hidden = true; $("#drawer-backdrop").hidden = true; }

async function renderDrawer() {
  if (!state.drawerId) return;
  const r = await api("/api/events/" + state.drawerId);
  if (!state.drawerId) return;
  if (!r.ok) { $("#drawer-body").innerHTML = `<p class="muted">${r.status === 404 ? "Event not found." : "Could not load this event."}</p>`; return; }
  const e = r.body;
  const created = new Date(e.created_at).getTime();
  const timeline = [{ at: e.created_at, text: "Committed: business change and outbox record written in one transaction" }];
  if (e.published_at) timeline.push({ at: e.published_at, text: `Relay published to the broker (${fmtMs(new Date(e.published_at) - created)} after commit)` });
  for (const d of e.deliveries) if (d.delivered_at) timeline.push({ at: d.delivered_at, text: `${d.channel} delivered (${fmtMs(new Date(d.delivered_at) - created)} after commit)` });
  timeline.sort((a, b) => new Date(a.at) - new Date(b.at));

  $("#drawer-body").innerHTML = `
    <div class="card"><div class="card-head"><h2>${esc(e.type)}</h2>${chip(e.status)}</div>
      <dl class="kv"><dt>Outbox attempts</dt><dd>${e.attempts}</dd>
      <dt>Last error</dt><dd>${e.last_error ? esc(e.last_error) : "<span class=muted>none</span>"}</dd>
      <dt>Created</dt><dd>${fmtTime(e.created_at)} <span class="muted">(${ago(e.created_at)})</span></dd>
      <dt>Published</dt><dd>${fmtTime(e.published_at)}</dd><dt>Aggregate</dt><dd class="mono">${esc(e.aggregate_id)}</dd></dl></div>
    <div class="card"><div class="card-head"><h2>Channel deliveries</h2></div>
      <div class="table-wrap"><table><thead><tr><th>Channel</th><th>State</th><th>Attempts</th><th>Detail</th></tr></thead><tbody>
      ${e.deliveries.map((d) => `<tr><td>${esc(d.channel)}</td><td>${chip(d.status)}</td><td>${d.attempts}</td>
        <td>${d.last_error ? esc(d.last_error) : d.delivered_at ? `delivered ${fmtTime(d.delivered_at)}` : "<span class=muted>–</span>"}${d.duplicate_count ? `<div class="muted small">${d.duplicate_count} duplicate message${d.duplicate_count > 1 ? "s" : ""} ignored</div>` : ""}</td></tr>`).join("")}
      </tbody></table></div></div>
    <div class="card"><div class="card-head"><h2>Timeline</h2></div>
      <ul class="timeline">${timeline.map((t) => `<li><div class="when">${fmtTime(t.at)}</div>${esc(t.text)}</li>`).join("")}</ul></div>
    <div class="card"><div class="card-head"><h2>Payload</h2></div><pre class="json">${esc(JSON.stringify(e.payload, null, 2))}</pre></div>`;
}

// ---------- routing, polling ----------
const mounted = new Set();
function show(route) {
  if (!sections[route]) route = "overview";
  state.route = route;
  const s = sections[route];
  document.title = `${s.title} · Notification Platform Console`;
  $("#page-title").textContent = s.title; $("#page-desc").textContent = s.desc;
  for (const a of document.querySelectorAll("#nav a")) {
    a.dataset.route === route ? a.setAttribute("aria-current", "page") : a.removeAttribute("aria-current");
  }
  for (const el of document.querySelectorAll("#views > section")) el.hidden = el.id !== "view-" + route;
  let view = $("#view-" + route);
  if (!view) {
    view = document.createElement("section"); view.id = "view-" + route; $("#views").appendChild(view);
  }
  if (!mounted.has(route)) { mounted.add(route); s.mount.call(s, view); }
  view.hidden = false;
  s.update.call(s);
}

async function refresh() {
  try {
    const [sum, ev] = await Promise.all([api("/api/summary"), api("/api/events?limit=100")]);
    if (!sum.ok || !ev.ok) throw new Error("API returned " + (sum.ok ? ev.status : sum.status));
    state.summary = sum.body; state.events = ev.body; state.updated = new Date(); state.error = null;
  } catch (err) { state.error = err; }
  $("#updated").textContent = state.error ? "Disconnected, retrying…" : "Updated " + fmtTime(state.updated.toISOString());
  const s = sections[state.route]; s.update.call(s);
  if (state.drawerId) renderDrawer();
}

async function checkApi() {
  const pill = $("#api-pill"), text = $("#api-pill-text");
  try {
    const r = await api("/healthz");
    const bad = Object.entries(r.body?.checks || {}).filter(([, v]) => v !== "ok").map(([k]) => k);
    pill.className = "pill " + (r.ok ? "pill-ok" : "pill-warn");
    text.textContent = r.ok ? "API, Postgres, NATS healthy" : "Degraded: " + bad.join(", ");
  } catch { pill.className = "pill pill-bad"; text.textContent = "API unreachable"; }
}

$("#live").onchange = (e) => { state.live = e.target.checked; if (state.live) refresh(); };
$("#drawer-close").onclick = closeDrawer; $("#drawer-backdrop").onclick = closeDrawer;
document.addEventListener("keydown", (e) => { if (e.key === "Escape" && state.drawerId) closeDrawer(); });
// #/<section> or #/<section>/<event id>
function parseHash() { const [route, id] = location.hash.replace(/^#\//, "").split("/"); return { route: route || "overview", id }; }
function navigate() {
  const { route, id } = parseHash();
  show(route);
  if (id) openDrawer(id); else if (state.drawerId) closeDrawer();
}
window.addEventListener("hashchange", navigate);

navigate();
refresh(); checkApi();
api("/api/config").then((r) => { if (r.ok) state.config = r.body; });
setInterval(() => { if (state.live && !document.hidden) refresh(); }, 1500);
setInterval(() => { if (!document.hidden) checkApi(); }, 5000);

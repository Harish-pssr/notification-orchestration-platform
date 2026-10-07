"use strict";
/*
 * Dashboard shell. Each section is an object in `sections` with:
 *   title, desc   shown in the page header
 *   mount(root)   called once, builds the static DOM (so form input survives refreshes)
 *   update()      called after every data refresh while the section is visible
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
const state = { live: true, route: "overview", summary: null, events: [], updated: null, error: null, drawerId: null, filters: { q: "", state: "all", type: "all" }, config: { faults_enabled: false, create_requires_token: false }, experiment: null };

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
const sections = {
  start: {
    title: "Start here",
    desc: "What this project is, how one event travels through it, and a two-minute way to try it.",
    mount(root) {
      root.innerHTML = `
        <div class="card prose">
          <div class="card-head"><h2>What this is</h2></div>
          <p>When something happens in a business system (a payment succeeds, a booking is confirmed), the people affected should hear about it by email, push notification or webhook. This project is a notification platform built to make sure <strong>every event is delivered, even when parts of the system crash, and nobody is notified twice</strong>.</p>
          <p>It is the kind of plumbing that sits behind products you use every day. The interesting part is not sending a notification: it is what happens when the database, the message broker, a worker or the external email service fails halfway.</p>
        </div>

        <div class="card prose">
          <div class="card-head"><h2>The journey of one event</h2><p>Each step has a matching counter on the Overview page.</p></div>
          <ol class="steps">
            <li><strong>The API saves the event.</strong> It writes the business change (say, the payment) and a record of the event to the database in <em>one transaction</em>. Either both are saved or neither is.</li>
            <li><strong>The outbox holds it.</strong> The event record is a to-do list entry in the same database, "saved, not yet handed on". Because it is committed with the payment, an event can never be lost between "the payment happened" and "we told someone".</li>
            <li><strong>The relay hands it to the broker.</strong> A background worker picks up outbox entries and publishes one small message per channel to a message broker (NATS JetStream), which stores each message until somebody confirms it.</li>
            <li><strong>A consumer asks the provider to send it.</strong> There is one consumer for each channel (email, push, webhook). It takes a message and calls the provider, which stands in for the real external service.</li>
            <li><strong>The provider sends it exactly once.</strong> Every notification carries an idempotency key. If the same notification arrives twice, the provider recognises the key and does not send it again. The delivery is then marked <em>Delivered</em>.</li>
          </ol>
        </div>

        <div class="card prose">
          <div class="card-head"><h2>Why you can trust it</h2></div>
          <ul class="points">
            <li><strong>Nothing is lost.</strong> An event exists only once it is committed, and the outbox guarantees it is handed on, even if the relay crashes halfway through.</li>
            <li><strong>Repeats are harmless.</strong> The system delivers <em>at least once</em>: after a crash a message may be sent again. The provider makes sure each notification still happens once. (This system does not claim "exactly once" delivery.)</li>
            <li><strong>Failures are retried.</strong> Temporary errors are retried with growing delays. Permanent ones go to <a href="#/dlq">Dead letters</a>, where an operator can fix the cause and replay them.</li>
            <li><strong>Outages heal.</strong> While a part is down, events wait safely in the database or the broker, and drain when it returns.</li>
          </ul>
        </div>

        <div class="card prose">
          <div class="card-head"><h2>Try it in two minutes</h2></div>
          <ol class="steps" id="st-try"></ol>
        </div>

        <div class="card prose">
          <div class="card-head"><h2>What has been measured</h2><p>On one laptop (Docker Desktop on Windows); details and limits are in the repository's docs/benchmarks.md.</p></div>
          <ul class="points">
            <li><strong>Reliability under failure:</strong> 99,999 events with 3 relay crashes, about 15,000 injected duplicate messages and about 2% failing provider calls. Every delivery completed, 0 dead letters, <strong>0 duplicate notifications</strong>. Switching the protections off produced 1,425 duplicates, so the counter can see them.</li>
            <li><strong>Outages:</strong> the relay, and separately the message broker, stopped for 60 seconds under load (3 times each). Nothing was lost, and the backlog drained about 100 seconds after each restart.</li>
            <li><strong>Capacity:</strong> roughly 310 to 370 events per second end to end on that laptop. A figure from a laptop is a measurement of that laptop, not a promise.</li>
          </ul>
        </div>

        <div class="card prose">
          <div class="card-head"><h2>Words used on these pages</h2></div>
          <dl class="glossary">
            <dt>Event</dt><dd>Something that happened in the business, such as a payment succeeding. One event can be delivered on several channels.</dd>
            <dt>Channel</dt><dd>A way to notify: email, push or webhook. Each event type is routed to some of them.</dd>
            <dt>Delivery</dt><dd>One event on one channel. A payment event has three deliveries; each is tracked separately.</dd>
            <dt>Outbox</dt><dd>The list, in the database, of events that are saved but not yet handed to the broker. A long outbox means the relay is behind or stopped.</dd>
            <dt>Relay</dt><dd>The worker that moves events from the outbox to the broker. Several can run at once.</dd>
            <dt>Broker</dt><dd>The message queue (NATS JetStream) that stores messages until a consumer confirms them.</dd>
            <dt>Consumer</dt><dd>The worker for one channel: takes messages from the broker and asks the provider to send them.</dd>
            <dt>Provider</dt><dd>The external service that actually sends the notification. Here it is a mock, with its own database, so it behaves like something outside the platform.</dd>
            <dt>Idempotency key</dt><dd>A unique id for one operation. Repeating a request with the same key returns the first result instead of doing the work again; this is how duplicates are made harmless.</dd>
            <dt>At least once</dt><dd>The delivery guarantee: a message is never lost but may arrive more than once.</dd>
            <dt>Retry and backoff</dt><dd>After a temporary failure, try again after a growing, slightly randomised delay.</dd>
            <dt>Dead letter</dt><dd>A delivery that gave up (retries used up, or rejected for good). It is kept with its reason so it can be replayed.</dd>
            <dt>Replay</dt><dd>Send a dead-lettered delivery again once the cause is fixed. Safe, because the provider recognises work it already did.</dd>
            <dt>Backlog / in flight</dt><dd>Work that is waiting or not yet confirmed. It grows when a part is slow or down and drains when it recovers.</dd>
          </dl>
        </div>`;
    },
    update() {
      const cfg = state.config;
      const steps = [];
      steps.push(`<li><a href="#/create">Create an event.</a> Send a PaymentSucceeded: it is routed to email, push and webhook.${cfg.create_requires_token ? " On this deployment creating events needs the admin token (the box at the top right); everything else can be viewed freely." : ""}</li>`);
      steps.push(`<li>Watch it on the <a href="#/overview">Overview</a>: the counters and the pipeline move as the event passes each step.</li>`);
      steps.push(`<li>Open it in <a href="#/events">Events</a> (click the row) to see its timeline: when it was created, when it was published, and when each channel delivered it.</li>`);
      steps.push(cfg.faults_enabled
        ? `<li>Break something on purpose in <a href="#/faults">Failure controls</a>: make the webhook provider return errors, create an event, and watch it retry and then land in <a href="#/dlq">Dead letters</a>. Restore the provider and replay it. Or pause the relay on <a href="#/health">Health</a> and watch the backlog build and drain.</li>`
        : `<li>Failure controls are switched off on this deployment so visitors cannot break it. The cases they cover (a crashing relay, a failing provider, a broker outage, duplicate messages) are described in the repository's docs/failure-scenarios.md, and <a href="#/idempotency">Idempotency</a> shows where duplicates are stopped.</li>`);
      $("#st-try").innerHTML = steps.join("");
    },
  },

  overview: {
    title: "Overview",
    desc: "A live picture of the pipeline: how many events exist, where they are right now, and whether anything needs attention. Counted exactly from the database.",
    mount(root) {
      root.innerHTML = `
        <div class="tiles" id="ov-tiles"></div>
        <div class="card">
          <div class="card-head"><h2>Pipeline</h2><p>Where events are right now, from saved to delivered. New here? <a href="#/start">Start here</a> explains each step.</p></div>
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
        tile("Events created", num(created), "saved by the API"),
        tile("Waiting for relay", num(backlog), "saved, not yet passed on; normally near 0"),
        tile("Published to broker", num(ev.PUBLISHED || 0), "handed to the message broker"),
        tile("Deliveries completed", `${num(delivered)}<span class="muted small"> / ${num(total)}</span>`, "one per event and channel"),
        tile("Needs attention", num(trouble), "retrying or gave up; 0 is healthy", trouble > 0),
        tile("Latency (p50 / p95)", lat.count ? fmtPair(lat.p50_ms, lat.p95_ms) : "–", `saved to delivered, last ${num(lat.count)} deliveries`),
      ].join("");

      $("#ov-pipeline").innerHTML = `
        <div class="node"><h3>1 · API</h3><div class="big">${num(created)}</div><div class="sub">saves each event with its business change in one transaction</div></div>
        <div class="arrow" aria-hidden="true">→</div>
        <div class="node"><h3>2 · Outbox</h3><div class="big">${num(backlog)}</div><div class="sub">saved, waiting for the relay to pick them up (normally near 0)${ev.DLQ ? ` · ${num(ev.DLQ)} dead-lettered` : ""}</div></div>
        <div class="arrow" aria-hidden="true">→</div>
        <div class="node"><h3>3 · Relay → Broker</h3><div class="big">${num(ev.PUBLISHED || 0)}</div><div class="sub">handed to the message broker (NATS), which keeps them until a consumer confirms</div></div>
        <div class="arrow" aria-hidden="true">→</div>
        <div class="fan">${CHANNELS.map((ch) => {
          const c = s.channels[ch] || {};
          const inflight = (c.PENDING || 0) + (c.RETRYING || 0);
          return `<div class="channel-node"><strong>${ch}</strong><span class="big">${num(c.DELIVERED || 0)}</span>
            <div class="channel-stats"><span>delivered</span><span title="Waiting for a consumer, or being retried">${num(inflight)} not yet delivered</span>${c.DLQ ? `<span style="color:var(--bad)">${num(c.DLQ)} dead-lettered</span>` : ""}</div></div>`;
        }).join("")}</div>`;

      const recent = state.events.slice(0, 8);
      $("#ov-recent").innerHTML = recent.length ? recent.map((e) => eventRow(e, false)).join("")
        : `<tr><td colspan="4" class="empty">No events yet. <a href="#/create">Create one</a> and watch it travel through the pipeline.</td></tr>`;
    },
  },

  create: {
    title: "Create events",
    desc: "Send an event to the API and watch it flow through. The API saves it, with a to-do record, in one database transaction; the relay then delivers it on every channel that event type is routed to.",
    mount(root) {
      root.innerHTML = `
        <div class="grid-2">
          <div class="card">
            <div class="card-head"><h2>New event</h2></div>
            <div class="type-cards" role="radiogroup" aria-label="Event type" id="cr-types"></div>
            <div id="cr-fields"></div>
            <label class="field"><span>Idempotency-Key: a unique id for this request. Send the same key again (use Resend) and you get the first result back instead of a second event</span>
              <input type="text" id="cr-key" class="mono" autocomplete="off"></label>
            <div class="row">
              <label class="field" style="width:110px"><span>How many</span><input type="number" id="cr-count" min="1" max="1000" value="1"></label>
              <button class="btn btn-primary" id="cr-send" style="margin-bottom:10px">Send</button>
              <button class="btn" id="cr-resend" style="margin-bottom:10px" disabled title="Repeat the last request with the same key">Resend last request</button>
            </div>
            <p class="muted small">With more than one, values are randomised and every event gets its own key.</p>
            <p class="small" id="cr-needs"></p>
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
      const headers = { "Content-Type": "application/json", "Idempotency-Key": key };
      if (getToken()) headers.Authorization = "Bearer " + getToken(); // needed when the server asks for it (D19)
      const r = await api("/api/events/" + id, { method: "POST", headers, body: JSON.stringify(body) });
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
    update() {
      // On a public deployment writes need the admin token; viewing never does.
      const needs = state.config.create_requires_token && !getToken();
      $("#cr-send").disabled = needs;
      $("#cr-needs").textContent = needs ? "Creating events needs the admin token (top right). Viewing everything else does not." : "";
    },
  },

  events: {
    title: "Events",
    desc: "Every event, with its state in the outbox and the delivery status of each channel. Click a row to see its full timeline.",
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

  health: {
    title: "Health",
    desc: "Which parts of the system are running and how much work is waiting in front of each. Workers come and go, so health is per role (the relay, or one channel's consumer): UP while at least one instance reports in, DOWN when none does, PAUSED when an operator paused it.",
    data: null,
    mount(root) {
      root.innerHTML = `
        <div id="hl-roles" class="tiles"></div>
        <div class="card">
          <div class="card-head"><h2>Instances</h2><p id="hl-note" class="muted small"></p></div>
          <div class="table-wrap"><table>
            <thead><tr><th>Instance</th><th>Role</th><th>State</th><th>Settings</th><th>Last heartbeat</th></tr></thead>
            <tbody id="hl-instances"></tbody></table></div>
          <p style="margin-top:10px"><button class="btn btn-ghost" id="hl-gone" hidden></button></p>
        </div>`;
      root.addEventListener("click", async (e) => {
        if (e.target.closest("#hl-gone")) { this.showGone = !this.showGone; this.render(); return; }
        const b = e.target.closest("button[data-pause]");
        if (!b) return;
        const point = b.dataset.pause, resume = b.dataset.action === "resume";
        try {
          if (resume) await must(adminApi("DELETE", "/api/faults/" + point));
          else await must(adminApi("PUT", "/api/faults/" + point, { action: "pause" }));
          toast(resume ? "Resumed" : "Paused");
        } catch (err) { toast(err.message, "bad"); }
        this.update();
      });
    },
    async update() {
      const r = await api("/api/health").catch(() => null);
      if (r && r.ok) { this.data = r.body; this.render(); }
    },
    render() {
      const d = this.data, usable = state.config.faults_enabled && getToken();
      const pretty = { "relay": "Relay", "consumer-email": "Email consumer", "consumer-push": "Push consumer", "consumer-webhook": "Webhook consumer" };
      const pausePoint = { "relay": "relay.pause", "consumer-email": "consumer.email.pause", "consumer-push": "consumer.push.pause", "consumer-webhook": "consumer.webhook.pause" };
      const statusChip = { UP: chip("DELIVERED", "UP"), DOWN: chip("DLQ", "DOWN"), PAUSED: chip("RETRYING", "PAUSED") };

      $("#hl-roles").innerHTML = (d.roles || []).map((role) => {
        let backlog = "";
        if (role.name === "relay") {
          backlog = d.backlog ? `${num(d.backlog.outbox)} event(s) waiting in the outbox` : "";
        } else {
          const ch = role.name.replace("consumer-", ""), b = d.broker && d.broker.channels && d.broker.channels[ch];
          const waiting = d.backlog && d.backlog.deliveries ? (d.backlog.deliveries[ch] || 0) : 0;
          backlog = (b ? `${num(b.pending)} waiting in broker, ${num(b.ack_pending)} in flight<br>` : "") + `${num(waiting)} not yet delivered`;
        }
        const paused = role.status === "PAUSED";
        const btn = `<button class="btn" style="margin-top:8px" data-pause="${pausePoint[role.name]}" data-action="${paused ? "resume" : "pause"}" ${usable ? "" : "disabled"}>${paused ? "Resume" : "Pause"}</button>`;
        return `<div class="tile"><div class="tile-label">${pretty[role.name] || esc(role.name)}</div>
          <div style="margin:4px 0">${statusChip[role.status] || esc(role.status)}</div>
          <div class="tile-note">${role.live} live instance${role.live === 1 ? "" : "s"}</div>
          <div class="tile-note" style="margin-top:6px">${backlog}</div>${btn}</div>`;
      }).join("");

      // Instances that went away recently stay visible as evidence of an outage; ones
      // gone for more than five minutes (old deploys) are hidden as noise.
      const all = (d.roles || []).flatMap((role) => role.instances).filter((i) => i.fresh || i.age_s < 300);
      // Stopped instances (scaled down or restarted) look like a fault, so they are hidden unless asked for.
      const stopped = all.filter((i) => !i.fresh);
      const shown = this.showGone ? all : all.filter((i) => i.fresh);
      const goneBtn = $("#hl-gone");
      goneBtn.hidden = stopped.length === 0;
      goneBtn.textContent = this.showGone ? "Hide stopped instances" : `Show ${stopped.length} stopped instance${stopped.length === 1 ? "" : "s"}`;
      $("#hl-instances").innerHTML = shown.length ? shown.map((i) => `<tr>
          <td class="mono">${esc(i.name)}</td><td>${esc(i.role)}</td>
          <td>${i.fresh ? (i.state === "PAUSED" ? chip("RETRYING", "PAUSED") : chip("DELIVERED", "RUNNING")) : chip("DLQ", "GONE")}</td>
          <td class="muted small">${esc(settingsText(i.config))}</td>
          <td class="muted">${i.age_s < 2 ? "just now" : Math.round(i.age_s) + " s ago"}</td></tr>`).join("")
        : `<tr><td colspan="5" class="empty">No worker has sent a heartbeat yet.</td></tr>`;
      $("#hl-note").textContent = `Running means a heartbeat within the last ${d.stale_after_s} s. Instances that stopped (scaled down or restarted) are hidden by default.` +
        (usable ? "" : " Pause and resume need the admin token and fault injection enabled.");
    },
  },

  experiment: {
    title: "Experiment",
    desc: "Appears while a benchmark or failure experiment is running: what is being changed, which phase the run is in, and live counters that should end at nothing lost and nothing duplicated.",
    samples: [],
    provider: null,
    mount(root) {
      root.innerHTML = `
        <div class="card" id="ex-banner"></div>
        <div class="tiles" id="ex-tiles"></div>
        <div class="grid-2">
          <div class="card"><div class="card-head"><h2>Backlog over time</h2><p class="muted small">Events not yet handed to the broker, sampled while this page is open. Dashed lines are phase changes.</p></div><div id="ex-chart"></div></div>
          <div class="card"><div class="card-head"><h2>Phases</h2></div><ul class="timeline" id="ex-phases"></ul></div>
        </div>`;
    },
    async update() {
      const x = state.experiment;
      if (!x || !x.run) {
        $("#ex-banner").innerHTML = `<div class="empty">No experiment has been recorded. Run <span class="mono">bench reliability</span> or <span class="mono">bench recovery</span>.</div>`;
        $("#ex-tiles").innerHTML = ""; $("#ex-chart").innerHTML = ""; $("#ex-phases").innerHTML = "";
        return;
      }
      const r = await api("/api/idempotency").catch(() => null);
      if (r && r.ok) this.provider = r.body;
      this.render(x);
    },
    render(x) {
      const startedMs = Date.parse(x.markers[0].created_at), last = x.markers[x.markers.length - 1];
      const params = Object.assign({}, ...x.markers.map((m) => m.params || {}));
      const elapsed = Math.max(0, Math.round(((x.active ? Date.now() : Date.parse(last.created_at)) - startedMs) / 1000));
      const phase = [...x.markers].reverse().find((m) => m.kind !== "end") || last;
      $("#ex-banner").innerHTML = `
        <div class="card-head"><h2>${esc(x.run)}</h2>${x.active ? chip("RETRYING", "RUNNING") : chip("DELIVERED", "FINISHED")}</div>
        <p style="margin:6px 0"><strong>Phase:</strong> ${esc(phase.label)} <span class="muted">· ${elapsed} s since start</span></p>
        <p class="muted small" style="margin-bottom:6px">What is being varied in this run:</p>
        <div>${Object.entries(params).map(([k, v]) => `<span class="chip s-PUBLISHED">${esc(k.replace(/_/g, " "))}: ${esc(typeof v === "object" ? JSON.stringify(v) : v)}</span>`).join(" ") || "-"}</div>`;

      const s = state.summary, d = this.provider || {}, p = d.provider || {};
      if (s) {
        const sum = (o) => Object.values(o || {}).reduce((a, b) => a + b, 0), ev = s.events;
        const created = sum(ev), backlog = (ev.PENDING || 0) + (ev.PROCESSING || 0) + (ev.RETRYING || 0);
        let delivered = 0, total = 0, dead = 0;
        for (const ch of CHANNELS) { const c = s.channels[ch] || {}; delivered += c.DELIVERED || 0; total += sum(c); dead += c.DLQ || 0; }
        if (x.active) { this.samples.push({ t: Date.now(), v: backlog }); if (this.samples.length > 300) this.samples.shift(); }
        $("#ex-tiles").innerHTML = [
          tile("Events created", num(created), "accepted by the API"),
          tile("Deliveries completed", num(delivered), `of ${num(total)} planned (${total ? Math.round(100 * delivered / total) : 0}%)`),
          tile("Backlog", num(backlog), "events not yet published"),
          tile("Dead-lettered", num(dead), "deliveries", dead > 0),
          tile("Duplicates absorbed", num((d.duplicates_ignored_by_consumers || 0) + (d.duplicate_requests_suppressed_by_provider || 0)), "stopped by consumers or the provider"),
          tile("Duplicate side effects", num(p.duplicate_effects), "must stay at 0", (p.duplicate_effects || 0) > 0),
        ].join("");
      }
      $("#ex-chart").innerHTML = this.chart(x);
      $("#ex-phases").innerHTML = x.markers.map((m) => `<li><div class="when">${fmtTime(m.created_at)}</div>${esc(m.label)}</li>`).join("");
    },
    chart(x) {
      const pts = this.samples;
      if (pts.length < 2) return `<div class="empty">Collecting samples…</div>`;
      const W = 520, H = 140, t0 = pts[0].t, t1 = pts[pts.length - 1].t, vmax = Math.max(1, ...pts.map((q) => q.v));
      const X = (t) => ((t - t0) / Math.max(1, t1 - t0)) * W, Y = (v) => H - 8 - (v / vmax) * (H - 20);
      const line = pts.map((q) => `${X(q.t).toFixed(1)},${Y(q.v).toFixed(1)}`).join(" ");
      const marks = x.markers.map((m) => Date.parse(m.created_at)).filter((t) => t > t0 && t < t1)
        .map((t) => `<line x1="${X(t).toFixed(1)}" x2="${X(t).toFixed(1)}" y1="0" y2="${H}" stroke="currentColor" stroke-dasharray="3 3" opacity=".4"/>`).join("");
      return `<svg viewBox="0 0 ${W} ${H}" style="width:100%;height:auto" role="img" aria-label="Backlog over time">${marks}
        <polyline points="${line}" fill="none" stroke="currentColor" stroke-width="2"/>
        <text x="4" y="12" font-size="11" fill="currentColor">peak ${num(vmax)}</text></svg>`;
    },
  },

  dlq: {
    title: "Dead letters",
    desc: "Deliveries that gave up: their retries ran out, or the provider rejected them for good. Read the reason, fix the cause, then replay. Replaying is safe because the provider recognises work it already did.",
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
    desc: "Break things on purpose and watch the guarantees hold. These controls need the admin token, and they are switched off on public deployments.",
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
            <p class="muted small" style="margin-top:8px">Crash kills a relay right after the broker accepted the publish and before the database is updated; another relay recovers it once the lease expires. To pause or resume the relay or a consumer, use the Health page.</p>
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
    desc: "Duplicate messages are expected: the system delivers at least once. This page shows where each duplicate was stopped, and proves that no duplicate notification was actually sent.",
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
        normal: "Each channel's consumer absorbed the second copy: if the first was already delivered it ignored the copy, and if both were in progress at the same moment the provider recognised the idempotency key and suppressed it. Either way no second side effect happened.",
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
// An instance's settings as "key value" text, so what a run is configured with is visible (D16).
const settingsText = (c) => Object.entries(c || {}).map(([k, v]) => `${k.replace(/_/g, " ")} ${v}`).join(" · ") || "-";
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
  if (route === "start") { try { localStorage.setItem("seen-start", "1"); } catch { /* ignore */ } }
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
    const [sum, ev, ex] = await Promise.all([api("/api/summary"), api("/api/events?limit=100"), api("/api/experiment").catch(() => null)]);
    if (!sum.ok || !ev.ok) throw new Error("API returned " + (sum.ok ? ev.status : sum.status));
    state.summary = sum.body; state.events = ev.body; state.updated = new Date(); state.error = null;
    state.experiment = ex && ex.ok ? ex.body : null;
    // The Experiment page is offered only while a run is active (or when you are already on it).
    $("#nav-experiment").hidden = !(state.experiment && state.experiment.active) && state.route !== "experiment";
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
function parseHash() {
  const [route, id] = location.hash.replace(/^#\//, "").split("/");
  if (route) return { route, id };
  // First visit: explain the project. After that the Overview is the landing page.
  let seen = false;
  try { seen = !!localStorage.getItem("seen-start"); } catch { /* storage can be unavailable */ }
  return { route: seen ? "overview" : "start", id };
}
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

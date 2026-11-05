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
// "21:05:34": a plain clock time for labels. fmtTime (with milliseconds) is kept for the event timeline,
// where the order of events within a second is the point.
function fmtClock(iso) {
  if (!iso) return "–";
  const d = new Date(iso);
  return `${pad(d.getHours())}:${pad(d.getMinutes())}:${pad(d.getSeconds())}`;
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

// Plain-language version of an email error. The technical text stays in the browser console and the provider's log.
function emailError(raw) {
  console.warn("email error:", raw);
  const m = String(raw || "");
  if (/valid email address/i.test(m)) return "That is not a valid email address.";
  if (/set a recipient|save a recipient/i.test(m)) return "Save a recipient address first.";
  if (/admin token|unauthori/i.test(m)) return "Enter the admin token (top right) to change this.";
  if (/not configured/i.test(m)) return "Email is not configured on the server.";
  if (/hourly limit/i.test(m)) return "The hourly limit for live emails has been reached.";
  if (/provider unreachable|timed out|deadline|did not respond/i.test(m)) return "The email service did not respond. Please try again.";
  if (/unencrypted/i.test(m)) return "The mail server must support encryption before a password can be sent.";
  if (/smtp login|535|authenticat|credential/i.test(m)) return "The mail server rejected the sign-in. Check the SMTP username and app password in .env.";
  if (/rcpt|recipient/i.test(m)) return "The mail server refused the recipient address.";
  if (/connect|no such host|refused|dial|network/i.test(m)) return "Could not connect to the mail server. Check the host and port.";
  return "The email could not be sent.";
}

function toast(msg, kind) {
  const el = document.createElement("div");
  el.className = "toast" + (kind === "bad" ? " bad" : kind === "good" ? " good" : "");
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
    title: "Start",
    heading: "Getting started",
    desc: "What this project is and how one event travels through it.",
    mount(root) {
      const point = (title, text) => `<div class="point"><h3>${title}</h3><p>${text}</p></div>`;
      const term = (t, d) => `<div><dt>${t}</dt><dd>${d}</dd></div>`;
      root.innerHTML = `
        <div class="card">
          <div class="card-head"><h2>About this project</h2></div>
          <div class="prose">
            <p class="lead-p">When something happens in a business system (a payment succeeds, a booking is confirmed), the people affected should hear about it by email, push notification or webhook. This project makes sure <strong>every event is delivered, even when parts of the system crash, and nobody is notified twice</strong>.</p>
            <p class="muted">Delivery is at least once, and the provider makes repeats harmless. Built with Go, PostgreSQL, NATS JetStream and Docker Compose.</p>
          </div>
        </div>

        <div class="card flow" id="st-flow">
          <div class="card-head head-left">
            <h2>How an event flows</h2>
            <label class="switch switch-chip" title="Mark where each component can fail, with how the system recovers">
              <input type="checkbox" id="st-fail"><span class="switch-track"></span><span class="switch-label">Show failure points</span>
            </label>
          </div>
          <p class="muted flow-hint">Hover over a number, or the Reconciler box, to see what happens there. Switch on <strong>Show failure points</strong> to see where it can fail.</p>
          <div class="flow-stage" id="st-stage">
            <div class="flow-scroll">
            <svg class="flow-svg" viewBox="0 0 1200 460" role="group" aria-label="Diagram: a client sends an event to the API, which saves it in PostgreSQL; a relay publishes it to NATS JetStream; one consumer per channel calls the provider, which sends it once per idempotency key.">
              <defs><marker id="flow-arrow" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="7" markerHeight="7" orient="auto"><path class="arrowhead" d="M0,0 L10,5 L0,10 z"/></marker></defs>
              <rect class="zone" x="135" y="20" width="740" height="425" rx="14"/><text class="zone-label" x="152" y="42">Your platform</text>
              <rect class="zone" x="905" y="20" width="285" height="425" rx="14"/><text class="zone-label" x="922" y="42">External system (a mock here)</text>
              <path class="ar" d="M110,80 H158"/><path class="ar" d="M220,110 V168"/><path class="ar" d="M280,230 H343"/><path class="ar" d="M445,230 H518"/>
              <path class="ar" d="M645,162 H733"/><path class="ar" d="M645,238 H733"/><path class="ar" d="M645,314 H733"/>
              <path class="ar" d="M835,162 H933"/><path class="ar" d="M835,238 H933"/><path class="ar" d="M835,314 H933"/>
              <path class="ar" d="M1045,320 V353"/><path class="ar" d="M785,350 V392 H220 V292"/>
              <g class="fn"><rect x="10" y="50" width="100" height="60" rx="10"/><text x="60" y="78">Client</text><text class="t2" x="60" y="96">your app</text></g>
              <g class="fn"><rect x="160" y="50" width="120" height="60" rx="10"/><text x="220" y="78">API</text><text class="t2" x="220" y="96">one transaction</text></g>
              <g class="fn"><path class="cyl" d="M160,178 a60,8 0 0 1 120,0 v104 a60,8 0 0 1 -120,0 z"/><path class="cyl-lip" d="M160,178 a60,8 0 0 0 120,0"/><text x="220" y="212">PostgreSQL</text><text class="t2" x="220" y="234">business row</text><text class="t2" x="220" y="250">outbox record</text><text class="t2" x="220" y="266">delivery rows</text></g>
              <g class="fn"><rect x="345" y="200" width="100" height="60" rx="10"/><text x="395" y="228">Relay</text><text class="t2" x="395" y="246">claim, publish</text></g>
              <g class="fn"><rect class="grp" x="520" y="100" width="140" height="250" rx="12"/><text x="590" y="124">NATS JetStream</text>
                <rect class="lane" x="535" y="140" width="110" height="44" rx="8"/><text x="590" y="166">email</text>
                <rect class="lane" x="535" y="216" width="110" height="44" rx="8"/><text x="590" y="242">push</text>
                <rect class="lane" x="535" y="292" width="110" height="44" rx="8"/><text x="590" y="318">webhook</text></g>
              <g class="fn"><rect class="grp" x="725" y="100" width="120" height="250" rx="12"/><text x="785" y="124">Consumers</text>
                <rect class="lane" x="735" y="140" width="100" height="44" rx="8"/><text x="785" y="158">Consumer</text><text class="t2" x="785" y="173">email</text>
                <rect class="lane" x="735" y="216" width="100" height="44" rx="8"/><text x="785" y="234">Consumer</text><text class="t2" x="785" y="249">push</text>
                <rect class="lane" x="735" y="292" width="100" height="44" rx="8"/><text x="785" y="310">Consumer</text><text class="t2" x="785" y="325">webhook</text></g>
              <g class="fn"><rect x="935" y="120" width="220" height="200" rx="12"/><text x="1045" y="152">Provider</text>
                <text class="t2" x="1045" y="176">stands in for the real</text><text class="t2" x="1045" y="192">email, push and webhook</text><text class="t2" x="1045" y="208">services</text>
                <text class="t2" x="1045" y="252">sends one notification</text><text class="t2" x="1045" y="268">per idempotency key</text></g>
              <g class="fn"><path class="cyl" d="M985,363 a60,8 0 0 1 120,0 v64 a60,8 0 0 1 -120,0 z"/><path class="cyl-lip" d="M985,363 a60,8 0 0 0 120,0"/><text x="1045" y="392">provider DB</text><text class="t2" x="1045" y="408">key + effect,</text><text class="t2" x="1045" y="421">one transaction</text></g>
              <path class="ar dashed" d="M395,305 V263"/><path class="ar dashed" d="M345,333 H312 V262 H284"/>
              <g class="fn" data-tip="rec" tabindex="0" role="button" aria-label="Reconciler, a background safety net"><rect x="345" y="305" width="100" height="56" rx="10"/><text x="395" y="330">Reconciler</text><text class="t2" x="395" y="348">safety net</text></g>
              <g class="badge" data-tip="s1" tabindex="0" role="button" aria-label="Step 1"><circle class="hit" cx="134" cy="80" r="17"/><circle cx="134" cy="80" r="10"/><text x="134" y="80">1</text></g><g class="badge" data-tip="s2" tabindex="0" role="button" aria-label="Step 2"><circle class="hit" cx="220" cy="139" r="17"/><circle cx="220" cy="139" r="10"/><text x="220" y="139">2</text></g>
              <g class="badge" data-tip="s3" tabindex="0" role="button" aria-label="Step 3"><circle class="hit" cx="311" cy="230" r="17"/><circle cx="311" cy="230" r="10"/><text x="311" y="230">3</text></g><g class="badge" data-tip="s4" tabindex="0" role="button" aria-label="Step 4"><circle class="hit" cx="482" cy="230" r="17"/><circle cx="482" cy="230" r="10"/><text x="482" y="230">4</text></g>
              <g class="badge" data-tip="s5" tabindex="0" role="button" aria-label="Step 5"><circle class="hit" cx="690" cy="238" r="17"/><circle cx="690" cy="238" r="10"/><text x="690" y="238">5</text></g><g class="badge" data-tip="s6" tabindex="0" role="button" aria-label="Step 6"><circle class="hit" cx="884" cy="238" r="17"/><circle cx="884" cy="238" r="10"/><text x="884" y="238">6</text></g>
              <g class="badge" data-tip="s7" tabindex="0" role="button" aria-label="Step 7"><circle class="hit" cx="470" cy="392" r="17"/><circle cx="470" cy="392" r="10"/><text x="470" y="392">7</text></g>
              <g class="fail-mark" data-tip="fA" tabindex="0" role="button" aria-label="Failure point A"><circle class="hit" cx="280" cy="50" r="17"/><circle cx="280" cy="50" r="10"/><text x="280" y="50">A</text></g>
              <g class="fail-mark" data-tip="fB" tabindex="0" role="button" aria-label="Failure point B"><circle class="hit" cx="445" cy="200" r="17"/><circle cx="445" cy="200" r="10"/><text x="445" y="200">B</text></g>
              <g class="fail-mark" data-tip="fC" tabindex="0" role="button" aria-label="Failure point C"><circle class="hit" cx="660" cy="100" r="17"/><circle cx="660" cy="100" r="10"/><text x="660" y="100">C</text></g>
              <g class="fail-mark" data-tip="fD" tabindex="0" role="button" aria-label="Failure point D"><circle class="hit" cx="845" cy="100" r="17"/><circle cx="845" cy="100" r="10"/><text x="845" y="100">D</text></g>
              <g class="fail-mark" data-tip="fE" tabindex="0" role="button" aria-label="Failure point E"><circle class="hit" cx="1155" cy="120" r="17"/><circle cx="1155" cy="120" r="10"/><text x="1155" y="120">E</text></g>
              <g class="fail-mark" data-tip="fF" tabindex="0" role="button" aria-label="Failure point F"><circle class="hit" cx="520" cy="100" r="17"/><circle cx="520" cy="100" r="10"/><text x="520" y="100">F</text></g>
            </svg>
            </div>
            <div class="flow-tip" id="st-tip" role="tooltip" hidden></div>
          </div>
        </div>

        <div class="card">
          <div class="card-head"><h2>Guarantees</h2></div>
          <div class="point-grid">
            ${point("No lost events", "An event exists only once it is committed, and the outbox guarantees it is handed on, even if the relay crashes halfway through.")}
            ${point("Harmless repeats", "The system delivers <em>at least once</em>: after a crash a message may be sent again. The provider makes sure each notification still happens once. (It does not claim \"exactly once\" delivery.)")}
            ${point("Retried failures", "Temporary errors are retried with growing delays. Permanent ones go to <a href=\"#/dlq\">Dead letters</a>, where an operator can fix the cause and replay them.")}
            ${point("Self-healing", "While a part is down, events wait safely in the database or the broker, and drain when it returns.")}
          </div>
        </div>

        <div class="card" id="st-glossary">
          <div class="card-head head-left">
            <h2>Glossary</h2>
            <label class="switch switch-chip" title="Show the definitions of the words used in this console">
              <input type="checkbox" id="st-gloss"><span class="switch-track"></span><span class="switch-label">Show definitions</span>
            </label>
          </div>
          <dl class="glossary">
              ${term("Event", "Something that happened in the business, such as a payment succeeding. One event can be delivered on several channels.")}
              ${term("Channel", "A way to notify: email, push or webhook. Each event type is routed to some of them.")}
              ${term("Delivery", "One event on one channel. A payment event has three deliveries, each tracked separately.")}
              ${term("Outbox", "The list, in the database, of events that are saved but not yet handed to the broker. A long outbox means the relay is behind or stopped.")}
              ${term("Relay", "The worker that moves events from the outbox to the broker. Several can run at once.")}
              ${term("Broker", "The message queue (NATS JetStream) that stores messages until a consumer confirms them.")}
              ${term("Consumer", "The worker for one channel: takes messages from the broker and asks the provider to send them.")}
              ${term("Provider", "The external service that actually sends the notification. Here it is a mock with its own database, so it behaves like something outside the platform.")}
              ${term("Idempotency key", "A unique id for one operation. Repeating a request with the same key returns the first result instead of doing the work again; this is how duplicates are made harmless.")}
              ${term("At least once", "The delivery guarantee: a message is never lost but may arrive more than once.")}
              ${term("Retry and backoff", "After a temporary failure, try again after a growing, slightly randomised delay.")}
              ${term("Dead letter", "A delivery that gave up (retries used up, or rejected for good). It is kept with its reason so it can be replayed.")}
              ${term("Replay", "Send a dead-lettered delivery again once the cause is fixed. Safe, because the provider recognises work it already did.")}
              ${term("Reconciler", "A background check inside the relay. If the broker loses a message, the delivery would wait forever; the reconciler notices deliveries that stay waiting far too long and sends them through again. Safe, because repeats are harmless.")}
              ${term("Backlog", "Work that is waiting or not yet confirmed. It grows when a part is slow or down and drains when it recovers.")}
            </dl>
        </div>`;
      $("#st-fail").onchange = (e) => $("#st-flow").classList.toggle("show-fail", e.target.checked);
      // Hover (or focus, or tap) a numbered step or a failure marker to read what happens there.
      const TIPS = {rec: {"t": "Reconciler (a background safety net)", "d": "Not a step in the flow. It runs inside the relay and checks the database for deliveries that stay unfinished long after their event was published, which means the broker lost the message. It sends them back through the relay to be published again. Repeats are harmless, so a wrong guess does no damage."}, s1: {"t": "1 · Send", "d": "The business system sends an event with an Idempotency-Key. A retry with the same key never creates a second event."}, s2: {"t": "2 · Save", "d": "The API writes the business row, an outbox record and one delivery row per channel in one transaction: all saved, or none."}, s3: {"t": "3 · Claim", "d": "A relay claims due outbox records with a short lease. If it dies, the lease expires and another relay takes over."}, s4: {"t": "4 · Publish", "d": "The relay publishes one small message per channel to NATS JetStream and waits for its confirmation."}, s5: {"t": "5 · Pull", "d": "Each channel's consumer pulls its messages. Anything not acknowledged is delivered again."}, s6: {"t": "6 · Send", "d": "The consumer calls the provider with an idempotency key. The provider sends once per key, however many repeats arrive."}, s7: {"t": "7 · Record", "d": "The consumer marks the delivery as delivered and acknowledges the message."}, fA: {"t": "A · API failure before commit", "d": "No state is written. The client retries with the same idempotency key, and the event is created once."}, fB: {"t": "B · Relay failure after publishing", "d": "The lease expires and another relay takes over the record. The republished message is harmless, because delivery is idempotent downstream."}, fC: {"t": "C · Broker unavailable", "d": "Events remain durable in PostgreSQL. Relays back off and retry without consuming attempts, and publishing resumes when the broker recovers."}, fD: {"t": "D · Consumer failure after the provider call", "d": "The unacknowledged message is redelivered. The provider recognises the idempotency key and does not send a second notification."}, fF: {"t": "F · Broker loses its messages", "d": "The event says published, but the message is gone, so the delivery never arrives. The reconciler notices deliveries that stay silent for too long, sends them through the relay again, and dead-letters any it cannot recover after three repairs."}, fE: {"t": "E · Provider failure", "d": "Transient errors are retried with exponential backoff. Deliveries that exhaust their retries move to Dead letters and can be replayed once the cause is resolved."}};
      const stage = $("#st-stage"), tip = $("#st-tip");
      const showTip = (el) => {
        const info = TIPS[el.dataset.tip];
        if (!info) return;
        tip.innerHTML = `<strong>${esc(info.t)}</strong><p>${esc(info.d)}</p>`;
        tip.classList.toggle("fail", el.dataset.tip.startsWith("f")); // failure points get a red border
        tip.hidden = false;
        const box = stage.getBoundingClientRect(), r = el.getBoundingClientRect(), w = tip.offsetWidth;
        tip.style.left = Math.max(0, Math.min(r.left - box.left + r.width / 2 - w / 2, box.width - w)) + "px";
        tip.style.top = (r.bottom - box.top + 8) + "px";
      };
      const hideTip = () => { tip.hidden = true; };
      const target = (e) => e.target.closest && e.target.closest("[data-tip]");
      stage.addEventListener("pointerover", (e) => { const el = target(e); if (el) showTip(el); });
      stage.addEventListener("pointerout", (e) => { if (target(e)) hideTip(); });
      stage.addEventListener("focusin", (e) => { const el = target(e); if (el) showTip(el); });
      stage.addEventListener("focusout", hideTip);
      stage.addEventListener("click", (e) => { const el = target(e); if (el) showTip(el); });
      document.addEventListener("click", (e) => { if (!stage.contains(e.target)) hideTip(); });
      $("#st-gloss").onchange = (e) => $("#st-glossary").classList.toggle("show-glossary", e.target.checked);
    },
    update() {},
  },

  overview: {
    title: "Overview",
    heading: "Pipeline overview",
    desc: "A live picture of the pipeline: how many events exist, where they are right now, and whether anything needs attention.",
    mount(root) {
      root.innerHTML = `
        <div class="tiles" id="ov-tiles"></div>
        <div class="card">
          <div class="card-head"><h2>Pipeline</h2><p>New here? <a href="#/start">Start</a> explains each step.</p></div>
          <div class="pipeline" id="ov-pipeline"></div>
        </div>
        <div class="card">
          <div class="card-head"><h2>Latest events</h2><a href="#/events">View all events</a></div>
          <div class="table-wrap"><table><thead><tr><th>Event</th><th>Type</th><th>Email</th><th>Push</th><th>Webhook</th><th>Created</th></tr></thead>
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
        tile("Deliveries completed", `${num(delivered)}<span class="muted small"> / ${num(total)}</span>`, "one per event and channel"),
        tile("Needs attention", num(trouble), trouble ? "retrying or gave up" : "nothing is stuck", trouble > 0),
        tile("Latency (p50 / p95)", lat.count ? fmtPair(lat.p50_ms, lat.p95_ms) : "–", "saved to delivered, recent events"),
        // Only after a repair: normally nothing is lost, so the Overview stays as it was (D22).
        s.reconciled_deliveries > 0 ? tile("Repaired by reconciler", num(s.reconciled_deliveries), "lost by the broker and sent again", true) : "",
      ].join("");

      $("#ov-pipeline").innerHTML = `
        <div class="node"><h3>1 · API</h3><div class="big">${num(created)}</div><div class="sub">saved in one transaction</div></div>
        <div class="arrow" aria-hidden="true">→</div>
        <div class="node"><h3>2 · Outbox</h3><div class="big">${num(backlog)}</div><div class="sub">waiting for the relay${ev.DLQ ? ` · ${num(ev.DLQ)} gave up` : ""}</div></div>
        <div class="arrow" aria-hidden="true">→</div>
        <div class="node"><h3>3 · Broker</h3><div class="big">${num(ev.PUBLISHED || 0)}</div><div class="sub">handed to the message broker</div></div>
        <div class="arrow" aria-hidden="true">→</div>
        <div class="fan">${CHANNELS.map((ch) => {
          const c = s.channels[ch] || {};
          const open = (c.PENDING || 0) + (c.RETRYING || 0);
          const extra = [open ? `${num(open)} not yet delivered` : "", c.DLQ ? `<span style="color:var(--bad)">${num(c.DLQ)} dead-lettered</span>` : ""].filter(Boolean);
          return `<div class="channel-node"><strong>${ch}</strong><span class="big">${num(c.DELIVERED || 0)}<span class="muted small"> delivered</span></span>
            ${extra.length ? `<div class="channel-stats">${extra.map((x) => `<span>${x}</span>`).join("")}</div>` : ""}</div>`;
        }).join("")}</div>`;

      const recent = state.events.slice(0, 6);
      $("#ov-recent").innerHTML = recent.length ? recent.map((e) => eventRow(e, false)).join("")
        : `<tr><td colspan="6" class="empty">No events yet. <a href="#/create">Create one</a> and watch it travel through the pipeline.</td></tr>`;
    },
  },

  create: {
    title: "Create",
    heading: "Create events",
    desc: "Send an event to the API and watch it flow through. The API saves it, with a to-do record, in one database transaction; the relay then delivers it on every channel that event type is routed to.",
    mount(root) {
      root.innerHTML = `
        <div id="cr-notice"></div>
        <div class="card">
          <div class="card-head"><h2>1 · Event type</h2><p>Each type is delivered on the channels shown on its card.</p></div>
          <div class="type-cards" role="radiogroup" aria-label="Event type" id="cr-types"></div>
        </div>
        <div class="grid-2">
          <div class="card">
            <div class="card-head"><h2>2 · Details</h2><p id="cr-routes"></p></div>
            <div class="fields-grid" id="cr-fields"></div>
            <details class="advanced">
              <summary>Advanced: idempotency key and sending many at once</summary>
              <label class="field"><span>Idempotency key: a unique id for this request. Send the same key again (use Resend) and you get the first result back instead of a second event</span>
                <input type="text" id="cr-key" class="mono" autocomplete="off"></label>
              <label class="field" style="max-width:180px"><span>How many events</span><input type="number" id="cr-count" min="1" max="1000" value="1"></label>
              <p class="muted small">With more than one, the values are randomised and every event gets its own key.</p>
            </details>
            <div class="actions">
              <button class="btn btn-primary btn-lg" id="cr-send">Send event</button>
              <button class="btn" id="cr-resend" disabled title="Repeat the last request with the same key">Resend last request</button>
            </div>
          </div>
          <div class="card">
            <div class="card-head"><h2>3 · Request log</h2><p>Requests sent from this page</p></div>
            <ul class="log" id="cr-log"><li class="muted"><span>Nothing sent yet. Choose a type and press <strong>Send event</strong>; then follow it on the <a href="#/overview">Overview</a> or open it from here.</span></li></ul>
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
      $("#cr-routes").textContent = "Delivered on: " + EVENT_TYPES[id].routes.join(", ");
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
      setNotice("cr-notice", needs ? "Creating events needs the admin token (top right). Viewing everything else does not." : "");
    },
  },

  events: {
    title: "Events",
    heading: "Event log",
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
            <thead><tr><th>Event</th><th>Type</th><th>Outbox</th><th>Attempts</th><th>Email</th><th>Push</th><th>Webhook</th><th>Created</th></tr></thead>
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
        : `<tr><td colspan="8" class="empty">${state.events.length ? "No events match these filters." : "No events yet."}</td></tr>`;
    },
  },

  health: {
    title: "Health",
    heading: "System health",
    desc: "Which parts of the system are running, and how much work is waiting in front of each. A part (the relay, or one channel's consumer) is UP while at least one of its instances reports in.",
    data: null,
    showGone: false,
    mount(root) {
      root.innerHTML = `
        <div id="hl-notice"></div>
        <div class="toolbar"><span class="muted small" id="hl-note"></span><span class="spacer"></span><button class="btn btn-ghost" id="hl-gone" hidden></button></div>
        <div id="hl-roles"></div>`;
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
      setNotice("hl-notice", usable ? "" : !state.config.faults_enabled
        ? "Fault injection is switched off on this server, so Pause and Resume are not available."
        : "Enter the admin token (top right) to use Pause and Resume.");
      const pretty = { "relay": "Relay", "consumer-email": "Email consumer", "consumer-push": "Push consumer", "consumer-webhook": "Webhook consumer" };
      const pausePoint = { "relay": "relay.pause", "consumer-email": "consumer.email.pause", "consumer-push": "consumer.push.pause", "consumer-webhook": "consumer.webhook.pause" };
      const statusChip = { UP: chip("DELIVERED", "UP"), DOWN: chip("DLQ", "DOWN"), PAUSED: chip("RETRYING", "PAUSED") };

      // Instances that stopped (scaled down or restarted) look like a fault, so they are hidden unless asked for.
      const stopped = (d.roles || []).flatMap((role) => role.instances).filter((i) => !i.fresh && i.age_s < 300);
      const goneBtn = $("#hl-gone");
      goneBtn.hidden = stopped.length === 0;
      goneBtn.textContent = this.showGone ? "Hide stopped instances" : `Show ${stopped.length} stopped instance${stopped.length === 1 ? "" : "s"}`;
      $("#hl-note").textContent = `Running means a heartbeat within the last ${d.stale_after_s} s.`;

      $("#hl-roles").innerHTML = (d.roles || []).map((role) => {
        let backlog;
        if (role.name === "relay") {
          backlog = d.backlog ? `${num(d.backlog.outbox)} event${d.backlog.outbox === 1 ? "" : "s"} waiting in the outbox` : "";
        } else {
          const ch = role.name.replace("consumer-", ""), b = d.broker && d.broker.channels && d.broker.channels[ch];
          const waiting = d.backlog && d.backlog.deliveries ? (d.backlog.deliveries[ch] || 0) : 0;
          backlog = [b ? `${num(b.pending)} waiting in the broker` : "", b ? `${num(b.ack_pending)} being processed` : "", `${num(waiting)} not yet delivered`].filter(Boolean).join(" · ");
        }
        const paused = role.status === "PAUSED";
        const what = { "relay": "Moves saved events from the outbox to the message broker.",
          "consumer-email": "Takes email messages from the broker and asks the provider to send them.",
          "consumer-push": "Takes push messages from the broker and asks the provider to send them.",
          "consumer-webhook": "Takes webhook messages from the broker and asks the provider to call them." }[role.name] || "";
        const rows = role.instances.filter((i) => i.fresh || (this.showGone && i.age_s < 300));
        const instRow = (i) => `<div class="inst-row">
            <span class="mono inst-name">${esc(i.name)}</span>
            ${i.fresh ? (i.state === "PAUSED" ? chip("RETRYING", "PAUSED") : chip("DELIVERED", "RUNNING")) : chip("DLQ", "STOPPED")}
            <span class="inst-settings muted small">${esc(settingsText(i.config))}</span>
            <span class="inst-age muted small">${i.age_s < 2 ? "just now" : Math.round(i.age_s) + " s ago"}</span></div>`;
        return `<div class="card role">
          <div class="role-head">
            <div class="role-title"><h2>${pretty[role.name] || esc(role.name)}</h2>${statusChip[role.status] || esc(role.status)}
              <span class="muted small">${role.live} live instance${role.live === 1 ? "" : "s"}</span>
              <div class="role-desc muted small">${esc(what)}</div>
              ${role.name === "relay" ? `<div class="role-desc muted small">${reconcilerLine(role, state.summary)}</div>` : ""}</div>
            <div class="role-backlog muted small">${backlog}</div>
            <button class="btn" data-pause="${pausePoint[role.name]}" data-action="${paused ? "resume" : "pause"}" ${usable ? "" : "disabled"}>${paused ? "Resume" : "Pause"}</button>
          </div>
          <div class="inst-list">${rows.length ? rows.map(instRow).join("") : `<div class="inst-empty muted small">No instance is reporting in.</div>`}</div>
        </div>`;
      }).join("");
    },
  },

  experiment: {
    title: "Experiment",
    heading: "Live experiment",
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
      $("#ex-phases").innerHTML = x.markers.map((m) => `<li><div class="when">${fmtClock(m.created_at)}</div>${esc(m.label)}</li>`).join("");
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
    heading: "Dead letters",
    desc: "Deliveries that gave up: their retries ran out, or the provider rejected them for good, or the broker kept losing them and the reconciler gave up. Read the reason, fix the cause, then replay. Replaying is safe because the provider recognises work it already did.",
    selected: new Set(),
    data: { deliveries: [], events: [] },
    mount(root) {
      root.innerHTML = `
        <div id="dl-notice"></div>
        <div class="card">
          <div class="card-head"><h2>Dead-lettered deliveries</h2><p id="dl-count" class="muted small"></p></div>
          <div class="toolbar">
            <button class="btn btn-primary" id="dl-replay-sel" disabled>Replay selected</button>
            <select id="dl-channel" aria-label="Filter by channel"><option value="">All channels</option><option>email</option><option>push</option><option>webhook</option></select>
            <button class="btn" id="dl-replay-all" disabled>Replay all</button>
          </div>
          <div class="table-wrap"><table>
            <thead><tr><th></th><th>Event</th><th>Type</th><th>Channel</th><th>Attempts</th><th>Replays</th><th>Reason</th></tr></thead>
            <tbody id="dl-body"></tbody></table></div>
        </div>
        <div class="card" id="dl-events-card" hidden>
          <div class="card-head"><h2>Relay failures</h2><p>The relay could not publish these to the broker.</p></div>
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
      // The channel box filters the table and scopes "Replay all" to the same channel.
      $("#dl-channel").onchange = () => {
        const ch = $("#dl-channel").value;
        for (const k of [...this.selected]) if (ch && k.split("|")[1] !== ch) this.selected.delete(k); // never replay rows that are hidden
        this.render();
      };
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
      const all = this.data.deliveries, key = (d) => d.event_id + "|" + d.channel;
      const live = new Set(all.map(key));
      for (const k of [...this.selected]) if (!live.has(k)) this.selected.delete(k); // it was replayed or removed
      const channel = $("#dl-channel").value;
      const rows = channel ? all.filter((d) => d.channel === channel) : all;
      $("#dl-count").textContent = !all.length ? "" : channel ? `${rows.length} of ${all.length} dead (${channel})` : `${all.length} dead`;
      $("#dl-body").innerHTML = rows.length ? rows.map((d) => `<tr>
          <td><input type="checkbox" data-key="${esc(key(d))}" ${this.selected.has(key(d)) ? "checked" : ""} aria-label="Select this delivery"></td>
          <td><a href="#" data-event="${esc(d.event_id)}" class="mono">${shortId(d.event_id)}</a></td>
          <td>${esc(d.event_type)}</td><td>${esc(d.channel)}</td><td>${d.attempts}</td><td>${d.replay_count}</td>
          <td>${esc(d.last_error || "")}</td></tr>`).join("")
        : `<tr><td colspan="7" class="empty">${all.length ? `No dead-lettered deliveries on ${esc(channel)}.` : "No dead-lettered deliveries. Deliveries land here when their retries run out, the provider rejects them for good, or the broker keeps losing them."}</td></tr>`;
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
      const channel = $("#dl-channel").value, shown = channel ? this.data.deliveries.filter((d) => d.channel === channel).length : this.data.deliveries.length;
      $("#dl-replay-all").textContent = channel ? `Replay all on ${channel}` : "Replay all";
      $("#dl-replay-all").disabled = !token || shown === 0;
      setNotice("dl-notice", token ? "" : "Enter the admin token (top right) to replay deliveries.");
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
    title: "Failures",
    heading: "Failure injection",
    desc: "Break things on purpose and watch the guarantees hold. Each card is one kind of failure; the status bar shows what is currently injected.",
    mount(root) {
      root.innerHTML = `
        <div id="fc-notice"></div>
        <div class="card status-bar">
          <div><h2>Active failures</h2><div id="fc-active" class="muted">Loading…</div></div>
          <button class="btn" id="fc-restore-all">Restore everything</button>
        </div>
        <div class="scenario-grid">
          <div class="card scenario">
            <h2>Provider failure</h2>
            <p class="muted">The external service for one channel returns errors or answers slowly. Temporary errors are retried with growing delays and then dead-lettered; a 4xx error goes straight to <a href="#/dlq">Dead letters</a>. A delay longer than the consumer's 5 s timeout counts as a failure.</p>
            <div class="fields-grid">
              <label class="field"><span>Channel</span><select id="fc-channel"><option>webhook</option><option>email</option><option>push</option></select></label>
              <label class="field"><span>Respond with</span><select id="fc-status">
                <option value="500">500 Server error (temporary)</option><option value="503">503 Unavailable (temporary)</option>
                <option value="429">429 Too many requests (temporary)</option><option value="400">400 Bad request (permanent)</option>
                <option value="0">No error, delay only</option></select></label>
              <label class="field"><span>Extra delay (ms)</span><input type="number" id="fc-delay" min="0" max="60000" value="0"></label>
              <label class="field"><span>For the next N calls</span><input type="number" id="fc-count" min="1" placeholder="all"></label>
            </div>
            <div class="actions"><button class="btn btn-primary" id="fc-apply">Apply failure</button><button class="btn" id="fc-clear">Restore provider</button></div>
          </div>
          <div class="card scenario">
            <h2>Duplicate delivery</h2>
            <p class="muted">The relay publishes the next event's messages a second time, skipping the broker's own de-duplication. Consumers and the provider must make sure no one is notified twice. The <a href="#/idempotency">Duplicates</a> page runs this as a complete experiment and shows the counts.</p>
            <div class="actions"><button class="btn btn-primary" id="fc-dup">Duplicate the next event</button></div>
          </div>
          <div class="card scenario">
            <h2>Relay crash</h2>
            <p class="muted">A relay is killed right after the broker accepted the messages and before the database was updated, the worst moment. Another relay takes the event over once the lease expires and publishes it again; the duplicate is then absorbed downstream.</p>
            <div class="actions"><button class="btn btn-primary" id="fc-crash">Crash a relay once</button></div>
          </div>
          <div class="card scenario">
            <h2>Paused worker</h2>
            <p class="muted">Pausing the relay or a consumer stops it taking work. Nothing is lost: events wait safely in the outbox or in the broker and drain when it resumes. Pause and resume live on the Health page, next to each worker.</p>
            <div class="actions"><a class="btn btn-primary" href="#/health">Open Health</a></div>
          </div>
          <div class="card scenario scenario-wide">
            <h2>Broker loses its messages</h2>
            <p class="muted">Deletes every message the broker is holding, as if its storage had been lost. The events stay safe in the database, but deliveries waiting in the broker will never arrive on their own. The reconciler sends deliveries through again once they have shown no sign of life for <strong id="fc-recon-after">10m</strong>, and gives up after three repairs (they then appear in <a href="#/dlq">Dead letters</a>). The repair count is on <a href="#/health">Health</a>. To watch it quickly on a development stack, start it with a short wait (<span class="mono">RECONCILE_AFTER=30s</span>).</p>
            <div class="actions"><button class="btn btn-primary" id="fc-purge">Delete broker messages</button></div>
          </div>
        </div>`;
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
      $("#fc-purge").onclick = async () => {
        if (!confirm("Delete every message the broker is holding?\n\nThe events stay in the database. Deliveries that were waiting in the broker will only arrive again after the reconciler repairs them (see Health).")) return;
        try { await must(adminApi("POST", "/api/broker/purge", {})); toast("Broker messages deleted"); } catch (e) { toast(e.message, "bad"); }
        this.update();
      };
      $("#fc-restore-all").onclick = act(async () => {
        await must(adminApi("DELETE", "/api/faults"));
        await must(adminApi("POST", "/api/provider/failure/clear", {}));
        await must(adminApi("POST", "/api/provider/dedupe", { enabled: true }));
      }, "Everything restored");
    },
    async update() {
      const usable = state.config.faults_enabled && getToken();
      setNotice("fc-notice", usable ? "" : !state.config.faults_enabled
        ? "Fault injection is switched off on this server, so these controls are view-only."
        : "Enter the admin token (top right) to use these controls.");
      for (const id of ["fc-apply", "fc-clear", "fc-dup", "fc-crash", "fc-purge", "fc-restore-all"]) $("#" + id).disabled = !usable;
      // How long the reconciler waits is reported by the relay's heartbeat.
      api("/api/health").then((hr) => {
        const relay = hr.ok && (hr.body.roles || []).find((r) => r.name === "relay");
        const inst = relay && relay.instances.find((i) => i.fresh && i.config && i.config.reconcile_after);
        const el = $("#fc-recon-after");
        if (inst && el) el.textContent = inst.config.reconcile_after;
      }).catch(() => {});
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
      $("#fc-active").innerHTML = lines.length ? lines.map((l) => `<div class="active-line">${l}</div>`).join("")
        : `<span>Nothing. The system is behaving normally.</span>`;
    },
  },

  idempotency: {
    title: "Duplicates",
    heading: "Duplicate protection",
    desc: "Duplicate messages are expected: the system delivers at least once. This page shows where each duplicate was stopped, and proves that no duplicate notification was actually sent.",
    running: false,
    mount(root) {
      const mode = (id, title, text) => `<button type="button" class="mode-card" role="radio" aria-checked="false" data-mode="${id}"><strong>${title}</strong><span class="muted small">${text}</span></button>`;
      root.innerHTML = `
        <div id="id-notice"></div>
        <div class="card">
          <div class="card-head"><h2>Protection layers</h2><p id="id-state"></p></div>
          <div class="pipeline" id="id-layers"></div>
          <div class="reqs" id="id-requests"></div>
        </div>
        <div class="card" id="id-email" hidden>
          <div class="card-head head-left"><h2>Email delivery</h2><span id="em-state"></span></div>
          <div id="em-notice"></div>
          <p class="muted">By default the email channel is a mock and nothing is sent. With live sending on, every email notification goes to the one recipient below.</p>
          <div class="email-grid">
            <div class="email-col">
              <div class="email-label">Live sending</div>
              <label class="switch switch-chip"><input type="checkbox" id="em-real"><span class="switch-track"></span><span class="switch-label">Send live emails</span></label>
              <p class="muted small" style="margin-top:8px">Always starts off after a restart.</p>
            </div>
            <div class="email-col">
              <label class="field" style="margin-bottom:0"><span class="email-label" style="display:block;margin-bottom:8px">Recipient</span>
                <input type="email" id="em-to" autocomplete="off" aria-label="Recipient email address" placeholder="recipient@example.com">
              </label>
              <div class="actions">
                <button class="btn" id="em-save">Save recipient</button>
                <button class="btn" id="em-test">Send test email</button>
              </div>
            </div>
          </div>
          <div class="email-foot">
            <p class="muted small" id="em-usage"></p>
            <p class="muted small">Live sending is capped per hour, and the benchmark tools refuse to run while it is on. A live send takes several seconds, so set <span class="mono">CALL_TIMEOUT=30s</span> in <span class="mono">.env</span>; otherwise the consumer times out and retries (with the protections off, each retry sends another email).</p>
          </div>
        </div>
        <div class="grid-2">
          <div class="card">
            <div class="card-head"><h2>Experiment</h2><p>One payment, every message published twice</p></div>
            <div class="mode-cards" role="radiogroup" aria-label="Protections" id="id-modes">
              ${mode("normal", "Both protections on", "The normal system. Consumers and the provider both guard against duplicates.")}
              ${mode("provider", "Consumer check off", "Only the provider's idempotency key stands between a duplicate and a second notification.")}
              ${mode("control", "Negative control: both off", "Duplicates should get through. This proves the counters can detect them, so the zeros mean something.")}
            </div>
            <div class="actions" style="margin-top:12px"><button class="btn btn-primary btn-lg" id="id-run" disabled>Run experiment</button></div>
            <p id="id-progress" class="muted small" style="margin-top:8px" aria-live="polite"></p>
          </div>
          <div class="card"><div class="card-head"><h2>Result</h2></div>
            <div id="id-result" class="muted">Choose a mode and run the experiment to see the outcome.</div></div>
        </div>`;
      this.mode = "normal";
      const modes = $("#id-modes");
      const paint = () => { for (const b of modes.querySelectorAll("[data-mode]")) b.setAttribute("aria-checked", String(b.dataset.mode === this.mode)); };
      modes.addEventListener("click", (ev) => { const b = ev.target.closest("[data-mode]"); if (b && !this.running) { this.mode = b.dataset.mode; paint(); } });
      paint();
      $("#id-run").onclick = () => this.run();
      this.wireEmail();
    },
    wireEmail() {
      const refresh = () => this.updateEmail();
      $("#em-real").onchange = async (e) => {
        const want = e.target.checked, typed = $("#em-to").value.trim();
        const to = typed || (this.email && this.email.recipient) || "(no recipient saved)";
        if (want && !confirm(`Switch LIVE email sending on?\n\nEvery email notification will be sent as a live email to ${to}, up to ${this.email ? this.email.max_per_hour : 20} per hour.`)) {
          e.target.checked = false; return;
        }
        try {
          const body = { real: want };
          if (typed) body.recipient = typed;
          await must(adminApi("POST", "/api/provider/email", body));
          $("#em-to").value = "";
          toast(want ? "Live email sending is on" : "Live email sending is off");
        } catch (err) { toast(emailError(err.message), "bad"); }
        refresh();
      };
      $("#em-save").onclick = async () => {
        const typed = $("#em-to").value.trim();
        if (!typed) { toast("Type the recipient's email address first", "bad"); return; }
        try { await must(adminApi("POST", "/api/provider/email", { recipient: typed })); $("#em-to").value = ""; toast("Recipient saved", "good"); }
        catch (err) { toast(emailError(err.message), "bad"); }
        refresh();
      };
      $("#em-test").onclick = async () => {
        const btn = $("#em-test"); btn.disabled = true;
        try { await must(adminApi("POST", "/api/provider/email/test", {})); toast("Test email sent to " + ((this.email && this.email.recipient) || "the recipient"), "good"); }
        catch (err) { toast("Test email failed: " + emailError(err.message), "bad"); }
        refresh();
      };
    },
    async updateEmail() {
      const r = await api("/api/provider/email").catch(() => null);
      const card = $("#id-email");
      if (!r || !r.ok) { card.hidden = true; return; }
      card.hidden = false;
      const st = this.email = r.body;
      state.email = st; renderEmailPill();
      const usable = state.config.faults_enabled && getToken() && st.configured;
      $("#em-state").innerHTML = !st.configured ? neutralChip("Mock only") : st.real ? chip("RETRYING", "Live emails ON") + (st.recipient ? ` <span class="muted small">to ${esc(st.recipient)}</span>` : "") : neutralChip("Mock: nothing is sent");
      setNotice("em-notice", !st.configured
        ? "SMTP is not configured on the server, so emails are always mock. Set EMAIL_SMTP_HOST, EMAIL_SMTP_USER and EMAIL_SMTP_PASS in .env and restart the provider."
        : !state.config.faults_enabled ? "Fault injection is switched off on this server, so these settings are view-only."
        : !getToken() ? "Enter the admin token (top right) to change these settings." : "");
      const sw = $("#em-real");
      sw.checked = !!st.real; sw.disabled = !usable;
      $("#em-save").disabled = !usable; $("#em-to").disabled = !usable; $("#em-test").disabled = !usable || !st.has_recipient;
      $("#em-to").placeholder = st.has_recipient ? "Saved: " + st.recipient : "recipient@example.com";
      $("#em-usage").textContent = st.configured
        ? `${st.sent_this_hour} of ${st.max_per_hour} live emails used in the last hour. "Accepted" means the mail server took the message; it can still land in spam. A request stuck in progress is sent again after ${st.stuck_after_s} s${st.stuck_resends ? ` (${st.stuck_resends} so far)` : ""}, which can rarely mean a duplicate.`
        : "";
    },
    async update() {
      this.updateEmail();
      const r = await api("/api/idempotency").catch(() => null);
      if (r && r.ok) this.renderLayers(r.body);
      const canRun = state.config.faults_enabled && getToken();
      $("#id-run").disabled = !canRun || this.running;
      setNotice("id-notice", canRun ? "" : !state.config.faults_enabled
        ? "Fault injection is switched off on this server, so the experiment cannot be run. The counters below are still live."
        : "Enter the admin token (top right) to run the experiment.");
    },
    renderLayers(d) {
      const p = d.provider || {};
      const ignored = d.duplicates_ignored_by_consumers || 0, suppressed = d.duplicate_requests_suppressed_by_provider || 0, dup = p.duplicate_effects || 0;
      $("#id-state").innerHTML = p.dedupe === false ? chip("DLQ", "provider protection OFF (negative control)") : chip("DELIVERED", "protections on");
      $("#id-layers").innerHTML = `
        <div class="node"><h3>Duplicates absorbed</h3><div class="big">${num(ignored + suppressed)}</div><div class="sub">repeats stopped by a layer</div></div>
        <div class="arrow" aria-hidden="true">→</div>
        <div class="node"><h3>Layer 1 · Consumer</h3><div class="big">${num(ignored)}</div><div class="sub">already marked delivered</div></div>
        <div class="arrow" aria-hidden="true">→</div>
        <div class="node"><h3>Layer 2 · Provider</h3><div class="big">${num(suppressed)}</div><div class="sub">idempotency key already seen</div></div>
        <div class="arrow" aria-hidden="true">→</div>
        <div class="node${dup ? " bad" : ""}"><h3>Notifications sent</h3><div class="big">${num(p.effects)}</div><div class="sub">${dup ? `<strong>${num(dup)} duplicate${dup === 1 ? "" : "s"} got through</strong>` : "0 duplicates, as it must be"}</div></div>`;
      // Every request the provider received ends up in exactly one of three places, so the parts add up to the total.
      const part = (value, label, cls) => `<div class="eq-part${cls ? " " + cls : ""}"><div class="eq-value">${num(value)}</div><div class="eq-label">${label}</div></div>`;
      $("#id-requests").innerHTML = `
        <h3>Where every provider request went</h3>
        <div class="equation">
          ${part(p.requests, "requests received", "eq-total")}<span class="eq-op" aria-hidden="true">=</span>
          ${part(p.effects, "sent")}<span class="eq-op" aria-hidden="true">+</span>
          ${part(suppressed, "repeats suppressed")}<span class="eq-op" aria-hidden="true">+</span>
          ${part(p.injected_failures, "answered with an injected failure")}
        </div>`;
    },
    async run() {
      if (this.running) return;
      if (this.mode === "control" && this.email && this.email.real &&
          !confirm("Live email sending is ON. This experiment turns the protections off, so it will send DUPLICATE live emails to " + this.email.recipient + ". Continue?")) return;
      this.running = true; $("#id-run").disabled = true;
      const mode = this.mode, out = $("#id-result"), say = (t) => { $("#id-progress").textContent = t; };
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
// The reconciler (D22) runs inside the relay; its settings arrive with the relay's heartbeat.
function reconcilerLine(role, summary) {
  const inst = (role.instances || []).find((i) => i.fresh && i.config && i.config.reconcile_after);
  const n = summary ? summary.reconciled_deliveries || 0 : 0;
  const when = inst
    ? `every ${esc(inst.config.reconcile_every)} it looks for deliveries still waiting ${esc(inst.config.reconcile_after)} after being published`
    : "it looks for deliveries that are still waiting long after being published";
  return `Also runs the reconciler: ${when} (a sign the broker lost them) and sends them through again. Repaired so far: <strong>${num(n)}</strong>${n ? "" : " (normally none)"}.`;
}
const tile = (label, value, note, warn) => `<div class="tile${warn ? " attention" : ""}"><div class="tile-label">${label}</div><div class="tile-value">${value}</div><div class="tile-note">${note}</div></div>`;

// One table cell per channel with a short status, so channels line up in columns and are easy to scan.
// A channel the event type is not routed to shows a dash.
function channelCells(deliveries) {
  return CHANNELS.map((c) => `<td>${deliveries[c] ? chip(deliveries[c], LABEL[deliveries[c]] || deliveries[c]) : `<span class="muted">–</span>`}</td>`).join("");
}

// The one place pages say "you need the admin token / this is switched off", in the same style everywhere.
function setNotice(id, message) {
  const el = $("#" + id);
  if (el) el.innerHTML = message ? `<div class="notice" role="note">${esc(message)}</div>` : "";
}

function eventRow(e, full) {
  const link = `<a href="#" data-event="${esc(e.id)}" class="mono">${shortId(e.id)}</a>`;
  return full
    ? `<tr class="clickable" data-event="${esc(e.id)}"><td>${link}</td><td>${esc(e.type)}</td><td>${chip(e.status)}</td><td>${e.attempts}</td>${channelCells(e.deliveries)}<td class="muted" title="${esc(e.created_at)}">${ago(e.created_at)}</td></tr>`
    : `<tr class="clickable" data-event="${esc(e.id)}"><td>${link}</td><td>${esc(e.type)}</td>${channelCells(e.deliveries)}<td class="muted">${ago(e.created_at)}</td></tr>`;
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
  document.title = pageTitle();
  $("#drawer").hidden = false; $("#drawer-backdrop").hidden = false;
  $("#drawer-body").innerHTML = `<p class="muted">Loading…</p>`;
  await renderDrawer();
  $("#drawer-close").focus();
}
function closeDrawer() { state.drawerId = null; history.replaceState(null, "", `#/${state.route}`); $("#drawer").hidden = true; $("#drawer-backdrop").hidden = true; document.title = pageTitle(); }

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
      <dt>Created</dt><dd>${fmtClock(e.created_at)} <span class="muted">(${ago(e.created_at)})</span></dd>
      <dt>Published</dt><dd>${fmtClock(e.published_at)}</dd><dt>Aggregate</dt><dd class="mono">${esc(e.aggregate_id)}</dd></dl></div>
    <div class="card"><div class="card-head"><h2>Channel deliveries</h2></div>
      <div class="table-wrap"><table><thead><tr><th>Channel</th><th>State</th><th>Attempts</th><th>Detail</th></tr></thead><tbody>
      ${e.deliveries.map((d) => `<tr><td>${esc(d.channel)}</td><td>${chip(d.status)}</td><td>${d.attempts}</td>
        <td>${d.last_error ? esc(d.last_error) : d.delivered_at ? `delivered ${fmtClock(d.delivered_at)}` : "<span class=muted>–</span>"}${d.duplicate_count ? `<div class="muted small">${d.duplicate_count} duplicate message${d.duplicate_count > 1 ? "s" : ""} ignored</div>` : ""}${d.reconcile_count ? `<div class="muted small">republished by the reconciler${d.reconcile_count > 1 ? " " + d.reconcile_count + " times" : ""}</div>` : ""}</td></tr>`).join("")}
      </tbody></table></div></div>
    <div class="card"><div class="card-head"><h2>Timeline</h2></div>
      <ul class="timeline">${timeline.map((t) => `<li><div class="when">${fmtTime(t.at)}</div>${esc(t.text)}</li>`).join("")}</ul></div>
    <div class="card"><div class="card-head"><h2>Payload</h2></div><pre class="json">${esc(JSON.stringify(e.payload, null, 2))}</pre></div>`;
}

// ---------- routing, polling ----------
const APP_NAME = "Notification Platform";
// The browser tab title: "<page heading> · Notification Platform", or the open event's short id.
function pageTitle() {
  if (state.drawerId) return `Event ${shortId(state.drawerId)} · ${APP_NAME}`;
  const s = sections[state.route];
  return s ? `${s.heading || s.title} · ${APP_NAME}` : APP_NAME;
}
const mounted = new Set();
function show(route) {
  if (!sections[route]) route = "overview";
  state.route = route;
  if (route === "start") { try { localStorage.setItem("seen-start", "1"); } catch { /* ignore */ } }
  const s = sections[route];
  document.title = pageTitle();
  $("#page-title").textContent = s.heading || s.title; $("#page-desc").textContent = s.desc;
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

// The status next to the Auto-refresh switch: how fresh the data is, in words.
function renderUpdated() {
  const el = $("#updated");
  if (!state.live) { el.textContent = "Paused: not refreshing"; el.classList.add("is-paused"); return; }
  el.classList.remove("is-paused");
  if (state.error) { el.textContent = "Can't reach the server, retrying…"; return; }
  if (!state.updated) { el.textContent = "Loading…"; return; }
  const secs = Math.floor((Date.now() - state.updated.getTime()) / 1000);
  el.textContent = secs < 3 ? "Updated just now" : secs < 60 ? `Updated ${secs} s ago` : `Updated ${Math.floor(secs / 60)} min ago`;
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
  renderUpdated();
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
  const em = await api("/api/provider/email").catch(() => null);
  state.email = em && em.ok ? em.body : null;
  renderEmailPill();
}

// Real email is a global setting: while it is on, every email notification from any page is a real email. The
// sidebar therefore always shows whether it is ON or OFF (once SMTP is configured; with no SMTP there is nothing to
// switch) and links to the card that changes it.
function renderEmailPill() {
  const pill = $("#email-pill"), em = state.email;
  pill.hidden = !(em && em.configured);
  if (pill.hidden) return;
  const on = !!em.real;
  pill.className = "pill " + (on ? "pill-warn" : "pill-off");
  $("#email-pill-text").textContent = "Live email sending: " + (on ? "ON" : "OFF");
  pill.title = on
    ? `Live emails are being sent to ${em.recipient}. Click to change.`
    : "Email notifications are mock: nothing is sent. Click to switch live sending on.";
}

$("#live").onchange = (e) => { state.live = e.target.checked; renderUpdated(); if (state.live) refresh(); };

// Colour theme: Auto follows the system; Light and Dark override it and are remembered in this browser.
const THEME_ICONS = {
  auto: `<svg viewBox="0 0 24 24" aria-hidden="true"><circle cx="12" cy="12" r="8.5"/><path d="M12 3.5a8.5 8.5 0 0 1 0 17z" fill="currentColor"/></svg>`,
  light: `<svg viewBox="0 0 24 24" aria-hidden="true"><circle cx="12" cy="12" r="4"/><path d="M12 2.5v2M12 19.5v2M2.5 12h2M19.5 12h2M5.3 5.3l1.4 1.4M17.3 17.3l1.4 1.4M5.3 18.7l1.4-1.4M17.3 6.7l1.4-1.4"/></svg>`,
  dark: `<svg viewBox="0 0 24 24" aria-hidden="true"><path d="M20.5 13.2A8.5 8.5 0 1 1 10.8 3.5a6.8 6.8 0 0 0 9.7 9.7z"/></svg>`,
};
const THEMES = ["auto", "light", "dark"];
let theme = "auto";
try { theme = localStorage.getItem("theme") || "auto"; } catch { /* storage unavailable */ }
if (!THEMES.includes(theme)) theme = "auto";
// The tab icon is the logo in the current accent colours. CSS variables cannot be read from an icon, so the colours are
// resolved here and the icon is rebuilt whenever the theme changes (button, or the system setting while on Auto).
function resolvedColor(cssVar) {
  const probe = document.createElement("span");
  probe.style.color = `var(${cssVar})`;
  document.body.appendChild(probe);
  const color = getComputedStyle(probe).color;
  probe.remove();
  return color;
}
function updateFavicon() {
  const link = document.querySelector('link[rel="icon"]');
  if (!link) return;
  const svg = `<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 64 64"><rect width="64" height="64" rx="14" fill="${resolvedColor("--accent")}"/>` +
    `<text x="32" y="45" font-size="38" font-family="Arial,sans-serif" font-weight="700" text-anchor="middle" fill="${resolvedColor("--accent-fg")}">N</text></svg>`;
  link.href = "data:image/svg+xml," + encodeURIComponent(svg);
}
window.matchMedia("(prefers-color-scheme: dark)").addEventListener("change", updateFavicon);
function applyTheme() {
  if (theme === "auto") delete document.documentElement.dataset.theme; else document.documentElement.dataset.theme = theme;
  const next = THEMES[(THEMES.indexOf(theme) + 1) % THEMES.length], btn = $("#theme");
  btn.innerHTML = THEME_ICONS[theme];
  $("#theme-label").textContent = "Theme: " + theme[0].toUpperCase() + theme.slice(1);
  updateFavicon();
  btn.title = `Theme: ${theme} (click for ${next}). Auto follows your system setting.`;
  btn.setAttribute("aria-label", `Colour theme: ${theme}. Click for ${next}.`);
}
$("#theme").onclick = () => {
  theme = THEMES[(THEMES.indexOf(theme) + 1) % THEMES.length];
  try { localStorage.setItem("theme", theme); } catch { /* ignore */ }
  applyTheme();
};
applyTheme();

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
setInterval(renderUpdated, 1000); // keeps "Updated N s ago" honest between refreshes
setInterval(() => { if (!document.hidden) checkApi(); }, 5000);

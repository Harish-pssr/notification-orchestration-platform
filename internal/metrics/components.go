package metrics

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

// ---------- API ----------

// API holds the API's metrics.
type API struct {
	created   *prometheus.CounterVec
	replays   prometheus.Counter
	rejected  *prometheus.CounterVec
	createDur prometheus.Histogram
}

// NewAPI registers the API's metrics on reg.
func NewAPI(reg prometheus.Registerer) *API {
	m := &API{
		created: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: Namespace, Name: "events_created_total",
			Help: "Events committed to the outbox, by event type.",
		}, []string{"event_type"}),
		replays: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: Namespace, Name: "api_idempotent_replays_total",
			Help: "Create requests answered from a stored response because the Idempotency-Key was already used.",
		}),
		rejected: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: Namespace, Name: "api_rejected_requests_total",
			Help: "Create requests refused, by reason (bad_request, key_reuse, error).",
		}, []string{"reason"}),
		createDur: prometheus.NewHistogram(prometheus.HistogramOpts{
			Namespace: Namespace, Name: "event_create_duration_seconds",
			Help:    "Time to write an event (idempotency record, business row, outbox row, delivery rows) in one transaction.",
			Buckets: opBuckets,
		}),
	}
	reg.MustRegister(m.created, m.replays, m.rejected, m.createDur)
	return m
}

// Created records a successfully created event and how long the transaction took.
func (m *API) Created(eventType string, d time.Duration) {
	if m == nil {
		return
	}
	m.created.WithLabelValues(eventType).Inc()
	m.createDur.Observe(seconds(d))
}

// Replayed records a request answered from a stored response.
func (m *API) Replayed() {
	if m == nil {
		return
	}
	m.replays.Inc()
}

// Rejected records a refused create request.
func (m *API) Rejected(reason string) {
	if m == nil {
		return
	}
	m.rejected.WithLabelValues(reason).Inc()
}

// ---------- Relay ----------

// Relay holds the relay's metrics.
type Relay struct {
	claimedEvents prometheus.Counter
	publishedEvts prometheus.Counter
	messages      *prometheus.CounterVec
	failures      prometheus.Counter
	fenceLost     prometheus.Counter
	deadLettered  prometheus.Counter
	claimDur      prometheus.Histogram
	publishDur    prometheus.Histogram
	finishDur     prometheus.Histogram

	reconcilePasses   prometheus.Counter
	reconcileRepaired prometheus.Counter
	reconcileDead     prometheus.Counter
	reconcileErrors   prometheus.Counter
}

// NewRelay registers the relay's metrics on reg.
func NewRelay(reg prometheus.Registerer) *Relay {
	m := &Relay{
		claimedEvents: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: Namespace, Name: "relay_claimed_events_total", Help: "Outbox rows claimed by this relay.",
		}),
		publishedEvts: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: Namespace, Name: "relay_published_events_total", Help: "Events fully published and recorded as PUBLISHED.",
		}),
		messages: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: Namespace, Name: "relay_published_messages_total", Help: "Broker messages accepted, by channel.",
		}, []string{"channel"}),
		failures: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: Namespace, Name: "relay_publish_failures_total", Help: "Publish attempts that failed (the event is retried later).",
		}),
		fenceLost: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: Namespace, Name: "relay_fence_lost_total", Help: "Times this relay finished too late and the fence rejected it (lease lost).",
		}),
		deadLettered: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: Namespace, Name: "relay_dead_lettered_total", Help: "Events the relay gave up on (outbox DLQ).",
		}),
		claimDur: prometheus.NewHistogram(prometheus.HistogramOpts{
			Namespace: Namespace, Name: "relay_claim_duration_seconds", Help: "Time to claim a batch of outbox rows.", Buckets: opBuckets,
		}),
		publishDur: prometheus.NewHistogram(prometheus.HistogramOpts{
			Namespace: Namespace, Name: "relay_publish_duration_seconds",
			Help: "Time to publish one event's messages and receive every broker acknowledgement.", Buckets: opBuckets,
		}),
		finishDur: prometheus.NewHistogram(prometheus.HistogramOpts{
			Namespace: Namespace, Name: "relay_finish_duration_seconds", Help: "Time for the fenced update that records an event as published.", Buckets: opBuckets,
		}),
		reconcilePasses: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: Namespace, Name: "reconciler_passes_total", Help: "Reconciler passes run by this relay.",
		}),
		reconcileRepaired: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: Namespace, Name: "reconciler_repaired_total",
			Help: "Deliveries that looked lost by the broker and were sent back through the relay (should stay at zero in normal operation).",
		}),
		reconcileDead: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: Namespace, Name: "reconciler_dead_lettered_total", Help: "Deliveries the reconciler gave up on after the repair limit.",
		}),
		reconcileErrors: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: Namespace, Name: "reconciler_errors_total", Help: "Reconciler passes that failed.",
		}),
	}
	reg.MustRegister(m.claimedEvents, m.publishedEvts, m.messages, m.failures, m.fenceLost, m.deadLettered,
		m.claimDur, m.publishDur, m.finishDur,
		m.reconcilePasses, m.reconcileRepaired, m.reconcileDead, m.reconcileErrors)
	return m
}

// Claimed records a claim round: how many rows and how long it took.
func (m *Relay) Claimed(n int, d time.Duration) {
	if m == nil {
		return
	}
	m.claimDur.Observe(seconds(d))
	m.claimedEvents.Add(float64(n))
}

// PublishDone records one event's publish: its duration and, on success, the messages
// accepted per channel; on failure, a failure.
func (m *Relay) PublishDone(d time.Duration, channels []string, err error) {
	if m == nil {
		return
	}
	m.publishDur.Observe(seconds(d))
	if err != nil {
		m.failures.Inc()
		return
	}
	for _, ch := range channels {
		m.messages.WithLabelValues(ch).Inc()
	}
}

// Finished records the fenced update after a successful publish.
func (m *Relay) Finished(d time.Duration, recorded bool) {
	if m == nil {
		return
	}
	m.finishDur.Observe(seconds(d))
	if recorded {
		m.publishedEvts.Inc()
	}
}

// FenceLost records that the relay's finish was rejected.
func (m *Relay) FenceLost() {
	if m == nil {
		return
	}
	m.fenceLost.Inc()
}

// DeadLettered records that the relay gave up on an event.
func (m *Relay) DeadLettered() {
	if m == nil {
		return
	}
	m.deadLettered.Inc()
}

// Reconciled records one reconciler pass (D22): deliveries it sent back through the
// relay because they looked lost, and deliveries it gave up on.
func (m *Relay) Reconciled(repaired, deadLettered int) {
	if m == nil {
		return
	}
	m.reconcilePasses.Inc()
	m.reconcileRepaired.Add(float64(repaired))
	m.reconcileDead.Add(float64(deadLettered))
}

// ReconcileFailed records a reconciler pass that returned an error.
func (m *Relay) ReconcileFailed() {
	if m == nil {
		return
	}
	m.reconcileErrors.Inc()
}

// ---------- Consumer ----------

// Outcome names for Consumer.Outcome. Every received message ends in exactly one.
const (
	OutcomeDelivered       = "delivered"
	OutcomeRetried         = "retried"
	OutcomeDeadLettered    = "dead_lettered"
	OutcomeDuplicateIgnore = "duplicate_ignored"
	OutcomeDiscarded       = "discarded"
	OutcomeError           = "error"
)

// Consumer holds one consumer instance's metrics.
type Consumer struct {
	channel     string
	received    *prometheus.CounterVec
	outcomes    *prometheus.CounterVec
	providerDur *prometheus.HistogramVec
	latency     *prometheus.HistogramVec
	inFlight    *prometheus.GaugeVec
}

// NewConsumer registers a consumer's metrics on reg; channel labels every series.
func NewConsumer(reg prometheus.Registerer, channel string) *Consumer {
	m := &Consumer{
		channel: channel,
		received: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: Namespace, Name: "consumer_messages_received_total", Help: "Broker messages received, by channel (includes redeliveries).",
		}, []string{"channel"}),
		outcomes: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: Namespace, Name: "consumer_outcomes_total",
			Help: "What happened to each received message: delivered, retried, dead_lettered, duplicate_ignored, discarded, error.",
		}, []string{"channel", "outcome"}),
		providerDur: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: Namespace, Name: "consumer_provider_call_duration_seconds", Help: "Duration of calls to the provider.", Buckets: opBuckets,
		}, []string{"channel", "result"}),
		latency: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: Namespace, Name: "delivery_latency_seconds",
			Help:    "Event created to delivered, per channel. Live and approximate (bucketed); benchmarks use exact database timestamps instead.",
			Buckets: latencyBuckets,
		}, []string{"channel"}),
		inFlight: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: Namespace, Name: "consumer_in_flight", Help: "Messages being processed right now by this consumer instance.",
		}, []string{"channel"}),
	}
	reg.MustRegister(m.received, m.outcomes, m.providerDur, m.latency, m.inFlight)
	return m
}

// Received counts one message taken from the broker.
func (m *Consumer) Received() {
	if m == nil {
		return
	}
	m.received.WithLabelValues(m.channel).Inc()
}

// Outcome records how a message ended.
func (m *Consumer) Outcome(outcome string) {
	if m == nil {
		return
	}
	m.outcomes.WithLabelValues(m.channel, outcome).Inc()
}

// ProviderCall records one call to the provider.
func (m *Consumer) ProviderCall(d time.Duration, ok bool) {
	if m == nil {
		return
	}
	result := "ok"
	if !ok {
		result = "error"
	}
	m.providerDur.WithLabelValues(m.channel, result).Observe(seconds(d))
}

// InFlight adds delta (+1 when a message starts, -1 when it ends) to the in-progress gauge.
func (m *Consumer) InFlight(delta float64) {
	if m == nil {
		return
	}
	m.inFlight.WithLabelValues(m.channel).Add(delta)
}

// Latency records an end-to-end latency in seconds (event created to delivered).
func (m *Consumer) Latency(sec float64) {
	if m == nil || sec < 0 {
		return
	}
	m.latency.WithLabelValues(m.channel).Observe(sec)
}

// ---------- Provider ----------

// Request result names for Provider.Request.
const (
	ResultExecuted = "executed"
	ResultReplayed = "replayed"
	ResultInjected = "injected_failure"
	ResultError    = "error"
)

// Provider holds the mock provider's metrics.
type Provider struct {
	requests *prometheus.CounterVec
	duration *prometheus.HistogramVec
}

// NewProvider registers the provider's metrics on reg.
func NewProvider(reg prometheus.Registerer) *Provider {
	m := &Provider{
		requests: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: Namespace, Name: "provider_requests_total",
			Help: "Provider requests by channel and result (executed, replayed, injected_failure, error).",
		}, []string{"channel", "result"}),
		duration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: Namespace, Name: "provider_request_duration_seconds", Help: "Provider request duration.", Buckets: opBuckets,
		}, []string{"channel"}),
	}
	reg.MustRegister(m.requests, m.duration)
	return m
}

// Request records one provider request.
func (m *Provider) Request(channel, result string, d time.Duration) {
	if m == nil {
		return
	}
	m.requests.WithLabelValues(channel, result).Inc()
	m.duration.WithLabelValues(channel).Observe(seconds(d))
}

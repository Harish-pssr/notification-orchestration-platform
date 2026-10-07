// Package events defines the event types, notification channels, routing (D6)
// and the small message the relay publishes to the broker. It is the only
// package every binary shares.
package events

import "github.com/google/uuid"

// Type is a business event type.
type Type string

const (
	PaymentSucceeded Type = "PaymentSucceeded"
	BookingConfirmed Type = "BookingConfirmed"
	PasswordChanged  Type = "PasswordChanged"
)

// Channel is a notification channel; each has its own subject and consumer.
type Channel string

const (
	Email   Channel = "email"
	Push    Channel = "push"
	Webhook Channel = "webhook"
)

// AllChannels lists every channel in a stable order.
var AllChannels = []Channel{Email, Push, Webhook}

// Subject is the NATS subject messages for this channel are published to.
func (c Channel) Subject() string { return "notify." + string(c) }

// Valid reports whether c is a known channel.
func (c Channel) Valid() bool {
	for _, k := range AllChannels {
		if c == k {
			return true
		}
	}
	return false
}

// routes is the event-to-channel routing table (D6). It is read when an event
// is created and frozen into channel_deliveries rows.
var routes = map[Type][]Channel{
	PaymentSucceeded: {Email, Push, Webhook},
	PasswordChanged:  {Email, Push},
	BookingConfirmed: {Email},
}

// RoutesFor returns the channels an event type is delivered to. The result is a
// copy, so callers cannot change the table. Unknown types route nowhere.
func RoutesFor(t Type) []Channel {
	return append([]Channel(nil), routes[t]...)
}

// Message is the thin message published per delivery (D6): identifiers only.
// Consumers read the payload from Postgres.
type Message struct {
	EventID   uuid.UUID `json:"event_id"`
	Channel   Channel   `json:"channel"`
	EventType Type      `json:"event_type"`
}

// DedupKey identifies one delivery. It is used as the NATS message ID and as the
// Idempotency-Key sent to the provider (D5), so both layers agree on identity.
func DedupKey(eventID uuid.UUID, c Channel) string {
	return eventID.String() + ":" + string(c)
}

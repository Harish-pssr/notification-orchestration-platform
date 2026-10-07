package httpapi

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"strconv"
	"strings"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"notifplatform/internal/events"
	"notifplatform/internal/outbox"
)

// eventSpec describes one event endpoint: its event type and how to turn a
// request body into the business write that goes in the same transaction.
type eventSpec struct {
	typ    events.Type
	decode func(body []byte) (outbox.BusinessWrite, error)
}

type eventsHandler struct{ store *outbox.Store }

const maxBody = 1 << 20

// create handles POST /api/events/<type> with the idempotency rules of D12.
func (h *eventsHandler) create(spec eventSpec) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		key := strings.TrimSpace(r.Header.Get("Idempotency-Key"))
		if key == "" {
			writeError(w, http.StatusBadRequest, "Idempotency-Key header is required")
			return
		}
		body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, maxBody))
		if err != nil {
			writeError(w, http.StatusBadRequest, "could not read request body")
			return
		}
		write, err := spec.decode(body)
		if err != nil {
			writeError(w, http.StatusBadRequest, err.Error())
			return
		}

		res, err := h.store.CreateEvent(r.Context(), outbox.CreateInput{
			Key:         key,
			RequestHash: requestHash(r.Method, r.URL.Path, body),
			EventType:   spec.typ,
			Write:       write,
		})
		switch {
		case errors.Is(err, outbox.ErrKeyReuse):
			writeError(w, http.StatusUnprocessableEntity, "Idempotency-Key was already used with a different request")
			return
		case err != nil:
			slog.Error("create event failed", "error", err, "event_type", spec.typ)
			writeError(w, http.StatusInternalServerError, "could not create event")
			return
		}

		w.Header().Set("Content-Type", "application/json")
		if res.Replayed {
			w.Header().Set("Idempotent-Replayed", "true")
		}
		w.WriteHeader(res.StatusCode)
		_, _ = w.Write(res.Body)
	}
}

func (h *eventsHandler) summary(w http.ResponseWriter, r *http.Request) {
	sum, err := h.store.Summary(r.Context())
	if err != nil {
		slog.Error("summary failed", "error", err)
		writeError(w, http.StatusInternalServerError, "could not compute summary")
		return
	}
	writeJSON(w, http.StatusOK, sum)
}

func (h *eventsHandler) list(w http.ResponseWriter, r *http.Request) {
	// ?limit= defaults to 50 and is capped so one request cannot read the whole table.
	limit := 50
	if v, err := strconv.Atoi(r.URL.Query().Get("limit")); err == nil && v > 0 {
		limit = min(v, 200)
	}
	list, err := h.store.ListEvents(r.Context(), limit)
	if err != nil {
		slog.Error("list events failed", "error", err)
		writeError(w, http.StatusInternalServerError, "could not list events")
		return
	}
	writeJSON(w, http.StatusOK, list)
}

func (h *eventsHandler) get(w http.ResponseWriter, r *http.Request) {
	id, err := uuid.Parse(r.PathValue("id"))
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid event id")
		return
	}
	e, err := h.store.GetEvent(r.Context(), id)
	switch {
	case err != nil:
		slog.Error("get event failed", "error", err)
		writeError(w, http.StatusInternalServerError, "could not load event")
	case e == nil:
		writeError(w, http.StatusNotFound, "event not found")
	default:
		writeJSON(w, http.StatusOK, e)
	}
}

// requestHash identifies a request for idempotency comparison (D12).
func requestHash(method, path string, body []byte) string {
	h := sha256.New()
	h.Write([]byte(method + " " + path + "\n"))
	h.Write(body)
	return hex.EncodeToString(h.Sum(nil))
}

// strictDecode parses JSON and rejects unknown fields, so a typo in a field name
// is an error instead of a silently ignored value.
func strictDecode(body []byte, v any) error {
	dec := json.NewDecoder(bytes.NewReader(body))
	dec.DisallowUnknownFields()
	if err := dec.Decode(v); err != nil {
		return errors.New("invalid JSON body: " + err.Error())
	}
	return nil
}

// --- the three event endpoints ---------------------------------------------

var paymentSucceeded = eventSpec{typ: events.PaymentSucceeded, decode: func(body []byte) (outbox.BusinessWrite, error) {
	var in struct {
		AccountID   string `json:"account_id"`
		AmountCents int64  `json:"amount_cents"`
		Currency    string `json:"currency"`
	}
	if err := strictDecode(body, &in); err != nil {
		return nil, err
	}
	if in.AccountID == "" || in.Currency == "" || in.AmountCents <= 0 {
		return nil, errors.New("account_id, currency and a positive amount_cents are required")
	}
	return func(ctx context.Context, tx pgx.Tx, eventID uuid.UUID) (outbox.Aggregate, error) {
		id, _ := uuid.NewV7()
		_, err := tx.Exec(ctx,
			`INSERT INTO payments (id, account_id, amount_cents, currency, status) VALUES ($1, $2, $3, $4, 'SUCCESS')`,
			id, in.AccountID, in.AmountCents, in.Currency)
		return outbox.Aggregate{Type: "payment", ID: id.String(), Payload: map[string]any{
			"payment_id": id, "account_id": in.AccountID, "amount_cents": in.AmountCents, "currency": in.Currency,
		}}, err
	}, nil
}}

var bookingConfirmed = eventSpec{typ: events.BookingConfirmed, decode: func(body []byte) (outbox.BusinessWrite, error) {
	var in struct {
		CustomerID string `json:"customer_id"`
		Item       string `json:"item"`
	}
	if err := strictDecode(body, &in); err != nil {
		return nil, err
	}
	if in.CustomerID == "" || in.Item == "" {
		return nil, errors.New("customer_id and item are required")
	}
	return func(ctx context.Context, tx pgx.Tx, eventID uuid.UUID) (outbox.Aggregate, error) {
		id, _ := uuid.NewV7()
		_, err := tx.Exec(ctx,
			`INSERT INTO bookings (id, customer_id, item, status) VALUES ($1, $2, $3, 'CONFIRMED')`,
			id, in.CustomerID, in.Item)
		return outbox.Aggregate{Type: "booking", ID: id.String(), Payload: map[string]any{
			"booking_id": id, "customer_id": in.CustomerID, "item": in.Item,
		}}, err
	}, nil
}}

var passwordChanged = eventSpec{typ: events.PasswordChanged, decode: func(body []byte) (outbox.BusinessWrite, error) {
	var in struct {
		UserID string `json:"user_id"`
	}
	if err := strictDecode(body, &in); err != nil {
		return nil, err
	}
	if in.UserID == "" {
		return nil, errors.New("user_id is required")
	}
	return func(ctx context.Context, tx pgx.Tx, eventID uuid.UUID) (outbox.Aggregate, error) {
		id, _ := uuid.NewV7()
		_, err := tx.Exec(ctx, `INSERT INTO password_changes (id, user_id) VALUES ($1, $2)`, id, in.UserID)
		return outbox.Aggregate{Type: "password_change", ID: id.String(), Payload: map[string]any{
			"change_id": id, "user_id": in.UserID,
		}}, err
	}, nil
}}

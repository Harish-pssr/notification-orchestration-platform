package events

import (
	"reflect"
	"testing"

	"github.com/google/uuid"
)

func TestRoutesFor(t *testing.T) {
	cases := map[Type][]Channel{
		PaymentSucceeded: {Email, Push, Webhook},
		PasswordChanged:  {Email, Push},
		BookingConfirmed: {Email},
		"Nonsense":       nil,
	}
	for typ, want := range cases {
		if got := RoutesFor(typ); !reflect.DeepEqual(got, want) {
			t.Errorf("RoutesFor(%s) = %v, want %v", typ, got, want)
		}
	}
}

func TestRoutesForReturnsACopy(t *testing.T) {
	got := RoutesFor(PaymentSucceeded)
	got[0] = "tampered"
	if RoutesFor(PaymentSucceeded)[0] != Email {
		t.Fatal("mutating the result changed the routing table")
	}
}

func TestSubjectAndDedupKey(t *testing.T) {
	if Webhook.Subject() != "notify.webhook" {
		t.Errorf("subject = %s", Webhook.Subject())
	}
	id := uuid.MustParse("0190a1b2-c3d4-7e5f-8a9b-0c1d2e3f4a5b")
	if got := DedupKey(id, Email); got != "0190a1b2-c3d4-7e5f-8a9b-0c1d2e3f4a5b:email" {
		t.Errorf("DedupKey = %s", got)
	}
}

#!/usr/bin/env bash
# Smoke test for a running deployment: is it healthy, does an event flow end to end, is the
# idempotency contract intact, and are the protected endpoints really protected?
#
#   scripts/smoke.sh https://notifications.example.com
#   scripts/smoke.sh https://localhost:8443 -k      # self-signed local certificate
#
# It creates one real event (a test payment), so run it against a demo system, not one that
# sends real notifications.
set -euo pipefail

BASE=${1:?usage: smoke.sh BASE_URL [curl options]}
shift || true
CURL=(curl -sS --max-time 15 "$@")
fail() { echo "FAIL: $*" >&2; exit 1; }
ok() { echo "ok:   $*"; }

# 1. healthy: API, Postgres and NATS reachable
code=$("${CURL[@]}" -o /dev/null -w '%{http_code}' "$BASE/healthz") || fail "cannot reach $BASE"
[ "$code" = 200 ] || fail "/healthz returned $code"
ok "healthz is 200"

# 2. a deployment that asks for the admin token on writes (EXPECT_AUTH=1, set by deploy.sh)
#    must refuse an unauthenticated create; reading stays open
auth=()
if [ -n "${ADMIN_TOKEN:-}" ]; then auth=(-H "Authorization: Bearer $ADMIN_TOKEN"); fi
if [ "${EXPECT_AUTH:-0}" = 1 ]; then
  code=$("${CURL[@]}" -o /dev/null -w '%{http_code}' -X POST "$BASE/api/events/payment-succeeded" -H 'Idempotency-Key: smoke-noauth' -H 'Content-Type: application/json' -d '{}') || true
  [ "$code" = 401 ] || fail "creating an event without the admin token returned $code, want 401"
  ok "creating an event without the admin token is refused (401)"
fi

# 3. create an event with a fresh idempotency key
key="smoke-$(date +%s)-$RANDOM"
body='{"account_id":"smoke","amount_cents":100,"currency":"USD"}'
resp=$("${CURL[@]}" "${auth[@]+"${auth[@]}"}" -X POST "$BASE/api/events/payment-succeeded" -H "Idempotency-Key: $key" -H 'Content-Type: application/json' -d "$body" -w '\n%{http_code}')
status=${resp##*$'\n'}
[ "$status" = 201 ] || fail "creating an event returned $status: $resp"
id=$(printf '%s' "$resp" | sed -n 's/.*"event_id":"\([^"]*\)".*/\1/p')
[ -n "$id" ] || fail "no event id in the response: $resp"
ok "created event $id"

# 4. the same key again must replay the stored answer, not create a second event
again=$("${CURL[@]}" "${auth[@]+"${auth[@]}"}" -X POST "$BASE/api/events/payment-succeeded" -H "Idempotency-Key: $key" -H 'Content-Type: application/json' -d "$body" -D - -o /dev/null | tr -d '\r')
printf '%s' "$again" | grep -qi '^idempotent-replayed: true' || fail "a repeated key was not replayed: $again"
ok "the same key replays the stored response"

# 5. it is published and every delivery completes
for _ in $(seq 40); do
  detail=$("${CURL[@]}" "$BASE/api/events/$id")
  if printf '%s' "$detail" | grep -q '"status":"PUBLISHED"' && ! printf '%s' "$detail" | grep -Eq '"status":"(PENDING|RETRYING|PROCESSING|DLQ)"'; then
    ok "event published and every delivery is delivered"
    done_ok=1
    break
  fi
  sleep 0.5
done
[ "${done_ok:-0}" = 1 ] || fail "event $id did not complete within 20 s: $detail"

# 6. protected endpoints refuse requests without the admin token
for path in "PUT /api/faults/relay.pause" "POST /api/replay"; do
  method=${path%% *}; route=${path#* }
  code=$("${CURL[@]}" -o /dev/null -w '%{http_code}' -X "$method" "$BASE$route" -H 'Content-Type: application/json' -d '{}')
  case "$code" in 401|403|404) ok "$method $route without a token is refused ($code)";; *) fail "$method $route without a token returned $code";; esac
done

echo "smoke test passed"

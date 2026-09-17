#!/usr/bin/env bash
# End-to-end demo against a running docker-compose stack.
# Usage: ./scripts/demo.sh [api_base_url]
# When running the stack outside compose, set TARGET_BASE to the receiver
# address reachable BY THE WORKER, e.g.:
#   TARGET_BASE=http://127.0.0.1:9000 ./scripts/demo.sh http://127.0.0.1:8000
set -euo pipefail

API="${1:-http://localhost:8000}"
RECEIVER="http://localhost:9000"
TARGET_BASE="${TARGET_BASE:-http://receiver:9000}"   # callback URL as seen by the worker
SECRET="whsec_dev_receiver_secret"   # matches RECEIVER_SECRET in docker-compose.yml

jget() { python3 -c "import sys, json; d = json.load(sys.stdin); print(eval(sys.argv[1]))" "$1"; }

echo "==> API health"
curl -sf "$API/healthz" && echo

echo; echo "==> 1. Create subscription -> receiver /callback (secret shown once)"
SUB=$(curl -sf -X POST "$API/subscriptions" -H 'Content-Type: application/json' -d "{
  \"source\": \"shop\",
  \"target_url\": \"$TARGET_BASE/callback\",
  \"secret\": \"$SECRET\"
}")
echo "$SUB" | python3 -m json.tool

echo; echo "==> 2. Submit an event (twice: second call is a no-op duplicate)"
curl -s -X POST "$API/events" -H 'Content-Type: application/json' -d '{
  "source": "shop", "event_id": "order-1001", "type": "order.created",
  "payload": {"order_id": 1001, "total": 99.50}
}' | python3 -m json.tool
echo "--- duplicate submission returns HTTP:"
curl -s -o /dev/null -w "%{http_code}\n" -X POST "$API/events" -H 'Content-Type: application/json' -d '{
  "source": "shop", "event_id": "order-1001", "type": "order.created", "payload": {}
}'

sleep 2
echo; echo "==> 3. Delivery succeeded:"
curl -s "$API/deliveries?status=success" | python3 -m json.tool | grep -E '"(id|status|attempts|last_status_code)"' || true

echo; echo "==> 4. Receiver got the webhook (signature verified):"
curl -s "$RECEIVER/received" | python3 -c "import sys, json; d = json.load(sys.stdin); print(json.dumps(d[0], indent=2)[:400] if d else 'NONE')"

echo; echo "==> 5. Create a FAILING subscription (receiver always 500) + event"
curl -sf -X POST "$API/subscriptions" -H 'Content-Type: application/json' -d "{
  \"source\": \"payments\",
  \"target_url\": \"$TARGET_BASE/callback/fail\",
  \"secret\": \"$SECRET\"
}" > /dev/null
curl -s -X POST "$API/events" -H 'Content-Type: application/json' -d '{
  "source": "payments", "event_id": "pay-1", "type": "payment.failed", "payload": {"id": 1}
}' > /dev/null

echo "    waiting for 6 attempts with exponential backoff (1+2+4+8+16+32s ~= 63s)..."
for _ in $(seq 1 30); do
  sleep 3
  N=$(curl -s "$API/dead-letters" | python3 -c "import sys, json; print(len(json.load(sys.stdin)))")
  [ "$N" -ge 1 ] && break
done

echo; echo "==> 6. Dead letter after 6 failed attempts:"
DL=$(curl -s "$API/dead-letters")
echo "$DL" | python3 -m json.tool | grep -E '"(id|status|attempts|last_error)"' | head -4
DLID=$(echo "$DL" | jget "d[0]['id']")

echo; echo "==> 7. Replay the dead letter (new chain; old record preserved)"
REPLAY=$(curl -sf -X POST "$API/dead-letters/$DLID/replay")
echo "$REPLAY" | python3 -m json.tool | grep -E '"(id|chain_id|status|attempts|replayed_from)"'

sleep 3
echo; echo "==> 8. Original dead letter untouched, replay linked:"
curl -s "$API/deliveries/$DLID" | python3 -c "
import sys, json
d = json.load(sys.stdin)
print('status:', d['status'], '| attempts:', d['attempts'], '| history rows:', len(d['history']), '| replays:', d['replays'])"

echo; echo "Demo finished. Try: curl $API/deliveries | python3 -m json.tool"

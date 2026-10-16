#!/usr/bin/env bash
# Usage: ./scripts/smoke_test.sh [BASE_URL]
set -euo pipefail

BASE_URL="${1:-http://localhost:8000}"
TXN_ID="txn_smoke_$(date +%s)"
BODY=$(cat <<EOF
{"transaction_id":"$TXN_ID","source_account":"acc_user_789","destination_account":"acc_merchant_456","amount":1500,"currency":"INR"}
EOF
)

echo "== Health"
curl -s "$BASE_URL/"; echo

echo "== Sending the same webhook 5 times"
for _ in 1 2 3 4 5; do
  curl -s -o /dev/null -w "HTTP %{http_code} in %{time_total}s\n" \
    -X POST "$BASE_URL/v1/webhooks/transactions" \
    -H "Content-Type: application/json" -d "$BODY"
done

echo "== Status right away (expect PROCESSING)"
curl -s "$BASE_URL/v1/transactions/$TXN_ID"; echo

echo "== Waiting 35s..."
sleep 35

echo "== Status after delay (expect PROCESSED)"
curl -s "$BASE_URL/v1/transactions/$TXN_ID"; echo

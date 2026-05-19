#!/usr/bin/env bash
# =============================================================================
# test_curl_commands.sh
# --------------------
# curl test commands for the AeroSense REST API.
#
# Usage:
#   1. Start Docker:  docker compose up -d
#   2. Start API:   python src/api/app.py
#   3. Run tests:   bash tests/test_curl_commands.sh
#
# Author: Exam Candidate
# Date:   2024-2025
# =============================================================================

BASE_URL="http://localhost:5000"

echo "=============================================="
echo " AeroSense API - curl Test Suite"
echo "=============================================="
echo ""

# ---------------------------------------------------------------------------
# 1. Health check
# ---------------------------------------------------------------------------
echo "[1] GET /api/v1/health  (200)"
curl -s -w "\nHTTP_CODE: %{http_code}\n" "$BASE_URL/api/v1/health" | python3 -m json.tool
echo ""

# ---------------------------------------------------------------------------
# 2. List sensor types
# ---------------------------------------------------------------------------
echo "[2] GET /api/v1/sensors  (200)"
curl -s -w "\nHTTP_CODE: %{http_code}\n" "$BASE_URL/api/v1/sensors" | python3 -m json.tool
echo ""

# ---------------------------------------------------------------------------
# 3. Latest reading - valid sensor
# ---------------------------------------------------------------------------
echo "[3] GET /api/v1/sensors/temperature/latest  (200 or 404 if no data yet)"
curl -s -w "\nHTTP_CODE: %{http_code}\n" "$BASE_URL/api/v1/sensors/temperature/latest" | python3 -m json.tool
echo ""

# ---------------------------------------------------------------------------
# 4. Latest reading - invalid sensor (404)
# ---------------------------------------------------------------------------
echo "[4] GET /api/v1/sensors/invalid_type/latest  (404)"
curl -s -w "\nHTTP_CODE: %{http_code}\n" "$BASE_URL/api/v1/sensors/invalid_type/latest" | python3 -m json.tool
echo ""

# ---------------------------------------------------------------------------
# 5. Stats - valid sensor, 7 days
# ---------------------------------------------------------------------------
echo "[5] GET /api/v1/sensors/temperature/stats?days=7  (200)"
curl -s -w "\nHTTP_CODE: %{http_code}\n" "$BASE_URL/api/v1/sensors/temperature/stats?days=7" | python3 -m json.tool
echo ""

# ---------------------------------------------------------------------------
# 6. Stats - days out of range (400)
# ---------------------------------------------------------------------------
echo "[6] GET /api/v1/sensors/temperature/stats?days=0  (400)"
curl -s -w "\nHTTP_CODE: %{http_code}\n" "$BASE_URL/api/v1/sensors/temperature/stats?days=0" | python3 -m json.tool
echo ""

# ---------------------------------------------------------------------------
# 7. Anomalies list - valid
# ---------------------------------------------------------------------------
echo "[7] GET /api/v1/anomalies?sensor=temperature&limit=5  (200)"
curl -s -w "\nHTTP_CODE: %{http_code}\n" "$BASE_URL/api/v1/anomalies?sensor=temperature&limit=5" | python3 -m json.tool
echo ""

# ---------------------------------------------------------------------------
# 8. Anomalies list - missing sensor param (400)
# ---------------------------------------------------------------------------
echo "[8] GET /api/v1/anomalies?limit=5  (400 - missing sensor param)"
curl -s -w "\nHTTP_CODE: %{http_code}\n" "$BASE_URL/api/v1/anomalies?limit=5" | python3 -m json.tool
echo ""

# ---------------------------------------------------------------------------
# 9. POST a valid reading (201)
# ---------------------------------------------------------------------------
echo '[9] POST /api/v1/readings  (201 - valid reading)'
curl -s -w "\nHTTP_CODE: %{http_code}\n" \
  -X POST \
  -H "Content-Type: application/json" \
  -d '{
    "sensor": "temperature",
    "value": 28.5,
    "unit": "C",
    "timestamp": 1737543600000,
    "source": "site-A-rack-12",
    "anomaly": false
  }' \
  "$BASE_URL/api/v1/readings" | python3 -m json.tool
echo ""

# ---------------------------------------------------------------------------
# 10. POST an out-of-range value (422)
# ---------------------------------------------------------------------------
echo '[10] POST /api/v1/readings  (422 - out of range value)'
curl -s -w "\nHTTP_CODE: %{http_code}\n" \
  -X POST \
  -H "Content-Type: application/json" \
  -d '{
    "sensor": "temperature",
    "value": 100.0,
    "unit": "C",
    "timestamp": 1737543600000,
    "source": "site-A-rack-12",
    "anomaly": true
  }' \
  "$BASE_URL/api/v1/readings" | python3 -m json.tool
echo ""

# ---------------------------------------------------------------------------
# 11. POST malformed JSON (400)
# ---------------------------------------------------------------------------
echo '[11] POST /api/v1/readings  (400 - malformed JSON)'
curl -s -w "\nHTTP_CODE: %{http_code}\n" \
  -X POST \
  -H "Content-Type: application/json" \
  -d 'not valid json' \
  "$BASE_URL/api/v1/readings" | python3 -m json.tool
echo ""

# ---------------------------------------------------------------------------
# 12. GET unknown endpoint (404)
# ---------------------------------------------------------------------------
echo "[12] GET /api/v1/unknown  (404)"
curl -s -w "\nHTTP_CODE: %{http_code}\n" "$BASE_URL/api/v1/unknown" | python3 -m json.tool
echo ""

echo "=============================================="
echo " Test suite complete."
echo "=============================================="

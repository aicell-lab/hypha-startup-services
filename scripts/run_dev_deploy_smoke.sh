#!/usr/bin/env bash

set -euo pipefail

server_url=""
token=""
app_id=""
wait_seconds="180"
poll_seconds="6"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --server-url)
      server_url="$2"
      shift 2
      ;;
    --token)
      token="$2"
      shift 2
      ;;
    --app-id)
      app_id="$2"
      shift 2
      ;;
    --wait-seconds)
      wait_seconds="$2"
      shift 2
      ;;
    --poll-seconds)
      poll_seconds="$2"
      shift 2
      ;;
    *)
      echo "Unknown argument: $1"
      exit 1
      ;;
  esac
done

if [[ -z "$server_url" || -z "$token" || -z "$app_id" ]]; then
  echo "Missing required arguments: --server-url --token --app-id"
  exit 1
fi

set -x
python3 scripts/deploy_weaviate_app.py \
  --server-url "$server_url" \
  --token "$token" \
  --app-id "$app_id" \
  --source "weaviate-app/app_dev.py" \
  --manifest "weaviate-app/manifest.yaml" \
  --non-fatal-start

python3 scripts/test_weaviate_app.py \
  --server-url "$server_url" \
  --token "$token" \
  --app-id "default@$app_id" \
  --wait-seconds "$wait_seconds" \
  --poll-seconds "$poll_seconds"

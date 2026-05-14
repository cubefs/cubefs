# shellcheck shell=bash
# HTTP helpers wrapping curl. Every request goes through these so we get:
#   - consistent admin-token injection
#   - JSON envelope decode + envelope sanity check (code/msg fields)
#   - retry-with-backoff on 5xx
#   - verbose mode prints the curl line + raw response

SYNCNODE_BASE() { echo "http://${SYNCNODE_HOST}:${SYNCNODE_HTTP_PORT}"; }
MASTER_BASE()   { echo "${MASTER_HTTP%/}"; }

_curl_common_args=(
  -sS                              # silent + show errors
  --connect-timeout 5
  --max-time "${HTTP_MAX_TIME:-60}"
)

_curl_auth_header() {
  local kind="$1"      # syncnode|master
  local tok
  case "$kind" in
    syncnode) tok="${SYNCNODE_TOKEN:-}" ;;
    master)   tok="${MASTER_TOKEN:-}"   ;;
    *)        log_err "unknown auth kind: $kind"; return 1 ;;
  esac
  if [ -n "$tok" ]; then
    printf -- '-H\nAuthorization: Bearer %s\n' "$tok"
  fi
}

# Internal: run curl, retry up to N on 5xx, return body. Sets HTTP_STATUS.
_curl_with_retry() {
  local max_attempts="${HTTP_RETRY:-3}"
  local backoff=1
  local attempt
  local out status
  # Initialise so callers can safely reference these even when all retries fail.
  HTTP_BODY=""
  HTTP_STATUS=""
  for ((attempt=1; attempt<=max_attempts; attempt++)); do
    if [ "${RUN_VERBOSE:-0}" = "1" ]; then
      log_debug "curl[$attempt] $*"
    fi
    # Use -w to capture status separately from body.
    out=$(curl "${_curl_common_args[@]}" -w '\n___STATUS:%{http_code}' "$@") || {
      log_warn "curl returned $? on attempt $attempt"
      sleep $backoff; backoff=$((backoff*2)); continue
    }
    status="${out##*___STATUS:}"
    out="${out%___STATUS:*}"
    out="${out%$'\n'}"           # strip trailing newline before sentinel
    export HTTP_STATUS="$status"
    export HTTP_BODY="$out"
    if [ "$status" -ge 500 ]; then
      log_warn "5xx on attempt $attempt (status=$status); retrying after ${backoff}s"
      sleep $backoff; backoff=$((backoff*2)); continue
    fi
    [ "${RUN_VERBOSE:-0}" = "1" ] && log_debug "← status=$status body=$out"
    return 0
  done
  return 1
}

# syncnode_get <path>     → echoes JSON body, exports HTTP_STATUS
syncnode_get() {
  local path="$1"
  local args=(); while IFS= read -r line; do args+=("$line"); done < <(_curl_auth_header syncnode)
  _curl_with_retry -X GET ${args[@]+"${args[@]}"} "$(SYNCNODE_BASE)$path"
  echo "$HTTP_BODY"
}

# syncnode_post <path> <json-body>
syncnode_post() {
  local path="$1" body="${2:-}"
  local args=(); while IFS= read -r line; do args+=("$line"); done < <(_curl_auth_header syncnode)
  if [ -n "$body" ]; then
    _curl_with_retry -X POST ${args[@]+"${args[@]}"} -H 'Content-Type: application/json' --data "$body" \
      "$(SYNCNODE_BASE)$path"
  else
    _curl_with_retry -X POST ${args[@]+"${args[@]}"} "$(SYNCNODE_BASE)$path"
  fi
  echo "$HTTP_BODY"
}

# master_get / master_post — parallel for the master HTTP surface
master_get() {
  local path="$1"
  local args=(); while IFS= read -r line; do args+=("$line"); done < <(_curl_auth_header master)
  _curl_with_retry -X GET ${args[@]+"${args[@]}"} "$(MASTER_BASE)$path"
  echo "$HTTP_BODY"
}
master_post() {
  local path="$1" body="${2:-}"
  local args=(); while IFS= read -r line; do args+=("$line"); done < <(_curl_auth_header master)
  if [ -n "$body" ]; then
    _curl_with_retry -X POST ${args[@]+"${args[@]}"} -H 'Content-Type: application/json' --data "$body" \
      "$(MASTER_BASE)$path"
  else
    _curl_with_retry -X POST ${args[@]+"${args[@]}"} "$(MASTER_BASE)$path"
  fi
  echo "$HTTP_BODY"
}

# expect_code <body> <wanted-code> [hint]
#   The syncnode envelope is `{code, msg, data}`. wanted-code is the
#   integer in the `code` field. 0 = success on syncnode side, anything
#   else is a typed error (see syncnode/api/api.go for the table).
expect_code() {
  local body="$1" want="$2" hint="${3:-}"
  local got
  got=$(echo "$body" | jq -r '.code // 999')
  if [ "$got" != "$want" ]; then
    log_err "expected code=$want got code=$got${hint:+ ($hint)}"
    log_err "body: $body"
    return 1
  fi
}

# expect_status <wanted-http-status>
expect_status() {
  local want="$1"
  if [ "${HTTP_STATUS:-}" != "$want" ]; then
    log_err "expected HTTP $want got ${HTTP_STATUS:-?}"
    log_err "body: ${HTTP_BODY:-}"
    return 1
  fi
}

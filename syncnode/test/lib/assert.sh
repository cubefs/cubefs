# shellcheck shell=bash
# Assertion helpers. Every assert_* exits the test with a clear message
# on failure; they DON'T return — the caller doesn't need to check
# return values.

# assert_eq <expected> <actual> [context]
assert_eq() {
  local expected="$1" actual="$2" ctx="${3:-}"
  if [ "$expected" != "$actual" ]; then
    log_err "assert_eq failed${ctx:+ ($ctx)}: expected=$expected actual=$actual"
    exit 1
  fi
}

# assert_ne — value MUST differ
assert_ne() {
  local a="$1" b="$2" ctx="${3:-}"
  if [ "$a" = "$b" ]; then
    log_err "assert_ne failed${ctx:+ ($ctx)}: $a == $b"
    exit 1
  fi
}

# assert_contains <haystack> <needle> [context]
assert_contains() {
  local hay="$1" needle="$2" ctx="${3:-}"
  case "$hay" in
    *"$needle"*) return 0 ;;
    *) log_err "assert_contains failed${ctx:+ ($ctx)}: needle=$needle"
       log_err "haystack: $hay"; exit 1 ;;
  esac
}

# assert_json_eq <json> <jq-filter> <expected>
assert_json_eq() {
  local body="$1" filter="$2" expected="$3"
  local actual
  actual=$(echo "$body" | jq -r "$filter // empty")
  assert_eq "$expected" "$actual" "$filter"
}

# assert_json_ne — jq value must NOT equal expected
assert_json_ne() {
  local body="$1" filter="$2" notExpected="$3"
  local actual
  actual=$(echo "$body" | jq -r "$filter // empty")
  assert_ne "$notExpected" "$actual" "$filter"
}

# assert_json_gte / _lte — numeric bounds
assert_json_gte() {
  local body="$1" filter="$2" bound="$3"
  local v; v=$(echo "$body" | jq -r "$filter // 0")
  if (( $(echo "$v < $bound" | bc -l) )); then
    log_err "assert_json_gte failed: $filter=$v < $bound"; exit 1
  fi
}
assert_json_lte() {
  local body="$1" filter="$2" bound="$3"
  local v; v=$(echo "$body" | jq -r "$filter // 0")
  if (( $(echo "$v > $bound" | bc -l) )); then
    log_err "assert_json_lte failed: $filter=$v > $bound"; exit 1
  fi
}

# assert_file_exists / _missing
assert_file_exists() {
  if [ ! -e "$1" ]; then log_err "expected file exists: $1"; exit 1; fi
}
assert_file_missing() {
  if [ -e "$1" ]; then log_err "expected file missing: $1"; exit 1; fi
}

# assert_md5_match <local-file> <expected-md5>
assert_md5_match() {
  local actual; actual=$(md5_of "$1")
  assert_eq "$2" "$actual" "md5 of $1"
}

# shellcheck shell=bash
# Color-friendly logging helpers. Respects $NO_COLOR + non-TTY.

if [ -t 1 ] && [ -z "${NO_COLOR:-}" ]; then
  _C_RED='\033[0;31m'
  _C_GRN='\033[0;32m'
  _C_YEL='\033[0;33m'
  _C_BLU='\033[0;34m'
  _C_DIM='\033[0;90m'
  _C_OFF='\033[0m'
else
  _C_RED='' _C_GRN='' _C_YEL='' _C_BLU='' _C_DIM='' _C_OFF=''
fi

_log() {
  local prefix="$1"; shift
  printf '%b%s%b %s\n' "$prefix" "$(date +%H:%M:%S)" "$_C_OFF" "$*" >&2
}

log_info() { _log "${_C_BLU}[INFO]${_C_OFF}" "$@"; }
log_ok()   { _log "${_C_GRN}[ OK ]${_C_OFF}" "$@"; }
log_warn() { _log "${_C_YEL}[WARN]${_C_OFF}" "$@"; }
log_err()  { _log "${_C_RED}[FAIL]${_C_OFF}" "$@"; }
log_debug() {
  [ "${RUN_VERBOSE:-0}" = "1" ] || return 0
  _log "${_C_DIM}[DBG ]${_C_OFF}" "$@"
}

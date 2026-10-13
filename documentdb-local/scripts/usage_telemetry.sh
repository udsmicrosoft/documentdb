#!/bin/bash
# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.
#
# usage_telemetry.sh: the documentdb-local usage-telemetry emitter.
#
# Sends two low-frequency event types to an event-collection endpoint so the
# project can count real running deployments by version and platform: a
# one-time `emulator_launch` and a periodic `emulator_heartbeat`. Both carry
# only the fixed host attributes resolved by usage_telemetry_query() -- no
# database, collection, index or user name, no query, no document content and
# no credentials. See
# https://github.com/documentdb/documentdb/blob/main/documentdb-local/PRIVACY.md
#
# This is entirely separate from the gateway's operational OpenTelemetry
# metrics (ENABLE_TELEMETRY / OTEL_*), which this script neither reads nor
# affects, and it is scoped to the documentdb-local container image: no
# gateway code participates.
#
# Usable two ways: executed (emulator_entrypoint.sh backgrounds it) or sourced
# by tests, which get the helpers without the launch/heartbeat loop.

# The canonical boolean parser lives with the settings table so a value cannot
# mean one thing to the entrypoint's validation and the opposite here -- a
# disagreement that would silently ignore an opt-out. Sourced, not duplicated.
# shellcheck source=documentdb_local_settings.sh
. "$(dirname "${BASH_SOURCE[0]}")/documentdb_local_settings.sh" || {
    echo "[TELEMETRY] cannot load documentdb_local_settings.sh beside this script; usage telemetry is inactive." >&2
    return 0 2>/dev/null || exit 0
}

# Defaults. The endpoint is the analytics package's event-collection route: a
# route configured as "event collection only", which records the query
# parameters as event attributes instead of redirecting. The path is the
# route's configured path, matched literally -- any other path on the domain
# returns 404, and because sending is fire-and-forget that failure would show
# up only as missing data. Per-event attributes travel as query parameters.
USAGE_TELEMETRY_DEFAULT_ENDPOINT="https://documentdb.gateway.scarf.sh/telemetry"
USAGE_TELEMETRY_DEFAULT_INTERVAL_S=3600
# Lower bound on the heartbeat interval. Guards against an override that would
# turn the loop into a busy spin hammering the endpoint.
USAGE_TELEMETRY_MIN_INTERVAL_S=60
USAGE_TELEMETRY_VERSION_FILE="${USAGE_TELEMETRY_VERSION_FILE:-/version.txt}"
# Where the disclosure lives inside the image. Telemetry is on by default, so
# the document describing it must be readable without network access.
USAGE_TELEMETRY_PRIVACY_DOC="${USAGE_TELEMETRY_PRIVACY_DOC:-/home/documentdb/PRIVACY.md}"
# Hard cap per request. Every send is backgrounded on top of this, so a slow,
# blocked or non-existent endpoint cannot delay startup or serving.
USAGE_TELEMETRY_TIMEOUT_S=3

# usage_telemetry_opted_out: true when either opt-out variable asks us not to
# track. NO_ANALYTICS is this project's name; DO_NOT_TRACK is the cross-vendor
# convention. No vendor-specific variable is read: the analytics provider is an
# implementation detail and must not appear in anything an operator types.
#
# Presence is the signal, which is why this tests whether the variable is SET
# rather than whether it is non-empty. "${VAR:-}" collapses unset and
# set-but-empty into the same thing, so an operator passing `-e NO_ANALYTICS`
# with no value, or `NO_ANALYTICS=` in a compose file, would have been tracked
# despite having named the opt-out explicitly.
#
# Only an explicit off spelling ("0", "false", "no") means "you may track me";
# every other value, including one we cannot parse, one that is empty, and one
# carrying stray whitespace such as "1 " from a .env file, is honored as an
# opt-out. These variables are asymmetric on purpose: nobody sets NO_ANALYTICS
# hoping to be tracked, so a value we cannot read has exactly one plausible
# intent, and guessing the other way would silently keep tracking someone who
# asked us not to.
usage_telemetry_opted_out() {
    local name value parsed
    for name in NO_ANALYTICS DO_NOT_TRACK; do
        # Set-but-empty counts as set; genuinely unset does not.
        if [ -z "${!name+set}" ]; then
            continue
        fi
        value="${!name}"
        # A bare `-e NO_ANALYTICS` is an opt-out, not a value to parse.
        if [ -z "$value" ]; then
            return 0
        fi
        if parsed="$(documentdb_local_parse_boolish "$value")"; then
            if [ "$parsed" = "true" ]; then
                return 0
            fi
        else
            # Unrecognized: honor the request rather than discard it.
            return 0
        fi
    done
    return 1
}

# usage_telemetry_in_ci: true when this container's OWN environment looks like
# an automated build agent. A CI run is not a deployment -- counting it would
# inflate the adoption numbers this exists to measure -- and nobody reads the
# startup notice on a build agent, so consent there is theoretical.
#
# Note the limit of this check: it reads the environment inside the container,
# and `docker run` does not forward the host's variables. A pipeline that sets
# CI on the runner does NOT set it here unless it passes `-e CI` explicitly, so
# this cannot be relied on as automatic for containerized runs. The project's
# own test scripts therefore pass DOCUMENTDB_USAGE_TELEMETRY=false rather than
# depending on this, and PRIVACY.md documents the same. The check is kept
# because it is correct whenever the variable is genuinely present, such as a
# compose file forwarding it or a direct run of the entrypoint on an agent.
usage_telemetry_in_ci() {
    [ -n "${CI:-}" ] || [ -n "${GITHUB_ACTIONS:-}" ] || [ -n "${TF_BUILD:-}" ]
}

# usage_telemetry_enabled: usage telemetry is on unless it was turned off.
# The opt-out is checked first and wins over any explicit enable.
usage_telemetry_enabled() {
    if usage_telemetry_opted_out; then
        return 1
    fi
    if usage_telemetry_in_ci; then
        return 1
    fi
    [ "$(documentdb_local_parse_boolish "${DOCUMENTDB_USAGE_TELEMETRY:-true}")" = "true" ]
}

# usage_telemetry_endpoint: the configured collection endpoint.
usage_telemetry_endpoint() {
    printf '%s' "${DOCUMENTDB_USAGE_TELEMETRY_ENDPOINT:-$USAGE_TELEMETRY_DEFAULT_ENDPOINT}"
}

# usage_telemetry_interval: heartbeat period in seconds. A non-numeric or
# too-small override falls back to the default rather than failing the
# container over a telemetry setting.
usage_telemetry_interval() {
    local value="${DOCUMENTDB_USAGE_TELEMETRY_INTERVAL_S:-}"
    if [ -z "$value" ]; then
        printf '%s' "$USAGE_TELEMETRY_DEFAULT_INTERVAL_S"
        return 0
    fi
    if ! [[ "$value" =~ ^[0-9]+$ ]]; then
        echo "Warning: invalid DOCUMENTDB_USAGE_TELEMETRY_INTERVAL_S='${value}'; using ${USAGE_TELEMETRY_DEFAULT_INTERVAL_S}s." >&2
        printf '%s' "$USAGE_TELEMETRY_DEFAULT_INTERVAL_S"
        return 0
    fi
    # Normalize base 10 so a zero-padded value is not read as octal.
    value=$((10#$value))
    if [ "$value" -lt "$USAGE_TELEMETRY_MIN_INTERVAL_S" ]; then
        echo "Warning: DOCUMENTDB_USAGE_TELEMETRY_INTERVAL_S=${value} is below the ${USAGE_TELEMETRY_MIN_INTERVAL_S}s minimum; using ${USAGE_TELEMETRY_MIN_INTERVAL_S}s." >&2
        value="$USAGE_TELEMETRY_MIN_INTERVAL_S"
    fi
    printf '%s' "$value"
}

# usage_telemetry_version: the release version of this image. /version.txt
# carries the version followed by build details in parentheses; only the
# leading version token is reported.
usage_telemetry_version() {
    local line=""
    if [ -r "$USAGE_TELEMETRY_VERSION_FILE" ]; then
        read -r line < "$USAGE_TELEMETRY_VERSION_FILE" || true
    fi
    printf '%s' "${line%% *}"
}

# usage_telemetry_urlencode VALUE: percent-encode everything outside the
# unreserved set, so an unexpected character in uname output or a version
# string cannot inject extra query parameters.
usage_telemetry_urlencode() {
    local string="$1" i char out=""
    for (( i = 0; i < ${#string}; i++ )); do
        char="${string:i:1}"
        case "$char" in
            [A-Za-z0-9.~_-]) out="${out}${char}" ;;
            *) out="${out}$(printf '%%%02X' "'${char}")" ;;
        esac
    done
    printf '%s' "$out"
}

# usage_telemetry_platform: the operating system, lowercased ("linux").
# Reported as `platform` because that is one of the variable names the
# collector recognizes for its built-in breakdowns (alongside `version`); an
# arbitrary name such as `os` is recorded but does not populate them. Its
# recognized values are lowercase, so `uname -s` is normalized rather than
# passed through capitalized.
usage_telemetry_platform() {
    uname -s 2>/dev/null | tr '[:upper:]' '[:lower:]'
}

# usage_telemetry_query EVENT: the complete query string of an event. This is
# the single place that decides what leaves the container; every transmitted
# field is listed here.
usage_telemetry_query() {
    printf 'event=%s&version=%s&platform=%s&arch=%s&db_system=documentdb' \
        "$(usage_telemetry_urlencode "$1")" \
        "$(usage_telemetry_urlencode "$(usage_telemetry_version)")" \
        "$(usage_telemetry_urlencode "$(usage_telemetry_platform)")" \
        "$(usage_telemetry_urlencode "$(uname -m 2>/dev/null)")"
}

# usage_telemetry_send EVENT: fire-and-forget GET. Backgrounded in a subshell
# with a short timeout and all output discarded, so an unreachable or slow
# endpoint has no effect on PostgreSQL, the gateway or client latency.
usage_telemetry_send() {
    local url
    url="$(usage_telemetry_endpoint)?$(usage_telemetry_query "$1")"
    ( curl -fsS --max-time "$USAGE_TELEMETRY_TIMEOUT_S" -o /dev/null "$url" >/dev/null 2>&1 & ) >/dev/null 2>&1
}

usage_telemetry_main() {
    if ! usage_telemetry_enabled; then
        return 0
    fi
    if ! command -v curl >/dev/null 2>&1; then
        echo "[TELEMETRY] curl not found; usage telemetry is inactive." >&2
        return 0
    fi

    local interval
    interval="$(usage_telemetry_interval)"

    # First-run notice: default-on telemetry must not be a surprise, so say
    # what is sent, where, and how to turn it off. The disclosure is named by
    # its in-image path first and the URL second, so an operator on an
    # air-gapped host can still read it.
    echo "[TELEMETRY] Anonymous usage telemetry is on. documentdb-local reports only its version, platform and CPU architecture to $(usage_telemetry_endpoint) at startup and every ${interval}s."
    echo "[TELEMETRY] No database, collection or user names, queries, document contents or credentials are ever sent. Opt out with --disable-usage-telemetry, DOCUMENTDB_USAGE_TELEMETRY=false or NO_ANALYTICS=1. Details: ${USAGE_TELEMETRY_PRIVACY_DOC} in this container, or https://github.com/documentdb/documentdb/blob/main/documentdb-local/PRIVACY.md"

    usage_telemetry_send emulator_launch
    while sleep "$interval"; do
        usage_telemetry_send emulator_heartbeat
    done
}

# Sourcing (tests) stops here; executing runs the emitter.
if [ "${BASH_SOURCE[0]}" = "$0" ]; then
    usage_telemetry_main
fi

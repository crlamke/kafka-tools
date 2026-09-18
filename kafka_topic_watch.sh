#!/usr/bin/env bash
#
# kafka_topic_watch.sh
#
# Watches a Kafka topic and prints two kinds of events, live, until stopped:
#   [PRODUCED]  a new message was written to the topic
#   [CONSUMED]  a consumer group's committed offset advanced (i.e. it read
#               past that message)
#
# Built for running inside a container: no persistent state on disk (besides
# an in-memory offset map), no daemonizing, and it responds correctly to
# SIGTERM (what `docker stop` sends) as well as SIGINT (Ctrl-C) even if this
# script is PID 1 in the container.
#
# It uses only the shell tools that ship with Kafka:
#   bin/kafka-console-consumer.sh   -> streams newly produced messages
#   bin/kafka-consumer-groups.sh    -> polled periodically to detect
#                                      committed-offset movement
#
# Usage:
#   ./kafka_topic_watch.sh -t TOPIC [options]
#
# Options:
#   -t, --topic TOPIC                  Topic to watch (required)
#   -b, --bootstrap-server HOST:PORT   Bootstrap server (default: localhost:9092)
#   -k, --kafka-home DIR               Kafka install dir containing bin/*.sh
#                                       (default: $KAFKA_HOME or /opt/kafka)
#   -g, --group GROUP                  Consumer group whose offsets to watch
#                                       for [CONSUMED] events (optional -- if
#                                       omitted, only [PRODUCED] is shown)
#   -e, --from-beginning               Start the producer stream from the
#                                       earliest offset instead of latest
#   --poll-interval SECONDS            How often to poll consumer-group
#                                       offsets (default: 5)
#   --command-config FILE              Optional client config (SASL/SSL)
#                                       passed to kafka-*.sh via
#                                       --command-config
#   -q, --quiet-consumer-logs          Suppress kafka-console-consumer.sh's
#                                       own stderr (SLF4J/log4j noise).
#                                       On by default; use -v to see it.
#   -v, --verbose                      Show the console consumer's stderr
#                                       log output too
#   -h, --help                         Show this help
#
# Stop it with Ctrl-C, or `docker stop` / `kill -TERM <pid>` if containerized.
#
set -u -o pipefail

# ---------------------------------------------------------------------------
# Defaults
# ---------------------------------------------------------------------------
TOPIC=""
BOOTSTRAP="localhost:9092"
KAFKA_HOME="${KAFKA_HOME:-/opt/kafka}"
GROUP=""
FROM_BEGINNING=0
POLL_INTERVAL=5
COMMAND_CONFIG=""
VERBOSE=0

CONSUMER_PID=""
WATCH_PID=""

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
usage() { sed -n '2,40p' "$0" | sed 's/^# \{0,1\}//'; }

log() { printf '[%s] %s\n' "$(date '+%Y-%m-%d %H:%M:%S')" "$*"; }

die() { echo "Error: $*" >&2; exit 3; }

# ---------------------------------------------------------------------------
# Arg parsing
# ---------------------------------------------------------------------------
while [[ $# -gt 0 ]]; do
    case "$1" in
        -t|--topic)                 TOPIC="$2"; shift 2;;
        -b|--bootstrap-server)      BOOTSTRAP="$2"; shift 2;;
        -k|--kafka-home)            KAFKA_HOME="$2"; shift 2;;
        -g|--group)                 GROUP="$2"; shift 2;;
        -e|--from-beginning)        FROM_BEGINNING=1; shift;;
        --poll-interval)            POLL_INTERVAL="$2"; shift 2;;
        --command-config)           COMMAND_CONFIG="$2"; shift 2;;
        -q|--quiet-consumer-logs)   VERBOSE=0; shift;;
        -v|--verbose)               VERBOSE=1; shift;;
        -h|--help)                  usage; exit 0;;
        *) echo "Unknown option: $1" >&2; usage; exit 3;;
    esac
done

[[ -z "$TOPIC" ]] && die "missing required -t/--topic"

BIN="$KAFKA_HOME/bin"
CONSOLE_CONSUMER="$BIN/kafka-console-consumer.sh"
CONSUMER_GROUPS="$BIN/kafka-consumer-groups.sh"

[[ -x "$CONSOLE_CONSUMER" ]] || die "$CONSOLE_CONSUMER not found or not executable (check --kafka-home)"
if [[ -n "$GROUP" ]]; then
    [[ -x "$CONSUMER_GROUPS" ]] || die "$CONSUMER_GROUPS not found or not executable (check --kafka-home)"
fi

CLIENT_CONFIG_ARGS=()
if [[ -n "$COMMAND_CONFIG" ]]; then
    CLIENT_CONFIG_ARGS=(--command-config "$COMMAND_CONFIG")
fi

# ---------------------------------------------------------------------------
# Cleanup / signal handling
#
# In a container, `docker stop` sends SIGTERM and, if the process hasn't
# exited within the timeout, SIGKILL. If this script runs as PID 1 with no
# init system, bash's default handling of SIGTERM as PID 1 is to ignore it
# UNLESS a trap is installed -- so we install one explicitly for both TERM
# and INT (Ctrl-C), and use it to kill our background children by PID
# rather than relying on job control / process groups, since job control is
# normally disabled in non-interactive shells.
# ---------------------------------------------------------------------------
CLEANING_UP=0
cleanup() {
    # Idempotent: if a second TERM/INT arrives while we're already shutting
    # down (e.g. sent again by a container runtime, or by a wrapper script),
    # ignore it rather than re-entering and double-running the shutdown
    # sequence.
    if (( CLEANING_UP )); then
        return
    fi
    CLEANING_UP=1
    trap '' INT TERM   # stop reacting to further signals while we clean up

    log "Stopping (signal received) ..."
    [[ -n "$CONSUMER_PID" ]] && kill "$CONSUMER_PID" 2>/dev/null
    [[ -n "$WATCH_PID" ]]    && kill "$WATCH_PID" 2>/dev/null
    [[ -n "$CONSUMER_PID" ]] && wait "$CONSUMER_PID" 2>/dev/null
    [[ -n "$WATCH_PID" ]]    && wait "$WATCH_PID" 2>/dev/null
    log "Stopped."
    exit 0
}
trap cleanup INT TERM

# ---------------------------------------------------------------------------
# [PRODUCED] stream: kafka-console-consumer.sh, tagged and re-emitted live
# ---------------------------------------------------------------------------
start_produced_watch() {
    local from_flag=()
    (( FROM_BEGINNING )) && from_flag=(--from-beginning)

    local err_target=/dev/null
    (( VERBOSE )) && err_target=/dev/stderr

    # stdbuf forces line-buffered stdout so messages appear immediately
    # rather than waiting on a full pipe buffer -- important for a "live
    # tail" tool.
    local stdbuf_cmd=()
    command -v stdbuf >/dev/null 2>&1 && stdbuf_cmd=(stdbuf -oL -eL)

    "${stdbuf_cmd[@]}" "$CONSOLE_CONSUMER" \
        --bootstrap-server "$BOOTSTRAP" \
        "${CLIENT_CONFIG_ARGS[@]}" \
        --topic "$TOPIC" \
        "${from_flag[@]}" \
        --property print.timestamp=true \
        --property print.partition=true \
        --property print.offset=true \
        --property print.key=true \
        --property key.separator=" | " \
        2>"$err_target" \
        > >(while IFS= read -r line; do printf '[PRODUCED] %s\n' "$line"; done) &
    CONSUMER_PID=$!
}

# ---------------------------------------------------------------------------
# [CONSUMED] stream: poll kafka-consumer-groups.sh --describe and diff
# CURRENT-OFFSET per partition between polls
# ---------------------------------------------------------------------------
start_consumed_watch() {
    (
        declare -A prev_offset
        while true; do
            sleep "$POLL_INTERVAL"

            local desc
            desc=$("$CONSUMER_GROUPS" --bootstrap-server "$BOOTSTRAP" \
                   "${CLIENT_CONFIG_ARGS[@]}" --describe --group "$GROUP" 2>/dev/null)
            [[ -z "$desc" ]] && continue

            while IFS= read -r line; do
                [[ -z "$line" ]] && continue
                [[ "$line" == GROUP* ]] && continue          # header row
                [[ "$line" == *"no active members"* ]] && continue

                # Standard column order from kafka-consumer-groups.sh --describe:
                # GROUP TOPIC PARTITION CURRENT-OFFSET LOG-END-OFFSET LAG CONSUMER-ID HOST CLIENT-ID
                read -r g_topic_grp g_topic g_part g_current g_logend g_lag _rest <<< "$line"

                [[ "$g_topic" != "$TOPIC" ]] && continue
                [[ "$g_current" =~ ^[0-9]+$ ]] || continue

                local key="${g_topic}-${g_part}"
                if [[ -n "${prev_offset[$key]:-}" && "${prev_offset[$key]}" != "$g_current" ]]; then
                    printf '[CONSUMED] %s group=%s topic=%s partition=%s offset %s -> %s (lag=%s)\n' \
                        "$(date '+%Y-%m-%d %H:%M:%S')" "$GROUP" "$g_topic" "$g_part" \
                        "${prev_offset[$key]}" "$g_current" "$g_lag"
                fi
                prev_offset[$key]="$g_current"
            done <<< "$desc"
        done
    ) &
    WATCH_PID=$!
}

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
log "Watching topic '$TOPIC' on $BOOTSTRAP (Ctrl-C or SIGTERM to stop)"
(( FROM_BEGINNING )) && log "Producer stream: from-beginning" || log "Producer stream: latest only"

start_produced_watch

if [[ -n "$GROUP" ]]; then
    log "Watching consumer group '$GROUP' offsets every ${POLL_INTERVAL}s"
    start_consumed_watch
else
    log "No --group given -- only [PRODUCED] events will be shown"
fi

# Block here until interrupted; the trap handles cleanup and exit.
wait "$CONSUMER_PID"

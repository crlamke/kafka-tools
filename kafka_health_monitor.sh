#!/usr/bin/env bash
#
# kafka_health_monitor.sh
#
# Monitors Kafka broker health, JVM/host resource usage, and disk space
# using the shell tools shipped with Kafka (bin/*.sh) plus standard Linux
# commands. No external dependencies (no Python, no JMX client libraries).
#
# Usage:
#   ./kafka_health_monitor.sh [options]
#
# Options:
#   -b, --bootstrap-server HOST:PORT   Bootstrap server (default: localhost:9092)
#   -k, --kafka-home DIR               Kafka install dir containing bin/*.sh
#                                       (default: $KAFKA_HOME or /opt/kafka)
#   -p, --pid-file FILE                File holding the Kafka broker PID
#                                       (default: autodetect via pgrep)
#   -l, --log-dirs DIR[,DIR...]        Comma-separated Kafka log.dirs to check
#                                       (default: parsed from server.properties
#                                       if found, else /var/lib/kafka/data)
#   -w, --warn-pct PCT                 Disk usage warn threshold (default: 80)
#   -c, --crit-pct PCT                 Disk usage critical threshold (default: 90)
#   -g, --consumer-groups              Also list consumer groups + lag summary
#   --command-config FILE              Optional client config (SASL/SSL) passed
#                                       to kafka-*.sh via --command-config
#   -i, --interval SECONDS             Repeat every N seconds (default: run once)
#   -o, --once                         Force single run even if -i was given
#   -h, --help                         Show this help
#
# Exit codes:
#   0 = OK, 1 = WARNING, 2 = CRITICAL, 3 = script/usage error
#
set -u -o pipefail

# ---------------------------------------------------------------------------
# Defaults
# ---------------------------------------------------------------------------
BOOTSTRAP="localhost:9092"
KAFKA_HOME="${KAFKA_HOME:-/opt/kafka}"
PID_FILE=""
LOG_DIRS=""
WARN_PCT=80
CRIT_PCT=90
SHOW_GROUPS=0
COMMAND_CONFIG=""
INTERVAL=0
FORCE_ONCE=0

# Overall status tracking (0 OK, 1 WARN, 2 CRIT) — take the max seen.
OVERALL=0

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
color() { # color CODE TEXT
    local code="$1"; shift
    if [[ -t 1 ]]; then printf "\033[%sm%s\033[0m" "$code" "$*"; else printf "%s" "$*"; fi
}
ok()   { color "32" "OK"; }
warn() { color "33" "WARN"; }
crit() { color "31" "CRIT"; }

section() { printf "\n=== %s ===\n" "$*"; }

raise() { # raise LEVEL   (1=warn, 2=crit) -- bump OVERALL if higher
    local lvl="$1"
    if (( lvl > OVERALL )); then OVERALL=$lvl; fi
}

usage() { sed -n '2,30p' "$0" | sed 's/^# \{0,1\}//'; }

require_bin() { # require_bin path-to-script
    if [[ ! -x "$1" ]]; then
        echo "  (skipped: $1 not found or not executable)"
        return 1
    fi
    return 0
}

human_bytes() { # human_bytes KB  -> human readable from a df-style KB value
    local kb="$1"
    awk -v kb="$kb" 'BEGIN {
        split("KB MB GB TB PB", u, " ");
        v = kb; i = 1;
        while (v >= 1024 && i < 5) { v /= 1024; i++ }
        printf "%.1f%s", v, u[i]
    }'
}

human_bytes_raw() { # human_bytes_raw BYTES -> human readable from a raw byte count
    local b="$1"
    awk -v b="$b" 'BEGIN {
        split("B KB MB GB TB PB", u, " ");
        v = b; i = 1;
        while (v >= 1024 && i < 6) { v /= 1024; i++ }
        printf "%.1f%s", v, u[i]
    }'
}

in_container() { # exit 0 if we appear to be running inside a container
    [[ -f /.dockerenv ]] && return 0
    [[ -f /run/.containerenv ]] && return 0
    grep -qE '/(docker|kubepods|containerd|lxc|libpod)/' /proc/1/cgroup 2>/dev/null && return 0
    return 1
}

# ---------------------------------------------------------------------------
# Arg parsing
# ---------------------------------------------------------------------------
while [[ $# -gt 0 ]]; do
    case "$1" in
        -b|--bootstrap-server) BOOTSTRAP="$2"; shift 2;;
        -k|--kafka-home)       KAFKA_HOME="$2"; shift 2;;
        -p|--pid-file)         PID_FILE="$2"; shift 2;;
        -l|--log-dirs)         LOG_DIRS="$2"; shift 2;;
        -w|--warn-pct)         WARN_PCT="$2"; shift 2;;
        -c|--crit-pct)         CRIT_PCT="$2"; shift 2;;
        -g|--consumer-groups)  SHOW_GROUPS=1; shift;;
        --command-config)      COMMAND_CONFIG="$2"; shift 2;;
        -i|--interval)         INTERVAL="$2"; shift 2;;
        -o|--once)             FORCE_ONCE=1; shift;;
        -h|--help)             usage; exit 0;;
        *) echo "Unknown option: $1" >&2; usage; exit 3;;
    esac
done

BIN="$KAFKA_HOME/bin"
CLIENT_CONFIG_ARGS=()
if [[ -n "$COMMAND_CONFIG" ]]; then
    CLIENT_CONFIG_ARGS=(--command-config "$COMMAND_CONFIG")
fi

# ---------------------------------------------------------------------------
# Autodetect log dirs from server.properties if not given
# ---------------------------------------------------------------------------
find_log_dirs() {
    if [[ -n "$LOG_DIRS" ]]; then
        echo "$LOG_DIRS"
        return
    fi
    local props=""
    for candidate in "$KAFKA_HOME/config/server.properties" /etc/kafka/server.properties; do
        [[ -f "$candidate" ]] && props="$candidate" && break
    done
    if [[ -n "$props" ]]; then
        local dirs
        dirs=$(grep -E '^(log\.dirs|log\.dir)=' "$props" 2>/dev/null | tail -1 | cut -d= -f2-)
        if [[ -n "$dirs" ]]; then
            echo "$dirs"
            return
        fi
    fi
    echo "/var/lib/kafka/data"
}

# ---------------------------------------------------------------------------
# 1. Broker process health (Linux ps / pgrep)
# ---------------------------------------------------------------------------
check_process() {
    section "Broker Process"
    local pid=""

    if [[ -n "$PID_FILE" && -f "$PID_FILE" ]]; then
        pid=$(cat "$PID_FILE" 2>/dev/null)
    fi
    if [[ -z "$pid" ]] || ! kill -0 "$pid" 2>/dev/null; then
        pid=$(pgrep -f 'kafka.Kafka' | head -1)
    fi

    if [[ -z "$pid" ]]; then
        echo "Status : $(crit)  no Kafka broker process found"
        raise 2
        return
    fi

    echo "PID    : $pid"
    echo "Status : $(ok)  process is running"

    # Uptime, CPU%, MEM% straight from ps
    if ps -p "$pid" -o pid,etime,pcpu,pmem,rss,cmd --no-headers >/tmp/kmon_ps.$$ 2>/dev/null; then
        read -r _pid etime pcpu pmem rss _ < /tmp/kmon_ps.$$
        rm -f /tmp/kmon_ps.$$
        echo "Uptime : $etime"
        echo "CPU%   : $pcpu"
        echo "MEM%   : $pmem  (RSS: $(human_bytes "$rss"))"
        local cpu_int=${pcpu%.*}
        if (( cpu_int >= 90 )); then
            echo "  -> $(crit) CPU usage very high"; raise 2
        elif (( cpu_int >= 75 )); then
            echo "  -> $(warn) CPU usage high"; raise 1
        fi
    fi

    # Open file descriptor count vs limit (Kafka is FD-hungry)
    if [[ -d "/proc/$pid/fd" ]]; then
        local fd_count fd_limit
        fd_count=$(ls "/proc/$pid/fd" 2>/dev/null | wc -l)
        fd_limit=$(awk '/Max open files/ {print $4}' "/proc/$pid/limits" 2>/dev/null)
        echo "FDs    : $fd_count open (limit: ${fd_limit:-unknown})"
        if [[ -n "$fd_limit" && "$fd_limit" =~ ^[0-9]+$ ]]; then
            local fd_pct=$(( fd_count * 100 / fd_limit ))
            if (( fd_pct >= 90 )); then
                echo "  -> $(crit) FD usage at ${fd_pct}% of limit"; raise 2
            elif (( fd_pct >= 75 )); then
                echo "  -> $(warn) FD usage at ${fd_pct}% of limit"; raise 1
            fi
        fi
    fi
}

# ---------------------------------------------------------------------------
# 2. Cluster / API reachability (kafka-broker-api-versions.sh)
# ---------------------------------------------------------------------------
check_broker_api() {
    section "Broker API Reachability"
    local script="$BIN/kafka-broker-api-versions.sh"
    if ! require_bin "$script"; then raise 1; return; fi

    if out=$("$script" --bootstrap-server "$BOOTSTRAP" "${CLIENT_CONFIG_ARGS[@]}" 2>&1); then
        echo "Status : $(ok)  broker responded to API versions request"
        echo "$out" | head -1
    else
        echo "Status : $(crit)  broker did not respond on $BOOTSTRAP"
        echo "$out" | tail -5
        raise 2
    fi
}

# ---------------------------------------------------------------------------
# 3. Under-replicated / offline partitions (kafka-topics.sh)
# ---------------------------------------------------------------------------
check_partitions() {
    section "Partition Health"
    local script="$BIN/kafka-topics.sh"
    if ! require_bin "$script"; then raise 1; return; fi

    local under_repl offline
    under_repl=$("$script" --bootstrap-server "$BOOTSTRAP" "${CLIENT_CONFIG_ARGS[@]}" \
                 --describe --under-replicated-partitions 2>/dev/null)
    offline=$("$script" --bootstrap-server "$BOOTSTRAP" "${CLIENT_CONFIG_ARGS[@]}" \
                 --describe --unavailable-partitions 2>/dev/null)

    local ur_count off_count
    ur_count=$(printf '%s\n' "$under_repl" | grep -c 'Topic:' || true)
    off_count=$(printf '%s\n' "$offline" | grep -c 'Topic:' || true)

    if (( off_count > 0 )); then
        echo "Offline partitions        : $(crit) $off_count"
        printf '%s\n' "$offline" | sed 's/^/    /'
        raise 2
    else
        echo "Offline partitions        : $(ok) 0"
    fi

    if (( ur_count > 0 )); then
        echo "Under-replicated partitions: $(warn) $ur_count"
        printf '%s\n' "$under_repl" | sed 's/^/    /'
        raise 1
    else
        echo "Under-replicated partitions: $(ok) 0"
    fi
}

# ---------------------------------------------------------------------------
# 4. Kafka's own log-dir / disk-space report (kafka-log-dirs.sh) + df
# ---------------------------------------------------------------------------
check_disk() {
    section "Disk Space"
    local dirs
    dirs=$(find_log_dirs)
    echo "Log dirs checked: $dirs"

    IFS=',' read -ra dir_array <<< "$dirs"
    for d in "${dir_array[@]}"; do
        d=$(echo "$d" | xargs)  # trim whitespace
        [[ -z "$d" ]] && continue
        if [[ ! -d "$d" ]]; then
            echo "  $d : $(warn) directory not found"
            raise 1
            continue
        fi
        local line
        line=$(df -kP "$d" | tail -1)
        local avail_kb used_pct mount
        avail_kb=$(echo "$line" | awk '{print $4}')
        used_pct=$(echo "$line" | awk '{print $5}' | tr -d '%')
        mount=$(echo "$line" | awk '{print $6}')
        echo "  $d (mount: $mount)"
        echo "    Used: ${used_pct}%   Free: $(human_bytes "$avail_kb")"
        if (( used_pct >= CRIT_PCT )); then
            echo "    -> $(crit) usage >= ${CRIT_PCT}%"; raise 2
        elif (( used_pct >= WARN_PCT )); then
            echo "    -> $(warn) usage >= ${WARN_PCT}%"; raise 1
        else
            echo "    -> $(ok)"
        fi
    done

    # Kafka's own per-topic-partition size breakdown, if the tool is present
    local script="$BIN/kafka-log-dirs.sh"
    if require_bin "$script"; then
        echo
        echo "Per-broker log-dir size report (kafka-log-dirs.sh):"
        if out=$("$script" --bootstrap-server "$BOOTSTRAP" "${CLIENT_CONFIG_ARGS[@]}" \
                 --describe 2>&1); then
            # Output is a JSON-ish line per broker; just surface total size per dir path
            echo "$out" | grep -o '"logDir":"[^"]*"\|"size":[0-9]*' | \
                paste - - | sed 's/"logDir":"/  /; s/"//g; s/,"size":/  ->  bytes: /' \
                | sort -k3 -n -r | head -20
        else
            echo "  (kafka-log-dirs.sh call failed, see below)"
            echo "$out" | tail -5
        fi
    fi
}

# ---------------------------------------------------------------------------
# 5. Host / container resource usage
#
# Inside a container, /proc/meminfo and nproc typically report the HOST's
# totals, not the container's cgroup limits -- so a broker with a 2GB
# container memory limit can look like it's using 5% of RAM when it's
# actually close to being OOM-killed. This checks cgroup v2 first, then
# cgroup v1, and falls back to free/nproc when neither is present (bare
# metal / VM).
# ---------------------------------------------------------------------------
check_host_resources() {
    section "Host Resources"

    local is_container=0
    in_container && is_container=1
    if (( is_container )); then
        echo "Environment: container detected"
    else
        echo "Environment: bare metal / VM (no container markers found)"
    fi

    # --- Memory: prefer cgroup limits over /proc/meminfo when containerized ---
    local mem_limit_bytes="" mem_used_bytes="" mem_source=""

    if [[ -r /sys/fs/cgroup/memory.max ]]; then
        # cgroup v2 (unified hierarchy)
        local raw_max
        raw_max=$(cat /sys/fs/cgroup/memory.max 2>/dev/null)
        if [[ "$raw_max" != "max" && -n "$raw_max" ]]; then
            mem_limit_bytes="$raw_max"
            mem_used_bytes=$(cat /sys/fs/cgroup/memory.current 2>/dev/null)
            mem_source="cgroup v2 (memory.max)"
        fi
    elif [[ -r /sys/fs/cgroup/memory/memory.limit_in_bytes ]]; then
        # cgroup v1
        local raw_limit
        raw_limit=$(cat /sys/fs/cgroup/memory/memory.limit_in_bytes 2>/dev/null)
        # Unset cgroup v1 limits show up as a huge number (close to 2^63/PAGE_SIZE);
        # treat anything under ~1PB as a real, imposed limit.
        if [[ -n "$raw_limit" && "$raw_limit" -lt 1125899906842624 ]]; then
            mem_limit_bytes="$raw_limit"
            mem_used_bytes=$(cat /sys/fs/cgroup/memory/memory.usage_in_bytes 2>/dev/null)
            mem_source="cgroup v1 (memory.limit_in_bytes)"
        fi
    fi

    if [[ -n "$mem_limit_bytes" ]]; then
        echo "Memory (container limit via $mem_source):"
        echo "  Limit: $(human_bytes_raw "$mem_limit_bytes")   Used: $(human_bytes_raw "$mem_used_bytes")"
        local mem_used_pct
        mem_used_pct=$(awk -v u="$mem_used_bytes" -v l="$mem_limit_bytes" 'BEGIN{printf "%d", (u/l*100)}')
        echo "  Used%: ${mem_used_pct}%"
        if (( mem_used_pct >= 90 )); then
            echo "  -> $(crit) container memory usage ${mem_used_pct}% of limit -- risk of OOM kill"; raise 2
        elif (( mem_used_pct >= 80 )); then
            echo "  -> $(warn) container memory usage ${mem_used_pct}% of limit"; raise 1
        fi
        # Also show host-level free for context, clearly labeled
        if command -v free >/dev/null 2>&1; then
            echo "  (host-level free -h, for reference only -- not what the container is bound by):"
            free -h | sed 's/^/    /'
        fi
    elif command -v free >/dev/null 2>&1; then
        echo "Memory (no cgroup limit found -- using host /proc/meminfo via free):"
        free -h | sed 's/^/  /'
        local mem_used_pct
        mem_used_pct=$(free | awk '/Mem:/ {printf "%d", $3/$2*100}')
        if (( mem_used_pct >= 90 )); then
            echo "  -> $(crit) memory usage ${mem_used_pct}%"; raise 2
        elif (( mem_used_pct >= 80 )); then
            echo "  -> $(warn) memory usage ${mem_used_pct}%"; raise 1
        fi
    fi

    # --- CPU: prefer cgroup quota/period over nproc when containerized ---
    local ncpu="" cpu_source="host (nproc)"

    if [[ -r /sys/fs/cgroup/cpu.max ]]; then
        # cgroup v2: "<quota> <period>" in microseconds, or "max <period>"
        local quota period
        read -r quota period < /sys/fs/cgroup/cpu.max 2>/dev/null
        if [[ "$quota" != "max" && -n "$quota" && -n "$period" && "$period" -gt 0 ]]; then
            ncpu=$(awk -v q="$quota" -v p="$period" 'BEGIN{printf "%.2f", q/p}')
            cpu_source="cgroup v2 (cpu.max)"
        fi
    elif [[ -r /sys/fs/cgroup/cpu/cpu.cfs_quota_us && -r /sys/fs/cgroup/cpu/cpu.cfs_period_us ]]; then
        local quota period
        quota=$(cat /sys/fs/cgroup/cpu/cpu.cfs_quota_us 2>/dev/null)
        period=$(cat /sys/fs/cgroup/cpu/cpu.cfs_period_us 2>/dev/null)
        if [[ -n "$quota" && "$quota" -gt 0 && -n "$period" && "$period" -gt 0 ]]; then
            ncpu=$(awk -v q="$quota" -v p="$period" 'BEGIN{printf "%.2f", q/p}')
            cpu_source="cgroup v1 (cpu.cfs_quota_us/period_us)"
        fi
    fi

    if [[ -z "$ncpu" ]]; then
        ncpu=$(nproc 2>/dev/null || echo 1)
    fi

    if [[ -r /proc/loadavg ]]; then
        local load1
        load1=$(awk '{print $1}' /proc/loadavg)
        echo "Load avg (1m): $load1  (effective cores: $ncpu, source: $cpu_source)"
        # Compare load1 to core count using awk to avoid bash float math
        local over
        over=$(awk -v l="$load1" -v n="$ncpu" 'BEGIN{print (l > n*1.5) ? 1 : (l > n ? 2 : 0)}')
        if [[ "$over" == "1" ]]; then
            echo "  -> $(crit) load average exceeds 1.5x effective core count"; raise 2
        elif [[ "$over" == "2" ]]; then
            echo "  -> $(warn) load average exceeds effective core count"; raise 1
        fi
        if (( is_container )) && [[ "$cpu_source" == "host (nproc)" ]]; then
            echo "  (note: no cpu.max/cfs_quota limit found -- container may have unlimited CPU, or is not CPU-cgroup-constrained)"
        fi
    fi

    if command -v iostat >/dev/null 2>&1; then
        echo "Disk I/O (iostat -dx 1 2, last sample):"
        iostat -dx 1 2 2>/dev/null | awk 'BEGIN{n=0} /^$/{n++} n>=3{print "  " $0}' | tail -10
    fi
}

# ---------------------------------------------------------------------------
# 6. JVM heap / GC snapshot for the broker (jstat / jcmd, ship with the JDK
#    Kafka runs on -- not a Kafka script, but standard for JVM health)
# ---------------------------------------------------------------------------
check_jvm() {
    section "JVM Heap (broker)"
    local pid=""
    [[ -n "$PID_FILE" && -f "$PID_FILE" ]] && pid=$(cat "$PID_FILE" 2>/dev/null)
    [[ -z "$pid" ]] && pid=$(pgrep -f 'kafka.Kafka' | head -1)

    if [[ -z "$pid" ]]; then
        echo "  (skipped: broker PID unknown)"
        return
    fi

    if command -v jstat >/dev/null 2>&1; then
        echo "Heap (jstat -gc):"
        if jstat -gc "$pid" 2>/dev/null | tail -1 > /tmp/kmon_jstat.$$; then
            read -r s0c s1c s0u s1u ec eu oc ou mc mu ccsc ccsu ygc ygct fgc fgct gct < /tmp/kmon_jstat.$$
            rm -f /tmp/kmon_jstat.$$
            awk -v ou="$ou" -v oc="$oc" -v eu="$eu" -v ec="$ec" 'BEGIN{
                printf "  Old gen : %.0f/%.0f KB (%.1f%%)\n", ou, oc, (ou/oc*100);
                printf "  Eden    : %.0f/%.0f KB (%.1f%%)\n", eu, ec, (eu/ec*100);
            }'
            echo "  Young GC count/time: $ygc / ${ygct}s   Full GC count/time: $fgc / ${fgct}s"
            local old_pct
            old_pct=$(awk -v ou="$ou" -v oc="$oc" 'BEGIN{printf "%d", (ou/oc*100)}')
            if (( old_pct >= 90 )); then
                echo "  -> $(crit) old-gen heap usage ${old_pct}%"; raise 2
            elif (( old_pct >= 75 )); then
                echo "  -> $(warn) old-gen heap usage ${old_pct}%"; raise 1
            fi
        else
            echo "  (jstat failed -- may need to run as same user as broker)"
        fi
    elif command -v jcmd >/dev/null 2>&1; then
        echo "Heap (jcmd GC.heap_info):"
        jcmd "$pid" GC.heap_info 2>/dev/null | sed 's/^/  /'
    else
        echo "  (skipped: neither jstat nor jcmd found on PATH)"
    fi
}

# ---------------------------------------------------------------------------
# 7. Consumer group lag summary (optional, kafka-consumer-groups.sh)
# ---------------------------------------------------------------------------
check_consumer_groups() {
    section "Consumer Groups"
    local script="$BIN/kafka-consumer-groups.sh"
    if ! require_bin "$script"; then raise 1; return; fi

    local groups
    groups=$("$script" --bootstrap-server "$BOOTSTRAP" "${CLIENT_CONFIG_ARGS[@]}" --list 2>/dev/null)
    if [[ -z "$groups" ]]; then
        echo "  (no consumer groups found)"
        return
    fi

    while IFS= read -r g; do
        [[ -z "$g" ]] && continue
        echo "Group: $g"
        local desc
        desc=$("$script" --bootstrap-server "$BOOTSTRAP" "${CLIENT_CONFIG_ARGS[@]}" \
               --describe --group "$g" 2>/dev/null)
        echo "$desc" | awk 'NR==1{print "  " $0; next} NF{print "  " $0}' | head -20
        local max_lag
        max_lag=$(echo "$desc" | awk 'NR>1 && $6 ~ /^[0-9]+$/ {print $6}' | sort -n | tail -1)
        if [[ -n "$max_lag" && "$max_lag" =~ ^[0-9]+$ ]]; then
            if (( max_lag > 100000 )); then
                echo "  -> $(crit) max partition lag $max_lag"; raise 2
            elif (( max_lag > 10000 )); then
                echo "  -> $(warn) max partition lag $max_lag"; raise 1
            fi
        fi
    done <<< "$groups"
}

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
run_all() {
    printf "Kafka Health Check @ %s\n" "$(date '+%Y-%m-%d %H:%M:%S')"
    printf "Bootstrap: %s   Kafka home: %s\n" "$BOOTSTRAP" "$KAFKA_HOME"
    OVERALL=0

    check_process
    check_broker_api
    check_partitions
    check_disk
    check_host_resources
    check_jvm
    (( SHOW_GROUPS )) && check_consumer_groups

    section "Summary"
    case $OVERALL in
        0) echo "Overall status: $(ok)";;
        1) echo "Overall status: $(warn)";;
        2) echo "Overall status: $(crit)";;
    esac
}

if (( INTERVAL > 0 && ! FORCE_ONCE )); then
    while true; do
        run_all
        sleep "$INTERVAL"
    done
else
    run_all
fi

exit "$OVERALL"

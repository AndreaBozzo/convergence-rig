#!/bin/sh
# Minimal POSIX reproduction of pg2iceberg skipping staged changes after the
# Iceberg REST catalog database (Lakekeeper on PostgreSQL) is restored from an
# earlier backup while pg2iceberg keeps running.
#
# Usage:
#   ./repro.sh          Run the reproduction
#   ./repro.sh down     Stop and remove only Compose project p2irestore
#
# Environment:
#   P2I_VERSION          0.12.0 (default) or main (commit 4344888eb6)
#   P2I_PG2ICEBERG_IMAGE full image reference, overrides P2I_VERSION
#   P2I_CONTROL=1        skip the restore (control run: the detector must find no gap)
#   P2I_PG_PORT, P2I_LAKEKEEPER_PORT, P2I_SILO_PORT   host ports on 127.0.0.1
#
# Exit codes: 1 when the skip reproduces (a gap in the applied log range),
# 0 when it does not, 2 when the run is invalid (a precondition failed).
set -eu

# Git Bash on Windows rewrites arguments such as /etc/x into C:/Program Files/Git/etc/x
# before docker.exe sees them. This turns it off; it is a no-op on Linux and macOS.
MSYS_NO_PATHCONV=1
export MSYS_NO_PATHCONV

SCRIPT_DIR=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
cd "$SCRIPT_DIR"

PROJECT="p2irestore"
COMPOSE_FILE="docker-compose.yml"
LAKEKEEPER_PORT="${P2I_LAKEKEEPER_PORT:-38181}"
LAKEKEEPER_URL="http://127.0.0.1:${LAKEKEEPER_PORT}"
SNAPSHOT_TIMEOUT="${P2I_SNAPSHOT_TIMEOUT:-60}"

# Both references are multi-arch index digests, resolved with
# `docker buildx imagetools inspect` on 2026-11-20.
IMAGE_0_12_0="polynyadev/pg2iceberg:0.12.0@sha256:c6b97dc3e4d1e41deee3bcd84c1dc8c655c24b0bbc2061a087afc27b242a7916"
IMAGE_MAIN="polynyadev/pg2iceberg:sha-4344888@sha256:964f0be1f4d038947d5a85905dbbc04733292d20cdcc33eb2224e1dcb663cd68"

P2I_VERSION="${P2I_VERSION:-0.12.0}"
if [ -z "${P2I_PG2ICEBERG_IMAGE:-}" ]; then
    case "$P2I_VERSION" in
        0.12.0) P2I_PG2ICEBERG_IMAGE="$IMAGE_0_12_0" ;;
        main) P2I_PG2ICEBERG_IMAGE="$IMAGE_MAIN" ;;
        *)
            printf 'Unknown P2I_VERSION: %s (use 0.12.0 or main)\n' "$P2I_VERSION" >&2
            exit 2
            ;;
    esac
fi
export P2I_PG2ICEBERG_IMAGE

log() {
    printf '[%s] %s\n' "$(date -u '+%Y-%m-%dT%H:%M:%SZ')" "$*"
}

invalid() {
    log "INVALID RUN: $*"
    printf 'RESULT: invalid run (%s)\n' "$*"
    exit 2
}

dc() {
    docker compose -p "$PROJECT" -f "$COMPOSE_FILE" --profile pg2iceberg "$@"
}

psql_src() {
    dc exec -T pg-source psql -v ON_ERROR_STOP=1 -U postgres -d repro "$@"
}

psql_lk() {
    dc exec -T lakekeeper-db psql -v ON_ERROR_STOP=1 -U postgres -d postgres "$@"
}

extract_json_string() {
    sed -n "s/.*\"$1\"[[:space:]]*:[[:space:]]*\"\([^\"]*\)\".*/\1/p" | head -n 1
}

table_json() {
    curl -fsS "${LAKEKEEPER_URL}/catalog/v1/${WAREHOUSE_PREFIX}/namespaces/public/tables/items" 2>/dev/null | tr -d '\r' || true
}

# One line per snapshot, ordered by sequence number:
#   <seq> <snapshot-id> <parent-id|-> <log-start|-> <log-end|->
snapshot_rows() {
    awk '
    function val(s, key,   i, t) {
        i = index(s, key)
        if (i == 0) return "-"
        t = substr(s, i + length(key))
        if (match(t, /^[0-9]+/)) return substr(t, RSTART, RLENGTH)
        return "-"
    }
    { all = all $0 }
    END {
        i = index(all, "\"snapshots\":[")
        if (i == 0) exit
        s = substr(all, i)
        j = index(s, "\"config\":")
        if (j > 0) s = substr(s, 1, j - 1)
        n = split(s, parts, "{\"snapshot-id\":")
        for (k = 2; k <= n; k++) {
            p = parts[k]
            id = val("\"snapshot-id\":" p, "\"snapshot-id\":")
            printf "%s %s %s %s %s\n", val(p, "\"sequence-number\":"), id, \
                val(p, "\"parent-snapshot-id\":"), \
                val(p, "\"pg2iceberg.log-start\":\""), val(p, "\"pg2iceberg.log-end\":\"")
        }
    }' | sort -n -k1,1
}

current_snapshot_id() {
    sed -n 's/.*"current-snapshot-id"[[:space:]]*:[[:space:]]*\([0-9][0-9]*\).*/\1/p' | head -n 1
}

table_properties() {
    awk '
    { all = all $0 }
    END {
        i = index(all, "\"properties\":{")
        if (i == 0) { print "none"; exit }
        s = substr(all, i)
        j = index(s, ",\"current-snapshot-id\"")
        if (j > 0) s = substr(s, 1, j - 1)
        print s
    }'
}

count_lines() {
    awk 'END { print NR }'
}

# Prints the table's snapshots (seq id parent log-start log-end), its
# properties, and the pg2iceberg cursor. Sets ROWS, CUR_ID and SNAP_COUNT.
show_state() {
    label="$1"
    json=$(table_json)
    ROWS=$(printf '%s\n' "$json" | snapshot_rows)
    CUR_ID=$(printf '%s\n' "$json" | current_snapshot_id)
    SNAP_COUNT=$(printf '%s\n' "$ROWS" | grep . | count_lines || true)
    log "--- ${label}: ${SNAP_COUNT} snapshots, current-snapshot-id=${CUR_ID:-none}"
    log "    seq  snapshot-id  parent  log-start  log-end"
    printf '%s\n' "$ROWS" | grep . | sed 's/^/    /' || true
    log "    table properties: $(printf '%s\n' "$json" | table_properties)"
    log "    _pg2iceberg.mat_cursor: $(mat_cursor)"
}

mat_cursor() {
    psql_src -t -A -c "SELECT COALESCE((SELECT last_offset::text FROM _pg2iceberg.mat_cursor WHERE group_name = 'default' AND table_name = 'public.items'), 'none');" 2>/dev/null | tr -d '\r' || printf 'none'
}

staged_end() {
    psql_src -t -A -c "SELECT COALESCE((SELECT next_offset::text FROM _pg2iceberg.log_seq WHERE table_name = 'public.items'), 'none');" 2>/dev/null | tr -d '' || printf 'none'
}

pg2iceberg_container() {
    dc ps -aq pg2iceberg | tr -d '\r' | head -n 1
}

pg2iceberg_status() {
    cid=$(pg2iceberg_container)
    if [ -z "$cid" ]; then
        printf 'missing'
        return 0
    fi
    docker inspect --format '{{.State.Status}} exit_code={{.State.ExitCode}}' "$cid" | tr -d '\r'
}

require_pg2iceberg_running() {
    st=$(pg2iceberg_status)
    case "$st" in
        running*) ;;
        *)
            dc logs --no-color --tail=25 pg2iceberg || true
            invalid "pg2iceberg is not running (${st}) $1"
            ;;
    esac
}

# Waits until the table has more than $1 snapshots; sets SNAP_COUNT.
wait_for_snapshot_count_above() {
    want="$1"
    elapsed=0
    while [ "$elapsed" -lt "$SNAPSHOT_TIMEOUT" ]; do
        require_pg2iceberg_running "while waiting for a snapshot"
        json=$(table_json)
        SNAP_COUNT=$(printf '%s\n' "$json" | snapshot_rows | grep . | count_lines || true)
        if [ "${SNAP_COUNT:-0}" -gt "$want" ]; then
            return 0
        fi
        sleep 1
        elapsed=$((elapsed + 1))
    done
    return 1
}

# Waits until every staged change is committed: the cursor, the end of the
# staged log and the log-end of the current snapshot are equal. The first
# pause lets the flush interval stage what was just written.
wait_until_applied() {
    sleep 3
    elapsed=0
    while [ "$elapsed" -lt "$SNAPSHOT_TIMEOUT" ]; do
        require_pg2iceberg_running "while waiting for the log to be applied"
        json=$(table_json)
        rows=$(printf '%s
' "$json" | snapshot_rows)
        id=$(printf '%s
' "$json" | current_snapshot_id)
        snap_end=$(printf '%s
' "$rows" | awk -v id="$id" '$2 == id { print $5; exit }')
        cur=$(mat_cursor)
        staged=$(staged_end)
        if [ "$cur" = "$staged" ] && [ "$cur" = "$snap_end" ]; then
            return 0
        fi
        sleep 1
        elapsed=$((elapsed + 1))
    done
    return 1
}

# Field $2 (1-based) of the row of the current snapshot.
current_row_field() {
    printf '%s\n' "$ROWS" | awk -v id="$CUR_ID" -v f="$1" '$2 == id { print $f; exit }'
}

bootstrap_stack() {
    log "Starting base services (pg-source, lakekeeper-db, silo, lakekeeper-migrate, lakekeeper)..."
    dc up -d --wait pg-source lakekeeper-db silo lakekeeper

    log "Creating S3 bucket 'repro' in Silo (idempotent)..."
    dc exec -T silo sh -c 'mc alias set local http://127.0.0.1:9000 repro-silo-user repro-silo-pass >/dev/null && mc mb --ignore-existing local/repro'

    log "Bootstrapping Lakekeeper at ${LAKEKEEPER_URL} (idempotent)..."
    info_body=$(curl -fsS "${LAKEKEEPER_URL}/management/v1/info")
    case "$info_body" in
        *'"bootstrapped":true'*)
            log "Lakekeeper already bootstrapped."
            ;;
        *)
            curl -fsS -o /dev/null -X POST "${LAKEKEEPER_URL}/management/v1/bootstrap" \
                -H 'Content-Type: application/json' \
                -d '{"accept-terms-of-use":true}'
            log "Lakekeeper bootstrapped."
            ;;
    esac

    log "Creating Lakekeeper warehouse 'repro' pointing at http://silo:9000 (idempotent)..."
    cfg_resp=$(curl -sS -w '\n%{http_code}' "${LAKEKEEPER_URL}/catalog/v1/config?warehouse=repro" || true)
    cfg_code=$(printf '%s\n' "$cfg_resp" | tail -n 1 | tr -d '\r')
    cfg_body=$(printf '%s\n' "$cfg_resp" | sed '$d')
    if [ "$cfg_code" != "200" ]; then
        curl -fsS -o /dev/null -X POST "${LAKEKEEPER_URL}/management/v1/warehouse" \
            -H 'Content-Type: application/json' \
            -d '{
                "warehouse-name": "repro",
                "project-id": "00000000-0000-0000-0000-000000000000",
                "storage-profile": {
                    "type": "s3",
                    "bucket": "repro",
                    "key-prefix": "warehouse",
                    "assume-role-arn": null,
                    "endpoint": "http://silo:9000",
                    "sts-endpoint": "http://silo:9000",
                    "region": "local-01",
                    "path-style-access": true,
                    "flavor": "s3-compat",
                    "sts-enabled": true
                },
                "storage-credential": {
                    "type": "s3",
                    "credential-type": "access-key",
                    "access-key-id": "repro-silo-user",
                    "secret-access-key": "repro-silo-pass"
                }
            }'
        cfg_body=$(curl -fsS "${LAKEKEEPER_URL}/catalog/v1/config?warehouse=repro")
    fi

    WAREHOUSE_PREFIX=$(printf '%s\n' "$cfg_body" | extract_json_string "prefix")
    if [ -z "$WAREHOUSE_PREFIX" ]; then
        invalid "failed to resolve the Lakekeeper warehouse prefix"
    fi
    log "Warehouse 'repro' ready (prefix=${WAREHOUSE_PREFIX})."
}

wait_for_lakekeeper_ready() {
    elapsed=0
    while [ "$elapsed" -lt 60 ]; do
        if curl -fsS -o /dev/null "${LAKEKEEPER_URL}/management/v1/info" 2>/dev/null; then
            return 0
        fi
        sleep 1
        elapsed=$((elapsed + 1))
    done
    return 1
}

run_down() {
    log "Removing Compose project '${PROJECT}' (containers, volumes, networks)..."
    dc down -v --remove-orphans
    log "Project '${PROJECT}' removed."
}

run_repro() {
    log "pg2iceberg image: ${P2I_PG2ICEBERG_IMAGE}"
    log "Step 1: start and bootstrap"
    bootstrap_stack

    log "Step 2: create public.items (primary key) with 30 rows"
    psql_src <<'SQL'
CREATE TABLE IF NOT EXISTS public.items (
    id BIGINT PRIMARY KEY,
    body TEXT NOT NULL,
    rev INT NOT NULL DEFAULT 0
);
INSERT INTO public.items (id, body)
SELECT g, 'initial-' || g FROM generate_series(1, 30) AS g
ON CONFLICT (id) DO NOTHING;
SQL

    log "Step 3: start pg2iceberg and wait for the first snapshot"
    dc up -d pg2iceberg
    wait_for_snapshot_count_above 0 || invalid "no first snapshot within ${SNAPSHOT_TIMEOUT} s"
    show_state "after the first snapshot"

    log "Step 4: one small change, so the backup holds a snapshot with a log range"
    psql_src -c "INSERT INTO public.items (id, body) SELECT g, 'baseline-' || g FROM generate_series(31, 35) AS g;"
    wait_for_snapshot_count_above 1 || invalid "no snapshot for the baseline change within ${SNAPSHOT_TIMEOUT} s"
    wait_until_applied || invalid "the baseline change was not applied within ${SNAPSHOT_TIMEOUT} s"
    show_state "before the backup"
    DUMP_COUNT="$SNAP_COUNT"
    DUMP_ID="$CUR_ID"
    DUMP_END=$(current_row_field 5)
    if [ -z "$DUMP_END" ] || [ "$DUMP_END" = "-" ]; then
        invalid "the snapshot to back up has no pg2iceberg.log-end"
    fi

    log "Step 5: pg_dump of the Lakekeeper catalog database (inside lakekeeper-db)"
    dc exec -T lakekeeper-db sh -c 'pg_dump -U postgres -d lakekeeper -f /tmp/lakekeeper.sql && wc -c /tmp/lakekeeper.sql'
    show_state "right after the backup"
    if [ "$CUR_ID" != "$DUMP_ID" ]; then
        invalid "a snapshot was committed while the backup ran"
    fi
    log "Backup holds snapshot ${DUMP_ID} (snapshots=${DUMP_COUNT}, log-end=${DUMP_END})"

    log "Step 6: insert, update and delete rows in several transactions; wait for two more snapshots"
    psql_src -c "INSERT INTO public.items (id, body) SELECT g, 'after-backup-' || g FROM generate_series(101, 110) AS g;"
    psql_src -c "UPDATE public.items SET rev = rev + 1 WHERE id <= 5;"
    wait_for_snapshot_count_above "$DUMP_COUNT" || invalid "no snapshot after the backup within ${SNAPSHOT_TIMEOUT} s"
    psql_src -c "DELETE FROM public.items WHERE id BETWEEN 6 AND 8;"
    psql_src -c "INSERT INTO public.items (id, body) SELECT g, 'after-backup-' || g FROM generate_series(111, 115) AS g;"
    wait_for_snapshot_count_above $((DUMP_COUNT + 1)) || invalid "fewer than two snapshots after the backup within ${SNAPSHOT_TIMEOUT} s"
    wait_until_applied || invalid "the changes after the backup were not all applied within ${SNAPSHOT_TIMEOUT} s"
    show_state "before the restore"
    PRE_COUNT="$SNAP_COUNT"
    PRE_ID="$CUR_ID"
    PRE_END=$(current_row_field 5)
    PRE_CURSOR=$(mat_cursor)
    if [ "$PRE_COUNT" -lt $((DUMP_COUNT + 2)) ]; then
        invalid "fewer than two snapshots landed after the backup"
    fi
    if [ "$PRE_END" = "-" ] || [ "$PRE_END" -le "$DUMP_END" ]; then
        invalid "the log-end did not advance past the backup (${DUMP_END} -> ${PRE_END})"
    fi
    if [ "$PRE_CURSOR" = "none" ] || [ "$PRE_CURSOR" -le "$DUMP_END" ]; then
        invalid "mat_cursor (${PRE_CURSOR}) is not ahead of the backup log-end (${DUMP_END})"
    fi
    require_pg2iceberg_running "before the restore"

    RESTORE_START=$(date -u '+%Y-%m-%dT%H:%M:%SZ')
    if [ "${P2I_CONTROL:-0}" = "1" ]; then
        log "Step 7: CONTROL run, no restore (the detector must report no gap and verify must pass)"
        BASE_COUNT="$PRE_COUNT"
    else
        log "Step 7: stop Lakekeeper, restore the dump into a fresh catalog database, start Lakekeeper"
        dc stop -t 3 lakekeeper
        psql_lk -c "DROP DATABASE lakekeeper WITH (FORCE);" -c "CREATE DATABASE lakekeeper;"
        dc exec -T lakekeeper-db sh -c 'psql -v ON_ERROR_STOP=1 -q -U postgres -d lakekeeper -f /tmp/lakekeeper.sql >/dev/null'
        dc start lakekeeper
        wait_for_lakekeeper_ready || invalid "Lakekeeper did not return after the restore"
        RESTORE_END=$(date -u '+%Y-%m-%dT%H:%M:%SZ')
        log "Restore window: ${RESTORE_START} to ${RESTORE_END}"
        require_pg2iceberg_running "after the restore (it must survive the restore window)"
        show_state "after the restore"
        if [ "$CUR_ID" != "$DUMP_ID" ] || [ "$SNAP_COUNT" != "$DUMP_COUNT" ]; then
            invalid "the catalog did not return to the backed up snapshot"
        fi
        log "Catalog rolled back: snapshot ${PRE_ID} (log-end ${PRE_END}) -> ${DUMP_ID} (log-end ${DUMP_END}); mat_cursor stays ${PRE_CURSOR}"
        BASE_COUNT="$DUMP_COUNT"
    fi

    log "Step 8: one more change in a single transaction; wait for the next snapshot"
    psql_src -c "BEGIN; INSERT INTO public.items (id, body) VALUES (201, 'after-restore'); UPDATE public.items SET body = 'after-restore', rev = rev + 1 WHERE id = 9; COMMIT;"
    committed="yes"
    if ! wait_for_snapshot_count_above "$BASE_COUNT"; then
        committed="no"
        log "No new snapshot within ${SNAPSHOT_TIMEOUT} s after the restore."
    else
        wait_until_applied || log "Not everything staged was applied within ${SNAPSHOT_TIMEOUT} s."
    fi
    show_state "after the post-restore change"
    POST_STATUS=$(pg2iceberg_status)

    log "Step 9: pg2iceberg warnings and errors since the restore began"
    dc logs --no-color --since "$RESTORE_START" pg2iceberg > "${TMPDIR:-/tmp}/p2irestore-pg2iceberg.log" 2>&1 || true
    ISSUES=$(grep -E '"level":"(WARN|ERROR)"' "${TMPDIR:-/tmp}/p2irestore-pg2iceberg.log" || true)
    ISSUE_COUNT=$(printf '%s\n' "$ISSUES" | grep . | count_lines || true)
    if [ "$ISSUE_COUNT" -gt 0 ]; then
        printf '%s\n' "$ISSUES"
    else
        log "(none)"
    fi
    log "pg2iceberg status: ${POST_STATUS}"

    log "Step 10: pg2iceberg verify against PostgreSQL"
    verify_rc=0
    dc run --rm --no-deps pg2iceberg verify --config /etc/pg2iceberg/config.yaml > "${TMPDIR:-/tmp}/p2irestore-verify.log" 2>&1 || verify_rc=$?
    tr -d '\r' < "${TMPDIR:-/tmp}/p2irestore-verify.log" | tail -n 40
    log "pg2iceberg verify exit code: ${verify_rc}"
    VERIFY_SUMMARY=$(grep -E 'diff detected|total row-level diffs' "${TMPDIR:-/tmp}/p2irestore-verify.log" | tr -d '\r' | tail -n 3 || true)
    if [ "$verify_rc" -ne 0 ] && [ -z "$VERIFY_SUMMARY" ]; then
        invalid "pg2iceberg verify failed without reporting diffs (see the output above)"
    fi

    log "Step 11: applied log range per snapshot"
    json=$(table_json)
    ROWS=$(printf '%s\n' "$json" | snapshot_rows)
    printf '%s\n' "$ROWS" | grep . | sed 's/^/    /'
    GAPS=$(printf '%s\n' "$ROWS" | awk '
        { id = $2 ""; parent = $3 ""; start = $4; end = $5; ends[id] = end }
        start != "-" {
            prev = 0
            if (parent != "-" && (parent in ends) && ends[parent] != "-") prev = ends[parent]
            if (start + 0 > prev + 0) gaps = gaps (gaps == "" ? "" : " ") sprintf("(%d,%d]", prev, start)
            else if (start + 0 < prev + 0) overlaps = overlaps (overlaps == "" ? "" : " ") sprintf("(%d,%d]", start, prev)
        }
        END { printf "gaps=%s overlaps=%s\n", (gaps == "" ? "none" : gaps), (overlaps == "" ? "none" : overlaps) }')
    log "Detected: ${GAPS}"
    log "DETAIL: backup log-end=${DUMP_END}; before restore log-end=${PRE_END}, mat_cursor=${PRE_CURSOR}; post-restore snapshot committed=${committed}; ${GAPS}"
    if [ -n "$VERIFY_SUMMARY" ]; then
        printf '%s\n' "$VERIFY_SUMMARY" | sed 's/^/DETAIL: verify: /'
    fi

    case "$GAPS" in
        gaps=none*) gap_found="no" ;;
        *) gap_found="yes" ;;
    esac
    if [ "$verify_rc" -eq 0 ]; then
        verify_text="verify reported no diffs"
    else
        verify_text="verify reported diffs"
    fi
    printf 'RESULT: gap in the applied log range after the catalog restore: %s; %s (exit %s); pg2iceberg warnings or errors since the restore: %s\n' \
        "$gap_found" "$verify_text" "$verify_rc" "$ISSUE_COUNT"
    if [ "$gap_found" = "yes" ]; then
        exit 1
    fi
    exit 0
}

cmd="${1:-run}"
case "$cmd" in
    run)
        run_repro
        ;;
    down)
        run_down
        ;;
    -h|--help|help)
        cat <<'EOF'
Usage: ./repro.sh [run|down]
  run   Run the catalog-restore reproduction (default).
        P2I_VERSION=0.12.0 (default) or P2I_VERSION=main selects the pg2iceberg image.
  down  Stop and remove only Compose project p2irestore.
EOF
        ;;
    *)
        printf 'Unknown command: %s\nUsage: %s [run|down]\n' "$cmd" "$0" >&2
        exit 2
        ;;
esac

#!/bin/sh
# Minimal POSIX reproduction of pg2iceberg 0.12.0 exiting on a transient
# Iceberg REST catalog outage.
#
# Usage:
#   ./repro.sh          Run single-process reproduction (pg2iceberg run)
#   ./repro.sh split    Run split-mode comparison (stream-only + materializer-only)
#   ./repro.sh down     Stop and remove only Compose project p2irepro
set -eu

SCRIPT_DIR=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
cd "$SCRIPT_DIR"

PROJECT="p2irepro"
COMPOSE_FILE="docker-compose.yml"
LAKEKEEPER_PORT="${P2I_LAKEKEEPER_PORT:-28181}"
LAKEKEEPER_URL="http://127.0.0.1:${LAKEKEEPER_PORT}"
WAIT_SECONDS="${P2I_WAIT_SECONDS:-30}"

log() {
    printf '[%s] %s\n' "$(date -u '+%Y-%m-%dT%H:%M:%SZ')" "$*"
}

dc() {
    docker compose -p "$PROJECT" -f "$COMPOSE_FILE" "$@"
}

psql_src() {
    dc exec -T pg-source psql -v ON_ERROR_STOP=1 -U postgres -d repro "$@"
}

extract_json_string() {
    # Extract string value for key "$1" from JSON on stdin (POSIX BRE).
    sed -n "s/.*\"$1\"[[:space:]]*:[[:space:]]*\"\([^\"]*\)\".*/\1/p" | head -n 1
}

extract_current_snapshot_id() {
    # Extract positive integer current-snapshot-id from LoadTable JSON on stdin.
    sed -n 's/.*"current-snapshot-id"[[:space:]]*:[[:space:]]*\([0-9][0-9]*\).*/\1/p' | head -n 1
}

get_container_id() {
    dc --profile single --profile split ps -aq "$1" | tr -d '\r' | head -n 1
}

inspect_status() {
    docker inspect --format '{{.State.Status}}' "$1" | tr -d '\r'
}

inspect_exit_code() {
    docker inspect --format '{{.State.ExitCode}}' "$1" | tr -d '\r'
}

inspect_summary() {
    docker inspect --format 'status={{.State.Status}} running={{.State.Running}} exit_code={{.State.ExitCode}} finished_at={{.State.FinishedAt}}' "$1" | tr -d '\r'
}

query_slot_active() {
    psql_src -t -A -c "SELECT COALESCE((SELECT CASE WHEN active THEN 'true' ELSE 'false' END FROM pg_replication_slots WHERE slot_name = 'pg2iceberg_slot'), 'missing');" | tr -d '\r'
}

print_slots() {
    log "pg_replication_slots:"
    psql_src -c "SELECT slot_name, slot_type, active, restart_lsn, confirmed_flush_lsn FROM pg_replication_slots ORDER BY slot_name;"
}

print_service_state() {
    svc="$1"
    cid=$(get_container_id "$svc")
    if [ -z "$cid" ]; then
        log "container $svc: not found"
        return 0
    fi
    log "docker inspect $svc: $(inspect_summary "$cid")"
    log "last log lines ($svc):"
    dc --profile single --profile split logs --no-color --tail=25 "$svc" || true
}

bootstrap_stack() {
    log "Starting base services (pg-source, lakekeeper-db, silo, lakekeeper-migrate, lakekeeper)..."
    dc up -d --wait

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
        log "ERROR: failed to resolve Lakekeeper warehouse prefix from /catalog/v1/config"
        exit 2
    fi
    log "Warehouse 'repro' ready (prefix=${WAREHOUSE_PREFIX})."
}

create_table_and_seed() {
    log "Creating table public.items with primary key and inserting initial rows..."
    psql_src <<'SQL'
CREATE TABLE IF NOT EXISTS public.items (
    id BIGINT PRIMARY KEY,
    body TEXT NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
INSERT INTO public.items (id, body)
VALUES
    (1, 'initial-1'),
    (2, 'initial-2'),
    (3, 'initial-3'),
    (4, 'initial-4'),
    (5, 'initial-5')
ON CONFLICT (id) DO NOTHING;
SQL
}

insert_outage_rows() {
    log "Inserting more rows and an update into public.items while Lakekeeper is stopped..."
    psql_src <<'SQL'
INSERT INTO public.items (id, body)
VALUES
    (101, 'during-outage-1'),
    (102, 'during-outage-2'),
    (103, 'during-outage-3'),
    (104, 'during-outage-4'),
    (105, 'during-outage-5')
ON CONFLICT (id) DO UPDATE SET body = EXCLUDED.body, updated_at = now();
UPDATE public.items SET body = 'updated-during-outage', updated_at = now() WHERE id = 1;
SQL
}

get_snapshot_id() {
    tbl_resp=$(curl -sS -w '\n%{http_code}' "${LAKEKEEPER_URL}/catalog/v1/${WAREHOUSE_PREFIX}/namespaces/public/tables/items" 2>/dev/null || true)
    tbl_code=$(printf '%s\n' "$tbl_resp" | tail -n 1 | tr -d '\r')
    tbl_body=$(printf '%s\n' "$tbl_resp" | sed '$d')
    if [ "$tbl_code" = "200" ]; then
        printf '%s\n' "$tbl_body" | extract_current_snapshot_id
    fi
}

wait_for_initial_snapshot() {
    watch_svc="$1"
    log "Waiting until a snapshot exists in Lakekeeper for public.items..."
    elapsed=0
    INITIAL_SNAPSHOT_ID=""
    while [ "$elapsed" -lt 90 ]; do
        snap=$(get_snapshot_id || true)
        if [ -n "$snap" ]; then
            INITIAL_SNAPSHOT_ID="$snap"
            log "Initial snapshot observed in Lakekeeper: current-snapshot-id=${INITIAL_SNAPSHOT_ID}"
            return 0
        fi
        cid=$(get_container_id "$watch_svc")
        if [ -n "$cid" ]; then
            st=$(inspect_status "$cid")
            if [ "$st" != "running" ]; then
                log "ERROR: ${watch_svc} exited before initial snapshot (status=${st})"
                print_service_state "$watch_svc"
                exit 2
            fi
        fi
        sleep 2
        elapsed=$((elapsed + 2))
    done
    log "ERROR: timed out waiting for initial snapshot in Lakekeeper."
    print_service_state "$watch_svc"
    exit 2
}

wait_for_lakekeeper_ready() {
    elapsed=0
    while [ "$elapsed" -lt 30 ]; do
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
    dc --profile single --profile split down -v --remove-orphans
    log "Project '${PROJECT}' removed."
}

run_single() {
    log "Mode: single-process (pg2iceberg run)"
    bootstrap_stack
    create_table_and_seed

    log "Starting pg2iceberg (single-process run mode)..."
    dc --profile single up -d pg2iceberg

    wait_for_initial_snapshot "pg2iceberg"
    print_slots

    log "Stopping Lakekeeper..."
    dc stop lakekeeper

    insert_outage_rows

    log "Waiting ${WAIT_SECONDS} s while Lakekeeper is down..."
    sleep "$WAIT_SECONDS"

    log "=== State while Lakekeeper is down ==="
    cid_p2i=$(get_container_id "pg2iceberg")
    status_down=$(inspect_status "$cid_p2i")
    exit_code_down=$(inspect_exit_code "$cid_p2i")
    print_service_state "pg2iceberg"
    print_slots
    slot_active_down=$(query_slot_active)
    log "Observed while down: status=${status_down} exit_code=${exit_code_down} slot_active=${slot_active_down}"

    log "Starting Lakekeeper..."
    dc start lakekeeper

    log "Waiting ${WAIT_SECONDS} s after starting Lakekeeper..."
    sleep "$WAIT_SECONDS"

    log "=== State after Lakekeeper returned ==="
    status_after=$(inspect_status "$cid_p2i")
    exit_code_after=$(inspect_exit_code "$cid_p2i")
    print_service_state "pg2iceberg"
    print_slots
    slot_active_after=$(query_slot_active)
    snap_after=""
    if wait_for_lakekeeper_ready; then
        snap_after=$(get_snapshot_id || true)
    fi
    log "Observed after return: status=${status_after} exit_code=${exit_code_after} slot_active=${slot_active_after} current-snapshot-id=${snap_after:-none}"

    if [ "$status_down" != "running" ]; then
        printf 'RESULT: pg2iceberg exited with code %s while the catalog was down; slot active=%s\n' \
            "$exit_code_down" "$slot_active_after"
        exit 1
    elif [ "$status_after" != "running" ]; then
        printf 'RESULT: pg2iceberg exited with code %s after the catalog returned; slot active=%s\n' \
            "$exit_code_after" "$slot_active_after"
        exit 1
    else
        printf 'RESULT: pg2iceberg stayed running (exit code 0) while the catalog was down; slot active=%s\n' \
            "$slot_active_after"
        exit 0
    fi
}

run_split() {
    log "Mode: split (pg2iceberg stream-only + pg2iceberg materializer-only)"
    bootstrap_stack
    create_table_and_seed

    log "Starting pg2iceberg-stream (stream-only) first so table registration and initial snapshot complete without racing..."
    dc --profile split up -d pg2iceberg-stream

    wait_for_initial_snapshot "pg2iceberg-stream"

    log "Starting pg2iceberg-materializer (materializer-only --worker-id worker-1)..."
    dc --profile split up -d pg2iceberg-materializer
    sleep 4
    cid_m_init=$(get_container_id "pg2iceberg-materializer")
    if [ "$(inspect_status "$cid_m_init")" != "running" ]; then
        log "ERROR: pg2iceberg-materializer exited right after startup."
        print_service_state "pg2iceberg-materializer"
        exit 2
    fi

    print_slots

    log "Stopping Lakekeeper..."
    dc stop lakekeeper

    insert_outage_rows

    log "Waiting ${WAIT_SECONDS} s while Lakekeeper is down..."
    sleep "$WAIT_SECONDS"

    log "=== Split-mode state while Lakekeeper is down ==="
    cid_s=$(get_container_id "pg2iceberg-stream")
    cid_m=$(get_container_id "pg2iceberg-materializer")
    stream_status_down=$(inspect_status "$cid_s")
    stream_exit_down=$(inspect_exit_code "$cid_s")
    mat_status_down=$(inspect_status "$cid_m")
    mat_exit_down=$(inspect_exit_code "$cid_m")
    print_service_state "pg2iceberg-stream"
    print_service_state "pg2iceberg-materializer"
    print_slots
    slot_active_down=$(query_slot_active)
    log "Observed while down: stream=${stream_status_down}(exit=${stream_exit_down}) materializer=${mat_status_down}(exit=${mat_exit_down}) slot_active=${slot_active_down}"

    log "Starting Lakekeeper..."
    dc start lakekeeper

    log "Waiting ${WAIT_SECONDS} s after starting Lakekeeper..."
    sleep "$WAIT_SECONDS"

    log "=== Split-mode state after Lakekeeper returned ==="
    stream_status_after=$(inspect_status "$cid_s")
    stream_exit_after=$(inspect_exit_code "$cid_s")
    mat_status_after=$(inspect_status "$cid_m")
    mat_exit_after=$(inspect_exit_code "$cid_m")
    print_service_state "pg2iceberg-stream"
    print_service_state "pg2iceberg-materializer"
    print_slots
    slot_active_after=$(query_slot_active)
    snap_after=""
    if wait_for_lakekeeper_ready; then
        snap_after=$(get_snapshot_id || true)
    fi
    snap_advanced="false"
    if [ -n "$snap_after" ] && [ "$snap_after" != "$INITIAL_SNAPSHOT_ID" ]; then
        snap_advanced="true"
    fi
    log "Observed after return: stream=${stream_status_after}(exit=${stream_exit_after}) materializer=${mat_status_after}(exit=${mat_exit_after}) slot_active=${slot_active_after} snapshot_before=${INITIAL_SNAPSHOT_ID} snapshot_after=${snap_after:-none} snapshot_advanced=${snap_advanced}"

    if [ "$stream_status_down" = "running" ] && [ "$mat_status_down" = "running" ] \
        && [ "$stream_status_after" = "running" ] && [ "$mat_status_after" = "running" ]; then
        printf 'RESULT: split mode survived catalog outage (stream=%s exit=%s, materializer=%s exit=%s); slot active=%s; snapshot_advanced=%s\n' \
            "$stream_status_after" "$stream_exit_after" "$mat_status_after" "$mat_exit_after" "$slot_active_after" "$snap_advanced"
        exit 0
    else
        printf 'RESULT: split mode did not survive catalog outage (stream=%s exit=%s, materializer=%s exit=%s); slot active=%s; snapshot_advanced=%s\n' \
            "$stream_status_after" "$stream_exit_after" "$mat_status_after" "$mat_exit_after" "$slot_active_after" "$snap_advanced"
        exit 1
    fi
}

cmd="${1:-single}"
case "$cmd" in
    single)
        run_single
        ;;
    split)
        run_split
        ;;
    down)
        run_down
        ;;
    -h|--help|help)
        cat <<'EOF'
Usage: ./repro.sh [single|split|down]
  single  Run the single-process catalog-outage reproduction (default).
  split   Run the same steps in split mode (stream-only + materializer-only).
  down    Stop and remove only Compose project p2irepro.
EOF
        ;;
    *)
        printf 'Unknown command: %s\nUsage: %s [single|split|down]\n' "$cmd" "$0" >&2
        exit 2
        ;;
esac

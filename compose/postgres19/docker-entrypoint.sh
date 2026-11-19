#!/usr/bin/env bash
set -euo pipefail

PGDATA="${PGDATA:-/var/lib/postgresql/19/data}"
POSTGRES_USER="${POSTGRES_USER:-postgres}"
POSTGRES_PASSWORD="${POSTGRES_PASSWORD:-postgres}"
POSTGRES_DB="${POSTGRES_DB:-$POSTGRES_USER}"

mkdir -p "$PGDATA" /var/run/postgresql
chown -R postgres:postgres "$PGDATA" /var/run/postgresql
chmod 700 "$PGDATA"

if [ ! -s "$PGDATA/PG_VERSION" ]; then
    pwfile="$(mktemp)"
    printf '%s' "$POSTGRES_PASSWORD" > "$pwfile"
    chown postgres:postgres "$pwfile"
    chmod 600 "$pwfile"

    gosu postgres initdb \
        -D "$PGDATA" \
        --username="$POSTGRES_USER" \
        --pwfile="$pwfile" \
        --auth-host=scram-sha-256 \
        --auth-local=trust \
        --encoding=UTF8 \
        --locale=en_US.UTF-8

    rm -f "$pwfile"

    {
        echo "host all all 0.0.0.0/0 scram-sha-256"
        echo "host all all ::/0 scram-sha-256"
        echo "host replication all 0.0.0.0/0 scram-sha-256"
        echo "host replication all ::/0 scram-sha-256"
    } >> "$PGDATA/pg_hba.conf"

    echo "listen_addresses = '*'" >> "$PGDATA/postgresql.conf"

    if [ "$POSTGRES_DB" != "postgres" ]; then
        gosu postgres pg_ctl -D "$PGDATA" -o "-c listen_addresses=''" -w start
        gosu postgres psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname postgres \
            -c "CREATE DATABASE \"${POSTGRES_DB}\";"
        gosu postgres pg_ctl -D "$PGDATA" -m fast -w stop
    fi
fi

exec gosu postgres postgres -D "$PGDATA" "$@"

"""Source table DDL and expected Iceberg schema mapping for public.orders."""

from __future__ import annotations

ORDERS_DDL = """
CREATE TABLE IF NOT EXISTS public.orders (
    id bigint PRIMARY KEY,
    status text NOT NULL,
    amount numeric(38, 9) NOT NULL,
    updated_at timestamptz NOT NULL,
    note text,
    payload jsonb,
    discount numeric(38, 9)
);
"""

# Explicit expected mapping from PostgreSQL 19 -> pg2iceberg v0.12.0 -> Iceberg v2.
EXPECTED_ICEBERG_FIELDS: list[dict[str, object]] = [
    {"name": "id", "type": "long", "required": True},
    {"name": "status", "type": "string", "required": True},
    {"name": "amount", "type": "decimal(38, 9)", "required": True},
    {"name": "updated_at", "type": "timestamptz", "required": True},
    {"name": "note", "type": "string", "required": False},
    {"name": "payload", "type": "string", "required": False},
    {"name": "discount", "type": "decimal(38, 9)", "required": False},
]

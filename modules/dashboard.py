from psycopg2.extras import RealDictCursor

try:
    from connections import open_gp_connection
except ImportError:
    from modules.connections import open_gp_connection


SESSION_LIMITS_SQL = """
WITH master_activity AS (
    SELECT
        -1::int AS segment_id,
        'MASTER/COORDINATOR'::text AS node_type,
        NULL::text AS hostname,
        inet_server_addr()::text AS address,
        inet_server_port() AS port,
        count(*) AS total_sessions,
        count(*) FILTER (WHERE state = 'active') AS active_sessions,
        count(*) FILTER (WHERE state = 'idle') AS idle_sessions,
        count(*) FILTER (WHERE state = 'idle in transaction') AS idle_in_transaction_sessions
    FROM pg_stat_activity
),
master_limits AS (
    SELECT
        max(CASE WHEN name = 'max_connections' THEN setting::int END) AS max_connections,
        max(CASE WHEN name = 'superuser_reserved_connections' THEN setting::int END) AS superuser_reserved_connections
    FROM pg_settings
    WHERE name IN ('max_connections', 'superuser_reserved_connections')
),
segment_activity AS (
    SELECT
        gp_execution_dbid() AS dbid,
        count(*) AS total_sessions,
        count(*) FILTER (WHERE state = 'active') AS active_sessions,
        count(*) FILTER (WHERE state = 'idle') AS idle_sessions,
        count(*) FILTER (WHERE state = 'idle in transaction') AS idle_in_transaction_sessions
    FROM gp_dist_random('pg_stat_activity')
    GROUP BY gp_execution_dbid()
),
segment_limits AS (
    SELECT
        gp_execution_dbid() AS dbid,
        max(CASE WHEN name = 'max_connections' THEN setting::int END) AS max_connections,
        max(CASE WHEN name = 'superuser_reserved_connections' THEN setting::int END) AS superuser_reserved_connections
    FROM gp_dist_random('pg_settings')
    WHERE name IN ('max_connections', 'superuser_reserved_connections')
    GROUP BY gp_execution_dbid()
)
SELECT
    node_type,
    segment_id,
    hostname,
    address,
    port,
    total_sessions,
    active_sessions,
    idle_sessions,
    idle_in_transaction_sessions,
    max_connections,
    superuser_reserved_connections,
    max_connections - total_sessions AS free_connections,
    round(total_sessions * 100.0 / NULLIF(max_connections, 0), 2) AS used_percent
FROM master_activity
CROSS JOIN master_limits

UNION ALL

SELECT
    'SEGMENT' AS node_type,
    g.content AS segment_id,
    g.hostname,
    g.address,
    g.port,
    COALESCE(a.total_sessions, 0) AS total_sessions,
    COALESCE(a.active_sessions, 0) AS active_sessions,
    COALESCE(a.idle_sessions, 0) AS idle_sessions,
    COALESCE(a.idle_in_transaction_sessions, 0) AS idle_in_transaction_sessions,
    l.max_connections,
    l.superuser_reserved_connections,
    l.max_connections - COALESCE(a.total_sessions, 0) AS free_connections,
    round(COALESCE(a.total_sessions, 0) * 100.0 / NULLIF(l.max_connections, 0), 2) AS used_percent
FROM gp_segment_configuration g
LEFT JOIN segment_activity a
    ON a.dbid = g.dbid
LEFT JOIN segment_limits l
    ON l.dbid = g.dbid
WHERE g.role = 'p'
  AND g.content >= 0

ORDER BY node_type, segment_id;
"""


def get_session_limits_stats(connection_id: int):
    conn = open_gp_connection(connection_id)

    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(SESSION_LIMITS_SQL)
            rows = cur.fetchall()
    finally:
        conn.close()

    rows = [dict(row) for row in rows]

    summary = {
        "nodes": len(rows),
        "total_sessions": sum(int(r.get("total_sessions") or 0) for r in rows),
        "active_sessions": sum(int(r.get("active_sessions") or 0) for r in rows),
        "idle_sessions": sum(int(r.get("idle_sessions") or 0) for r in rows),
        "idle_in_transaction_sessions": sum(int(r.get("idle_in_transaction_sessions") or 0) for r in rows),
    }

    return {
        "summary": summary,
        "rows": rows,
    }
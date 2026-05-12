from psycopg2.extras import RealDictCursor

from modules.connections import get_connection_by_id, open_gp_connection


SYSTEM_SCHEMAS = (
    "pg_catalog",
    "information_schema",
    "gp_toolkit",
    "pg_toast",
)


def get_object_tree(connection_id: int):
    cfg = get_connection_by_id(connection_id)

    if not cfg:
        raise ValueError("Подключение не найдено")

    sql = """
        SELECT
            n.nspname AS schema_name,
            c.relname AS table_name,
            c.relkind AS relkind
        FROM pg_class c
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE c.relkind IN ('r', 'p')
          AND n.nspname NOT IN ('pg_catalog', 'information_schema', 'gp_toolkit', 'pg_toast')
          AND n.nspname NOT LIKE 'pg_temp_%'
          AND n.nspname NOT LIKE 'pg_toast_temp_%'
          AND NOT EXISTS (
              SELECT 1
              FROM pg_inherits i
              WHERE i.inhrelid = c.oid
          )
        ORDER BY n.nspname, c.relname
    """

    conn = open_gp_connection(connection_id)

    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(sql)
            rows = cur.fetchall()
    finally:
        conn.close()

    schemas_map = {}

    for row in rows:
        schema_name = row["schema_name"]
        table_name = row["table_name"]
        relkind = row["relkind"]

        if schema_name not in schemas_map:
            schemas_map[schema_name] = []

        schemas_map[schema_name].append(
            {
                "table": table_name,
                "relkind": relkind,
                "full_name": f"{schema_name}.{table_name}",
            }
        )

    schemas = []

    for schema_name, tables in schemas_map.items():
        schemas.append(
            {
                "schema": schema_name,
                "tables": tables,
            }
        )

    return {
        "connection_id": connection_id,
        "database": cfg["database_name"],
        "schemas": schemas,
    }

"""Statement validation shared by metrics readers and the dashboard worker."""
import re
import duckdb


def _leading_comments(sql):
    return re.sub(r'\A(?:\s+|--[^\n]*(?:\n|$)|/\*.*?\*/)*', '', sql, flags=re.S)


def validate_read_only_sql(con, sql):
    """Accept one read-only statement; never execute a prefix-approved batch.

    This restricts statement types, not access to files or functions available to
    the connection. The dashboard itself is an authenticated administration tool.
    """
    if not isinstance(sql, str) or not sql.strip():
        raise ValueError("A query is required")
    statements = con.extract_statements(sql)
    if len(statements) != 1:
        raise ValueError("Exactly one read-only statement is required")
    statement = statements[0]
    if statement.type == duckdb.StatementType.SELECT:
        return
    text = _leading_comments(sql).strip().rstrip(';').strip()
    if statement.type == duckdb.StatementType.PRAGMA and re.fullmatch(
        r'PRAGMA\s+show_tables(?:\s*\(\s*\))?', text, re.I
    ):
        return
    if statement.type == duckdb.StatementType.EXPLAIN:
        inner = re.sub(r'^EXPLAIN\s+(?:ANALYZE\s+)?', '', text, flags=re.I)
        if inner != text:
            validate_read_only_sql(con, inner)
            return
    raise ValueError("Only SELECT/WITH, SHOW, DESCRIBE, PRAGMA show_tables, and EXPLAIN of a read-only query are allowed")

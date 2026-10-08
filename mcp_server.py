"""
Public econ-data MCP server for Morning Consult Economic Intelligence.

Separate service from the main Flask site (see
C:\\Users\\kayla\\.claude\\plans\\calm-dancing-newt.md for the full plan) --
shares the site's Postgres DB (API-key validation + query audit logging)
and R2 bucket (reads the "data/" prefix the main site's sync_to_cloud.py
never touches). Never proxies a live query to Morning Consult's internal
API or FRED under a shared credential -- it only serves the pre-synced R2
snapshots exported by export_mcp_datasets.py.

Run:
    python mcp_server.py
Requires in its environment (.env, same convention as the main site's
config.py): DATABASE_URL, R2_ENDPOINT_URL, R2_ACCESS_KEY_ID,
R2_SECRET_ACCESS_KEY, R2_BUCKET_NAME, MCP_PUBLIC_URL.
"""
import os
import re
import tempfile
import time
from collections import defaultdict, deque
from datetime import datetime, timezone

import boto3
import duckdb
import psycopg
from dotenv import load_dotenv
from mcp.server.auth.provider import AccessToken, TokenVerifier
from mcp.server.auth.settings import AuthSettings
from mcp.server.auth.middleware.auth_context import get_access_token
from mcp.server.fastmcp import FastMCP

load_dotenv()

DATABASE_URL = os.environ["DATABASE_URL"]
R2_ENDPOINT_URL = os.environ["R2_ENDPOINT_URL"]
R2_ACCESS_KEY_ID = os.environ["R2_ACCESS_KEY_ID"]
R2_SECRET_ACCESS_KEY = os.environ["R2_SECRET_ACCESS_KEY"]
R2_BUCKET_NAME = os.environ["R2_BUCKET_NAME"]
R2_DATA_PREFIX = "data"
R2_ECON_BIBLE_PREFIX = "econ_bible"
MCP_PUBLIC_URL = os.environ.get("MCP_PUBLIC_URL", "http://localhost:8800")

RATE_LIMIT_PER_HOUR = 200

# psycopg accepts the same postgresql:// URL Flask-SQLAlchemy uses.
_DB_DSN = DATABASE_URL.replace("postgresql+psycopg://", "postgresql://")


def _db():
    # connect_timeout matters a lot here: without it, a DB blip makes every
    # single request hang indefinitely (confirmed by testing against an
    # unreachable host before this was added) rather than failing fast.
    return psycopg.connect(_DB_DSN, autocommit=True, connect_timeout=5)


def _r2_client():
    return boto3.client(
        "s3",
        endpoint_url=R2_ENDPOINT_URL,
        aws_access_key_id=R2_ACCESS_KEY_ID,
        aws_secret_access_key=R2_SECRET_ACCESS_KEY,
    )


# ---------------------------------------------------------------------------
# Auth: bearer token -> the same User.api_token already issued by the main
# site's /connect page. No separate credential system.
# ---------------------------------------------------------------------------

class DbTokenVerifier(TokenVerifier):
    async def verify_token(self, token: str) -> AccessToken | None:
        # Fail closed: a DB outage/blip means no key can be verified, so
        # every request is correctly rejected rather than hanging or
        # crashing with a raw connection error (confirmed by testing
        # against an unreachable host before this guard was added).
        try:
            with _db() as conn:
                row = conn.execute(
                    """
                    SELECT id, email, api_token_expires_at
                    FROM "user"
                    WHERE api_token = %s
                    """,
                    (token,),
                ).fetchone()
        except psycopg.OperationalError as e:
            print(f"verify_token: DB unavailable ({e}) -- rejecting token")
            return None
        if row is None:
            return None
        user_id, email, expires_at = row
        if expires_at is None:
            return None
        if expires_at.tzinfo is None:
            expires_at = expires_at.replace(tzinfo=timezone.utc)
        if datetime.now(timezone.utc) >= expires_at:
            return None
        return AccessToken(
            token=token,
            client_id=str(user_id),
            scopes=[],
            expires_at=int(expires_at.timestamp()),
            subject=email,
        )


# In-memory per-token rate limit -- resets on restart, fine for a single
# process. A leaked/hammered key is the threat model here, not sustained
# distributed abuse; revisit if this ever runs multiple instances.
_recent_calls: dict[str, deque] = defaultdict(deque)


def _check_rate_limit(user_id: str):
    now = time.time()
    window = _recent_calls[user_id]
    while window and now - window[0] > 3600:
        window.popleft()
    if len(window) >= RATE_LIMIT_PER_HOUR:
        raise RuntimeError(f"Rate limit exceeded ({RATE_LIMIT_PER_HOUR} requests/hour). Try again later.")
    window.append(now)


def _current_user():
    """Returns (user_id, email) for the verified caller, or raises if
    somehow called without a verified token (shouldn't happen -- FastMCP's
    auth layer rejects the request before any tool runs).
    """
    access_token = get_access_token()
    if access_token is None:
        raise RuntimeError("No verified API key for this request.")
    return access_token.client_id, access_token.subject


def _log_query(user_id: str, tool_name: str, dataset: str | None, query_text: str | None):
    # Best-effort: a logging failure shouldn't take down an otherwise-
    # successful tool call. Kayla's "see everything" requirement is about
    # not silently dropping real usage, not about making the service as
    # fragile as its own audit trail.
    try:
        with _db() as conn:
            conn.execute(
                """
                INSERT INTO api_query_log (user_id, tool_name, dataset, query_text, ip_address)
                VALUES (%s, %s, %s, %s, %s)
                """,
                (int(user_id), tool_name, dataset, (query_text or "")[:2000], None),
            )
            conn.execute(
                """
                UPDATE "user"
                SET api_token_use_count = COALESCE(api_token_use_count, 0) + 1,
                    api_token_last_used_at = now()
                WHERE id = %s
                """,
                (int(user_id),),
            )
    except psycopg.OperationalError as e:
        print(f"_log_query: DB unavailable ({e}) -- query succeeded but was not logged")


# ---------------------------------------------------------------------------
# Data catalog -- derived live from whatever's under the R2 data/ prefix,
# not a separately-maintained manifest (export_mcp_datasets.py is the only
# thing that decides what's in scope).
# ---------------------------------------------------------------------------

def _list_catalog():
    """Returns {dataset_id: {"theme": ..., "filename": ..., "key": ...}},
    dataset_id = "{theme}/{name-without-extension}".
    """
    client = _r2_client()
    catalog = {}
    paginator = client.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=R2_BUCKET_NAME, Prefix=f"{R2_DATA_PREFIX}/"):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            rel = key[len(R2_DATA_PREFIX) + 1:]
            parts = rel.split("/", 1)
            if len(parts) != 2 or not parts[1].endswith(".csv"):
                continue
            theme, filename = parts
            dataset_id = f"{theme}/{filename[:-4]}"
            catalog[dataset_id] = {"theme": theme, "filename": filename, "key": key}
    return catalog


def _list_methodology_catalog():
    """Returns {doc_id: key}, doc_id = "{category}/{name-without-extension}",
    e.g. "data-series/consumer-health-index-chi". Vetted general-purpose
    methodology only (see export_econ_bible_mcp.py's docstring for what's
    deliberately excluded and why) -- never add a new R2 prefix here
    without the same brand/client-mention check that script describes.
    """
    client = _r2_client()
    catalog = {}
    paginator = client.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=R2_BUCKET_NAME, Prefix=f"{R2_ECON_BIBLE_PREFIX}/"):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            rel = key[len(R2_ECON_BIBLE_PREFIX) + 1:]
            parts = rel.split("/", 1)
            if len(parts) != 2 or not parts[1].endswith(".md"):
                continue
            category, filename = parts
            doc_id = f"{category}/{filename[:-3]}"
            catalog[doc_id] = key
    return catalog


def _download_dataset(key: str) -> str:
    client = _r2_client()
    fd, path = tempfile.mkstemp(suffix=".csv")
    os.close(fd)
    client.download_file(R2_BUCKET_NAME, key, path)
    return path


def _materialized_connection(path: str) -> "duckdb.DuckDBPyConnection":
    """Loads one CSV into a real in-memory TABLE (not a view -- a view would
    keep re-reading the file, which matters for what comes next), then
    disables all file/network access on this connection. Confirmed by
    testing: a plain SELECT can otherwise call DuckDB's own table functions
    (read_csv_auto, read_parquet, the httpfs extension, etc.) to read any
    file the server process can see, e.g. `SELECT * FROM
    read_csv_auto('/etc/passwd')` -- a keyword blocklist alone can't catch
    this since none of those are write/DDL keywords. This is the real
    control; `_validate_select_only` is just a fast first-pass reject.
    """
    con = duckdb.connect(":memory:")
    con.execute(f"CREATE TABLE data AS SELECT * FROM read_csv_auto('{path}')")
    con.execute("SET enable_external_access=false")
    return con


_SELECT_ONLY_RE = re.compile(r"^\s*SELECT\b", re.IGNORECASE)
_FORBIDDEN_RE = re.compile(
    r"\b(INSERT|UPDATE|DELETE|DROP|ALTER|CREATE|ATTACH|COPY|PRAGMA|CALL|EXPORT|IMPORT)\b",
    re.IGNORECASE,
)


def _validate_select_only(sql: str):
    if ";" in sql.strip().rstrip(";"):
        raise ValueError("Only a single statement is allowed (no semicolons except a single trailing one).")
    if not _SELECT_ONLY_RE.match(sql):
        raise ValueError("Only SELECT statements are allowed.")
    if _FORBIDDEN_RE.search(sql):
        raise ValueError("Query contains a disallowed keyword -- read-only SELECT access only.")


# ---------------------------------------------------------------------------
# MCP server
# ---------------------------------------------------------------------------

mcp = FastMCP(
    "mc-econ-data",
    instructions=(
        "Read-only access to Morning Consult Economic Intelligence's underlying "
        "datasets -- the same data behind the dashboards on the client portal -- "
        "plus Morning Consult's own methodology/interpretation guidance for "
        "reading them correctly. For data: list_datasets() first, "
        "describe_dataset() for its columns, then query_dataset() with a SELECT "
        "statement. For interpretation: list_methodology_docs() then "
        "get_methodology(doc). Check the relevant methodology doc before "
        "interpreting a metric you haven't used before -- names are easy to "
        "conflate (e.g. CHI vs. ICS) without it."
    ),
    host="0.0.0.0",
    port=8800,
    token_verifier=DbTokenVerifier(),
    auth=AuthSettings(
        issuer_url=os.environ.get("SITE_BASE_URL", "https://mc-econ-intel.onrender.com"),
        resource_server_url=MCP_PUBLIC_URL,
        required_scopes=None,
    ),
)


@mcp.tool()
def list_datasets() -> str:
    """List every dataset available to query, grouped by theme."""
    user_id, _ = _current_user()
    _check_rate_limit(user_id)
    catalog = _list_catalog()
    _log_query(user_id, "list_datasets", None, None)
    if not catalog:
        return "No datasets exported yet."
    lines = [f"Available datasets ({len(catalog)} total):\n"]
    for dataset_id in sorted(catalog):
        lines.append(f"  {dataset_id}")
    return "\n".join(lines)


@mcp.tool()
def describe_dataset(dataset: str) -> str:
    """Describe one dataset's columns and row count. `dataset` is one of the
    IDs returned by list_datasets(), e.g. "us_ics/ics_daily_5dma_100Kplus".
    """
    user_id, _ = _current_user()
    _check_rate_limit(user_id)
    _log_query(user_id, "describe_dataset", dataset, None)
    catalog = _list_catalog()
    if dataset not in catalog:
        return f"Unknown dataset: {dataset!r}. Call list_datasets() for valid IDs."
    path = _download_dataset(catalog[dataset]["key"])
    try:
        con = _materialized_connection(path)
        columns = con.execute("DESCRIBE data").fetchall()
        row_count = con.execute("SELECT COUNT(*) FROM data").fetchone()[0]
    finally:
        os.remove(path)
    lines = [f"{dataset}: {row_count} rows\n", "Columns:"]
    for col_name, col_type, *_rest in columns:
        lines.append(f"  {col_name:30s} {col_type}")
    return "\n".join(lines)


@mcp.tool()
def query_dataset(dataset: str, sql: str) -> str:
    """Run a read-only SELECT against one dataset. The table is always named
    `data` regardless of the dataset's filename -- e.g.
    `SELECT * FROM data ORDER BY date DESC LIMIT 10`. Only a single SELECT
    statement is allowed; no DDL, no writes, no multi-statement queries.
    """
    user_id, _ = _current_user()
    _check_rate_limit(user_id)
    # Log the attempt FIRST, before validation/execution -- a rejected or
    # malicious attempt (bad SQL, a sandbox-escape try) is arguably more
    # important to have in the audit trail than a successful query, and a
    # query that returns early below must never leave it unlogged.
    _log_query(user_id, "query_dataset", dataset, sql)
    try:
        _validate_select_only(sql)
    except ValueError as e:
        return f"Query rejected: {e}"
    catalog = _list_catalog()
    if dataset not in catalog:
        return f"Unknown dataset: {dataset!r}. Call list_datasets() for valid IDs."
    path = _download_dataset(catalog[dataset]["key"])
    try:
        con = _materialized_connection(path)
        try:
            result = con.execute(sql)
        except duckdb.PermissionException:
            return "Query rejected: file/network access is disabled on this connection -- only the loaded dataset table ('data') can be queried."
        rows = result.fetchmany(1000)
        columns = [d[0] for d in result.description]
    finally:
        os.remove(path)
    if not rows:
        return "(no rows)"
    lines = ["\t".join(columns)]
    for row in rows:
        lines.append("\t".join(str(v) for v in row))
    truncated = " (truncated at 1000 rows)" if len(rows) == 1000 else ""
    return "\n".join(lines) + truncated


# ---------------------------------------------------------------------------
# Methodology / interpretation guidance -- Morning Consult's own Econ Bible,
# the same reference material used internally to interpret this data
# correctly (what CHI actually measures vs. ICS, how PRI's components
# relate, etc.). A separate R2 prefix from the datasets above, populated by
# export_econ_bible_mcp.py -- deliberately a curated subset, not the whole
# Econ Bible (see that script's docstring).
# ---------------------------------------------------------------------------

@mcp.tool()
def list_methodology_docs() -> str:
    """List available methodology/interpretation docs -- Morning Consult's
    own reference notes on what each metric actually measures and how to
    read it correctly. Check this before interpreting any query_dataset
    result; metric names are easy to conflate (e.g. CHI vs. ICS) without it.
    """
    user_id, _ = _current_user()
    _check_rate_limit(user_id)
    catalog = _list_methodology_catalog()
    _log_query(user_id, "list_methodology_docs", None, None)
    if not catalog:
        return "No methodology docs exported yet."
    lines = [f"Available methodology docs ({len(catalog)} total):\n"]
    for doc_id in sorted(catalog):
        lines.append(f"  {doc_id}")
    return "\n".join(lines)


@mcp.tool()
def get_methodology(doc: str) -> str:
    """Get the full text of one methodology doc. `doc` is one of the IDs
    returned by list_methodology_docs(), e.g.
    "data-series/consumer-health-index-chi".
    """
    user_id, _ = _current_user()
    _check_rate_limit(user_id)
    _log_query(user_id, "get_methodology", doc, None)
    catalog = _list_methodology_catalog()
    if doc not in catalog:
        return f"Unknown doc: {doc!r}. Call list_methodology_docs() for valid IDs."
    client = _r2_client()
    obj = client.get_object(Bucket=R2_BUCKET_NAME, Key=catalog[doc])
    return obj["Body"].read().decode("utf-8")


if __name__ == "__main__":
    mcp.run(transport="streamable-http")

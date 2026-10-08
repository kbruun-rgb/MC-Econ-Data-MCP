# MC Econ Data MCP Server

Public, authenticated MCP server for Morning Consult Economic Intelligence
client-portal users -- read-only access to the underlying datasets behind
the site's dashboards. Separate service from the main Flask site
(`mc-economic-intelligence`), sharing its Postgres DB (API key validation +
query audit logging) and R2 bucket (reading a `data/` prefix the main
site's own sync never touches).

Full architecture/plan: see the plan written for this
(`calm-dancing-newt.md`) for the reasoning behind each design choice.

## Setup

```
pip install -r requirements.txt
cp .env.example .env   # fill in real values
python mcp_server.py
```

Runs on port 8800 (Streamable HTTP transport, path `/mcp`).

## How users connect

1. They generate a key at `https://mc-econ-intel.onrender.com/connect`
   (same site, "API key" section).
2. They connect any MCP client to this server's URL, using that key as a
   bearer token.
3. Five tools:
   - Data: `list_datasets()`, `describe_dataset(dataset)`,
     `query_dataset(dataset, sql)` (SELECT-only, against a DuckDB table
     always named `data`).
   - Interpretation: `list_methodology_docs()`, `get_methodology(doc)` --
     Morning Consult's own Econ Bible notes on what each metric actually
     measures (added 2026-10-08, so one MCP connection covers both data
     and how to read it correctly, instead of a separate prompt download).

## Data pipeline

This server never pulls data itself:
- `export_mcp_datasets.py` (Econ Data Library, run as part of the Monday
  tracker pipeline) populates the `data/` R2 prefix. Today that's just US
  ICS; adding another dataset means giving its dashboard's build script a
  raw-CSV-export step and registering it in that script's `DATASETS` dict.
- `export_econ_bible_mcp.py` (same folder) populates the `econ_bible/`
  prefix from `C:\Users\kayla\Desktop\Kayla\Econ Bible\`. **Deliberately a
  curated subset** -- only `data-series/` and `frameworks/`, and even
  within those, two frameworks files are excluded because they're built
  around specific brand case studies (HBO Max, Chick-fil-A, Nordstrom,
  etc. as worked examples), and two more because they're implementation
  notes pointing at a specific client's chart script, not methodology.
  `journal/`, `playbooks/`, `current-state/`, `analyses/`,
  `analysis-skills/`, `sectors/`, and `style-guide/` are never touched at
  all -- working notes, likely client-specific research, or (for
  style-guide) about Kayla's own writing voice, not useful to an external
  user's own analysis. **Adding a new file to data-series/ or frameworks/
  later does not auto-expose it** -- it has to be added to the relevant
  list in `export_econ_bible_mcp.py` after the same brand/client-mention
  check (see that script's docstring for the exact grep sweep used).

## Deploying

Not yet deployed. To stand this up for real:
1. Create a new Render web service pointed at this folder/repo.
2. Set the env vars from `.env.example` in Render's dashboard (same
   `DATABASE_URL`/R2 credentials as the main site -- share, don't
   duplicate -- plus this service's own `MCP_PUBLIC_URL` once Render
   assigns it a domain).
3. Update `MCP_SERVER_URL` in the main site's own env vars to point
   `/connect` at the real URL instead of the `localhost:8800` placeholder.
4. Rotate the FRED/MC API credentials currently hardcoded in
   `C:\Users\kayla\Desktop\Kayla\EconData\econ_data_server.py` if that
   script's exposure is a concern -- this new server never uses them (see
   the plan for why), but they were flagged as a pre-existing loose end
   while researching this.

## OAuth support for Claude Desktop / claude.ai (added 2026-10-08)

`DbTokenVerifier.verify_token` now checks two tables: `user.api_token` (the
manual key, Claude Code) and a new `oauth_token` table, populated by a real
OAuth 2.0 authorization server hand-rolled as plain Flask routes on the
*main site* (`app/oauth_server.py` there) -- RFC 8414 metadata, RFC 7591
Dynamic Client Registration, an `/authorize` consent step, and PKCE-verified
`/token` code exchange + refresh rotation. The issuer stays on the main
site's domain (already baked into this server's advertised
`oauth-protected-resource` metadata); this service only gained the second
read path. Fully additive -- the Claude Code bearer-token path is untouched.
Real end-to-end test (2026-10-08): registered a client, drove the full
authorize->consent->token-exchange flow against production, and confirmed
the resulting access token worked against this live deployed server
(`list_datasets`/`query_dataset`), with a garbage token still rejected.
Test user and all OAuth rows deleted afterward, production back to
baseline (19 users, 0 oauth_client/oauth_token/oauth_authorization_code
rows).

## What's been tested so far (no production DB touched)

- R2 catalog listing, dataset download, DuckDB describe/query -- against
  real exported data (US ICS), read-only.
- SQL validation (`query_dataset` rejects non-SELECT, multi-statement, and
  DDL/DML attempts).
- Full server boot + the FastMCP/AuthSettings/TokenVerifier wiring, via a
  live local request with no token (clean 401) and a garbage token against
  a deliberately-unreachable DB (clean 401, fails in ~5s via
  `connect_timeout` rather than hanging -- found and fixed during this
  testing).
- **Real vulnerability found and fixed during testing**: a plain SELECT can
  call DuckDB's own table functions (`read_csv_auto`, `read_parquet`, etc.)
  to read any file the server process can see -- confirmed with
  `SELECT * FROM read_csv_auto('C:/Windows/win.ini')` actually returning
  file contents. A keyword blocklist doesn't catch this (none of those are
  write/DDL keywords). Fixed by materializing each dataset into a real
  in-memory table and then calling `SET enable_external_access=false` on
  that connection before running the caller's SQL -- confirmed this blocks
  the same exploit attempt while the legitimate query against the loaded
  table still works, even after the source CSV file is deleted (proving
  the table is a real independent copy, not a lazy view).
- **Methodology tools tested (2026-10-08)**: real end-to-end run against
  production confirmed `list_methodology_docs()` returns exactly the 18
  vetted docs, `get_methodology()` returns real file content (confirmed on
  the CHI doc), and asking for an excluded doc (`frameworks/pri-pricing-
  headroom`, one of the brand-case-study files) correctly returns "Unknown
  doc" rather than serving it -- the exclusion list actually works, not
  just in the export script. All 7 calls (including that rejected one)
  showed up in `api_query_log`. Test user and its rows deleted afterward,
  production back to baseline (19 users, max id 72, 0 log rows).
- **Full real end-to-end test completed (2026-10-07)**: one disposable test
  user/token created directly in production Postgres, a real MCP client
  (the official SDK, not a hand-rolled HTTP call) did a full handshake
  against a locally-running instance of this server pointed at the real
  production DB and R2 bucket, and called all three tools for real --
  `list_datasets` (36 datasets), `describe_dataset`, and `query_dataset`
  (a real SELECT returning real recent ICS data). Confirmed in
  `api_query_log`: every call was recorded, including the blocked exploit
  attempt (first pass missed logging rejected queries -- fixed by logging
  the attempt before validation/execution, not after). Confirmed an
  invalid token gets a clean 401 before any tool runs. Test user, its 7
  log rows, and the local real-credential `.env` were all deleted
  afterward; production row counts verified back to the exact pre-test
  baseline (19 users, max id 72, 0 query-log rows).

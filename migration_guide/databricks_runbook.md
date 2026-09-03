# Databricks Runbook

Step-by-step path from an empty Databricks workspace to a green
`dbt build --target databricks` for the `barclays_migration` project.

Audience: a data engineer with workspace admin (or a Unity Catalog metastore admin
standing by for §2) who has cloned this repository.

Contents:

1. [Prerequisites](#1-prerequisites)
2. [Unity Catalog layout](#2-unity-catalog-layout)
3. [Authentication](#3-authentication)
4. [Compute](#4-compute)
5. [Local setup](#5-local-setup)
6. [Profile](#6-profile)
7. [First run](#7-first-run)
8. [Landing the raw data](#8-landing-the-raw-data)
9. [Scheduling in Databricks Workflows](#9-scheduling-in-databricks-workflows)
10. [Troubleshooting](#10-troubleshooting)
11. [Teardown](#11-teardown)

---

## 1. Prerequisites

| Requirement | Minimum | Why |
|---|---|---|
| Databricks workspace | Unity Catalog enabled | Three-level `catalog.schema.table` naming used throughout |
| SQL warehouse or cluster | Serverless SQL warehouse (recommended), or DBR 13.3 LTS+ | `CLUSTER BY` (liquid clustering) needs DBR 13.3+ |
| Python | 3.9 – 3.12 | dbt-databricks support window |
| dbt-core | >= 1.7 | |
| dbt-databricks | >= 1.7 | |
| Permissions | See §2.3 | |

Nothing in this runbook requires a cluster with Python libraries installed; the whole
project runs as SQL against a SQL warehouse.

---

## 2. Unity Catalog layout

### 2.1 Catalog and schemas

The project builds into one catalog with a schema per layer. Schema names come from
`dbt_project.yml` (`+schema:`) combined with the profile's base `schema`.

| Layer | Schema | Contents |
|---|---|---|
| Raw landing | `raw` | Seeded/ingested source tables (`customer`, `account`, `transaction`, `market_data`, `counterparty`) |
| Seeds | `seeds` | Non-source reference seeds (date spine, account type lookups) |
| Staging | `staging` | Views over raw |
| Intermediate | (ephemeral) | Compiled into downstream models; no objects created |
| Marts | `finance`, `risk`, `compliance` | Fact and dimension tables |
| Snapshots | `snapshots` | SCD Type 2 history |

Create them:

```sql
CREATE CATALOG IF NOT EXISTS barclays_migration
  MANAGED LOCATION 'abfss://<container>@<account>.dfs.core.windows.net/barclays_migration';
-- (AWS: 's3://<bucket>/barclays_migration'; omit MANAGED LOCATION to use the metastore root)

USE CATALOG barclays_migration;

CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS seeds;
CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS finance;
CREATE SCHEMA IF NOT EXISTS risk;
CREATE SCHEMA IF NOT EXISTS compliance;
CREATE SCHEMA IF NOT EXISTS snapshots;
```

Note: `dbt_project/macros/generate_schema_name.sql` overrides dbt's default so schema
names are absolute (`finance`, not `<profile_schema>_finance`). The schemas above are
therefore the exact names dbt will use — do not prefix them.

### 2.2 Environment separation

Use one catalog per environment rather than one schema per environment, so the schema
names stay identical across environments:

| Environment | Catalog | Target name |
|---|---|---|
| Development | `barclays_migration_dev` | `databricks` (personal token) |
| CI (if a workspace is available) | `barclays_migration_ci` | `databricks_ci` |
| Production | `barclays_migration` | `databricks_prod` |

Set the catalog through `DATABRICKS_CATALOG`; nothing else changes.

### 2.3 Grants

The identity dbt runs as needs:

```sql
GRANT USE CATALOG                       ON CATALOG barclays_migration TO `<principal>`;
GRANT CREATE SCHEMA                     ON CATALOG barclays_migration TO `<principal>`;

-- Per schema (repeat for raw, seeds, staging, finance, risk, compliance, snapshots):
GRANT USE SCHEMA, CREATE TABLE, CREATE VIEW, CREATE FUNCTION, MODIFY, SELECT
  ON SCHEMA barclays_migration.<schema> TO `<principal>`;
```

For the external landing location used by `COPY INTO` (§8):

```sql
GRANT READ FILES ON EXTERNAL LOCATION <landing_location> TO `<principal>`;
-- or, for a UC volume:
GRANT READ VOLUME ON VOLUME barclays_migration.raw.landing TO `<principal>`;
```

Warehouse access:

```
Workspace > SQL Warehouses > <warehouse> > Permissions > Can use  ->  <principal>
```

Consumers of the marts need only `USE CATALOG` + `USE SCHEMA` + `SELECT` on
`finance`, `risk` and `compliance`.

---

## 3. Authentication

### 3.1 Development: personal access token

```
Workspace > Settings > Developer > Access tokens > Generate new token
```

```bash
export DATABRICKS_HOST='adb-1234567890123456.7.azuredatabricks.net'   # no https://, no trailing slash
export DATABRICKS_HTTP_PATH='/sql/1.0/warehouses/abc123def456'
export DATABRICKS_TOKEN='dapi...'
export DATABRICKS_CATALOG='barclays_migration_dev'
```

### 3.2 Production/CI: service principal (OAuth M2M)

Personal tokens are tied to a leaver-risk identity; scheduled runs should use a service
principal.

1. Create the service principal: `Account console > User management > Service principals`
   (or `Workspace settings > Identity and access`).
2. Grant it workspace access and `Can use` on the SQL warehouse.
3. Apply the UC grants in §2.3 to the service principal.
4. Generate an OAuth secret: `Service principal > Secrets > Generate secret`. Record the
   client ID (the application ID) and the secret — the secret is shown once.
5. Configure dbt:

```yaml
databricks_prod:
  type: databricks
  host: "{{ env_var('DATABRICKS_HOST') }}"
  http_path: "{{ env_var('DATABRICKS_HTTP_PATH') }}"
  auth_type: oauth
  client_id: "{{ env_var('DATABRICKS_CLIENT_ID') }}"
  client_secret: "{{ env_var('DATABRICKS_CLIENT_SECRET') }}"
  catalog: "{{ env_var('DATABRICKS_CATALOG', 'barclays_migration') }}"
  schema: public
  threads: 8
```

### 3.3 Environment variable reference

| Variable | Required | Example | Notes |
|---|---|---|---|
| `DATABRICKS_HOST` | Yes | `adb-1234567890123456.7.azuredatabricks.net` | Hostname only. A leading `https://` or trailing `/` causes a connection error |
| `DATABRICKS_HTTP_PATH` | Yes | `/sql/1.0/warehouses/abc123def456` | From the warehouse's *Connection details* tab. Job clusters use `/sql/protocolv1/o/<org>/<cluster-id>` |
| `DATABRICKS_TOKEN` | PAT auth | `dapi...` | Mutually exclusive with the OAuth pair |
| `DATABRICKS_CLIENT_ID` | OAuth auth | UUID | Service-principal application ID |
| `DATABRICKS_CLIENT_SECRET` | OAuth auth | `dose...` | Store in a secret manager, never in the repo |
| `DATABRICKS_CATALOG` | No (default `barclays_migration`) | `barclays_migration_dev` | Must already exist |
| `DBT_RAW_DATABASE` | No | `barclays_migration` | Used by the source definitions |
| `DBT_RAW_SCHEMA` | No (default `raw`) | `raw` | |

Never commit any of these. Load them from a `.env` that is git-ignored, or from the
platform's secret store.

---

## 4. Compute

### 4.1 SQL warehouse vs job cluster

| | Serverless SQL warehouse | Pro/Classic SQL warehouse | Job cluster |
|---|---|---|---|
| Start latency | ~5 s | 2–5 min | 3–7 min |
| Best for | dbt runs, ad-hoc, CI | dbt runs where serverless is unavailable | Python/Spark tasks, very large rewrites |
| Cost model | Per-second DBU, auto-stop | Per-second DBU + cloud VM | Per-second DBU + cloud VM |
| dbt `http_path` | `/sql/1.0/warehouses/<id>` | `/sql/1.0/warehouses/<id>` | `/sql/protocolv1/o/<org>/<cluster-id>` |
| Recommendation | **Use this** | Fallback | Only if you add Python models |

This project is pure SQL, so a serverless SQL warehouse is the right choice.

### 4.2 Sizing

| Scenario | Size | Threads (`profiles.yml`) | Notes |
|---|---|---|---|
| Local development, seeds only | 2X-Small | 4 | |
| Full `dbt build` on demo data | 2X-Small | 4–8 | Demo volumes are tiny; size up only if the DAG is wide |
| Full historical backfill from Teradata extracts | Small–Medium | 8–16 | Scale-out (multi-cluster) helps a wide DAG more than scale-up |
| Concurrent BI + dbt | Small with max 4 clusters | 8 | Use a separate warehouse for BI so a backfill cannot starve dashboards |

Set auto-stop to 10 minutes for development warehouses and 5 minutes for CI. `threads` in
the profile controls dbt's parallelism, not warehouse size — raising `threads` above the
warehouse's concurrency just queues queries.

---

## 5. Local setup

```bash
git clone https://github.com/COG-GTM/brc-teradata-migration-demo.git
cd brc-teradata-migration-demo

python -m venv .venv && source .venv/bin/activate
pip install --upgrade pip
pip install dbt-core dbt-databricks

cd dbt_project
dbt deps          # installs dbt_utils, codegen, dbt_expectations
dbt --version     # confirm the databricks adapter is listed
```

---

## 6. Profile

Copy the example profile and keep credentials in environment variables:

```bash
mkdir -p ~/.dbt
cp dbt_project/profiles.yml.example ~/.dbt/profiles.yml
```

Verify the connection before building anything:

```bash
cd dbt_project
dbt debug --target databricks
```

`dbt debug` must report `Connection test: [OK connection ok]`. If it does not, stop here
and work through §10 — every later failure mode is harder to read.

---

## 7. First run

Run the steps individually the first time; `dbt build` interleaves them once they are
known-good.

```bash
cd dbt_project

# 1. Parse only — no warehouse needed. Catches Jinja/ref errors fast.
dbt parse

# 2. Load reference and demo source data.
dbt seed --target databricks --full-refresh

# 3. Build models.
dbt run --target databricks

# 4. Build SCD Type 2 history.
dbt snapshot --target databricks

# 5. Tests.
dbt test --target databricks

# 6. Everything, in DAG order, from now on.
dbt build --target databricks
```

Expected end state:

- `barclays_migration.staging` contains a view per source table.
- `barclays_migration.finance|risk|compliance` contain the mart tables.
- `barclays_migration.snapshots` contains `snap_customer_risk_rating`.
- `dbt test` reports zero failures.

Sanity check in SQL:

```sql
SELECT table_schema, table_name, table_type
FROM barclays_migration.information_schema.tables
WHERE table_schema IN ('raw','seeds','staging','finance','risk','compliance','snapshots')
ORDER BY 1, 2;
```

Post-build maintenance for tables that will grow:

```sql
OPTIMIZE barclays_migration.finance.fct_daily_transactions;
ANALYZE TABLE barclays_migration.finance.fct_daily_transactions COMPUTE STATISTICS FOR ALL COLUMNS;
```

---

## 8. Landing the raw data

The seeds under `dbt_project/seeds/raw_sources/` stand in for the Teradata extract so the
project builds with no external dependency. For a real load, replace the seeds with an
ingested `raw` schema:

1. Export from Teradata with TPT Export or FastExport to delimited files
   (UTF-8, `|`-delimited, `NULL` as empty, header row).
2. Upload to a UC volume or external location:

```sql
CREATE VOLUME IF NOT EXISTS barclays_migration.raw.landing;
```

```bash
databricks fs cp ./extract/transaction/ \
  dbfs:/Volumes/barclays_migration/raw/landing/transaction/ --recursive
```

3. Ingest with `COPY INTO` (idempotent — re-running skips already-loaded files):

```sql
COPY INTO barclays_migration.raw.transaction
FROM '/Volumes/barclays_migration/raw/landing/transaction/'
FILEFORMAT = CSV
FORMAT_OPTIONS ('header' = 'true', 'delimiter' = '|', 'nullValue' = '')
COPY_OPTIONS  ('mergeSchema' = 'false');
```

4. Point dbt at the ingested tables rather than the seeds by setting
   `DBT_RAW_SCHEMA=raw` and skipping `dbt seed` for the source tables.

Decimal columns must be declared explicitly in the target table before `COPY INTO`;
letting Databricks infer them produces `DOUBLE` and breaks precision parity
(see the syntax reference §2.2).

---

## 9. Scheduling in Databricks Workflows

A minimal daily job, replacing `teradata/scheduled_jobs/daily_etl_sequence.txt`:

| Task | Type | Command |
|---|---|---|
| `ingest` | SQL / notebook | `COPY INTO` for each raw table |
| `dbt_build` | dbt task | `dbt build --target databricks --exclude tag:monthly` |
| `optimize` | SQL | `OPTIMIZE` + `ANALYZE` on the large marts |

Notes:

- The dbt task type runs against a SQL warehouse and reads the project from Git — no
  cluster libraries required.
- Set the job to run as the service principal from §3.2.
- A failed run can be resumed with **Repair run**, which reruns only failed tasks — the
  equivalent of a BTEQ sequence restart.
- Add `--fail-fast` for the nightly run so a broken upstream model does not burn DBUs
  building everything downstream of it.

---

## 10. Troubleshooting

### Connection and auth

| Error | Cause | Fix |
|---|---|---|
| `Could not resolve host` / connection timeout | `DATABRICKS_HOST` includes `https://` or a trailing `/` | Hostname only |
| `Error during request to server: 403 Invalid access token` | Expired PAT, or the token belongs to another workspace | Regenerate; check the workspace URL matches the host |
| `HTTP Response code: 404, Path: /sql/1.0/warehouses/...` | Wrong `http_path`, or the warehouse was deleted | Copy from the warehouse's *Connection details* tab |
| `Invalid SessionHandle` mid-run | Warehouse auto-stopped, or the token was rotated | Raise auto-stop; retry |
| First query takes minutes then succeeds | Warehouse cold start (non-serverless) | Switch to serverless, or accept the warm-up |
| `PERMISSION_DENIED: User does not have USE SCHEMA` | Missing UC grant | §2.3 |
| OAuth: `invalid_client` | Client secret expired or belongs to a different account | Regenerate the service-principal secret |

### Catalog and schema

| Error | Cause | Fix |
|---|---|---|
| `Catalog 'barclays_migration' does not exist` | Catalog not created, or `DATABRICKS_CATALOG` typo | §2.1 |
| `Schema 'finance' does not exist` and dbt does not create it | The principal lacks `CREATE SCHEMA` | Grant it, or pre-create all schemas |
| Models land in `public_finance` instead of `finance` | The `generate_schema_name` override was bypassed (e.g. a `+schema` set at the profile level) | Keep the macro in `macros/generate_schema_name.sql`; do not override `schema` per-model |
| `TABLE_OR_VIEW_NOT_FOUND` for a source | `DBT_RAW_SCHEMA` points at a schema that was never seeded | Run `dbt seed`, or ingest per §8 |
| `UC_NOT_ENABLED` / two-level naming errors | Workspace has no metastore attached | Attach a UC metastore, or use the Postgres path for local work |

### SQL and model errors

| Error | Cause | Fix |
|---|---|---|
| `PARSE_SYNTAX_ERROR ... near 'QUALIFY'` | Snowflake-only syntax reached Databricks | Use the `qualify_row_number()` macro — syntax reference §3.2 |
| `UNRESOLVED_ROUTINE ... ZEROIFNULL` | Snowflake-only function | `COALESCE(x, 0)` |
| `DELTA_MULTIPLE_SOURCE_ROW_MATCHING_TARGET_ROW_IN_MERGE` | Incremental model's source has duplicate `unique_key` values | Deduplicate in the model before the merge |
| Incremental model silently full-refreshes | `unique_key` not set, or the table was dropped externally | Set `unique_key`; check `DESCRIBE HISTORY` |
| `AnalysisException: cannot resolve '<col>'` after a column rename | Delta schema mismatch on an incremental table | `dbt run --full-refresh --select <model>`, or enable `on_schema_change: append_new_columns` |
| `DELTA_FAILED_TO_MERGE_FIELDS` / decimal scale mismatch | A cast changed scale between runs | Cast explicitly in the model; full-refresh the table |
| Money columns are `NULL` where Teradata had values | Non-ANSI overflow returning NULL | `SET spark.sql.ansi.enabled = true` and re-run to surface the error |
| Dates off by months, or year 1970 | Java vs Teradata format patterns (`MM` vs `mm`, `DD` vs `dd`) | Syntax reference §2.3 |
| `ConcurrentAppendException` | Two jobs writing the same Delta table | Serialise the writes, or partition so they touch disjoint files |
| `CANNOT_UPDATE_PARTITION_COLUMN` | A merge tries to change a partition column | Delete + insert, or drop the partitioning (liquid clustering avoids this) |
| Views over `raw` fail after re-ingest | `COPY INTO` changed the inferred schema | Declare the raw table DDL explicitly; do not rely on inference |

### Performance

| Symptom | Likely cause | Fix |
|---|---|---|
| Many tiny files, slow scans | Explicit daily `PARTITIONED BY` on a small table | Drop the partitioning; use `CLUSTER BY` |
| Full-table scans despite a date filter | Filter column beyond the first 32 columns (no file stats) | Reorder columns, or raise `delta.dataSkippingNumIndexedCols` |
| Build time dominated by one model | Serial dependency chain | Raise `threads`, or split the model |
| Costs higher than expected | Warehouse never auto-stops | Set auto-stop; use serverless |

### Getting diagnostics

```bash
dbt --debug run --target databricks --select <model>   # full SQL + adapter logs
cat logs/dbt.log                                        # last run's log
cat target/run/barclays_migration/models/.../<model>.sql   # exact SQL sent to Databricks
```

In the workspace, `SQL Warehouses > <warehouse> > Query history` shows every statement dbt
issued with its profile and error, which is usually faster than reading dbt's stack trace.

---

## 11. Teardown

```sql
DROP CATALOG IF EXISTS barclays_migration_dev CASCADE;
```

Then stop or delete the SQL warehouse and revoke the service-principal secret. Managed
tables are removed with the catalog; files under an external location are not — delete
those separately.

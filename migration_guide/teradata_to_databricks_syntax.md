# Teradata to Databricks Syntax Reference

Reference for converting the Teradata artefacts under `teradata/` into Databricks SQL
(Delta Lake, Unity Catalog) as implemented by the dbt models under `dbt_project/`.
Databricks is the primary target platform for this project; the Snowflake path is
retained for comparison and is covered in the last section.

Contents:

1. [DDL](#1-ddl)
2. [Data types](#2-data-types)
3. [Functions and expressions](#3-functions-and-expressions)
4. [DML, MERGE and transactions](#4-dml-merge-and-transactions)
5. [Session, locking and statistics](#5-session-locking-and-statistics)
6. [Load and export utilities](#6-load-and-export-utilities)
7. [Scheduling and orchestration](#7-scheduling-and-orchestration)
8. [Snowflake vs Databricks review checklist](#8-snowflake-vs-databricks-review-checklist)

---

## 1. DDL

### 1.1 SET vs MULTISET

Teradata `SET` tables reject duplicate rows at insert time; `MULTISET` tables allow them.
Delta has no row-uniqueness enforcement, so `SET` semantics must be re-implemented in the
transformation.

```sql
-- Teradata
CREATE SET TABLE BARCLAYS_DWH.DIM_CUSTOMER (
    customer_id   VARCHAR(10) NOT NULL,
    customer_name VARCHAR(100)
)
PRIMARY INDEX (customer_id);
```

```sql
-- Databricks
CREATE TABLE barclays_migration.warehouse.dim_customer (
    customer_id   STRING NOT NULL,
    customer_name STRING
)
USING DELTA
CLUSTER BY (customer_id);
```

Duplicate suppression moves into the model. In dbt this is the
`qualify_row_number()` macro in `dbt_project/macros/teradata_compat/`:

```sql
-- Databricks has no QUALIFY; deduplicate in a sub-query
SELECT * FROM (
    SELECT
        s.*,
        ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY etl_loaded_ts DESC) AS rn
    FROM source s
) WHERE rn = 1
```

| Teradata | Databricks | Notes |
|---|---|---|
| `CREATE SET TABLE` | `CREATE TABLE ... USING DELTA` + dedup in model | No engine-side duplicate rejection |
| `CREATE MULTISET TABLE` | `CREATE TABLE ... USING DELTA` | Direct mapping |
| `CREATE VOLATILE TABLE` | CTE, or `CREATE OR REPLACE TEMP VIEW` | Prefer CTEs / ephemeral dbt models |
| `CREATE GLOBAL TEMPORARY TABLE` | Delta table in a scratch schema | Drop via a cleanup job |
| `CREATE TABLE ... AS ... WITH DATA` | `CREATE TABLE ... AS SELECT` | Direct mapping |
| `CREATE TABLE ... WITH NO DATA` | `CREATE TABLE ... LIKE` or `AS SELECT ... WHERE 1=0` | |
| `REPLACE VIEW` | `CREATE OR REPLACE VIEW` | |
| `CREATE MACRO` | dbt macro, or SQL UDF | See `teradata/macros/` |
| `CREATE PROCEDURE` | Decomposed into dbt models (see playbook §2.3) | No procedural SQL in dbt |

### 1.2 PRIMARY INDEX, UPI/NUPI and secondary indexes

Teradata's primary index drives both row distribution across AMPs and access path.
Databricks has neither AMPs nor indexes: distribution is handled by the file layout and
the query engine, so the PI becomes a file-clustering hint.

| Teradata | Databricks | Notes |
|---|---|---|
| `PRIMARY INDEX (col)` (NUPI) | `CLUSTER BY (col)` (liquid clustering) | Preferred on DBR 13.3+/UC managed tables |
| `PRIMARY INDEX (col)` on older runtimes | `OPTIMIZE t ZORDER BY (col)` | ZORDER is a maintenance operation, not DDL |
| `UNIQUE PRIMARY INDEX (col)` | `CLUSTER BY (col)` + a dbt `unique` test | Uniqueness is not enforced |
| `PRIMARY KEY` / `FOREIGN KEY` | Informational constraints only (UC) | Add `NOT ENFORCED`; test in dbt |
| `UNIQUE SECONDARY INDEX` | dbt `unique` test | No secondary indexes |
| `NON-UNIQUE SECONDARY INDEX` | Clustering column or Z-order column | Choose the highest-selectivity filter column |
| `JOIN INDEX` | Materialized view, or a pre-joined mart model | |
| `HASH INDEX` | None | Rely on file skipping |

Rules of thumb when converting a PI:

- A PI used mainly for **join co-location** becomes a clustering column on both sides.
- A PI used mainly for **point lookup** becomes a Z-order / clustering column.
- A PI on a **high-cardinality surrogate key** is usually not worth clustering on unless
  the table is queried by that key; prefer the date column used for pruning.
- Do not cluster on more than four columns; liquid clustering degrades beyond that.

### 1.3 Partitioned primary index (PPI)

```sql
-- Teradata
CREATE MULTISET TABLE BARCLAYS_DWH.FCT_TRANSACTION (
    transaction_id   VARCHAR(10),
    transaction_date DATE,
    amount           DECIMAL(18,2)
)
PRIMARY INDEX (transaction_id)
PARTITION BY RANGE_N(transaction_date
    BETWEEN DATE '2020-01-01' AND DATE '2030-12-31' EACH INTERVAL '1' DAY);
```

```sql
-- Databricks: daily partitions are almost always too fine-grained.
CREATE TABLE barclays_migration.finance.fct_daily_transactions (
    transaction_id   STRING,
    transaction_date DATE,
    amount           DECIMAL(18,2)
)
USING DELTA
PARTITIONED BY (transaction_month)   -- derived column, e.g. DATE_TRUNC('month', transaction_date)
CLUSTER BY (transaction_date, transaction_id);
```

| Teradata PPI form | Databricks equivalent |
|---|---|
| `RANGE_N(date BETWEEN ... EACH INTERVAL '1' DAY)` | `PARTITIONED BY (month_col)` + cluster on the date, or liquid clustering alone |
| `RANGE_N(col BETWEEN 1 AND 100 EACH 10)` | Derived bucket column, partitioned or clustered |
| `CASE_N(cond1, cond2, NO CASE)` | Derived category column used as the partition column |
| Multi-level PPI | One partition column plus clustering columns |
| `PARTITION BY COLUMN` (columnar) | Native: Delta/Parquet is already columnar |

Guidance: aim for partitions of at least ~1 GB. For the volumes in this demo, prefer
**liquid clustering only** (`CLUSTER BY`) and no `PARTITIONED BY` at all; explicit
partitioning on a small table produces many small files and slows scans.

### 1.4 Statistics

| Teradata | Databricks |
|---|---|
| `COLLECT STATISTICS ON t COLUMN (c)` | `ANALYZE TABLE t COMPUTE STATISTICS FOR COLUMNS c` |
| `COLLECT STATISTICS ON t INDEX (c)` | `ANALYZE TABLE t COMPUTE STATISTICS FOR COLUMNS c` |
| `COLLECT STATISTICS ON t` (all) | `ANALYZE TABLE t COMPUTE STATISTICS FOR ALL COLUMNS` |
| `HELP STATISTICS t` | `DESCRIBE EXTENDED t` / `DESCRIBE DETAIL t` |
| Recollect after each load | Delta keeps file-level stats automatically; run `ANALYZE` after large loads for the CBO |
| N/A | `OPTIMIZE t` (compaction), `VACUUM t` (file cleanup) — no Teradata equivalent |

Delta collects min/max/null-count statistics for the first 32 columns of each file at
write time. If a filter column sits beyond position 32, move it earlier in the column
order or raise `delta.dataSkippingNumIndexedCols`.

### 1.5 Other physical-design clauses

| Teradata | Databricks | Notes |
|---|---|---|
| `FALLBACK` | Drop | Cloud storage replication provides durability |
| `NO FALLBACK` | Drop | |
| `JOURNAL` / `NO JOURNAL` | Drop | Delta transaction log; query history via `DESCRIBE HISTORY` |
| `WITH JOURNAL TABLE` | Drop | Time travel: `SELECT ... VERSION AS OF n` / `TIMESTAMP AS OF ts` |
| `COMPRESS ('A','B')` | Drop | Parquet dictionary/RLE encoding is automatic |
| `BLOCKCOMPRESSION` | Drop | Set `delta.parquet.compression.codec` if needed |
| `FREESPACE n PERCENT` | Drop | |
| `DATABLOCKSIZE` | Drop | Tune `delta.targetFileSize` instead |
| `CHECKSUM` | Drop | |
| `DATABASE x` (object container) | Unity Catalog `catalog.schema` | See runbook §2 |
| `CREATE DATABASE ... PERM = n` | `CREATE SCHEMA` (no quota) | Cost control is via cluster policy/budget |

---

## 2. Data types

### 2.1 Mapping table

| Teradata | Databricks | Precision / behaviour caveats |
|---|---|---|
| `BYTEINT` | `TINYINT` | Both signed 8-bit |
| `SMALLINT` | `SMALLINT` | Direct |
| `INTEGER` | `INT` | Direct |
| `BIGINT` | `BIGINT` | Direct |
| `DECIMAL(p,s)` / `NUMERIC(p,s)` p<=38 | `DECIMAL(p,s)` | Direct; but see §2.2 on arithmetic |
| `DECIMAL(p,s)` p>38 | `DECIMAL(38,s)` or `DOUBLE` | Databricks caps precision at 38 |
| `NUMBER` (unconstrained) | `DECIMAL(38,10)` | Pick an explicit scale; do not default to `DOUBLE` for money |
| `FLOAT` / `REAL` / `DOUBLE PRECISION` | `DOUBLE` | Teradata `FLOAT` is 64-bit, so `DOUBLE` (not `FLOAT`) is the match |
| `CHAR(n)` | `STRING` | Teradata pads to `n`; Databricks `CHAR(n)` exists but is stored/compared without reliable padding — prefer `STRING` and `RPAD()` only where the padding is semantically required |
| `VARCHAR(n)` | `STRING` | Length is not enforced; add a dbt `dbt_expectations.expect_column_value_lengths_to_be_between` test if the limit matters downstream |
| `LONG VARCHAR` | `STRING` | |
| `CLOB` | `STRING` | 
| `BYTE(n)` / `VARBYTE(n)` | `BINARY` | |
| `BLOB` | `BINARY` | |
| `DATE` | `DATE` | Teradata stores dates as an integer offset; casting from `INTEGER` requires `TO_DATE(CAST(x AS STRING),'yyyyMMdd')` after converting the Teradata internal form |
| `TIME(n)` | `STRING` (`'HH:mm:ss'`) or seconds-since-midnight `INT` | No native `TIME` type |
| `TIME WITH TIME ZONE` | `STRING` | Offset must be carried explicitly |
| `TIMESTAMP(n)` | `TIMESTAMP` | Databricks stores microseconds (n<=6); Teradata `TIMESTAMP(6)` maps cleanly, `TIMESTAMP(0)` values gain trailing zeros |
| `TIMESTAMP WITH TIME ZONE` | `TIMESTAMP` (UTC) or `TIMESTAMP_NTZ` + offset column | `TIMESTAMP` is instant-based and rendered in the session time zone; set `spark.sql.session.timeZone=UTC` |
| `INTERVAL YEAR TO MONTH` | `INTERVAL YEAR TO MONTH` or `INT` months | Interval columns cannot be persisted in Delta — store the numeric component |
| `INTERVAL DAY TO SECOND` | `INTERVAL DAY TO SECOND` or `BIGINT` seconds | As above |
| `PERIOD(DATE)` | Two `DATE` columns (`valid_from`, `valid_to`) | No native period type; `NORMALIZE`/`P_INTERSECT` must be hand-written |
| `PERIOD(TIMESTAMP)` | Two `TIMESTAMP` columns | |
| `JSON` | `STRING` + `from_json()`, or `STRUCT`/`VARIANT` | `VARIANT` requires DBR 15.3+ |
| `XML` | `STRING` | Parse with `xpath_*` functions |
| `ST_GEOMETRY` | `STRING` (WKT) | Use a geospatial library (e.g. Sedona) if needed |
| `ARRAY` | `ARRAY<T>` | |
| `UDT` | `STRUCT` | |

### 2.2 Decimal and numeric caveats

These are the differences most likely to cause parity failures on financial columns:

1. **Division changes scale.** Teradata `DECIMAL(18,2) / DECIMAL(18,2)` yields
   `DECIMAL(18,2)` (truncating); Spark applies its own scale-promotion rules and returns a
   wider decimal. Cast the result explicitly:
   `CAST(a / NULLIF(b,0) AS DECIMAL(18,2))`.
2. **Overflow behaviour.** Teradata raises a numeric-overflow error; Spark returns `NULL`
   when `spark.sql.ansi.enabled=false` (the default). Enable ANSI mode
   (`SET spark.sql.ansi.enabled = true`) on the migration workspace so overflow surfaces as
   an error rather than silent nulls, or add `not_null` tests on computed money columns.
3. **Rounding.** Teradata `ROUND` uses round-half-up; Spark `ROUND` on `DECIMAL` is
   half-up but on `DOUBLE` uses the IEEE representation. Keep monetary values in `DECIMAL`
   end-to-end; never round in `DOUBLE`.
4. **Integer division.** Teradata `INTEGER / INTEGER` truncates; Spark returns a `DOUBLE`.
   Use `DIV` for integer division: `a DIV b`.
5. **Aggregate promotion.** `SUM(DECIMAL(18,2))` becomes `DECIMAL(38,2)` in Spark, which
   can overflow to `NULL` on very large sums under non-ANSI mode. Test the totals.
6. **Empty string vs NULL.** Teradata treats `''` as a zero-length string, as does
   Databricks; but Teradata `CHAR` comparison ignores trailing blanks and Databricks
   `STRING` does not. `TRIM()` on load if the source is `CHAR`.

### 2.3 Date and time caveats

- Teradata `DATE` arithmetic returns integer days; Databricks `date1 - date2` returns an
  `INTERVAL`. Use `DATEDIFF(date1, date2)` for a day count.
- `CURRENT_DATE`/`CURRENT_TIMESTAMP` are evaluated per-statement in Teradata and per-query
  in Databricks; both are fine, but pin the business date through the
  `get_business_date()` macro instead so runs are reproducible.
- Teradata's default session time zone is the system one; set
  `spark.sql.session.timeZone = 'UTC'` so `TIMESTAMP` rendering matches the Teradata
  extract used for reconciliation.
- Java (`yyyyMMdd`) rather than Teradata (`YYYYMMDD`) format patterns: `MM` is month,
  `mm` is minute, `DD` is day-of-year, `dd` is day-of-month. Getting this wrong produces
  plausible-but-wrong dates rather than an error.

---

## 3. Functions and expressions

### 3.1 Null handling and conditionals

| Teradata | Databricks | Notes |
|---|---|---|
| `ZEROIFNULL(x)` | `COALESCE(x, 0)` | No native function |
| `NULLIFZERO(x)` | `NULLIF(x, 0)` | |
| `COALESCE` / `NVL` | `COALESCE` / `NVL` | Direct |
| `CASE ... END` | `CASE ... END` | Direct |
| `DECODE(a,b,c,d)` | `CASE WHEN a=b THEN c ELSE d END` | Not available in Spark SQL |

### 3.2 Ranking, windowing and Teradata OLAP extensions

| Teradata | Databricks | Notes |
|---|---|---|
| `QUALIFY expr` | Sub-query filtering on the window value | **Not supported** — the single most common conversion error |
| `ROW_NUMBER() OVER (...)` | Same | Direct |
| `RANK()` / `DENSE_RANK()` | Same | Direct |
| `CSUM(c, ord)` | `SUM(c) OVER (ORDER BY ord ROWS UNBOUNDED PRECEDING)` | |
| `MSUM(c, n, ord)` | `SUM(c) OVER (ORDER BY ord ROWS BETWEEN n-1 PRECEDING AND CURRENT ROW)` | |
| `MAVG(c, n, ord)` | `AVG(c) OVER (ORDER BY ord ROWS BETWEEN n-1 PRECEDING AND CURRENT ROW)` | |
| `MDIFF(c, n, ord)` | `c - LAG(c, n) OVER (ORDER BY ord)` | |
| `MLINREG(...)` | Hand-written regression, or `regr_*` aggregates | |
| `QUANTILE(n, c)` | `NTILE(n) OVER (ORDER BY c)` | |
| `PERCENT_RANK()` | `PERCENT_RANK()` | Direct |
| `WIDTH_BUCKET(...)` | `WIDTH_BUCKET(...)` | Direct (DBR 10.4+) |
| `EXPAND ON period_col` | `EXPLODE(SEQUENCE(start, end, INTERVAL 1 DAY))` | |
| `NORMALIZE ON period_col` | Hand-written gaps-and-islands merge | No equivalent |
| `TOP n` | `LIMIT n` | `TOP n WITH TIES` → `RANK()` filter |
| `SAMPLE n` / `SAMPLE 0.1` | `TABLESAMPLE (n ROWS)` / `TABLESAMPLE (10 PERCENT)` | Sampling is non-deterministic on both |

`QUALIFY` conversion pattern:

```sql
-- Teradata
SELECT account_id, balance_date, closing_balance
FROM BARCLAYS_DWH.ACCOUNT_BALANCE
QUALIFY ROW_NUMBER() OVER (PARTITION BY account_id ORDER BY balance_date DESC) = 1;
```

```sql
-- Databricks
SELECT account_id, balance_date, closing_balance
FROM (
    SELECT
        account_id, balance_date, closing_balance,
        ROW_NUMBER() OVER (PARTITION BY account_id ORDER BY balance_date DESC) AS rn
    FROM barclays_migration.warehouse.account_balance
)
WHERE rn = 1;
```

### 3.3 String functions

| Teradata | Databricks | Notes |
|---|---|---|
| `str1 \|\| str2` | `str1 \|\| str2` or `CONCAT(...)` | `\|\|` returns NULL if either side is NULL on both platforms |
| `SUBSTR(s, p, n)` / `SUBSTRING(s FROM p FOR n)` | `SUBSTR(s, p, n)` | Direct |
| `POSITION(x IN s)` / `INDEX(s, x)` | `INSTR(s, x)` / `POSITION(x IN s)` | |
| `OREPLACE(s, from, to)` | `REPLACE(s, from, to)` | |
| `OTRANSLATE(s, from, to)` | `TRANSLATE(s, from, to)` | |
| `TRIM(BOTH ' ' FROM s)` | `TRIM(s)` | |
| `LPAD` / `RPAD` | `LPAD` / `RPAD` | Direct |
| `UPPER` / `LOWER` | `UPPER` / `LOWER` | Direct |
| `CHARACTER_LENGTH(s)` / `CHARACTERS(s)` | `LENGTH(s)` | `CHAR` trailing-blank difference applies |
| `SOUNDEX(s)` | `SOUNDEX(s)` | Direct |
| `EDITDISTANCE(a,b)` | `LEVENSHTEIN(a,b)` | |
| `REGEXP_SUBSTR(s, p)` | `REGEXP_EXTRACT(s, p, 0)` | Java regex, not POSIX |
| `REGEXP_REPLACE(s, p, r)` | `REGEXP_REPLACE(s, p, r)` | Java regex syntax; `\d` must be escaped in dbt Jinja |
| `REGEXP_SIMILAR(s, p)` | `s RLIKE p` | Returns boolean, not 1/0 |
| `LIKE ANY ('a%','b%')` | `s LIKE 'a%' OR s LIKE 'b%'` | Not supported |
| `LIKE ALL ('a%','%b')` | `s LIKE 'a%' AND s LIKE '%b'` | Not supported |
| `STRTOK(s, d, n)` | `SPLIT(s, d)[n-1]` | Zero-based index in Spark |
| `TO_CHAR(n, fmt)` | `FORMAT_NUMBER(n, d)` / `DATE_FORMAT(d, fmt)` | Format strings differ |

### 3.4 Date and time functions

| Teradata | Databricks | Notes |
|---|---|---|
| `CURRENT_DATE` | `CURRENT_DATE()` | |
| `CURRENT_TIMESTAMP` | `CURRENT_TIMESTAMP()` | |
| `DATE` (keyword literal) | `CURRENT_DATE()` | |
| `date1 - date2` | `DATEDIFF(date1, date2)` | Argument order is (end, start) — the opposite of Snowflake's `DATEDIFF('day', start, end)` |
| `date + n` | `DATE_ADD(date, n)` | |
| `ADD_MONTHS(d, n)` | `ADD_MONTHS(d, n)` | Direct |
| `EXTRACT(YEAR FROM d)` | `YEAR(d)` / `EXTRACT(YEAR FROM d)` | |
| `TD_MONTH_BEGIN(d)` | `DATE_TRUNC('MONTH', d)` | Returns a timestamp — cast to `DATE` |
| `TD_MONTH_END(d)` | `LAST_DAY(d)` | |
| `TD_DAY_OF_WEEK(d)` | `DAYOFWEEK(d)` | Both are 1=Sunday |
| `TD_QUARTER_BEGIN(d)` | `DATE_TRUNC('QUARTER', d)` | |
| `LAST_DAY(d)` | `LAST_DAY(d)` | Direct |
| `CAST(x AS DATE FORMAT 'YYYYMMDD')` | `TO_DATE(CAST(x AS STRING), 'yyyyMMdd')` | See `cast_teradata_date()` macro |
| `CAST(ts AS DATE)` | `CAST(ts AS DATE)` / `TO_DATE(ts)` | |
| `TO_DATE(s, fmt)` | `TO_DATE(s, java_fmt)` | Format pattern differs |
| `INTERVAL '1' DAY` | `INTERVAL 1 DAY` | No quotes in Spark |

### 3.5 Hashing and distribution functions

| Teradata | Databricks | Notes |
|---|---|---|
| `HASHROW(cols)` | `HASH(cols)` (32-bit) or `XXHASH64(cols)` | Values differ between platforms — never compare hashes across engines; hash the *concatenated business columns* with `MD5`/`SHA2` when you need a cross-platform checksum |
| `HASHBUCKET(HASHROW(x))` | `ABS(HASH(x)) % n` | See `hash_to_int_bucket()` macro |
| `HASHAMP()` / `HASHBAKAMP()` | None | AMP concepts do not exist |
| `MD5(x)` (via UDF) | `MD5(x)` | Use for cross-platform checksums |
| N/A | `SHA2(x, 256)` | Preferred for reconciliation checksums |

### 3.6 Aggregates and set operations

| Teradata | Databricks | Notes |
|---|---|---|
| `GROUP BY ... WITH ROLLUP` | `GROUP BY ... WITH ROLLUP` | Direct |
| `GROUPING SETS` | `GROUPING SETS` | Direct |
| `UNION` / `UNION ALL` | Same | Direct |
| `INTERSECT` / `MINUS` | `INTERSECT` / `EXCEPT` | `MINUS` is not a Spark keyword |
| `COUNT(DISTINCT x)` | `COUNT(DISTINCT x)` | Direct; `APPROX_COUNT_DISTINCT` for speed |
| Correlated scalar sub-query in `SELECT` | Supported with restrictions | Rewrite as a `LEFT JOIN` if Spark rejects it |
| Recursive `WITH RECURSIVE` | Not supported | Unroll to a fixed number of joins, or use a Python task |

---

## 4. DML, MERGE and transactions

| Teradata | Databricks | Notes |
|---|---|---|
| `INSERT INTO ... SELECT` | Same | Direct |
| `UPDATE t FROM s WHERE ...` | `MERGE INTO t USING s ON ... WHEN MATCHED THEN UPDATE` | Teradata's `UPDATE ... FROM` has no direct form |
| `DELETE FROM t WHERE ...` | Same | Delta rewrites affected files |
| `MERGE INTO` | `MERGE INTO` | Databricks requires the `ON` condition to be deterministic; multiple source matches raise an error unless deduplicated first |
| `UPSERT` (MultiLoad) | `MERGE INTO` | See §6 |
| `BEGIN TRANSACTION` / `END TRANSACTION` | None | Each statement is atomic; multi-statement atomicity must be designed away (write to a staging table, then a single `MERGE`) |
| `ROLLBACK` | Restore: `RESTORE TABLE t TO VERSION AS OF n` | |
| `DELETE ALL` | `TRUNCATE TABLE t` | |
| `INSERT ... ;` implicit commit | Auto-commit | |

Delta's `MERGE` fails with `DELTA_MULTIPLE_SOURCE_ROW_MATCHING_TARGET_ROW_IN_MERGE` when
the source has duplicate keys — the same condition Teradata `SET` tables would have
rejected at insert. Deduplicate the source in the model.

---

## 5. Session, locking and statistics

| Teradata | Databricks | Notes |
|---|---|---|
| `LOCK ROW FOR ACCESS` | Drop | Delta gives snapshot isolation for reads |
| `LOCK TABLE t FOR READ/WRITE` | Drop | Optimistic concurrency; conflicting writers retry or fail |
| `SET SESSION DATABASE x` | `USE CATALOG c; USE SCHEMA s;` | dbt sets these from the profile |
| `SET SESSION DATEFORM` | `spark.sql.session.timeZone`, `spark.sql.legacy.timeParserPolicy` | |
| `DIAGNOSTIC HELPSTATS` | `EXPLAIN COST` | |
| `EXPLAIN <query>` | `EXPLAIN [FORMATTED] <query>` | |
| `.SET ERRORLEVEL` (BTEQ) | Job task failure semantics / `--fail-fast` in dbt | |
| `ABORT` / `.QUIT ERRORCODE` | Raise from the orchestrator task | |

Concurrency note: two concurrent writers to the same Delta table can fail with
`ConcurrentAppendException`. Partition or cluster so that concurrent jobs touch disjoint
files, or serialise the writes in the workflow. Teradata's locking made this invisible.

---

## 6. Load and export utilities

The Teradata load scripts in this repo (`teradata/fastload/`, `teradata/multiload/`,
`teradata/tpt/`, `teradata/bteq/`) map onto Databricks ingestion as follows.

| Teradata utility | Purpose | Databricks equivalent |
|---|---|---|
| FastLoad (`*.fl`) | Bulk load into an empty table | `COPY INTO` from a volume/external location, or `CREATE TABLE AS SELECT` over a `read_files()` source |
| MultiLoad (`*.ml`) | Bulk insert/update/delete against a populated table | `MERGE INTO` on a Delta table |
| TPT Load operator | Parallel bulk load | `COPY INTO`, or Auto Loader for continuous arrival |
| TPT Export / FastExport | Extract to flat files | `INSERT OVERWRITE DIRECTORY` / write to a UC volume, or Delta Sharing for consumers on Databricks |
| TPump | Continuous low-volume DML | Auto Loader + `MERGE`, or Structured Streaming with `foreachBatch` |
| BTEQ (`*.bteq`) | Interactive/batch SQL script | Databricks SQL Editor, `databricks sql` CLI, or a SQL task in a Workflow; the transformation content belongs in dbt models |
| `.IMPORT` in BTEQ | Small file load | `COPY INTO` / `read_files()` |
| `.EXPORT REPORT` in BTEQ | Formatted extract | Query + download from SQL warehouse, or write a CSV to a volume |
| `.LOGON` / `.LOGOFF` | Session control | Service-principal OAuth (see runbook §3) |
| Error tables (`_ET`, `_UV`) | Rejected rows | `COPY INTO ... COPY_OPTIONS('badRecordsPath' = ...)`, or Auto Loader `badRecordsPath`; expose rejects as a Delta table |
| Checkpoint / restart | Restartable loads | `COPY INTO` idempotency (already-ingested files are skipped) and Auto Loader checkpoints |

FastLoad → `COPY INTO`:

```
/* Teradata FastLoad */
BEGIN LOADING BARCLAYS_RAW.MARKET_DATA ERRORFILES BARCLAYS_RAW.MD_ET, BARCLAYS_RAW.MD_UV;
DEFINE valuation_date (VARCHAR(10)), instrument_id (VARCHAR(10)), mid_price (VARCHAR(20))
  FILE = market_data.csv;
INSERT INTO BARCLAYS_RAW.MARKET_DATA VALUES (:valuation_date, :instrument_id, :mid_price);
END LOADING;
```

```sql
-- Databricks
COPY INTO barclays_migration.raw.market_data
FROM '/Volumes/barclays_migration/raw/landing/market_data/'
FILEFORMAT = CSV
FORMAT_OPTIONS ('header' = 'true', 'inferSchema' = 'false')
COPY_OPTIONS  ('mergeSchema' = 'false');
```

MultiLoad UPSERT → `MERGE INTO`:

```sql
MERGE INTO barclays_migration.warehouse.account_balance AS t
USING staged_balances AS s
   ON t.account_id = s.account_id AND t.balance_date = s.balance_date
WHEN MATCHED THEN UPDATE SET closing_balance = s.closing_balance
WHEN NOT MATCHED THEN INSERT *;
```

---

## 7. Scheduling and orchestration

| Teradata | Databricks |
|---|---|
| Job sequence files (`teradata/scheduled_jobs/*.txt`) | Databricks Workflow (Job) with task dependencies |
| BTEQ step in a sequence | dbt task in a Workflow, or a SQL task |
| Conditional `.IF ERRORCODE <> 0` | Task `depends_on` with `run_if` conditions |
| Restart from a failed step | "Repair run" — reruns only the failed tasks |
| Operator-managed calendar | Job schedule (cron) or file-arrival trigger |
| Resource partitions / TASM workload management | Cluster policies, serverless SQL warehouse sizing, job queues |

---

## 8. Snowflake vs Databricks review checklist

Use this when reviewing a model that was written for Snowflake and must run on Databricks.
Each row is a concrete thing to grep for.

| # | Feature | Snowflake | Databricks | Grep for |
|---|---|---|---|---|
| 1 | `QUALIFY` | Native | **Not supported** | `qualify` |
| 2 | `LIKE ANY` / `LIKE ALL` | Native | Not supported | `like any`, `like all` |
| 3 | `ZEROIFNULL` / `NULLIFZERO` | Native | Not supported (`COALESCE`/`NULLIF`) | `zeroifnull`, `nullifzero` |
| 4 | `DATEDIFF` argument order | `DATEDIFF('day', start, end)` | `DATEDIFF(end, start)` | `datediff` |
| 5 | `DATEADD` | `DATEADD('day', n, d)` | `DATE_ADD(d, n)` / `DATEADD(day, n, d)` | `dateadd` |
| 6 | `IFF(c, a, b)` | Native | `IF(c, a, b)` | `iff(` |
| 7 | `NVL2`, `DECODE` | Native | Not supported | `nvl2`, `decode(` |
| 8 | Semi-structured | `VARIANT`, `:` path, `FLATTEN` | `STRING` + `from_json`, `EXPLODE`, `VARIANT` on DBR 15.3+ | `variant`, `flatten`, `parse_json` |
| 9 | `TIME` type | Native | Not supported | `as time`, `::time` |
| 10 | Cast operator `::` | Supported | Supported (DBR 12+) but prefer `CAST()` | `::` |
| 11 | Clustering | `CLUSTER BY` (auto-maintained) | `CLUSTER BY` (liquid) or `OPTIMIZE ... ZORDER BY` | `cluster_by`, `zorder` |
| 12 | Partitioning | Automatic micro-partitions | Explicit `PARTITIONED BY` — usually unnecessary | `partition_by` |
| 13 | Transient/temporary tables | `TRANSIENT TABLE` | No equivalent; use a scratch schema | `transient` |
| 14 | Identifier case | Unquoted folds to UPPER | Unquoted folds to lower; UC identifiers are case-insensitive but case-preserving | mixed-case quoted identifiers |
| 15 | Quoting | `"col"` | `` `col` `` or `"col"` (with `spark.sql.ansi` off) | backticks vs double quotes |
| 16 | String concat with NULL | Returns NULL | Returns NULL | `\|\|` |
| 17 | `GENERATOR` / `SEQ4()` | Native | `SEQUENCE()` + `EXPLODE`, or `range()` | `generator`, `seq4` |
| 18 | `OBJECT_CONSTRUCT` | Native | `NAMED_STRUCT` / `TO_JSON` | `object_construct` |
| 19 | `ARRAY_AGG` | `ARRAY_AGG(x) WITHIN GROUP (ORDER BY y)` | `ARRAY_AGG(x)` (no `WITHIN GROUP`); use `SORT_ARRAY` or `COLLECT_LIST` over a window | `within group` |
| 20 | `LISTAGG` | Native | `CONCAT_WS(',', COLLECT_LIST(x))` | `listagg` |
| 21 | `MEDIAN` | Native | `PERCENTILE(x, 0.5)` | `median` |
| 22 | `TRY_CAST` | Native | Native | fine |
| 23 | Warehouse hints (`USE WAREHOUSE`) | Yes | Set via `http_path` on the profile | `use warehouse` |
| 24 | Zero-copy clone | `CREATE TABLE ... CLONE` | `CREATE TABLE ... SHALLOW CLONE` | `clone` |
| 25 | Time travel | `AT (TIMESTAMP => ...)` | `TIMESTAMP AS OF ...` / `VERSION AS OF ...` | `at(timestamp`, ` as of ` |
| 26 | Merge with duplicate source keys | Last-writer-wins (non-deterministic) | Raises an error | `merge into` |
| 27 | Case-insensitive string compare | Collation-dependent | Byte comparison; use `LOWER()` or a UC collation | equality on names/codes |
| 28 | Decimal division scale | Snowflake widens scale | Spark widens differently; cast explicitly | `/` on money columns |

In dbt, gate any construct that genuinely cannot be written once with the
`target_platform` var or `target.type`, rather than maintaining two model files:

```sql
{% if target.type == 'databricks' %}
    ... Databricks-specific expression ...
{% else %}
    ... Snowflake/Postgres expression ...
{% endif %}
```

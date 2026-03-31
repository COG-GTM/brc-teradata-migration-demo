# Teradata to Snowflake Syntax Mapping

## Table DDL

| Teradata | Snowflake | Notes |
|---|---|---|
| `CREATE SET TABLE` | `CREATE TABLE` | Snowflake tables are inherently MULTISET; use `QUALIFY` or `DISTINCT` for dedup |
| `CREATE MULTISET TABLE` | `CREATE TABLE` | Direct mapping |
| `PRIMARY INDEX (col)` | `CLUSTER BY (col)` | Optional; Snowflake auto-clusters small tables |
| `PARTITION BY RANGE_N(col BETWEEN ... AND ... EACH INTERVAL '1' MONTH)` | Automatic micro-partitioning | No manual partitioning needed |
| `FALLBACK` | N/A | Snowflake has built-in replication |
| `NO FALLBACK` | N/A | Default |
| `JOURNAL` | Time Travel (1-90 days) | `DATA_RETENTION_TIME_IN_DAYS = 90` |
| `COMPRESS ('val1', 'val2')` | Automatic | Snowflake compresses all data automatically |
| `CHARACTER SET LATIN` | `UTF-8` | Snowflake uses UTF-8 natively |
| `NOT CASESPECIFIC` | Default behaviour | Snowflake strings are case-insensitive by default |
| `FORMAT 'YYYY-MM-DD'` | N/A | Use `TO_CHAR()` / `TO_DATE()` for formatting |
| `TITLE 'Column Label'` | `COMMENT` | `COMMENT ON COLUMN table.col IS 'label'` |

## Data Types

| Teradata | Snowflake | Notes |
|---|---|---|
| `BYTEINT` | `TINYINT` or `NUMBER(3,0)` | |
| `SMALLINT` | `SMALLINT` | Direct mapping |
| `INTEGER` | `INTEGER` | Direct mapping |
| `BIGINT` | `BIGINT` | Direct mapping |
| `DECIMAL(p,s)` | `NUMBER(p,s)` | Direct mapping |
| `FLOAT` | `FLOAT` | Direct mapping |
| `CHAR(n)` | `CHAR(n)` | Max 16,777,216 in Snowflake |
| `VARCHAR(n)` | `VARCHAR(n)` | Max 16,777,216 in Snowflake |
| `CLOB` | `VARCHAR(16777216)` | |
| `BYTE(n)` | `BINARY(n)` | |
| `VARBYTE(n)` | `VARBINARY(n)` | |
| `BLOB` | `BINARY` | Max 8MB in Snowflake |
| `DATE` | `DATE` | Direct mapping |
| `TIME` | `TIME` | Direct mapping |
| `TIMESTAMP` | `TIMESTAMP_NTZ` | No timezone |
| `TIMESTAMP WITH TIME ZONE` | `TIMESTAMP_TZ` | With timezone |
| `PERIOD(DATE)` | Two columns: `valid_from DATE, valid_to DATE` | No native PERIOD type |
| `INTERVAL` | `DATEDIFF` / date arithmetic | No native INTERVAL type |
| `JSON` | `VARIANT` | Semi-structured data |

## Functions

| Teradata | Snowflake | Example |
|---|---|---|
| `ZEROIFNULL(x)` | `ZEROIFNULL(x)` or `COALESCE(x, 0)` | Both work in Snowflake |
| `NULLIFZERO(x)` | `NULLIF(x, 0)` | |
| `QUALIFY expr` | `QUALIFY expr` | Native support in Snowflake |
| `SAMPLE n` | `SAMPLE (n ROWS)` or `TABLESAMPLE` | |
| `TOP n` | `LIMIT n` | |
| `CSUM(col, order_col)` | `SUM(col) OVER (ORDER BY order_col ROWS UNBOUNDED PRECEDING)` | |
| `MAVG(col, n, order_col)` | `AVG(col) OVER (ORDER BY order_col ROWS BETWEEN n-1 PRECEDING AND CURRENT ROW)` | |
| `MDIFF(col, n, order_col)` | `col - LAG(col, n) OVER (ORDER BY order_col)` | |
| `MSUM(col, n, order_col)` | `SUM(col) OVER (ORDER BY order_col ROWS BETWEEN n-1 PRECEDING AND CURRENT ROW)` | |
| `MLINREG(col, n, order_col)` | Custom UDF required | Linear regression over window |
| `NORMALIZE ON period_col` | Custom SQL (merge overlapping periods) | No native equivalent |
| `HASHROW(cols)` | `HASH(cols)` | |
| `HASHBUCKET(HASHROW(x))` | `ABS(HASH(x))` | |
| `OREPLACE(str, from, to)` | `REPLACE(str, from, to)` | |
| `OTRANSLATE(str, from, to)` | `TRANSLATE(str, from, to)` | |
| `SOUNDEX(str)` | `SOUNDEX(str)` | Direct mapping |
| `LIKE ANY (list)` | `LIKE ANY (list)` | Native support in Snowflake |
| `LIKE ALL (list)` | `LIKE ALL (list)` | Native support in Snowflake |
| `EXTRACT(YEAR FROM date)` | `EXTRACT(YEAR FROM date)` or `YEAR(date)` | |
| `ADD_MONTHS(date, n)` | `DATEADD('month', n, date)` | |
| `date1 - date2` (integer) | `DATEDIFF('day', date2, date1)` | Returns integer |
| `CURRENT_DATE` | `CURRENT_DATE` | Direct mapping |
| `CURRENT_TIMESTAMP` | `CURRENT_TIMESTAMP` | Direct mapping |
| `CAST(x AS DATE FORMAT 'YYYYMMDD')` | `TO_DATE(x, 'YYYYMMDD')` | |

## Session & Locking

| Teradata | Snowflake | Notes |
|---|---|---|
| `LOCK ROW FOR ACCESS` | N/A | Snowflake uses MVCC (no row locks) |
| `LOCK TABLE x FOR READ` | N/A | Not needed |
| `BEGIN TRANSACTION` | `BEGIN TRANSACTION` | Direct mapping |
| `SET SESSION` | `ALTER SESSION SET` | |
| `COLLECT STATISTICS` | N/A | Snowflake collects stats automatically |
| `EXPLAIN` | `EXPLAIN` | Direct mapping |

## Utilities Mapping

| Teradata Utility | Snowflake Equivalent |
|---|---|
| BTEQ | SnowSQL CLI |
| FastLoad | `COPY INTO` from stage |
| MultiLoad | `MERGE INTO` |
| TPT (Load) | `COPY INTO` with parallel files |
| TPT (Export) | `COPY INTO @stage` |
| FastExport | `COPY INTO @stage` |
| Teradata scheduled jobs | dbt Cloud scheduled jobs / Airflow / GitHub Actions |

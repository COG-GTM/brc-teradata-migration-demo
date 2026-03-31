# Teradata to Databricks Syntax Mapping

## Table DDL

| Teradata | Databricks (Delta Lake) | Notes |
|---|---|---|
| `CREATE SET TABLE` | `CREATE TABLE ... USING DELTA` | Use `MERGE` or `DISTINCT` for dedup |
| `CREATE MULTISET TABLE` | `CREATE TABLE ... USING DELTA` | Direct mapping |
| `PRIMARY INDEX (col)` | `ZORDER BY (col)` | Optimize file layout |
| `PARTITION BY RANGE_N(...)` | `PARTITIONED BY (col)` | Date/region partitioning |
| `FALLBACK` | N/A | Delta replication handles HA |
| `JOURNAL` | Delta transaction log | Time travel via `VERSION AS OF` |
| `COMPRESS (...)` | Automatic (Parquet encoding) | |
| `COLLECT STATISTICS` | `ANALYZE TABLE ... COMPUTE STATISTICS` | |

## Data Types

| Teradata | Databricks | Notes |
|---|---|---|
| `BYTEINT` | `TINYINT` | |
| `SMALLINT` | `SMALLINT` | Direct mapping |
| `INTEGER` | `INT` | |
| `BIGINT` | `BIGINT` | Direct mapping |
| `DECIMAL(p,s)` | `DECIMAL(p,s)` | Direct mapping |
| `FLOAT` | `DOUBLE` | |
| `CHAR(n)` | `CHAR(n)` | Max 255 in Databricks |
| `VARCHAR(n)` | `STRING` | Databricks STRING is unbounded |
| `CLOB` | `STRING` | |
| `BYTE(n)` | `BINARY` | |
| `BLOB` | `BINARY` | |
| `DATE` | `DATE` | Direct mapping |
| `TIME` | `STRING` (formatted) | No native TIME type |
| `TIMESTAMP` | `TIMESTAMP` | Direct mapping |
| `TIMESTAMP WITH TIME ZONE` | `TIMESTAMP` | TZ stored in session config |
| `PERIOD(DATE)` | Two columns: `validFrom DATE, validTo DATE` | No native PERIOD |
| `JSON` | `STRING` or struct types | Use `from_json()` to parse |

## Functions

| Teradata | Databricks (Spark SQL) | Example |
|---|---|---|
| `ZEROIFNULL(x)` | `COALESCE(x, 0)` | |
| `NULLIFZERO(x)` | `NULLIF(x, 0)` | |
| `QUALIFY expr` | Sub-query: `SELECT * FROM (SELECT ..., ROW_NUMBER() OVER (...) AS rn ...) WHERE rn = 1` | No native QUALIFY |
| `SAMPLE n` | `TABLESAMPLE (n ROWS)` | |
| `TOP n` | `LIMIT n` | |
| `CSUM(col, orderCol)` | `SUM(col) OVER (ORDER BY orderCol ROWS UNBOUNDED PRECEDING)` | |
| `MAVG(col, n, orderCol)` | `AVG(col) OVER (ORDER BY orderCol ROWS BETWEEN n-1 PRECEDING AND CURRENT ROW)` | |
| `MDIFF(col, n, orderCol)` | `col - LAG(col, n) OVER (ORDER BY orderCol)` | |
| `MSUM(col, n, orderCol)` | `SUM(col) OVER (ORDER BY orderCol ROWS BETWEEN n-1 PRECEDING AND CURRENT ROW)` | |
| `NORMALIZE ON periodCol` | Custom SQL (merge overlapping periods) | No native equivalent |
| `HASHROW(cols)` | `HASH(cols)` or `MD5(CONCAT(cols))` | |
| `HASHBUCKET(HASHROW(x))` | `ABS(HASH(x))` | |
| `OREPLACE(str, from, to)` | `REPLACE(str, from, to)` | |
| `OTRANSLATE(str, from, to)` | `TRANSLATE(str, from, to)` | |
| `SOUNDEX(str)` | `SOUNDEX(str)` | Direct mapping |
| `LIKE ANY (list)` | Multiple `LIKE` with `OR` | No native `LIKE ANY` |
| `LIKE ALL (list)` | Multiple `LIKE` with `AND` | No native `LIKE ALL` |
| `EXTRACT(YEAR FROM date)` | `YEAR(date)` or `EXTRACT(YEAR FROM date)` | |
| `ADD_MONTHS(date, n)` | `ADD_MONTHS(date, n)` | Direct mapping |
| `date1 - date2` (integer) | `DATEDIFF(date1, date2)` | Note: arg order differs from Snowflake |
| `CURRENT_DATE` | `CURRENT_DATE()` | Parentheses required |
| `CURRENT_TIMESTAMP` | `CURRENT_TIMESTAMP()` | Parentheses required |
| `CAST(x AS DATE FORMAT 'YYYYMMDD')` | `TO_DATE(CAST(x AS STRING), 'yyyyMMdd')` | Java date format patterns |

## Session & Locking

| Teradata | Databricks | Notes |
|---|---|---|
| `LOCK ROW FOR ACCESS` | N/A | Delta uses MVCC (snapshot isolation) |
| `LOCK TABLE x FOR READ` | N/A | Not needed |
| `BEGIN TRANSACTION` | N/A (auto-commit) | Delta handles ACID per operation |
| `COLLECT STATISTICS` | `ANALYZE TABLE t COMPUTE STATISTICS` | For cost-based optimizer |
| `EXPLAIN` | `EXPLAIN` | Direct mapping |

## Utilities Mapping

| Teradata Utility | Databricks Equivalent |
|---|---|
| BTEQ | Databricks SQL Editor / `dbsqlcli` |
| FastLoad | `COPY INTO` / Auto Loader |
| MultiLoad | `MERGE INTO` with Delta |
| TPT (Load) | Auto Loader / `COPY INTO` |
| TPT (Export) | Delta Sharing / `dbfs cp` |
| FastExport | Delta Sharing / external tables |
| Teradata scheduled jobs | Databricks Workflows / Airflow / dbt Cloud |

## Key Differences from Snowflake

| Feature | Snowflake | Databricks |
|---|---|---|
| `QUALIFY` clause | Native support | Not supported (use sub-query) |
| `LIKE ANY` / `LIKE ALL` | Native support | Not supported (use `OR`/`AND`) |
| `ZEROIFNULL()` | Native support | Not supported (use `COALESCE`) |
| Date arithmetic | `DATEDIFF('day', d2, d1)` | `DATEDIFF(d1, d2)` (different arg order) |
| Semi-structured | `VARIANT` type | `STRING` + `from_json()` |
| Time type | `TIME` | No native `TIME` |
| Clustering | `CLUSTER BY` | `ZORDER BY` (with `OPTIMIZE`) |
| Partitioning | Automatic micro-partitioning | Explicit `PARTITIONED BY` |

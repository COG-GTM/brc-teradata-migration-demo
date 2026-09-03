# Databricks ingestion & orchestration

Databricks replacements for the Teradata load utilities and batch schedules under `teradata/`.
The legacy files are kept in the repo unchanged as the migration reference; nothing here modifies
`teradata/` or `dbt_project/`.

Transform logic is **not** duplicated here — the Teradata stored procedures already became dbt
models, so the workflows in `jobs/` invoke dbt for every transform step and this directory only
covers what dbt cannot do: landing raw files, upserting into Delta, exporting extracts, and
orchestrating the sequence.

## Legacy file → Databricks artifact

| Legacy Teradata file | Databricks artifact | Pattern |
| --- | --- | --- |
| `teradata/fastload/market_data_load.fl` | `ingestion/market_data_autoloader.py` | Auto Loader (`cloudFiles`) streaming append into bronze Delta, `availableNow` trigger |
| `teradata/fastload/market_data_load.fl` (SQL-warehouse variant) | `ingestion/market_data_copy_into.sql` | `COPY INTO` for bounded backfills |
| `teradata/tpt/transaction_load.tpt` | `ingestion/transactions_copy_into.sql` | `COPY INTO` with explicit casts; replaces the PRODUCER/LOAD operator pair |
| `teradata/multiload/account_balance_upsert.ml` | `ingestion/account_balance_merge.sql` | `COPY INTO` staging (acquisition phase) + `MERGE INTO` (application phase) |
| Source-system extracts feeding `BARCLAYS_RAW.CUSTOMER` / `ACCOUNT` / `COUNTERPARTY` | `ingestion/reference_data_copy_into.sql` | Truncate + `COPY INTO` full-snapshot loads |
| Load targets in `teradata/ddl` used by the utilities above | `ingestion/bronze_tables_ddl.sql` | Delta tables with liquid clustering, UC volume for the landing zone |
| `teradata/bteq/daily_batch_load.bteq` (pre-flight / `.LABEL NODATA`) | `ingestion/preflight_checks.py` | File-arrival assertions; task failure replaces `.GOTO ERRORHANDLER` |
| `teradata/bteq/daily_batch_load.bteq` (STEP 1–3 stored-procedure calls) | `jobs/daily_etl_job.yml` → `dbt_daily_transform` task | `dbt run` / `dbt test`; dbt's DAG replaces the procedural sequence |
| `teradata/bteq/daily_batch_load.bteq` (post-load validation) | `validation/post_load_validation.sql` | Count + reconciliation assertions via `RAISE_ERROR`, plus `OPTIMIZE` |
| `teradata/bteq/customer_export.bteq`, `teradata/tpt/customer_export.tpt` | `export/customer_export.py` | Single-file pipe-delimited write to a UC volume |
| `teradata/bteq/monthly_regulatory_report.bteq` (STEP 1–2) | `jobs/monthly_regulatory_job.yml` → `dbt_monthly_transform` task | `dbt build --select tag:risk tag:finance` |
| `teradata/bteq/monthly_regulatory_report.bteq` (STEP 3 extracts) | `export/regulatory_export.py` | Capital and P&L submission files |
| `teradata/scheduled_jobs/daily_etl_sequence.txt` | `jobs/daily_etl_job.yml` | Databricks Workflows job (asset bundle) |
| `teradata/scheduled_jobs/monthly_regulatory_sequence.txt` | `jobs/monthly_regulatory_job.yml` | Databricks Workflows job (asset bundle) |
| `BRCL_MONTH_007/008/009` (stats refresh, GL validation, archive) | `validation/monthly_validation_and_archive.sql` | `OPTIMIZE`/`ANALYZE`, assertions, Delta retention instead of a history copy |

### Scheduler job → task mapping

| Legacy scheduler job | Task key |
| --- | --- |
| `BRCL_DAILY_001_PREFLIGHT` | `preflight` |
| `BRCL_DAILY_002_FASTLOAD_MKTDATA` | `load_market_data` |
| `BRCL_DAILY_003_TPT_TRANSACTIONS` | `load_transactions` (+ `load_reference_data`) |
| `BRCL_DAILY_004_MLOAD_BALANCES` | `upsert_account_balances` |
| `BRCL_DAILY_005_BTEQ_MAIN` | `dbt_daily_transform` |
| `BRCL_DAILY_006_EXPORT_CUSTOMERS` | `export_customers` |
| `BRCL_DAILY_007_STATS_COLLECTION` | folded into `post_load_validation` |
| `BRCL_DAILY_008_VALIDATION` | `post_load_validation` |
| `BRCL_DAILY_009_NOTIFICATION` | job-level `email_notifications` |
| `BRCL_MONTH_001_PREFLIGHT` | `month_end_preflight` |
| `BRCL_MONTH_002_REG_CAPITAL`, `003_PNL_ROLLUP`, `004_BTEQ_REGULATORY` | `dbt_monthly_transform` |
| `BRCL_MONTH_005_EXPORT_REGULATORY`, `006_EXPORT_PNL` | `export_regulatory` |
| `BRCL_MONTH_007_STATS_REFRESH`, `008_VALIDATION`, `009_ARCHIVE` | `monthly_validation_and_archive` |
| `BRCL_MONTH_010_NOTIFICATION` | job-level `email_notifications` |

## Concept mapping

| Teradata utility concept | Databricks equivalent |
| --- | --- |
| FastLoad `CHECKPOINT n` / restart | Structured Streaming checkpoint (`checkpoint_path`) |
| ET / UV error tables (`MKTDATA_FL_ET`, `TXN_TPT_ET`, …) | `_rescued_data` column + `PERMISSIVE` parse mode |
| MultiLoad `.LOGTABLE` and load locks | Delta transaction log; readers never blocked |
| `DO INSERT FOR MISSING UPDATE ROWS` | `MERGE INTO … WHEN MATCHED / WHEN NOT MATCHED` |
| TPT operators + `MAXSESSIONS` | Cluster / SQL-warehouse parallelism; no session tuning |
| `SET RECORD VARTEXT '\|'`, `SKIPROWS '1'` | `FORMAT_OPTIONS('sep', 'header')` |
| `${INPUT_DIR}` / `${EXPORT_DIR}` NFS mounts | Unity Catalog volumes (`landing_path`, `export_path`) |
| `.LOGON …/barclays_etl,${ETL_PASSWORD}` | Workspace identity / service principal (`run_as`) |
| BTEQ `.IF ERRORCODE <> 0 THEN .GOTO ERRORHANDLER` | Task failure + `depends_on` skipping |
| BTEQ `.LABEL NODATA` / `.QUIT 0` | Pre-flight task exits 0 with a warning |
| `COLLECT STATISTICS` | `OPTIMIZE` / `ANALYZE` (predictive optimization usually suffices) |
| Archive to a history database | Delta time travel + retention properties |
| Teradata PI / PPI | Delta liquid clustering (`CLUSTER BY`) |
| `CURRENT_TIMESTAMP(6)` | `CURRENT_TIMESTAMP()` |
| `CAST(x AS DATE FORMAT 'YYYY-MM-DD')` | `TO_DATE(x, 'yyyy-MM-dd')` |

## Layout

```
databricks/
  databricks.yml                 # asset bundle root: variables, targets
  ingestion/
    bronze_tables_ddl.sql
    market_data_autoloader.py
    market_data_copy_into.sql
    transactions_copy_into.sql
    reference_data_copy_into.sql
    account_balance_merge.sql
    preflight_checks.py
  export/
    customer_export.py
    regulatory_export.py
  validation/
    post_load_validation.sql
    monthly_validation_and_archive.sql
  jobs/
    daily_etl_job.yml
    monthly_regulatory_job.yml
```

## Usage

```bash
cd databricks
databricks bundle validate -t dev
databricks bundle deploy  -t dev
databricks bundle run brcl_daily_etl -t dev
databricks bundle run brcl_monthly_regulatory -t dev
```

Set `warehouse_id` (the SQL warehouse used by the SQL and dbt tasks) per target, either in
`databricks.yml` or with `--var warehouse_id=<id>`.

Landing layout expected by the jobs (defaults to `/Volumes/barclays/raw/landing`, one
sub-directory per feed, matching the file names in `sample_data/`):

```
landing/market_data/market_data*.csv
landing/transactions/transactions*.csv
landing/customers/customers*.csv
landing/accounts/accounts*.csv
landing/counterparties/counterparties*.csv
landing/balances/daily_balances*.csv
```

`delimiter` defaults to `,` for the `sample_data/` CSVs; set it to `|` when replaying the legacy
VARTEXT `.dat` extracts.

## Assumptions and follow-ups

- **Not executed against a workspace.** No Databricks credentials were available, so these
  artifacts were validated by inspection (Python syntax/lint, YAML parse, and dialect review of
  the SQL). Row-level parity against the Teradata output still needs a workspace run.
- `daily_balances*.csv` has no counterpart in `sample_data/`; the MultiLoad layout in
  `account_balance_upsert.ml` was used as the column contract.
- Bronze keeps `transaction_id` / `account_id` / `counterparty_id` as `STRING` because the sample
  extracts use natural keys (`TXN0001`, `ACC001`) rather than the numeric keys the Teradata casts
  implied. `transaction_time` and `reference_number` are loaded as `NULL` for the same reason.
- The customer export reads `fct_kyc_status` (plus `stg_customers` for `date_of_birth`) because
  the dbt project has no `dim_customer`; `postcode`, `effective_to` and `is_current` are emitted
  as fixed values to preserve downstream field positions. Revisit if the CRM feed contract needs
  real values.
- `1st business day of month` is approximated by the quartz `1W` cron; add a UK bank-holiday check
  to `preflight_checks.py` if holiday-aware scheduling is required.
- Cluster `node_type_id` values are Azure SKUs; change them for AWS/GCP workspaces.
- Both jobs ship with `pause_status: PAUSED` so a deploy cannot start competing with the legacy
  Teradata schedule during parallel run.

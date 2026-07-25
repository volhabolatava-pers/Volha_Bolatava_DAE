
-----TASK1----

CREATE SCHEMA IF NOT EXISTS user_dilab;
CREATE TABLE IF NOT EXISTS user_dilab.DIM_STORES (
  STORE_SURR_ID      BIGINT NOT NULL PRIMARY KEY,
  STORE_SRC_ID       VARCHAR(100) NOT NULL,
  STORE_NAME         VARCHAR(100) NOT NULL,
  STORE_TYPE         VARCHAR(10) NOT NULL,
  HOUSE_NUMBER       INTEGER NOT NULL,
  CITY_NAME          VARCHAR(20) NOT NULL,
  ZIP_CODE           VARCHAR(5) NOT NULL,
  COUNTY_NAME        VARCHAR(20) NOT NULL,
  TA_INSERT_DT       DATE NOT NULL,
  TA_UPDATE_DT       DATE NOT NULL,
  TA_SOURCE_SYSTEM   VARCHAR(50) NOT NULL,
  TA_SOURCE_ENTITY   VARCHAR(50) NOT NULL
);


CREATE TABLE IF NOT EXISTS user_dilab.DIM_VENDORS (
  VENDOR_SURR_ID        BIGINT NOT NULL PRIMARY KEY, 
  VENDOR_SRC_ID         VARCHAR(10) NOT NULL,
  VENDOR_NAME           VARCHAR(100) NOT NULL,
  TA_INSERT_DT          DATE NOT NULL,
  TA_UPDATE_DT          DATE NOT NULL,
  TA_SOURCE_SYSTEM      VARCHAR(50) NOT NULL,
  TA_SOURCE_ENTITY      VARCHAR(50) NOT NULL
);


CREATE TABLE IF NOT EXISTS user_dilab.FCT_SALES_DD (
  EVENT_DT              DATE NOT NULL,
  DATE_ID               BIGINT NOT NULL,
  STORE_SURR_ID         BIGINT NOT NULL,
  VENDOR_SURR_ID        BIGINT NOT NULL,
  ITEM_SURR_ID          BIGINT NOT NULL, 
  PROMO_SURR_ID         BIGINT NOT NULL,
  FCT_BOTTLES_SOLD_QTY  INTEGER,
  FCT_SALE_AMT_USD      DECIMAL(15,3),
  FCT_VOLUME_SOLD_LTR   DECIMAL(12,3),
  FCT_PROFIT_AMT_USD    DECIMAL(15,3),
  TA_INSERT_DT          DATE NOT NULL
);


-- 1. Load store dimension data
COPY user_dilab.DIM_STORES
FROM ''
CREDENTIALS ''
REGION 'eu-central-1'
DELIMITER ',' CSV IGNOREHEADER 1;

-- 2. Load vendor dimension data
COPY user_dilab.DIM_VENDORS
FROM ''
CREDENTIALS ''
REGION 'eu-central-1'
DELIMITER ',' CSV IGNOREHEADER 1;

-- 3. Load fact table data
COPY user_dilab.FCT_SALES_DD
FROM ''
CREDENTIALS ''
REGION 'eu-central-1'
DELIMITER ',' CSV IGNOREHEADER 1;

-- Distribution style, sort key, size, and row count per table
SELECT
    "table",
    diststyle,
    sortkey1,
    size,
    tbl_rows
FROM svv_table_info
WHERE schema = 'user_dilab'
  AND "table" IN ('dim_stores', 'dim_vendors', 'fct_sales_dd');

-- search_path set so pg_table_def resolves objects in user_dilab by default
SET search_path TO user_dilab;

-- Column-level encoding (compression) and sort key flag for each table
SELECT 
    schemaname,
    tablename,
    "column",
    "type",
    encoding,
    sortkey
FROM pg_table_def
WHERE tablename IN ('dim_stores', 'dim_vendors', 'fct_sales_dd')
ORDER BY tablename, "column";

-- 3a. Snapshot copy = "default compression" baseline (as loaded by COPY)
CREATE TABLE user_dilab.fct_sales_dd_defaultcomp AS
SELECT * FROM user_dilab.fct_sales_dd;

-- Check the encoding Redshift actually applied to each column
SELECT
    a.attname AS "column",
    format_type(a.atttypid, a.atttypmod) AS "type",
    format_encoding(a.attencodingtype) AS encoding
FROM pg_attribute a
JOIN pg_class c ON c.oid = a.attrelid
JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE n.nspname = 'user_dilab'
  AND c.relname = 'fct_sales_dd_defaultcomp'
  AND a.attnum > 0
ORDER BY a.attnum;

-- 3b. Uncompressed baseline: every column forced to ENCODE RAW (no compression)
CREATE TABLE user_dilab.fct_sales_dd_withoutcomp (
  EVENT_DT              DATE          ENCODE RAW,
  DATE_ID               BIGINT        ENCODE RAW,
  STORE_SURR_ID         BIGINT        ENCODE RAW,
  VENDOR_SURR_ID        BIGINT        ENCODE RAW,
  ITEM_SURR_ID          BIGINT        ENCODE RAW,
  PROMO_SURR_ID         BIGINT        ENCODE RAW,
  FCT_BOTTLES_SOLD_QTY  INTEGER       ENCODE RAW,
  FCT_SALE_AMT_USD      DECIMAL(15,3) ENCODE RAW,
  FCT_VOLUME_SOLD_LTR   DECIMAL(12,3) ENCODE RAW,
  FCT_PROFIT_AMT_USD    DECIMAL(15,3) ENCODE RAW,
  TA_INSERT_DT          DATE          ENCODE RAW
);

-- Populate the uncompressed table with the exact same data as defaultcomp
INSERT INTO user_dilab.fct_sales_dd_withoutcomp
SELECT * FROM user_dilab.fct_sales_dd_defaultcomp;


-- Verify all columns really show "none" (= RAW) encoding
SELECT "column", encoding
FROM pg_table_def
WHERE tablename = 'fct_sales_dd_withoutcomp'
ORDER BY "column";

-- 3c. Ask Redshift to recommend the best encoding for each column,
-- based on a sample of the (currently uncompressed) data
ANALYZE COMPRESSION user_dilab.fct_sales_dd_withoutcomp;

-- Build a table using the recommended encodings (here, all came back RAW,
-- so the DDL mirrors withoutcomp)
CREATE TABLE user_dilab.fct_sales_dd_analyzedcomp (
  EVENT_DT              DATE          ENCODE RAW,
  DATE_ID               BIGINT        ENCODE RAW,
  STORE_SURR_ID         BIGINT        ENCODE RAW,
  VENDOR_SURR_ID        BIGINT        ENCODE RAW,
  ITEM_SURR_ID          BIGINT        ENCODE RAW,
  PROMO_SURR_ID         BIGINT        ENCODE RAW,
  FCT_BOTTLES_SOLD_QTY  INTEGER       ENCODE RAW,
  FCT_SALE_AMT_USD      DECIMAL(15,3) ENCODE RAW,
  FCT_VOLUME_SOLD_LTR   DECIMAL(12,3) ENCODE RAW,
  FCT_PROFIT_AMT_USD    DECIMAL(15,3) ENCODE RAW,
  TA_INSERT_DT          DATE          ENCODE RAW
);

-- Populate with the same data again for a fair size comparison
INSERT INTO user_dilab.fct_sales_dd_analyzedcomp
SELECT * FROM user_dilab.fct_sales_dd_defaultcomp;


-- Confirm the applied encoding matches the recommendation
SELECT "column", "type", encoding
FROM pg_table_def
WHERE tablename = 'fct_sales_dd_analyzedcomp'
ORDER BY "column";

-- 3d. Compare on-disk size of all three variants (same row count each)
SELECT
    "table",
    size AS size_mb,
    tbl_rows,
    encoded
FROM svv_table_info
WHERE schema = 'user_dilab'
  AND "table" IN ('fct_sales_dd_defaultcomp', 'fct_sales_dd_withoutcomp', 'fct_sales_dd_analyzedcomp')
ORDER BY "table";


-- 4. creating the procedure
CREATE OR REPLACE PROCEDURE user_dilab.sp_load_sales_report()
LANGUAGE plpgsql
AS $$
BEGIN
    
    DROP TABLE IF EXISTS user_dilab.rpt_sales_by_store_vendor;

    CREATE TABLE user_dilab.rpt_sales_by_store_vendor AS
    SELECT
        f.event_dt,
        s.store_name,
        s.city_name,
        s.county_name,
        v.vendor_name,
        SUM(f.fct_bottles_sold_qty)  AS total_bottles_sold,
        SUM(f.fct_sale_amt_usd)      AS total_sale_amt,
        SUM(f.fct_volume_sold_ltr)   AS total_volume_ltr,
        SUM(f.fct_profit_amt_usd)    AS total_profit_amt
    FROM user_dilab.fct_sales_dd f
    JOIN user_dilab.dim_stores  s ON f.store_surr_id  = s.store_surr_id
    JOIN user_dilab.dim_vendors v ON f.vendor_surr_id = v.vendor_surr_id
    GROUP BY
        f.event_dt,
        s.store_name,
        s.city_name,
        s.county_name,
        v.vendor_name;

    RAISE INFO 'Report user_dilab.rpt_sales_by_store_vendor loaded successfully';
END;
$$;

-- Run the procedure once to build the report
CALL user_dilab.sp_load_sales_report();

-- Sanity checks on the output
SELECT COUNT(*) FROM user_dilab.rpt_sales_by_store_vendor;
SELECT * FROM user_dilab.rpt_sales_by_store_vendor LIMIT 20;

-- Disable result caching so repeated CALLs measure real execution
-- time instead of returning a cached result instantly
SET enable_result_cache_for_session TO OFF;

-- Inspect the execution plan of the report's SELECT BEFORE optimization
EXPLAIN
SELECT
    f.event_dt,
    s.store_name,
    s.city_name,
    s.county_name,
    v.vendor_name,
    SUM(f.fct_bottles_sold_qty)  AS total_bottles_sold,
    SUM(f.fct_sale_amt_usd)      AS total_sale_amt,
    SUM(f.fct_volume_sold_ltr)   AS total_volume_ltr,
    SUM(f.fct_profit_amt_usd)    AS total_profit_amt
FROM user_dilab.fct_sales_dd f
JOIN user_dilab.dim_stores  s ON f.store_surr_id  = s.store_surr_id
JOIN user_dilab.dim_vendors v ON f.vendor_surr_id = v.vendor_surr_id
GROUP BY
    f.event_dt, s.store_name, s.city_name, s.county_name, v.vendor_name;

-- Run the procedure two more times: first is a "cold start"
-- (plan compilation + cold cache), later runs are the real baseline
CALL user_dilab.sp_load_sales_report();

CALL user_dilab.sp_load_sales_report();

-- Pull actual execution times from query history, tagging the
-- first run as "cold" and excluding it from the comparison
SELECT
    ROW_NUMBER() OVER (ORDER BY start_time) AS run_number,
    query_id,
    start_time,
    end_time,
    DATEDIFF(millisecond, start_time, end_time) AS duration_ms,
    CASE
        WHEN ROW_NUMBER() OVER (ORDER BY start_time) = 1
        THEN 'cold start — excluded from comparison'
        ELSE 'warm run — baseline'
    END AS note
FROM sys_query_history
WHERE query_text ILIKE 'CREATE TABLE user_dilab.rpt_sales_by_store_vendor%'
  AND status = 'success'
ORDER BY start_time;


-- Document the existing (pre-optimization) distribution style and sort keys
SELECT
    "table",
    diststyle,
    sortkey1,
    size,
    tbl_rows
FROM svv_table_info
WHERE schema = 'user_dilab'
  AND "table" IN ('fct_sales_dd', 'dim_stores', 'dim_vendors')
ORDER BY "table";

SELECT "table", diststyle, sortkey1
FROM svv_table_info
WHERE schema = 'user_dilab'
  AND "table" IN ('fct_sales_dd', 'dim_stores', 'dim_vendors');

-- Small, frequently-joined dimension tables: replicate fully on every node
ALTER TABLE user_dilab.dim_stores  ALTER DISTSTYLE ALL;
ALTER TABLE user_dilab.dim_vendors ALTER DISTSTYLE ALL;


-- Rebuild the fact table with an explicit KEY distribution
-- (DISTKEY cannot be added in place — requires a new table)
DROP TABLE IF EXISTS user_dilab.fct_sales_dd_opt;

CREATE TABLE user_dilab.fct_sales_dd_opt (
  EVENT_DT              DATE NOT NULL,
  DATE_ID               BIGINT NOT NULL,
  STORE_SURR_ID         BIGINT NOT NULL,
  VENDOR_SURR_ID        BIGINT NOT NULL,
  ITEM_SURR_ID          BIGINT NOT NULL,
  PROMO_SURR_ID         BIGINT NOT NULL,
  FCT_BOTTLES_SOLD_QTY  INTEGER,
  FCT_SALE_AMT_USD      DECIMAL(15,3),
  FCT_VOLUME_SOLD_LTR   DECIMAL(12,3),
  FCT_PROFIT_AMT_USD    DECIMAL(15,3),
  TA_INSERT_DT          DATE NOT NULL
)
DISTSTYLE KEY
DISTKEY(store_surr_id)
SORTKEY(event_dt, store_surr_id);

INSERT INTO user_dilab.fct_sales_dd_opt
SELECT * FROM user_dilab.fct_sales_dd;

-- Rebuild dim_stores with explicit ALL distribution + sort key on its PK
DROP TABLE IF EXISTS user_dilab.dim_stores_opt;
CREATE TABLE user_dilab.dim_stores_opt (
  STORE_SURR_ID      BIGINT NOT NULL,
  STORE_SRC_ID       VARCHAR(100) NOT NULL,
  STORE_NAME         VARCHAR(100) NOT NULL,
  STORE_TYPE         VARCHAR(10) NOT NULL,
  HOUSE_NUMBER       INTEGER NOT NULL,
  CITY_NAME          VARCHAR(20) NOT NULL,
  ZIP_CODE           VARCHAR(5) NOT NULL,
  COUNTY_NAME        VARCHAR(20) NOT NULL,
  TA_INSERT_DT       DATE NOT NULL,
  TA_UPDATE_DT       DATE NOT NULL,
  TA_SOURCE_SYSTEM   VARCHAR(50) NOT NULL,
  TA_SOURCE_ENTITY   VARCHAR(50) NOT NULL
)
DISTSTYLE ALL
SORTKEY(store_surr_id);

INSERT INTO user_dilab.dim_stores_opt
SELECT * FROM user_dilab.dim_stores;

-- Rebuild dim_vendors the same way
DROP TABLE IF EXISTS user_dilab.dim_vendors_opt;
CREATE TABLE user_dilab.dim_vendors_opt (
  VENDOR_SURR_ID        BIGINT NOT NULL,
  VENDOR_SRC_ID         VARCHAR(10) NOT NULL,
  VENDOR_NAME           VARCHAR(100) NOT NULL,
  TA_INSERT_DT          DATE NOT NULL,
  TA_UPDATE_DT          DATE NOT NULL,
  TA_SOURCE_SYSTEM      VARCHAR(50) NOT NULL,
  TA_SOURCE_ENTITY      VARCHAR(50) NOT NULL
)
DISTSTYLE ALL
SORTKEY(vendor_surr_id);

INSERT INTO user_dilab.dim_vendors_opt
SELECT * FROM user_dilab.dim_vendors;

-- Refresh planner statistics on the newly built tables so EXPLAIN's
-- cost/row estimates are accurate (fixes stale-stats effect after bulk load)
ANALYZE user_dilab.fct_sales_dd_opt;
ANALYZE user_dilab.dim_stores_opt;
ANALYZE user_dilab.dim_vendors_opt;


--creating procedure
CREATE OR REPLACE PROCEDURE user_dilab.sp_load_sales_report_opt()
LANGUAGE plpgsql
AS $$
BEGIN
    DROP TABLE IF EXISTS user_dilab.rpt_sales_by_store_vendor_opt;

    CREATE TABLE user_dilab.rpt_sales_by_store_vendor_opt AS
    SELECT
        f.event_dt,
        s.store_name,
        s.city_name,
        s.county_name,
        v.vendor_name,
        SUM(f.fct_bottles_sold_qty)  AS total_bottles_sold,
        SUM(f.fct_sale_amt_usd)      AS total_sale_amt,
        SUM(f.fct_volume_sold_ltr)   AS total_volume_ltr,
        SUM(f.fct_profit_amt_usd)    AS total_profit_amt
    FROM user_dilab.fct_sales_dd_opt f
    JOIN user_dilab.dim_stores_opt  s ON f.store_surr_id  = s.store_surr_id
    JOIN user_dilab.dim_vendors_opt v ON f.vendor_surr_id = v.vendor_surr_id
    GROUP BY
        f.event_dt, s.store_name, s.city_name, s.county_name, v.vendor_name;

    RAISE INFO 'Optimized report loaded successfully';
END;
$$;

-- Execution plan AFTER optimization, for direct comparison with the baseline plan

EXPLAIN
SELECT
    f.event_dt, s.store_name, s.city_name, s.county_name, v.vendor_name,
    SUM(f.fct_bottles_sold_qty) AS total_bottles_sold,
    SUM(f.fct_sale_amt_usd) AS total_sale_amt,
    SUM(f.fct_volume_sold_ltr) AS total_volume_ltr,
    SUM(f.fct_profit_amt_usd) AS total_profit_amt
FROM user_dilab.fct_sales_dd_opt f
JOIN user_dilab.dim_stores_opt  s ON f.store_surr_id  = s.store_surr_id
JOIN user_dilab.dim_vendors_opt v ON f.vendor_surr_id = v.vendor_surr_id
GROUP BY f.event_dt, s.store_name, s.city_name, s.county_name, v.vendor_name;

-- Run the optimized procedure 
CALL user_dilab.sp_load_sales_report_opt();

-- Measure execution time the same way as the baseline, for apples-to-apples comparison
SELECT
    ROW_NUMBER() OVER (ORDER BY start_time) AS run_number,
    query_id, start_time,
    DATEDIFF(millisecond, start_time, end_time) AS duration_ms
FROM sys_query_history
WHERE query_text ILIKE 'CREATE TABLE user_dilab.rpt_sales_by_store_vendor_opt%'
  AND status = 'success'
ORDER BY start_time;


-----TASK2----
-- UNLOAD the same data as a SINGLE file (PARALLEL OFF forces one output file)
UNLOAD ('SELECT * FROM user_dilab.fct_sales_dd')
TO ''
CREDENTIALS ''
REGION 'eu-central-1'
DELIMITER '|'
PARALLEL OFF
ALLOWOVERWRITE;

-- UNLOAD the same data with default parallel behavior -> one file per slice
UNLOAD ('SELECT * FROM user_dilab.fct_sales_dd')
TO ''
CREDENTIALS ''
REGION 'eu-central-1'
DELIMITER '|'
ALLOWOVERWRITE;

-- Target tables, structurally identical to the fact table
CREATE TABLE user_dilab.lineorder_1 (LIKE user_dilab.fct_sales_dd);
CREATE TABLE user_dilab.lineorder_2 (LIKE user_dilab.fct_sales_dd);

-- Load from the single-file source -> only 1 slice can read it, rest idle
COPY user_dilab.lineorder_1
FROM ''
CREDENTIALS ''
REGION 'eu-central-1'
DELIMITER '|';

-- Load from the multi-file source -> all slices load concurrently
COPY user_dilab.lineorder_2
FROM ''
CREDENTIALS ''
REGION 'eu-central-1'
DELIMITER '|';


-- Compare COPY durations: single-file load should be noticeably slower
SELECT
    query_id,
    LEFT(query_text, 60) AS query_preview,
    start_time,
    DATEDIFF(millisecond, start_time, end_time) AS duration_ms
FROM sys_query_history
WHERE query_text ILIKE 'COPY user_dilab.lineorder%'
ORDER BY start_time DESC
LIMIT 5;

--TASK3---

-- Create the external schema; also creates the Glue Catalog database
-- if it doesn't already exist 
CREATE EXTERNAL SCHEMA IF NOT EXISTS user_dilab_ext
FROM DATA CATALOG
DATABASE ''
IAM_ROLE ''
CREATE EXTERNAL DATABASE IF NOT EXISTS;

-- Confirm the external schema was registered correctly
SELECT * FROM svv_external_schemas WHERE schemaname = 'user_dilab_ext';

-- Export fact data to S3 in Parquet, partitioned by month:
-- automatically creates one subfolder per event_month value
-- (e.g. event_month=2024-01-01/)
UNLOAD (
    'SELECT
        event_dt,
        DATE_TRUNC(''month'', event_dt)::DATE AS event_month,
        store_surr_id,
        vendor_surr_id,
        item_surr_id,
        promo_surr_id,
        fct_bottles_sold_qty,
        fct_sale_amt_usd,
        fct_volume_sold_ltr,
        fct_profit_amt_usd
    FROM user_dilab.fct_sales_dd'
)
TO ''
CREDENTIALS ''
REGION 'eu-central-1'
PARTITION BY (event_month)
FORMAT AS PARQUET
ALLOWOVERWRITE;

-- Define the external table over that S3 location; event_month is
-- declared separately via PARTITIONED BY, not as a regular column
CREATE EXTERNAL TABLE user_dilab_ext.ext_student_partitioned (
    event_dt              DATE,
    store_surr_id         BIGINT,
    vendor_surr_id         BIGINT,
    item_surr_id           BIGINT,
    promo_surr_id           BIGINT,
    fct_bottles_sold_qty    INTEGER,
    fct_sale_amt_usd        DECIMAL(15,3),
    fct_volume_sold_ltr     DECIMAL(12,3),
    fct_profit_amt_usd      DECIMAL(15,3)
)
PARTITIONED BY (event_month DATE)
STORED AS PARQUET
LOCATION 's3://<YOUR_BUCKET>/dwh_database/spectrum/fct_sales_partitioned/';

-- At this point the table exists but has no known partitions yet
SELECT * FROM user_dilab_ext.ext_student_partitioned LIMIT 10;

-- Register all 24 monthly partitions (Jan 2024 - Dec 2025), mapping
-- each event_month value to its corresponding S3 subfolder
ALTER TABLE user_dilab_ext.ext_student_partitioned
ADD IF NOT EXISTS
PARTITION (event_month='2024-01-01') LOCATION 's3://<YOUR_BUCKET>/dwh_database/spectrum/fct_sales_partitioned/event_month=2024-01-01/'
PARTITION (event_month='2024-02-01') LOCATION 's3://<YOUR_BUCKET>/dwh_database/spectrum/fct_sales_partitioned/event_month=2024-02-01/'
PARTITION (event_month='2024-03-01') LOCATION 's3://<YOUR_BUCKET>/dwh_database/spectrum/fct_sales_partitioned/event_month=2024-03-01/'
PARTITION (event_month='2024-04-01') LOCATION 's3://<YOUR_BUCKET>/dwh_database/spectrum/fct_sales_partitioned/event_month=2024-04-01/'
PARTITION (event_month='2024-05-01') LOCATION 's3://<YOUR_BUCKET>/dwh_database/spectrum/fct_sales_partitioned/event_month=2024-05-01/'
PARTITION (event_month='2024-06-01') LOCATION 's3://<YOUR_BUCKET>/dwh_database/spectrum/fct_sales_partitioned/event_month=2024-06-01/'
PARTITION (event_month='2024-07-01') LOCATION 's3://<YOUR_BUCKET>/dwh_database/spectrum/fct_sales_partitioned/event_month=2024-07-01/'
PARTITION (event_month='2024-08-01') LOCATION 's3://<YOUR_BUCKET>/dwh_database/spectrum/fct_sales_partitioned/event_month=2024-08-01/'
PARTITION (event_month='2024-09-01') LOCATION 's3://<YOUR_BUCKET>/dwh_database/spectrum/fct_sales_partitioned/event_month=2024-09-01/'
PARTITION (event_month='2024-10-01') LOCATION 's3://<YOUR_BUCKET>/dwh_database/spectrum/fct_sales_partitioned/event_month=2024-10-01/'
PARTITION (event_month='2024-11-01') LOCATION 's3://<YOUR_BUCKET>/dwh_database/spectrum/fct_sales_partitioned/event_month=2024-11-01/'
PARTITION (event_month='2024-12-01') LOCATION 's3://<YOUR_BUCKET>/dwh_database/spectrum/fct_sales_partitioned/event_month=2024-12-01/'
PARTITION (event_month='2025-01-01') LOCATION 's3://<YOUR_BUCKET>/dwh_database/spectrum/fct_sales_partitioned/event_month=2025-01-01/'
PARTITION (event_month='2025-02-01') LOCATION 's3://<YOUR_BUCKET>/dwh_database/spectrum/fct_sales_partitioned/event_month=2025-02-01/'
PARTITION (event_month='2025-03-01') LOCATION 's3://<YOUR_BUCKET>/dwh_database/spectrum/fct_sales_partitioned/event_month=2025-03-01/'
PARTITION (event_month='2025-04-01') LOCATION 's3://<YOUR_BUCKET>/dwh_database/spectrum/fct_sales_partitioned/event_month=2025-04-01/'
PARTITION (event_month='2025-05-01') LOCATION 's3://<YOUR_BUCKET>/dwh_database/spectrum/fct_sales_partitioned/event_month=2025-05-01/'
PARTITION (event_month='2025-06-01') LOCATION 's3://<YOUR_BUCKET>/dwh_database/spectrum/fct_sales_partitioned/event_month=2025-06-01/'
PARTITION (event_month='2025-07-01') LOCATION 's3://s3bucketvolha/dwh_database/spectrum/fct_sales_partitioned/event_month=2025-07-01/'
PARTITION (event_month='2025-08-01') LOCATION 's3://<YOUR_BUCKET>/dwh_database/spectrum/fct_sales_partitioned/event_month=2025-08-01/'
PARTITION (event_month='2025-09-01') LOCATION 's3://<YOUR_BUCKET>/dwh_database/spectrum/fct_sales_partitioned/event_month=2025-09-01/'
PARTITION (event_month='2025-10-01') LOCATION 's3://<YOUR_BUCKET>/dwh_database/spectrum/fct_sales_partitioned/event_month=2025-10-01/'
PARTITION (event_month='2025-11-01') LOCATION 's3://<YOUR_BUCKET>/dwh_database/spectrum/fct_sales_partitioned/event_month=2025-11-01/'
PARTITION (event_month='2025-12-01') LOCATION 's3://<YOUR_BUCKET>/dwh_database/spectrum/fct_sales_partitioned/event_month=2025-12-01/';

-- Confirm all 24 partitions were registered correctly
SELECT * FROM svv_external_partitions
WHERE schemaname = 'user_dilab_ext' AND tablename = 'ext_student_partitioned'
ORDER BY values;

SELECT COUNT(*) FROM user_dilab_ext.ext_student_partitioned;

-- Per-month row-count comparison between the original table and the
-- external partitioned table; expects diff = 0 for every month.

WITH original AS (
    SELECT
        DATE_TRUNC('month', event_dt)::DATE AS event_month,
        COUNT(*) AS cnt_original
    FROM user_dilab.fct_sales_dd
    GROUP BY DATE_TRUNC('month', event_dt)::DATE
),
external_data AS (
    SELECT
        event_month,
        COUNT(*) AS cnt_external
    FROM user_dilab_ext.ext_student_partitioned
    GROUP BY event_month
)
SELECT
    COALESCE(o.event_month, e.event_month) AS event_month,
    COALESCE(o.cnt_original, 0) AS cnt_original,
    COALESCE(e.cnt_external, 0) AS cnt_external,
    COALESCE(o.cnt_original, 0) - COALESCE(e.cnt_external, 0) AS diff
FROM original o
FULL OUTER JOIN external_data e ON o.event_month = e.event_month
ORDER BY event_month;

-- WITH a filter on event_month: expect a much smaller estimated row
-- count, since Spectrum should scan only the matching partition
EXPLAIN
SELECT *
FROM user_dilab_ext.ext_student_partitioned
WHERE event_month = '2024-02-01';

-- WITHOUT a filter: expect the full row count (427,182), scanning
-- all 24 partitions, for comparison against the filtered plan above
EXPLAIN
SELECT * FROM user_dilab_ext.ext_student_partitioned;
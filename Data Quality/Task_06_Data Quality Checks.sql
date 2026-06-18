
-----------------------------

-------Smoke tests----------

-----------------------------



--С47
--- SOURCE 1 TABLES ---

-- 1. s1_channels
SELECT 's1_channels' AS table_name, src.src_cnt, trg.trg_cnt, (src.src_cnt - trg.trg_cnt) AS diff,
       CASE WHEN src.src_cnt = trg.trg_cnt THEN 'PASS' ELSE 'FAIL' END AS status
FROM (SELECT COUNT(*) AS src_cnt FROM dblink('dbname=dwh_src_hw_db user=postgres password=postgres host=localhost', 'SELECT channel_id FROM s1.s1_channels') AS rem(id VARCHAR(256))) src,
     (SELECT COUNT(*) AS trg_cnt FROM lnd.lnd_s1_channels) trg

UNION ALL

-- 2. s1_clients
SELECT 's1_clients' AS table_name, src.src_cnt, trg.trg_cnt, (src.src_cnt - trg.trg_cnt) AS diff,
       CASE WHEN src.src_cnt = trg.trg_cnt THEN 'PASS' ELSE 'FAIL' END AS status
FROM (SELECT COUNT(*) AS src_cnt FROM dblink('dbname=dwh_src_hw_db user=postgres password=postgres host=localhost', 'SELECT client_id FROM s1.s1_clients') AS rem(id VARCHAR(256))) src,
     (SELECT COUNT(*) AS trg_cnt FROM lnd.lnd_s1_clients) trg

UNION ALL

-- 3. s1_products
SELECT 's1_products' AS table_name, src.src_cnt, trg.trg_cnt, (src.src_cnt - trg.trg_cnt) AS diff,
       CASE WHEN src.src_cnt = trg.trg_cnt THEN 'PASS' ELSE 'FAIL' END AS status
FROM (SELECT COUNT(*) AS src_cnt FROM dblink('dbname=dwh_src_hw_db user=postgres password=postgres host=localhost', 'SELECT product_id FROM s1.s1_products') AS rem(id VARCHAR(256))) src,
     (SELECT COUNT(*) AS trg_cnt FROM lnd.lnd_s1_products) trg

UNION ALL

-- 4. s1_sales
SELECT 's1_sales' AS table_name, src.src_cnt, trg.trg_cnt, (src.src_cnt - trg.trg_cnt) AS diff,
       CASE WHEN src.src_cnt = trg.trg_cnt THEN 'PASS' ELSE 'FAIL' END AS status
FROM (SELECT COUNT(*) AS src_cnt FROM dblink('dbname=dwh_src_hw_db user=postgres password=postgres host=localhost', 'SELECT client_id FROM s1.s1_sales') AS rem(id VARCHAR(256))) src,
     (SELECT COUNT(*) AS trg_cnt FROM lnd.lnd_s1_sales) trg;


-- --- SOURCE 2 TABLES ---

-- 5. s2_channels
SELECT 's2_channels' AS table_name, src.src_cnt, trg.trg_cnt, (src.src_cnt - trg.trg_cnt) AS diff,
       CASE WHEN src.src_cnt = trg.trg_cnt THEN 'PASS' ELSE 'FAIL' END AS status
FROM (SELECT COUNT(*) AS src_cnt FROM dblink('dbname=dwh_src_hw_db user=postgres password=postgres host=localhost', 'SELECT channel_id FROM s2.s2_channels') AS rem(id VARCHAR(256))) src,
     (SELECT COUNT(*) AS trg_cnt FROM lnd.lnd_s2_channels) trg

UNION ALL

-- 6. s2_clients
SELECT 's2_clients' AS table_name, src.src_cnt, trg.trg_cnt, (src.src_cnt - trg.trg_cnt) AS diff,
       CASE WHEN src.src_cnt = trg.trg_cnt THEN 'PASS' ELSE 'FAIL' END AS status
FROM (SELECT COUNT(*) AS src_cnt FROM dblink('dbname=dwh_src_hw_db user=postgres password=postgres host=localhost', 'SELECT client_id FROM s2.s2_clients') AS rem(id VARCHAR(256))) src,
     (SELECT COUNT(*) AS trg_cnt FROM lnd.lnd_s2_clients) trg

UNION ALL

-- 7. s2_locations
SELECT 's2_locations' AS table_name, src.src_cnt, trg.trg_cnt, (src.src_cnt - trg.trg_cnt) AS diff,
       CASE WHEN src.src_cnt = trg.trg_cnt THEN 'PASS' ELSE 'FAIL' END AS status
FROM (SELECT COUNT(*) AS src_cnt FROM dblink('dbname=dwh_src_hw_db user=postgres password=postgres host=localhost', 'SELECT location_id FROM s2.s2_locations') AS rem(id VARCHAR(256))) src,
     (SELECT COUNT(*) AS trg_cnt FROM lnd.lnd_s2_locations) trg

UNION ALL

-- 8. s2_client_sales
SELECT 's2_client_sales' AS table_name, src.src_cnt, trg.trg_cnt, (src.src_cnt - trg.trg_cnt) AS diff,
       CASE WHEN src.src_cnt = trg.trg_cnt THEN 'PASS' ELSE 'FAIL' END AS status
FROM (SELECT COUNT(*) AS src_cnt FROM dblink('dbname=dwh_src_hw_db user=postgres password=postgres host=localhost', 'SELECT client_id FROM s2.s2_client_sales') AS rem(id VARCHAR(256))) src,
     (SELECT COUNT(*) AS trg_cnt FROM lnd.lnd_s2_client_sales) trg;


--С48
SELECT 
    'dwh_clients (Бизнес-ключи)' AS target_table,
    (SELECT COUNT(DISTINCT client_id) FROM lnd.lnd_s1_clients) + (SELECT COUNT(DISTINCT client_id) FROM lnd.lnd_s2_clients) AS expected_landing_rows,
    trg.dwh_keys AS actual_dwh_rows,
    ((SELECT COUNT(DISTINCT client_id) FROM lnd.lnd_s1_clients) + (SELECT COUNT(DISTINCT client_id) FROM lnd.lnd_s2_clients) - trg.dwh_keys) AS diff,
    CASE 
        WHEN ((SELECT COUNT(DISTINCT client_id) FROM lnd.lnd_s1_clients) + (SELECT COUNT(DISTINCT client_id) FROM lnd.lnd_s2_clients)) = trg.dwh_keys THEN 'PASS'
        ELSE 'FAIL (Check SCD2 Logic)'
    END AS status
FROM (SELECT COUNT(DISTINCT client_src_id) AS dwh_keys FROM dwh.dwh_clients) trg

--С49
SELECT 
    'dwh_products' AS target_table,
    (SELECT COUNT(*) FROM lnd.lnd_s1_products) + 
    (SELECT COUNT(DISTINCT product_id) FROM lnd.lnd_s2_client_sales WHERE product_id IS NOT NULL) AS expected_landing_rows,
    trg.dwh_rows AS actual_dwh_rows,
    ((SELECT COUNT(*) FROM lnd.lnd_s1_products) + (SELECT COUNT(DISTINCT product_id) FROM lnd.lnd_s2_client_sales WHERE product_id IS NOT NULL) - trg.dwh_rows) AS diff,
    CASE 
        WHEN ((SELECT COUNT(*) FROM lnd.lnd_s1_products) + (SELECT COUNT(DISTINCT product_id) FROM lnd.lnd_s2_client_sales WHERE product_id IS NOT NULL)) = trg.dwh_rows THEN 'PASS'
        ELSE 'FAIL'
    END AS status
FROM (SELECT COUNT(*) AS dwh_rows FROM dwh.dwh_products) trg

--С50
SELECT 
    'dwh_locations' AS target_table,
    (SELECT COUNT(DISTINCT channellocation) FROM lnd.lnd_s1_channels WHERE channellocation IS NOT NULL) + 
    (SELECT COUNT(*) FROM lnd.lnd_s2_locations) AS expected_landing_rows,
    trg.dwh_rows AS actual_dwh_rows,
    ((SELECT COUNT(DISTINCT channellocation) FROM lnd.lnd_s1_channels WHERE channellocation IS NOT NULL) + (SELECT COUNT(*) FROM lnd.lnd_s2_locations) - trg.dwh_rows) AS diff,
    CASE 
        WHEN ((SELECT COUNT(DISTINCT channellocation) FROM lnd.lnd_s1_channels WHERE channellocation IS NOT NULL) + (SELECT COUNT(*) FROM lnd.lnd_s2_locations)) = trg.dwh_rows THEN 'PASS'
        ELSE 'FAIL'
    END AS status
FROM (SELECT COUNT(*) AS dwh_rows FROM dwh.dwh_locations) trg

--С51
SELECT 
    'dwh_channels' AS target_table,
    (SELECT COUNT(*) FROM lnd.lnd_s1_channels) + (SELECT COUNT(*) FROM lnd.lnd_s2_channels) AS expected_landing_rows,
    trg.dwh_rows AS actual_dwh_rows,
    ((SELECT COUNT(*) FROM lnd.lnd_s1_channels) + (SELECT COUNT(*) FROM lnd.lnd_s2_channels) - trg.dwh_rows) AS diff,
    CASE 
        WHEN ((SELECT COUNT(*) FROM lnd.lnd_s1_channels) + (SELECT COUNT(*) FROM lnd.lnd_s2_channels)) = trg.dwh_rows THEN 'PASS'
        ELSE 'FAIL'
    END AS status
FROM (SELECT COUNT(*) AS dwh_rows FROM dwh.dwh_channels) trg


-- 3. LOCATIONS VALIDATION
SELECT 
    'dwh_locations' AS target_table,
    (SELECT COUNT(*) FROM lnd.lnd_s2_locations) AS expected_landing_rows, -- Локации есть только во 2-м источнике
    trg.dwh_rows AS actual_dwh_rows,
    ((SELECT COUNT(*) FROM lnd.lnd_s2_locations) - trg.dwh_rows) AS diff,
    CASE 
        WHEN (SELECT COUNT(*) FROM lnd.lnd_s2_locations) = trg.dwh_rows THEN 'PASS'
        ELSE 'FAIL (Data Loss or Duplication)'
    END AS status
FROM (SELECT COUNT(*) AS dwh_rows FROM dwh.dwh_locations) trg

-- 5. SALES FACT TABLE VALIDATION
SELECT 
    'dwh_sales' AS target_table,
    (SELECT COUNT(*) FROM lnd.lnd_s1_sales) + (SELECT COUNT(*) FROM lnd.lnd_s2_client_sales) AS expected_landing_rows,
    trg.dwh_rows AS actual_dwh_rows,
    ((SELECT COUNT(*) FROM lnd.lnd_s1_sales) + (SELECT COUNT(*) FROM lnd.lnd_s2_client_sales) - trg.dwh_rows) AS diff,
    CASE 
        WHEN ((SELECT COUNT(*) FROM lnd.lnd_s1_sales) + (SELECT COUNT(*) FROM lnd.lnd_s2_client_sales)) = trg.dwh_rows THEN 'PASS'
        ELSE 'FAIL (Review JOINs/Duplications)'
    END AS status
FROM (SELECT COUNT(*) AS dwh_rows FROM dwh.dwh_sales) trg;


SELECT 
    'dm_main_dashboard' AS target_mart_table,
    core.dwh_sales_count AS expected_rows_from_core,
    mart.dm_rows_count AS actual_mart_rows,
    (core.dwh_sales_count - mart.dm_rows_count) AS row_count_difference,
    CASE 
        WHEN core.dwh_sales_count = mart.dm_rows_count THEN 'PASS'
        WHEN core.dwh_sales_count > mart.dm_rows_count THEN 'FAIL (Data Loss: INNER JOIN dropped unmatched records)'
        ELSE 'FAIL (Data Duplication: Multiplying rows due to duplicate dimensions)'
    END AS validation_status
FROM 
    (
        SELECT COUNT(*) AS dwh_sales_count 
        FROM dwh.dwh_sales
    ) core,
    (
        SELECT COUNT(*) AS dm_rows_count 
        FROM dm.dm_main_dashboard
    ) mart;


--C54
SELECT 
    'dwh_clients' AS table_name, 
    'client_id' AS primary_key_column,
    COUNT(*) AS total_null_rows,
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL (PK Violates NOT NULL Constraint)' END AS test_status
FROM dwh.dwh_clients WHERE client_id IS NULL

UNION ALL

SELECT 
    'dwh_products' AS table_name, 
    'product_id' AS primary_key_column,
    COUNT(*) AS total_null_rows,
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL (PK Violates NOT NULL Constraint)' END AS test_status
FROM dwh.dwh_products WHERE product_id IS NULL

UNION ALL

SELECT 
    'dwh_locations' AS table_name, 
    'location_id' AS primary_key_column,
    COUNT(*) AS total_null_rows,
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL (PK Violates NOT NULL Constraint)' END AS test_status
FROM dwh.dwh_locations WHERE location_id IS NULL

UNION ALL

SELECT 
    'dwh_channels' AS table_name, 
    'channel_id' AS primary_key_column,
    COUNT(*) AS total_null_rows,
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL (PK Violates NOT NULL Constraint)' END AS test_status
FROM dwh.dwh_channels WHERE channel_id IS NULL

UNION ALL

SELECT 
    'dwh_sales' AS table_name, 
    'sale_id' AS primary_key_column,
    COUNT(*) AS total_null_rows,
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL (PK Violates NOT NULL Constraint)' END AS test_status
FROM dwh.dwh_sales WHERE sale_id IS NULL;




--C55
SELECT 
    'client_id' AS foreign_key_column,
    COUNT(*) AS null_occurrences,
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL (Orphan Sales Detected)' END AS status
FROM dwh.dwh_sales WHERE client_id IS NULL

UNION ALL

SELECT 
    'product_id' AS foreign_key_column,
    COUNT(*) AS null_occurrences,
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL (Orphan Sales Detected)' END AS status
FROM dwh.dwh_sales WHERE product_id IS NULL

UNION ALL

SELECT 
    'channel_id' AS foreign_key_column,
    COUNT(*) AS null_occurrences,
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL (Orphan Sales Detected)' END AS status
FROM dwh.dwh_sales WHERE channel_id IS NULL


--С56
SELECT 
    COUNT(CASE WHEN dm.id IS NULL THEN 1 END) AS missing_in_data_mart,
    
    COUNT(CASE WHEN s.sale_id IS NULL THEN 1 END) AS orphan_rows_in_mart,
    
    COUNT(*) AS total_discrepancies,
    
    CASE 
        WHEN COUNT(*) = 0 THEN 'PASS (Perfect 1:1 Matching)'
        ELSE 'FAIL (Data Asymmetry Detected)'
    END AS validation_status
FROM dwh.dwh_sales s
FULL OUTER JOIN dm.dm_main_dashboard dm ON s.sale_id = dm.id
WHERE s.sale_id IS NULL 
   OR dm.id IS NULL;
    
 --С57
    
    SELECT 
    'dwh_clients' AS table_name,
    'client_id' AS pk_column,
    COUNT(*) AS duplicate_pk_count,
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL (Duplicate PKs Detected!)' END AS test_status
FROM (
    SELECT client_id FROM dwh.dwh_clients GROUP BY client_id HAVING COUNT(*) > 1
) dup

UNION ALL

SELECT 
    'dwh_products' AS table_name,
    'product_id' AS pk_column,
    COUNT(*) AS duplicate_pk_count,
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL (Duplicate PKs Detected!)' END AS test_status
FROM (
    SELECT product_id FROM dwh.dwh_products GROUP BY product_id HAVING COUNT(*) > 1
) dup

UNION ALL

SELECT 
    'dwh_locations' AS table_name,
    'location_id' AS pk_column,
    COUNT(*) AS duplicate_pk_count,
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL (Duplicate PKs Detected!)' END AS test_status
FROM (
    SELECT location_id FROM dwh.dwh_locations GROUP BY location_id HAVING COUNT(*) > 1
) dup

UNION ALL

SELECT 
    'dwh_channels' AS table_name,
    'channel_id' AS pk_column,
    COUNT(*) AS duplicate_pk_count,
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL (Duplicate PKs Detected!)' END AS test_status
FROM (
    SELECT channel_id FROM dwh.dwh_channels GROUP BY channel_id HAVING COUNT(*) > 1
) dup

UNION ALL

SELECT 
    'dwh_sales' AS table_name,
    'sale_id' AS pk_column,
    COUNT(*) AS duplicate_pk_count,
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL (Duplicate PKs Detected!)' END AS test_status
FROM (
    SELECT sale_id FROM dwh.dwh_sales GROUP BY sale_id HAVING COUNT(*) > 1
) dup;


-----------------------------

------- Critical path -------

-----------------------------

--c58
SELECT COUNT(*) 
FROM dwh.dwh_clients c
JOIN lnd.lnd_s2_clients s2 ON c.client_src_id = s2.client_id 
WHERE c.middle_name <> 'N/A' 
   AND c.middle_name IS NOT NULL;

--c59

SELECT COUNT(*) 
FROM dwh.dwh_clients c 
JOIN lnd.lnd_s2_clients lnd ON c.client_src_id = lnd.client_id
WHERE c.phone_number <> (COALESCE(lnd.phone_code, '') || COALESCE(lnd.phone_number, ''))
   OR c.phone_number IS NULL;

--c60
SELECT COUNT(*) AS real_scd2_errors
FROM dwh.dwh_clients
WHERE client_src_id LIKE 'S2%'
  AND (
       (VALID_TO > '2021-01-20' AND IS_VALID IS DISTINCT FROM 'Y') 
    OR (VALID_TO <= '2021-01-20' AND IS_VALID IS DISTINCT FROM 'N')
  );

--c61
SELECT COUNT(*) AS total_s1_date_errors
FROM dwh.dwh_clients 
WHERE client_src_id LIKE 'S1%' 
  AND (valid_from <> '2000-01-01'::DATE OR valid_to <> '2100-01-01'::DATE);

--c62
SELECT COUNT(*) 
FROM dwh.dwh_clients c
JOIN lnd.lnd_s2_clients lnd ON c.client_src_id = lnd.client_id
WHERE c.valid_from <> lnd.valid_from::DATE 
   OR c.valid_to <> lnd.valid_to::DATE;


--c63
SELECT *
FROM dwh.dwh_locations 
WHERE 
    (location_src_id IS DISTINCT FROM 'N/A')
    AND location_name IN (
        SELECT DISTINCT channellocation 
        FROM lnd.lnd_s1_channels 
        WHERE channellocation IS NOT NULL
    );

--c64
SELECT 
    count(*)
FROM dwh.dwh_sales dwh
JOIN lnd.lnd_s1_sales s1 
  ON dwh.client_id = s1.client_id::INTEGER   
  AND dwh.product_id = s1.product_id::INTEGER 
  AND dwh.channel_id = s1.channel_id::INTEGER
  AND dwh.order_created = s1.sale_date::DATE 
WHERE dwh.order_created IS NULL OR s1.sale_date IS NULL;

SELECT COUNT(*) AS s2_date_mismatches
FROM dwh.dwh_sales dwh
JOIN lnd.lnd_s2_client_sales s2
  ON dwh.client_id = REGEXP_REPLACE(s2.client_id, '[^0-9]', '', 'g')::INTEGER   
  AND dwh.product_id = REGEXP_REPLACE(s2.product_id, '[^0-9]', '', 'g')::INTEGER 
  AND dwh.channel_id = REGEXP_REPLACE(s2.channel_id, '[^0-9]', '', 'g')::INTEGER
WHERE dwh.order_created <> s2.sold_date::DATE;

--с65
SELECT COUNT(*) AS chronology_errors
FROM dwh.dwh_sales
WHERE order_completed < order_created;

SELECT COUNT(*) AS null_date_errors
FROM dwh.dwh_sales
WHERE order_created IS NULL 
   OR order_completed IS NULL;

SELECT COUNT(*) AS s1_pure_mismatches
FROM dwh.dwh_sales dwh
JOIN lnd.lnd_s1_sales s1 
  ON dwh.client_id = s1.client_id::INTEGER   
  AND dwh.product_id = s1.product_id::INTEGER 
  AND dwh.channel_id = s1.channel_id::INTEGER
  AND dwh.order_completed = s1.purchase_date::DATE;


SELECT COUNT(*) AS s2_completion_mismatches
FROM dwh.dwh_sales dwh
JOIN lnd.lnd_s2_client_sales s2
  ON dwh.client_id = REGEXP_REPLACE(s2.client_id, '[^0-9]', '', 'g')::INTEGER   
  AND dwh.product_id = REGEXP_REPLACE(s2.product_id, '[^0-9]', '', 'g')::INTEGER 
  AND dwh.channel_id = REGEXP_REPLACE(s2.channel_id, '[^0-9]', '', 'g')::INTEGER
  AND dwh.order_created = s2.sold_date::DATE 
WHERE dwh.order_completed <> s2.sold_date::DATE;

--с66
SELECT 
    product_src_id, 
    COUNT(*) AS duplicate_count
FROM dwh.dwh_products 
WHERE product_src_id LIKE 'S2%' 
GROUP BY product_src_id 
HAVING COUNT(*) > 1;

--с67
SELECT COUNT(*) AS s1_product_name_mismatches
FROM dwh.dwh_products p
JOIN lnd.lnd_s1_products lnd 
  ON p.product_src_id = ('S1_' || lnd.product_id::TEXT)
WHERE p.product_name <> lnd.product_name;

SELECT COUNT(*) AS s2_product_name_mismatches
FROM dwh.dwh_products p
JOIN lnd.lnd_s2_client_sales lnd 
  ON p.product_src_id = ('S2_' || REGEXP_REPLACE(lnd.product_id, '[^0-9]', '', 'g'))
WHERE p.product_name <> lnd.product_name;

--с68
SELECT COUNT(*) AS s1_channel_name_mismatches
FROM dwh.dwh_channels ch
JOIN lnd.lnd_s1_channels lnd 
  ON ch.channel_src_id = ('S1_' || lnd.channel_id::TEXT)
WHERE ch.channel_name <> lnd.channel_name;

SELECT COUNT(*) AS s2_channel_name_mismatches
FROM dwh.dwh_channels ch
JOIN lnd.lnd_s2_channels lnd 
  ON ch.channel_src_id = ('S2_' || lnd.channel_id::TEXT)
WHERE ch.channel_name <> lnd.channel_name;

--с69
SELECT COUNT(*) AS s1_client_name_mismatches
FROM dwh.dwh_clients c
JOIN lnd.lnd_s1_clients lnd 
  ON c.client_src_id = ('S1_' || lnd.client_id::TEXT)
WHERE c.first_name <> lnd.first_name 
   OR c.last_name <> lnd.last_name;

SELECT COUNT(*) AS s2_client_name_mismatches
FROM dwh.dwh_clients c
JOIN lnd.lnd_s2_clients lnd 
  ON c.client_src_id = ('S2_' || REGEXP_REPLACE(lnd.client_id, '[^0-9]', '', 'g'))
WHERE c.VALID_TO > '2021-01-20'
  AND (c.first_name <> lnd.first_name OR c.last_name <> lnd.last_name);

--с70
SELECT COUNT(*) AS s2_location_name_mismatches
FROM dwh.dwh_locations loc
JOIN lnd.lnd_s2_locations lnd 
  ON loc.location_src_id = ('S2_' || lnd.location_id::TEXT)
WHERE loc.location_name <> lnd.location_name;

--с71
SELECT *
FROM dm.dm_main_dashboard dm
JOIN dwh.dwh_sales dwh ON dm.id = dwh.sale_id
JOIN dwh.dwh_products p ON dwh.product_id = p.product_id
WHERE dm.total_cost <> (dwh.quantity * p.product_cost)
   OR dm.total_cost IS NULL;

--с72
SELECT COUNT(*) AS negative_cost_errors
FROM dm.dm_main_dashboard
WHERE total_cost < 0;

--с73
SELECT COUNT(*) AS future_date_errors
FROM dm.dm_main_dashboard dm
JOIN dwh.dwh_sales dwh ON dm.id = dwh.sale_id
WHERE dwh.order_created > CURRENT_TIMESTAMP;

--с74
SELECT COUNT(*) AS null_dimension_errors
FROM dm.dm_main_dashboard
WHERE product_name IS NULL 
   OR location_name IS NULL 
   OR channel_name IS NULL;

--c75
SELECT COUNT(*) AS invalid_client_errors
FROM dm.dm_main_dashboard dm
JOIN dwh.dwh_clients c ON dm.email = c.email
WHERE c.is_valid = 'N';

--c76
SELECT 
    ABS(dm_total - dwh_total)::NUMERIC(16,2) AS critical_difference
FROM (
    SELECT SUM(total_cost) AS dm_total FROM dm.dm_main_dashboard
) dm
CROSS JOIN (
    SELECT SUM(dwh.quantity * p.product_cost) AS dwh_total 
    FROM dwh.dwh_sales dwh
    JOIN dwh.dwh_products p ON dwh.product_id = p.product_id
) dwh
WHERE ABS(dm_total - dwh_total) > 0.01;


-----------------------------

-------Extended path ----------

-----------------------------

--c77
SELECT COUNT(*) AS negative_value_errors
FROM dwh.dwh_sales dwh
JOIN dwh.dwh_products p ON dwh.product_id = p.product_id
WHERE dwh.quantity < 0 
   OR p.product_cost < 0;

--c78
SELECT COUNT(*) AS hidden_spaces_errors
FROM dwh.dwh_clients
WHERE first_name LIKE ' %' 
   OR first_name LIKE '% '
   OR last_name LIKE ' %' 
   OR last_name LIKE '% ';

--c79
SELECT 
    table_schema,
    table_name, 
    column_name, 
    data_type 
FROM information_schema.columns 
WHERE table_schema = 'dwh'
  AND table_name = 'dwh_clients' 
  AND column_name IN ('valid_from', 'valid_to')
  AND data_type NOT LIKE '%date%';

--c80
SELECT 
    table_schema,
    table_name, 
    column_name, 
    data_type 
FROM information_schema.columns 
WHERE table_schema = 'dwh'
  AND table_name = 'dwh_sales' 
  AND column_name IN ('order_created', 'order_completed')
  AND data_type NOT LIKE '%date%'
  AND data_type NOT LIKE '%timestamp%';

--c81
SELECT COUNT(*) 
FROM dwh.dwh_clients
WHERE TRIM(first_name) = '' 
   OR TRIM(last_name) = '';
--c82
SELECT COUNT(*) AS percent_sign_errors
FROM dwh.dwh_products
WHERE product_name LIKE '%\%%';
--c83
SELECT MAX(order_created) AS latest_load
FROM dwh.dwh_sales
HAVING MAX(order_created) < CURRENT_TIMESTAMP - INTERVAL '1 day';

--c84
SELECT COUNT(*) AS location_format_errors
FROM dwh.dwh_locations
WHERE location_name LIKE ' %' 
   OR location_name LIKE '% '
   OR location_name = LOWER(location_name);

--c85
SELECT COUNT(*) AS channel_whitespace_errors
FROM dwh.dwh_channels
WHERE channel_name LIKE ' %' 
   OR channel_name LIKE '% ';

--c86
SELECT COUNT(*) AS empty_dimension_errors
FROM dm.dm_main_dashboard
WHERE TRIM(product_name) = '' 
   OR TRIM(location_name) = '' 
   OR TRIM(channel_name) = '';

--c87
SELECT 
    dwh.sale_id,
    dwh.quantity, 
    p.product_cost 
FROM dwh.dwh_sales dwh
JOIN dwh.dwh_products p ON dwh.product_id = p.product_id
WHERE dwh.quantity > 1000 
   OR p.product_cost > 500000;


--c88
SELECT 'Missing -1 row in dwh.dwh_products' AS integrity_error
WHERE NOT EXISTS (SELECT 1 FROM dwh.dwh_products WHERE product_id = -1)

UNION ALL

SELECT 'Missing -1 row in dwh.dwh_location' AS integrity_error
WHERE NOT EXISTS (SELECT 1 FROM dwh.dwh_locations WHERE location_id = -1)

UNION ALL

SELECT 'Missing -1 row in dwh.dwh_channels' AS integrity_error
WHERE NOT EXISTS (SELECT 1 FROM dwh.dwh_channels WHERE channel_id = -1);

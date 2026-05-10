--Data Quality Checks for 3NF layer 
SELECT 
    'TEST 1: No duplicates' AS test_name,
    CASE WHEN COUNT(*) = 0 THEN 'PASSED' ELSE 'FAILED' END AS result,
    COUNT(*)::VARCHAR AS error_count
FROM (
    SELECT 1 FROM BL_3NF.CE_SALES
    GROUP BY DATE_ID, STORE_ID, ITEM_ID, PROMO_ID, BOTTLES_SOLD, SALE_DOLLARS, VOLUME_SOLD_LITERS
    HAVING COUNT(*) > 1) d

UNION ALL

SELECT 
    'TEST 2: No NULL mandatory keys',
    CASE WHEN COUNT(*) = 0 THEN 'PASSED' ELSE 'FAILED' END,
    COUNT(*)::VARCHAR
FROM BL_3NF.CE_SALES
WHERE DATE_ID IS NULL OR STORE_ID IS NULL OR ITEM_ID IS NULL OR PROMO_ID IS NULL

UNION ALL

SELECT 
    'TEST 3: All offline stores loaded',
    CASE WHEN COUNT(*) = 0 THEN 'PASSED' ELSE 'FAILED' END,
    COUNT(*)::VARCHAR
FROM (
    SELECT DISTINCT s.store_id::INTEGER FROM sa_offline_sales.src_offline_sales s
    WHERE NOT EXISTS (
        SELECT 1 FROM BL_CL.T_MAP_STORES m
        WHERE s.store_id::INTEGER = m.store_src_id AND m.source_system = 'sa_offline_sales')) t

UNION ALL

SELECT 
    'TEST 4: All online stores loaded',
    CASE WHEN COUNT(*) = 0 THEN 'PASSED' ELSE 'FAILED' END,
    COUNT(*)::VARCHAR
FROM (
    SELECT DISTINCT s.store_id::INTEGER FROM sa_online_sales.src_online_sales s
    WHERE NOT EXISTS (
        SELECT 1 FROM BL_CL.T_MAP_STORES m
        WHERE s.store_id::INTEGER = m.store_src_id AND m.source_system = 'sa_online_sales')) t

UNION ALL

SELECT 
    'TEST 5: All item keys loaded',
    CASE WHEN COUNT(*) = 0 THEN 'PASSED' ELSE 'FAILED' END,
    COUNT(*)::VARCHAR
FROM (
    SELECT DISTINCT s.item_id FROM sa_offline_sales.src_offline_sales s
    WHERE NOT EXISTS (
        SELECT 1 FROM BL_3NF.CE_ITEMS_SCD i WHERE s.item_id = i.ITEM_SRC_ID AND i.TA_IS_ACTIVE = 'Y')
    UNION
    SELECT DISTINCT s.item_id FROM sa_online_sales.src_online_sales s
    WHERE NOT EXISTS (
        SELECT 1 FROM BL_3NF.CE_ITEMS_SCD i WHERE s.item_id = i.ITEM_SRC_ID AND i.TA_IS_ACTIVE = 'Y')) t

UNION ALL

SELECT 
    'TEST 6: No fully unresolved records',
    CASE WHEN COUNT(*) = 0 THEN 'PASSED' ELSE 'FAILED' END,
    COUNT(*)::VARCHAR
FROM BL_3NF.CE_SALES
WHERE DATE_ID = -1 AND STORE_ID = -1 AND ITEM_ID = -1 AND PROMO_ID = -1;



--Data Quality Checks for DM layer 



SELECT * from(
SELECT 
    'TEST 1: No duplicates' AS test_name,
    CASE WHEN COUNT(*) = 0 THEN 'PASSED' ELSE 'FAILED' END AS result,
    COUNT(*)::VARCHAR AS error_count
FROM (
    SELECT 1 FROM BL_DM.FCT_SALES_DD
    GROUP BY EVENT_DT, STORE_SURR_ID, VENDOR_SURR_ID, ITEM_SURR_ID, PROMO_SURR_ID
    HAVING COUNT(*) > 1) duplicates

UNION ALL

SELECT 
    'TEST 2: Row count match',
    CASE WHEN src_cnt = dm_cnt THEN 'PASSED' ELSE 'FAILED' END,
    (src_cnt - dm_cnt)::VARCHAR
FROM (
    SELECT
        (SELECT COUNT(DISTINCT cs.DATE_ID || '-' || cs.STORE_ID || '-' || cs.ITEM_ID || '-' || cs.PROMO_ID)
         FROM BL_3NF.CE_SALES cs
         JOIN BL_3NF.CE_DATES d ON cs.DATE_ID = d.DATE_ID
         WHERE d.DATE_DT >= '2024-01-01') AS src_cnt,
        (SELECT COUNT(*) FROM BL_DM.FCT_SALES_DD) AS dm_cnt) counts

UNION ALL

SELECT 
    'TEST 3: Store integrity',
    CASE WHEN COUNT(*) = 0 THEN 'PASSED' ELSE 'FAILED' END,
    COUNT(*)::VARCHAR
FROM (
    SELECT DISTINCT s3.STORE_ID
    FROM BL_3NF.CE_SALES cs
    JOIN BL_3NF.CE_STORES s3 ON cs.STORE_ID = s3.STORE_ID
    WHERE NOT EXISTS (
        SELECT 1 FROM BL_DM.DIM_STORES ds
        WHERE s3.STORE_ID::VARCHAR = ds.STORE_SRC_ID)) missing

UNION ALL

SELECT 
    'TEST 4: Item integrity',
    CASE WHEN COUNT(*) = 0 THEN 'PASSED' ELSE 'FAILED' END,
    COUNT(*)::VARCHAR
FROM (
    SELECT DISTINCT i3.ITEM_ID
    FROM BL_3NF.CE_ITEMS_SCD i3
    WHERE i3.TA_IS_ACTIVE = 'Y'
    AND i3.ITEM_ID != -1
    AND NOT EXISTS (
        SELECT 1 FROM BL_DM.DIM_ITEMS_SCD di
        WHERE i3.ITEM_ID::VARCHAR = di.ITEM_SRC_ID
        AND di.TA_IS_ACTIVE = 'Y')) missing

UNION ALL

SELECT 
    'TEST 5: No NULL keys',
    CASE WHEN COUNT(*) = 0 THEN 'PASSED' ELSE 'FAILED' END,
    COUNT(*)::VARCHAR
FROM BL_DM.FCT_SALES_DD
WHERE EVENT_DT IS NULL OR DATE_ID IS NULL OR STORE_SURR_ID IS NULL 
   OR VENDOR_SURR_ID IS NULL OR ITEM_SURR_ID IS NULL OR PROMO_SURR_ID IS NULL

UNION ALL

SELECT 
    'TEST 6: No orphan records',
    CASE WHEN COUNT(*) = 0 THEN 'PASSED' ELSE 'FAILED' END,
    COUNT(*)::VARCHAR
FROM BL_DM.FCT_SALES_DD
WHERE STORE_SURR_ID = -1 AND VENDOR_SURR_ID = -1 
  AND ITEM_SURR_ID = -1 AND PROMO_SURR_ID = -1)
  ORDER BY test_name;
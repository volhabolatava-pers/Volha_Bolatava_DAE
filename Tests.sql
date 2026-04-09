--Offline_source 

SELECT 'Dates' AS table_name, 
       (SELECT COUNT(DISTINCT date::DATE) FROM sa_offline_sales.src_offline_sales WHERE date IS NOT NULL) AS source_rows,
       (SELECT COUNT(*) FROM BL_3NF.CE_DATES WHERE DATE_ID <> -1) AS target_rows,
       (SELECT COUNT(DISTINCT date::DATE) FROM sa_offline_sales.src_offline_sales WHERE date IS NOT NULL) - (SELECT COUNT(*) FROM BL_3NF.CE_DATES WHERE DATE_ID <> -1) AS difference

UNION ALL

SELECT 'Counties', 
       (SELECT COUNT(DISTINCT county_id) FROM sa_offline_sales.src_offline_sales WHERE county_id IS NOT NULL),
       (SELECT COUNT(*) FROM BL_3NF.CE_COUNTIES WHERE COUNTY_ID <> -1),
       (SELECT COUNT(DISTINCT county_id) FROM sa_offline_sales.src_offline_sales WHERE county_id IS NOT NULL) - (SELECT COUNT(*) FROM BL_3NF.CE_COUNTIES WHERE COUNTY_ID <> -1)

UNION ALL

SELECT 'Cities', 
       (SELECT COUNT(DISTINCT city_id) FROM sa_offline_sales.src_offline_sales WHERE city_id IS NOT NULL),
       (SELECT COUNT(*) FROM BL_3NF.CE_CITIES WHERE TA_SOURCE_ENTITY = 'src_offline_sales'),
       (SELECT COUNT(DISTINCT city_id) FROM sa_offline_sales.src_offline_sales WHERE city_id IS NOT NULL) - (SELECT COUNT(*) FROM BL_3NF.CE_CITIES WHERE TA_SOURCE_ENTITY = 'src_offline_sales')

UNION ALL

SELECT 'Streets', 
       (SELECT COUNT(DISTINCT COALESCE(NULLIF(TRIM(REGEXP_REPLACE(address, '^\d+\s*', '')), ''), 'n.a.')) FROM sa_offline_sales.src_offline_sales),
       (SELECT COUNT(*) FROM BL_3NF.CE_STREETS),
       (SELECT COUNT(DISTINCT COALESCE(NULLIF(TRIM(REGEXP_REPLACE(address, '^\d+\s*', '')), ''), 'n.a.')) FROM sa_offline_sales.src_offline_sales) - (SELECT COUNT(*) FROM BL_3NF.CE_STREETS)

UNION ALL

SELECT 'Addresses', 
       (SELECT COUNT(DISTINCT address) FROM sa_offline_sales.src_offline_sales WHERE address IS NOT NULL),
       (SELECT COUNT(*) FROM BL_3NF.CE_ADDRESSES WHERE TA_SOURCE_ENTITY = 'src_offline_sales'),
       (SELECT COUNT(DISTINCT address) FROM sa_offline_sales.src_offline_sales WHERE address IS NOT NULL) - (SELECT COUNT(*) FROM BL_3NF.CE_ADDRESSES WHERE TA_SOURCE_ENTITY = 'src_offline_sales')

UNION ALL

SELECT 'Stores', 
       (SELECT COUNT(DISTINCT store_id) FROM BL_CL.T_MAP_STORES WHERE source_system = 'sa_offline_sales'),
       (SELECT COUNT(*) FROM BL_3NF.CE_STORES WHERE TA_SOURCE_ENTITY = 'src_offline_sales'),
       (SELECT COUNT(DISTINCT store_id) FROM BL_CL.T_MAP_STORES WHERE source_system = 'sa_offline_sales') - (SELECT COUNT(*) FROM BL_3NF.CE_STORES WHERE TA_SOURCE_ENTITY = 'src_offline_sales')

UNION ALL

SELECT 'Vendors', 
       (SELECT COUNT(DISTINCT vendor_id) FROM sa_offline_sales.src_offline_sales WHERE vendor_id IS NOT NULL),
       (SELECT COUNT(*) FROM BL_3NF.CE_VENDORS WHERE TA_SOURCE_ENTITY = 'src_offline_sales'),
       (SELECT COUNT(DISTINCT vendor_id) FROM sa_offline_sales.src_offline_sales WHERE vendor_id IS NOT NULL) - (SELECT COUNT(*) FROM BL_3NF.CE_VENDORS WHERE TA_SOURCE_ENTITY = 'src_offline_sales')

UNION ALL

SELECT 'Categories', 
       (SELECT COUNT(DISTINCT category_id) FROM sa_offline_sales.src_offline_sales WHERE category_id IS NOT NULL),
       (SELECT COUNT(*) FROM BL_3NF.CE_CATEGORIES WHERE TA_SOURCE_ENTITY = 'src_offline_sales'),
       (SELECT COUNT(DISTINCT category_id) FROM sa_offline_sales.src_offline_sales WHERE category_id IS NOT NULL) - (SELECT COUNT(*) FROM BL_3NF.CE_CATEGORIES WHERE TA_SOURCE_ENTITY = 'src_offline_sales')

UNION ALL

SELECT 'Items (Active)', 
       (SELECT COUNT(DISTINCT item_id) FROM sa_offline_sales.src_offline_sales WHERE item_id IS NOT NULL),
       (SELECT COUNT(*) FROM BL_3NF.CE_ITEMS_SCD WHERE TA_IS_ACTIVE = 'Y' AND TA_SOURCE_ENTITY = 'src_offline_sales'),
       (SELECT COUNT(DISTINCT item_id) FROM sa_offline_sales.src_offline_sales WHERE item_id IS NOT NULL) - (SELECT COUNT(*) FROM BL_3NF.CE_ITEMS_SCD WHERE TA_IS_ACTIVE = 'Y' AND TA_SOURCE_ENTITY = 'src_offline_sales')

UNION ALL

SELECT 'Promotions', 
       (SELECT COUNT(DISTINCT promo_id) FROM sa_offline_sales.src_offline_sales WHERE promo_id IS NOT NULL),
       (SELECT COUNT(*) FROM BL_3NF.CE_PROMOTIONS WHERE TA_SOURCE_ENTITY = 'src_offline_sales'),
       (SELECT COUNT(DISTINCT promo_id) FROM sa_offline_sales.src_offline_sales WHERE promo_id IS NOT NULL) - (SELECT COUNT(*) FROM BL_3NF.CE_PROMOTIONS WHERE TA_SOURCE_ENTITY = 'src_offline_sales')


--Oтline_source 

SELECT 'Dates' AS table_name, 
       (SELECT COUNT(DISTINCT date::DATE) FROM sa_online_sales.src_online_sales WHERE date IS NOT NULL) AS source_rows,
       (SELECT COUNT(*) FROM BL_3NF.CE_DATES WHERE DATE_ID <> -1) AS target_rows,
       (SELECT COUNT(DISTINCT date::DATE) FROM sa_online_sales.src_online_sales WHERE date IS NOT NULL) - (SELECT COUNT(*) FROM BL_3NF.CE_DATES WHERE DATE_ID <> -1) AS difference

UNION ALL

SELECT 'Stores', 
       (SELECT COUNT(DISTINCT store_id) FROM BL_CL.T_MAP_STORES WHERE source_system = 'sa_online_sales'),
       (SELECT COUNT(*) FROM BL_3NF.CE_STORES WHERE TA_SOURCE_ENTITY = 'src_online_sales'),
       (SELECT COUNT(DISTINCT store_id) FROM BL_CL.T_MAP_STORES WHERE source_system = 'sa_online_sales') - (SELECT COUNT(*) FROM BL_3NF.CE_STORES WHERE TA_SOURCE_ENTITY = 'src_online_sales')

UNION ALL

SELECT 'Vendors', 
       (SELECT COUNT(DISTINCT vendor_id) FROM sa_online_sales.src_online_sales WHERE vendor_id IS NOT NULL),
       (SELECT COUNT(*) FROM BL_3NF.CE_VENDORS WHERE TA_SOURCE_ENTITY = 'src_online_sales'),
       (SELECT COUNT(DISTINCT vendor_id) FROM sa_online_sales.src_online_sales WHERE vendor_id IS NOT NULL) - (SELECT COUNT(*) FROM BL_3NF.CE_VENDORS WHERE TA_SOURCE_ENTITY = 'src_online_sales')

UNION ALL

SELECT 'Categories', 
       (SELECT COUNT(DISTINCT category_id) FROM sa_online_sales.src_online_sales WHERE category_id IS NOT NULL),
       (SELECT COUNT(*) FROM BL_3NF.CE_CATEGORIES WHERE TA_SOURCE_ENTITY = 'src_online_sales'),
       (SELECT COUNT(DISTINCT category_id) FROM sa_online_sales.src_online_sales WHERE category_id IS NOT NULL) - (SELECT COUNT(*) FROM BL_3NF.CE_CATEGORIES WHERE TA_SOURCE_ENTITY = 'src_online_sales')

UNION ALL

SELECT 'Items (Active)', 
       (SELECT COUNT(DISTINCT item_id) FROM sa_online_sales.src_online_sales WHERE item_id IS NOT NULL),
       (SELECT COUNT(*) FROM BL_3NF.CE_ITEMS_SCD WHERE TA_IS_ACTIVE = 'Y' AND TA_SOURCE_ENTITY = 'src_online_sales'),
       (SELECT COUNT(DISTINCT item_id) FROM sa_online_sales.src_online_sales WHERE item_id IS NOT NULL) - (SELECT COUNT(*) FROM BL_3NF.CE_ITEMS_SCD WHERE TA_IS_ACTIVE = 'Y' AND TA_SOURCE_ENTITY = 'src_online_sales')

UNION ALL

SELECT 'Promotions', 
       (SELECT COUNT(DISTINCT promo_id) FROM sa_online_sales.src_online_sales WHERE promo_id IS NOT NULL),
       (SELECT COUNT(DISTINCT PROMO_SRC_ID) 
        FROM BL_3NF.CE_PROMOTIONS 
        WHERE PROMO_SRC_ID IN (SELECT DISTINCT promo_id::text FROM sa_online_sales.src_online_sales)),
       (SELECT COUNT(DISTINCT promo_id) FROM sa_online_sales.src_online_sales WHERE promo_id IS NOT NULL) - (SELECT COUNT(DISTINCT PROMO_SRC_ID) 
        FROM BL_3NF.CE_PROMOTIONS 
        WHERE PROMO_SRC_ID IN (SELECT DISTINCT promo_id::text FROM sa_online_sales.src_online_sales))

        
--Sales_table         
        
        
SELECT 
    (SELECT COUNT(*) FROM sa_offline_sales.src_offline_sales) + 
    (SELECT COUNT(*) FROM sa_online_sales.src_online_sales) AS total_source_rows,
    (SELECT COUNT(*) FROM BL_3NF.CE_SALES) AS target_rows,
    ((SELECT COUNT(*) FROM sa_offline_sales.src_offline_sales) + 
     (SELECT COUNT(*) FROM sa_online_sales.src_online_sales)) - (SELECT COUNT(*) FROM BL_3NF.CE_SALES) AS difference;
CREATE SCHEMA IF NOT EXISTS BL_CL;
DO $$
BEGIN
   IF NOT EXISTS (SELECT FROM pg_catalog.pg_roles WHERE rolname = 'bl_cl') THEN
      CREATE ROLE bl_cl WITH LOGIN PASSWORD '1234';
   END IF;
END
$$;
GRANT USAGE ON SCHEMA sa_offline_sales TO BL_CL;
GRANT USAGE ON SCHEMA sa_online_sales  TO BL_CL;
GRANT USAGE ON SCHEMA BL_3NF           TO BL_CL;
GRANT SELECT ON ALL TABLES IN SCHEMA sa_offline_sales TO BL_CL;
GRANT SELECT ON ALL TABLES IN SCHEMA sa_online_sales  TO BL_CL;
GRANT SELECT, INSERT, UPDATE ON ALL TABLES IN SCHEMA BL_3NF TO BL_CL;
GRANT USAGE  ON ALL SEQUENCES IN SCHEMA BL_3NF TO BL_CL;
COMMIT;
  
CREATE TABLE IF NOT EXISTS BL_CL.MTA_LOGS (
	LOG_ID           BIGSERIAL PRIMARY KEY,
    LOG_DT           TIMESTAMP DEFAULT NOW(),
    PROCEDURE_NAME   VARCHAR(200),
    ROWS_AFFECTED    INTEGER,
    LOG_MESSAGE      TEXT
);

CREATE OR REPLACE PROCEDURE BL_CL.PRC_WRITE_LOG(
    p_proc IN VARCHAR,
    p_rows      IN INTEGER,
    p_message   IN TEXT
)
LANGUAGE plpgsql 
AS $$ 
BEGIN
	INSERT INTO BL_CL.MTA_LOGS (PROCEDURE_NAME, ROWS_AFFECTED, LOG_MESSAGE)
    VALUES (p_proc, p_rows, p_message);
END;
$$;

COMMIT;

CREATE OR REPLACE PROCEDURE BL_CL.LOAD_CE_DATES()
LANGUAGE plpgsql AS $$
DECLARE
    p_rows INTEGER := 0;
    p_proc VARCHAR  := 'BL_CL.LOAD_CE_DATES';
BEGIN
	INSERT INTO BL_3NF.CE_DATES (DATE_ID, DATE_DT, DAY, MONTH, QUARTER, YEAR)
SELECT
    NEXTVAL('BL_3NF.SEQ_CE_DATES'),
    src.date::DATE,
    EXTRACT(DAY FROM src.date::DATE)::INTEGER,
    EXTRACT(MONTH FROM src.date::DATE)::INTEGER,
    EXTRACT(QUARTER FROM src.date::DATE)::INTEGER,
    EXTRACT(YEAR FROM src.date::DATE)::INTEGER
FROM (
    SELECT DISTINCT date
    FROM sa_offline_sales.src_offline_sales
    UNION
    SELECT DISTINCT date
    FROM sa_online_sales.src_online_sales) AS src
WHERE NOT EXISTS (
    SELECT 1
    FROM BL_3NF.CE_DATES t
    WHERE t.DATE_DT = src.date::DATE);

GET DIAGNOSTICS p_rows = ROW_COUNT;

CALL BL_CL.PRC_WRITE_LOG(p_proc, p_rows,'SUCCESS');

EXCEPTION WHEN OTHERS THEN
CALL BL_CL.PRC_WRITE_LOG(p_proc, 0,'ERROR:'|| SQLERRM);
RAISE NOTICE 'Error during loading CE_DATES: %', SQLERRM;
END;
$$;

CREATE OR REPLACE PROCEDURE BL_CL.LOAD_CE_COUNTIES()
LANGUAGE plpgsql AS $$
DECLARE
    p_rows INTEGER := 0;
    p_proc VARCHAR  := 'BL_CL.LOAD_CE_COUNTIES';
BEGIN
INSERT INTO BL_3NF.CE_COUNTIES (COUNTY_ID, COUNTY_SRC_ID, COUNTY_NAME, TA_INSERT_DT, TA_UPDATE_DT, TA_SOURCE_SYSTEM, TA_SOURCE_ENTITY)
SELECT
    NEXTVAL('BL_3NF.SEQ_CE_COUNTIES'),
    COALESCE(src.county_id, 'n.a.'),
    COALESCE(src.county_name, 'n.a.'),
    CURRENT_DATE,
    CURRENT_DATE,
    'sa_offline_sales',
    'src_offline_sales'
FROM (
    SELECT DISTINCT  
        TRIM(county_id::VARCHAR) AS county_id,
        MAX(TRIM(county_name)) AS county_name
    FROM sa_offline_sales.src_offline_sales
    WHERE county_id IS NOT NULL
    AND TRIM(county_id::VARCHAR) <> ''
    GROUP BY TRIM(county_id::VARCHAR)) AS src
WHERE NOT EXISTS (
    SELECT 1
    FROM BL_3NF.CE_COUNTIES t
    WHERE t.COUNTY_SRC_ID = COALESCE(NULLIF(TRIM(src.county_id::VARCHAR), ''), src.county_name, 'n.a.'));
    
    GET DIAGNOSTICS p_rows = ROW_COUNT;

CALL BL_CL.PRC_WRITE_LOG(p_proc, p_rows,'SUCCESS');

EXCEPTION WHEN OTHERS THEN
CALL BL_CL.PRC_WRITE_LOG(p_proc, 0,'ERROR:'|| SQLERRM);
RAISE NOTICE 'Error during loading CE_COUNTIES: %', SQLERRM;
END;
$$;



CREATE OR REPLACE PROCEDURE BL_CL.LOAD_CE_CITIES()
LANGUAGE plpgsql AS $$
DECLARE
    p_rows INTEGER := 0;
    p_proc VARCHAR  := 'BL_CL.LOAD_CE_CITIES';
BEGIN
	INSERT INTO BL_3NF.CE_CITIES (CITY_ID, CITY_SRC_ID, CITY_NAME, ZIP_CODE, COUNTY_ID, TA_INSERT_DT, TA_UPDATE_DT, TA_SOURCE_SYSTEM, TA_SOURCE_ENTITY)
SELECT
    NEXTVAL('BL_3NF.SEQ_CE_CITIES'),
    COALESCE(src.city_id, 'n.a.'),
    COALESCE(src.city_name, 'n.a.'),
    COALESCE(src.zip_code, 'n.a.'),
    COALESCE(fk.COUNTY_ID, -1),
    CURRENT_DATE,
    CURRENT_DATE,
    'sa_offline_sales',
    'src_offline_sales'
FROM (
    SELECT 
        city_id, 
        MAX(city_name) as city_name, 
        MAX(zip_code) as zip_code, 
        MAX(county_id) as county_id
    FROM sa_offline_sales.src_offline_sales
    GROUP BY city_id) AS src
LEFT JOIN BL_3NF.CE_COUNTIES fk ON src.county_id = fk.COUNTY_SRC_ID
WHERE NOT EXISTS (
    SELECT 1
    FROM BL_3NF.CE_CITIES t
    WHERE t.CITY_SRC_ID = src.city_id);
    GET DIAGNOSTICS p_rows = ROW_COUNT;

CALL BL_CL.PRC_WRITE_LOG(p_proc, p_rows,'SUCCESS');

EXCEPTION WHEN OTHERS THEN
CALL BL_CL.PRC_WRITE_LOG(p_proc, 0,'ERROR:'|| SQLERRM);
RAISE NOTICE 'Error during loading CE_CITIES: %', SQLERRM;
END;
$$;


CREATE OR REPLACE PROCEDURE BL_CL.LOAD_CE_STREETS()
LANGUAGE plpgsql AS $$
DECLARE
    p_rows INTEGER := 0;
    p_proc VARCHAR  := 'BL_CL.LOAD_CE_STREETS';
BEGIN
	INSERT INTO BL_3NF.CE_STREETS (STREET_ID, STREET_SRC_ID, TA_INSERT_DT, TA_UPDATE_DT, TA_SOURCE_SYSTEM, TA_SOURCE_ENTITY)
SELECT
    NEXTVAL('BL_3NF.SEQ_CE_STREETS'),
    src.address,
    CURRENT_DATE,
    CURRENT_DATE,
    'sa_offline_sales',
    'src_offline_sales'
FROM (
    SELECT DISTINCT COALESCE(NULLIF(TRIM(REGEXP_REPLACE(address, '^\d+\s*', '')), ''), 'n.a.') AS address 
    FROM sa_offline_sales.src_offline_sales) AS src
WHERE NOT EXISTS (
    SELECT 1
    FROM BL_3NF.CE_STREETS t
     WHERE t.STREET_SRC_ID = src.address);

GET DIAGNOSTICS p_rows = ROW_COUNT;

CALL BL_CL.PRC_WRITE_LOG(p_proc, p_rows,'SUCCESS');

EXCEPTION WHEN OTHERS THEN
CALL BL_CL.PRC_WRITE_LOG(p_proc, 0,'ERROR:'|| SQLERRM);
RAISE NOTICE 'Error during loading CE_STREETS: %', SQLERRM;
END;
$$;


CREATE OR REPLACE PROCEDURE BL_CL.LOAD_CE_ADDRESSES()
LANGUAGE plpgsql AS $$
DECLARE
    p_rows INTEGER := 0;
    p_proc VARCHAR  := 'BL_CL.LOAD_CE_ADDRESSES';
BEGIN
INSERT INTO BL_3NF.CE_ADDRESSES (ADDRESS_ID, ADDRESS_SRC_ID, HOUSE_NUMBER, STREET_ID, CITY_ID, TA_INSERT_DT, TA_UPDATE_DT, TA_SOURCE_SYSTEM, TA_SOURCE_ENTITY)
WITH clean_addresses AS (
    SELECT DISTINCT 
        address AS raw_address,
        city_id,
        NULLIF(REGEXP_REPLACE(SPLIT_PART(TRIM(address), ' ', 1), '[^0-9]', '', 'g'), '') AS h_number,
        COALESCE(NULLIF(TRIM(REGEXP_REPLACE(address, '^\d+\s*', '')), ''), 'n.a.') AS street
    FROM sa_offline_sales.src_offline_sales
    WHERE address IS NOT NULL)
SELECT DISTINCT ON (src.raw_address)
    NEXTVAL('BL_3NF.SEQ_CE_ADDRESSES'),
    COALESCE(src.raw_address, 'n.a.'),
    COALESCE(src.h_number::INTEGER, -1),
    COALESCE(st.STREET_ID, -1),
    COALESCE(cit.CITY_ID, -1),    
    CURRENT_DATE,
    CURRENT_DATE,
    'sa_offline_sales',
    'src_offline_sales'
FROM clean_addresses src
LEFT JOIN BL_3NF.CE_CITIES cit ON src.city_id = cit.CITY_SRC_ID
LEFT JOIN BL_3NF.CE_STREETS st ON st.STREET_SRC_ID = src.street
WHERE NOT EXISTS ( 
    SELECT 1
    FROM BL_3NF.CE_ADDRESSES t
    WHERE t.ADDRESS_SRC_ID = src.raw_address)
ORDER BY src.raw_address, cit.CITY_ID;
GET DIAGNOSTICS p_rows = ROW_COUNT;

CALL BL_CL.PRC_WRITE_LOG(p_proc, p_rows,'SUCCESS');

EXCEPTION WHEN OTHERS THEN
CALL BL_CL.PRC_WRITE_LOG(p_proc, 0,'ERROR:'|| SQLERRM);
RAISE NOTICE 'Error during loading CE_ADDRESSES: %', SQLERRM;
END;
$$;


CREATE OR REPLACE FUNCTION BL_CL.STORES_FOR_LOAD()
RETURNS TABLE (
    store_id         BIGINT,
    store_src_id     VARCHAR(10),
    store_name       varchar(100),
    store_type       varchar(10),
    address_id       BIGINT,
    source_system    VARCHAR(50),
    source_entity    VARCHAR(50)
)
LANGUAGE plpgsql
AS $$
BEGIN
    RETURN QUERY
    SELECT * FROM ( 
	SELECT DISTINCT ON (map.store_id::INTEGER)
    map.store_id::BIGINT,
    map.store_src_id::VARCHAR(10),
    COALESCE(map.store_name, 'n.a.')::VARCHAR(100),   
    COALESCE(src.store_type, 'n.a.')::VARCHAR(10), 
    COALESCE(fk.ADDRESS_ID, -1)::BIGINT,
    'sa_offline_sales'::VARCHAR(50),
    'src_offline_sales'::VARCHAR(50)
FROM BL_CL.T_MAP_STORES map
LEFT JOIN sa_offline_sales.src_offline_sales src 
    ON map.store_src_id::INTEGER = src.store_id::INTEGER
    AND map.source_system = 'sa_offline_sales'
LEFT JOIN BL_3NF.CE_ADDRESSES fk ON src.address = fk.ADDRESS_SRC_ID
WHERE map.source_system = 'sa_offline_sales'
ORDER BY map.store_id::INTEGER) offline_stores
UNION ALL
	SELECT * FROM ( 
	SELECT DISTINCT ON (map.store_id::INTEGER)
    map.store_id::BIGINT,
    map.store_src_id::VARCHAR(10),
    COALESCE(map.store_name, 'n.a.')::VARCHAR(100),   
    COALESCE(src.store_type, 'n.a.')::VARCHAR(10), 
    -1::BIGINT,
    'sa_online_sales'::VARCHAR(50),
    'src_online_sales'::VARCHAR(50)
FROM BL_CL.T_MAP_STORES map
LEFT JOIN sa_online_sales.src_online_sales AS src
    ON map.store_src_id::INTEGER = src.store_id::INTEGER
    AND map.source_system = 'sa_online_sales'
    WHERE map.source_system = 'sa_online_sales'
	ORDER BY map.store_id::INTEGER
) online_stores;
END;
$$;

CREATE OR REPLACE PROCEDURE BL_CL.LOAD_CE_STORES()
LANGUAGE plpgsql AS $$
DECLARE
    p_rows INTEGER := 0;
    p_proc VARCHAR := 'BL_CL.LOAD_CE_STORES';
BEGIN
    INSERT INTO BL_3NF.CE_STORES (STORE_ID, STORE_SRC_ID, STORE_NAME, STORE_TYPE, ADDRESS_ID, TA_INSERT_DT, TA_UPDATE_DT, TA_SOURCE_SYSTEM, TA_SOURCE_ENTITY)
    SELECT 
        fn.store_id, 
        fn.store_src_id, 
        fn.store_name, 
        fn.store_type, 
        fn.address_id,
        CURRENT_DATE, 
        CURRENT_DATE, 
        fn.source_system, 
        fn.source_entity
    FROM BL_CL.STORES_FOR_LOAD() fn
    WHERE NOT EXISTS (
        SELECT 1 FROM BL_3NF.CE_STORES t 
        WHERE t.STORE_ID = fn.store_id
    );

    GET DIAGNOSTICS p_rows = ROW_COUNT;
    CALL BL_CL.PRC_WRITE_LOG(p_proc, p_rows, 'SUCCESS');

EXCEPTION WHEN OTHERS THEN
CALL BL_CL.PRC_WRITE_LOG(p_proc, 0,'ERROR:'|| SQLERRM);
RAISE NOTICE 'Error during loading CE_STORES: %', SQLERRM;
END;
$$;


CREATE OR REPLACE FUNCTION BL_CL.VENDORS_FOR_LOAD()
RETURNS TABLE (
  VENDOR_SRC_ID     VARCHAR(10),
  VENDOR_NAME       varchar(100),
  TA_SOURCE_SYSTEM  VARCHAR(50) ,
  TA_SOURCE_ENTITY  VARCHAR(50))
LANGUAGE plpgsql
AS $$
BEGIN
    RETURN QUERY
SELECT
   	COALESCE(src.v_id, 'n.a.')::VARCHAR(10),
    COALESCE(src.v_name, 'n.a.')::varchar(100),   
    'sa_offline_sales'::VARCHAR(50),
    'src_offline_sales'::VARCHAR(50)
FROM (
    SELECT   
        s_off.vendor_id as v_id, 
        MAX(s_off.vendor_name) AS v_name
    FROM sa_offline_sales.src_offline_sales as s_off
    WHERE vendor_id IS NOT NULL
    GROUP BY vendor_id) AS src
 UNION ALL
 SELECT
    	COALESCE(src.v_id, 'n.a.')::VARCHAR(10),
    COALESCE(src.v_name, 'n.a.')::varchar(100),   
    'sa_online_sales'::VARCHAR(50),
    'src_online_sales'::VARCHAR(50)
FROM (
    SELECT   
        s_on.vendor_id as v_id, 
        MAX(s_on.vendor_name) AS v_name
    FROM sa_online_sales.src_online_sales as s_on
    WHERE vendor_id IS NOT NULL
    GROUP BY vendor_id) AS src;
    END;
$$;
 
CREATE OR REPLACE PROCEDURE BL_CL.LOAD_CE_VENDORS()
LANGUAGE plpgsql AS $$
DECLARE
    p_rows INTEGER := 0;
    p_proc VARCHAR  := 'BL_CL.LOAD_CE_VENDORS';
BEGIN
    INSERT INTO BL_3NF.CE_VENDORS (
        VENDOR_ID, VENDOR_SRC_ID, VENDOR_NAME,
        TA_INSERT_DT, TA_UPDATE_DT, TA_SOURCE_SYSTEM, TA_SOURCE_ENTITY)
    SELECT
        NEXTVAL('BL_3NF.SEQ_CE_VENDORS'),
        fn.vendor_src_id,
        fn.vendor_name,
        CURRENT_DATE,
        CURRENT_DATE,
        fn.ta_source_system,
        fn.ta_source_entity
    FROM BL_CL.VENDORS_FOR_LOAD() fn
    WHERE NOT EXISTS (
    SELECT 1
    FROM BL_3NF.CE_VENDORS t
    WHERE t.VENDOR_SRC_ID = fn.vendor_src_id );
 GET DIAGNOSTICS p_rows = ROW_COUNT;
    CALL BL_CL.PRC_WRITE_LOG(p_proc, p_rows, 'SUCCESS');

EXCEPTION WHEN OTHERS THEN
CALL BL_CL.PRC_WRITE_LOG(p_proc, 0,'ERROR:'|| SQLERRM);
RAISE NOTICE 'Error during loading CE_VENDORS: %', SQLERRM;
END;
$$;


CREATE OR REPLACE PROCEDURE BL_CL. LOAD_CE_CATEGORIES()
LANGUAGE plpgsql AS $$
DECLARE
    p_rows INTEGER := 0;
    p_proc VARCHAR  := 'BL_CL.LOAD_CE_CATEGORIES';
BEGIN
	INSERT INTO BL_3NF.CE_CATEGORIES (CATEGORY_ID, CATEGORY_SRC_ID, CATEGORY_NAME, TA_INSERT_DT, TA_UPDATE_DT, TA_SOURCE_SYSTEM, TA_SOURCE_ENTITY)
SELECT
    NEXTVAL('BL_3NF.SEQ_CE_CATEGORIES'),
    COALESCE(src.category_id, 'n.a.'),
    COALESCE(src.category_name, 'n.a.'),   
    CURRENT_DATE,
    CURRENT_DATE,
    src.source_system,
    src.source_entity
FROM (
    SELECT  
        category_id, 
        MAX (category_name) AS category_name,
		'sa_offline_sales'::VARCHAR(50) AS source_system,
        'src_offline_sales'::VARCHAR(50) AS source_entity
    FROM sa_offline_sales.src_offline_sales
    WHERE category_id IS NOT NULL
    GROUP BY category_id
UNION ALL
SELECT  
        category_id, 
        MAX (category_name) AS category_name,
		'sa_online_sales'::VARCHAR(50) AS source_system,
        'src_online_sales'::VARCHAR(50) AS source_entity
    FROM sa_online_sales.src_online_sales
    WHERE category_id IS NOT NULL
    GROUP BY category_id
) AS src
WHERE NOT EXISTS (
    SELECT 1
    FROM BL_3NF.CE_CATEGORIES t
    WHERE t.CATEGORY_SRC_ID = src.category_id
	AND t.TA_SOURCE_SYSTEM = src.source_system);

GET DIAGNOSTICS p_rows = ROW_COUNT;

CALL BL_CL.PRC_WRITE_LOG(p_proc, p_rows,'SUCCESS');

EXCEPTION WHEN OTHERS THEN
CALL BL_CL.PRC_WRITE_LOG(p_proc, 0,'ERROR:'|| SQLERRM);
RAISE NOTICE 'Error during loading CE_CATEGORIES: %', SQLERRM;
END;
$$;

CREATE OR REPLACE PROCEDURE BL_CL.LOAD_CE_PROMOTIONS()
LANGUAGE plpgsql AS $$
DECLARE
    p_rows INTEGER := 0;
    p_proc VARCHAR  := 'BL_CL.LOAD_CE_PROMOTIONS';
BEGIN
	INSERT INTO BL_3NF.CE_PROMOTIONS (PROMO_ID, PROMO_SRC_ID, PROMO_NAME, DISCOUNT_PCT, TA_INSERT_DT, TA_UPDATE_DT, TA_SOURCE_SYSTEM, TA_SOURCE_ENTITY)
SELECT
    NEXTVAL('BL_3NF.SEQ_CE_PROMOTIONS'),
    COALESCE(src.promo_id, 'n.a.'),
    COALESCE(src.promo_name, 'n.a.'),
    COALESCE(src.discount_pct::INTEGER, -1),
    CURRENT_DATE,
    CURRENT_DATE,
    src.source_system,
    src.source_entity
FROM (
    SELECT DISTINCT  
        promo_id, 
        promo_name,
        discount_pct,
        'sa_offline_sales'::VARCHAR(50) AS source_system,
        'src_offline_sales'::VARCHAR(50) AS source_entity
    FROM sa_offline_sales.src_offline_sales

UNION ALL

SELECT DISTINCT  
        promo_id, 
        promo_name,
        discount_pct,
		'sa_online_sales'::VARCHAR(50) AS source_system,
        'src_online_sales'::VARCHAR(50) AS source_entity
    FROM sa_online_sales.src_online_sales) AS src
WHERE NOT EXISTS (
    SELECT 1
    FROM BL_3NF.CE_PROMOTIONS t
    WHERE t.PROMO_SRC_ID = src.promo_id);

GET DIAGNOSTICS p_rows = ROW_COUNT;

CALL BL_CL.PRC_WRITE_LOG(p_proc, p_rows,'SUCCESS');

EXCEPTION WHEN OTHERS THEN
CALL BL_CL.PRC_WRITE_LOG(p_proc, 0,'ERROR:'|| SQLERRM);
RAISE NOTICE 'Error during loading CE_PROMOTIONS: %', SQLERRM;
END;
$$;

CREATE OR REPLACE PROCEDURE BL_CL.LOAD_CE_SALES()
LANGUAGE plpgsql AS $$
DECLARE
    p_rows INTEGER := 0;
	p_rows_current INTEGER := 0;
    p_proc VARCHAR  := 'BL_CL.LOAD_CE_SALES';
BEGIN
    INSERT INTO BL_3NF.CE_SALES (
        DATE_ID, STORE_ID, ITEM_ID, PROMO_ID,
        BOTTLES_SOLD, SALE_DOLLARS, VOLUME_SOLD_LITERS)
    SELECT
        COALESCE(d.DATE_ID,  -1),
        COALESCE(m.STORE_ID, -1),
        COALESCE(i.ITEM_ID,  -1),
        COALESCE(p.PROMO_ID, -1),
        s.bottles_sold::INTEGER,
        COALESCE(REPLACE(NULLIF(s.sale_dollars,        '#VALUE!'), ',', '')::DECIMAL(15,3), 0),
        COALESCE(REPLACE(NULLIF(s.volume_sold_liters,  '#VALUE!'), ',', '')::DECIMAL(12,3), 0)
    FROM sa_offline_sales.src_offline_sales s
    LEFT JOIN BL_3NF.CE_DATES      d ON s.date::DATE          = d.DATE_DT
    LEFT JOIN BL_CL.T_MAP_STORES   m ON s.store_id::INTEGER   = m.store_src_id
                                     AND m.source_system       = 'sa_offline_sales'
    LEFT JOIN BL_3NF.CE_ITEMS_SCD  i ON s.item_id             = i.ITEM_SRC_ID
                                     AND i.TA_IS_ACTIVE        = 'Y'
    LEFT JOIN BL_3NF.CE_PROMOTIONS p ON s.promo_id            = p.PROMO_SRC_ID
    WHERE NOT EXISTS (
        SELECT 1 FROM BL_3NF.CE_SALES t
        WHERE t.DATE_ID  = d.DATE_ID
          AND t.STORE_ID = m.STORE_ID
          AND t.ITEM_ID  = i.ITEM_ID
    );

    GET DIAGNOSTICS p_rows_current = ROW_COUNT;
    p_rows := p_rows + p_rows_current;

    INSERT INTO BL_3NF.CE_SALES (
        DATE_ID, STORE_ID, ITEM_ID, PROMO_ID,
        BOTTLES_SOLD, SALE_DOLLARS, VOLUME_SOLD_LITERS)
    SELECT
        COALESCE(d.DATE_ID,  -1),
        COALESCE(m.STORE_ID, -1),
        COALESCE(i.ITEM_ID,  -1),
        COALESCE(p.PROMO_ID, -1),
        s.bottles_sold::INTEGER,
        COALESCE(REPLACE(NULLIF(s.sale_dollars,       '#VALUE!'), ',', '')::DECIMAL(15,3), 0),
        COALESCE(REPLACE(NULLIF(s.volume_sold_liters, '#VALUE!'), ',', '')::DECIMAL(12,3), 0)
    FROM sa_online_sales.src_online_sales s
    LEFT JOIN BL_3NF.CE_DATES      d ON s.date::DATE          = d.DATE_DT
    LEFT JOIN BL_CL.T_MAP_STORES   m ON s.store_id::INTEGER   = m.store_src_id
                                     AND m.source_system       = 'sa_online_sales'
    LEFT JOIN BL_3NF.CE_ITEMS_SCD  i ON s.item_id             = i.ITEM_SRC_ID
                                     AND i.TA_IS_ACTIVE        = 'Y'
    LEFT JOIN BL_3NF.CE_PROMOTIONS p ON s.promo_id            = p.PROMO_SRC_ID
    WHERE NOT EXISTS (
        SELECT 1 FROM BL_3NF.CE_SALES t
        WHERE t.DATE_ID  = d.DATE_ID
          AND t.STORE_ID = m.STORE_ID
          AND t.ITEM_ID  = i.ITEM_ID
    );

    GET DIAGNOSTICS p_rows_current = ROW_COUNT;
    p_rows := p_rows + p_rows_current;

    CALL BL_CL.PRC_WRITE_LOG(p_proc, p_rows, 'SUCCESS');

EXCEPTION WHEN OTHERS THEN
    CALL BL_CL.PRC_WRITE_LOG(p_proc, 0, 'ERROR: ' || SQLERRM);
    RAISE NOTICE 'Error during loading CE_SALES: %', SQLERRM;
END;
$$;

CREATE OR REPLACE PROCEDURE BL_CL.LOAD_CE_ITEMS_SCD()
LANGUAGE plpgsql AS $$
DECLARE
    v_system          TEXT;
    v_sources         TEXT[] := ARRAY['sa_offline_sales', 'sa_online_sales'];
    v_rows_updated    INT := 0;
    v_rows_inserted   INT := 0;
    v_rows_current    INT := 0;
    p_rows            INT := 0;
    p_proc            VARCHAR := 'BL_CL.LOAD_CE_ITEMS_SCD';
BEGIN
    FOREACH v_system IN ARRAY v_sources LOOP

        IF v_system = 'sa_offline_sales' THEN

            UPDATE BL_3NF.CE_ITEMS_SCD t
            SET TA_END_DT    = CURRENT_DATE - 1,
                TA_IS_ACTIVE = 'N'
            WHERE t.TA_IS_ACTIVE = 'Y'
              AND t.TA_SOURCE_SYSTEM = 'sa_offline_sales'
              AND NOT EXISTS (
                SELECT 1
                FROM sa_offline_sales.src_offline_sales src
                WHERE TRIM(src.item_id) = t.ITEM_SRC_ID
                  AND REPLACE(src.state_bottle_retail, ',', '')::DECIMAL(10,2) = t.STATE_BOTTLE_RETAIL
                  AND REPLACE(src.state_bottle_cost,   ',', '')::DECIMAL(10,2) = t.STATE_BOTTLE_COST
              )
              AND EXISTS (
                SELECT 1
                FROM sa_offline_sales.src_offline_sales src
                WHERE TRIM(src.item_id) = t.ITEM_SRC_ID
              );
            GET DIAGNOSTICS v_rows_current = ROW_COUNT;
            v_rows_updated := v_rows_updated + v_rows_current;
            p_rows := p_rows + v_rows_current;

            INSERT INTO BL_3NF.CE_ITEMS_SCD (
                ITEM_ID, ITEM_SRC_ID, ITEM_NAME, PACK, BOTTLE_VOLUME_ML,
                STATE_BOTTLE_RETAIL, STATE_BOTTLE_COST, CATEGORY_ID, VENDOR_ID,
                TA_START_DT, TA_END_DT, TA_IS_ACTIVE, TA_INSERT_DT, TA_SOURCE_SYSTEM, TA_SOURCE_ENTITY)
            SELECT DISTINCT ON (s.item_id)
                NEXTVAL('BL_3NF.SEQ_CE_ITEMS_SCD'),
                COALESCE(s.item_id, 'n.a.'),
                COALESCE(s.item_name, 'n.a.'),
                s.pack::INTEGER,
                s.bottle_volume_ml::INTEGER,
                REPLACE(s.state_bottle_retail, ',', '')::DECIMAL(10,2),
                REPLACE(s.state_bottle_cost,   ',', '')::DECIMAL(10,2),
                COALESCE(cat.CATEGORY_ID, -1),
                COALESCE(ven.VENDOR_ID, -1),
                CURRENT_DATE, '9999-12-31', 'Y', CURRENT_DATE,
                'sa_offline_sales', 'src_offline_sales'
            FROM sa_offline_sales.src_offline_sales s
            LEFT JOIN BL_3NF.CE_CATEGORIES cat ON s.category_id = cat.CATEGORY_SRC_ID
            LEFT JOIN BL_3NF.CE_VENDORS     ven ON s.vendor_id   = ven.VENDOR_SRC_ID
            WHERE NOT EXISTS (
                SELECT 1 FROM BL_3NF.CE_ITEMS_SCD t
                WHERE t.ITEM_SRC_ID = s.item_id
                  AND t.TA_IS_ACTIVE = 'Y'
                  AND t.TA_SOURCE_SYSTEM = 'sa_offline_sales'
            )
            ORDER BY s.item_id;
            GET DIAGNOSTICS v_rows_current = ROW_COUNT;
            v_rows_inserted := v_rows_inserted + v_rows_current;
            p_rows := p_rows + v_rows_current;

        ELSIF v_system = 'sa_online_sales' THEN

            UPDATE BL_3NF.CE_ITEMS_SCD t
            SET TA_END_DT    = CURRENT_DATE - 1,
                TA_IS_ACTIVE = 'N'
            WHERE t.TA_IS_ACTIVE = 'Y'
              AND t.TA_SOURCE_SYSTEM = 'sa_online_sales'
              AND NOT EXISTS (
                SELECT 1
                FROM sa_online_sales.src_online_sales src
                WHERE TRIM(src.item_id) = t.ITEM_SRC_ID
                  AND REPLACE(src.state_bottle_retail, ',', '')::DECIMAL(10,2) = t.STATE_BOTTLE_RETAIL
                  AND REPLACE(src.state_bottle_cost,   ',', '')::DECIMAL(10,2) = t.STATE_BOTTLE_COST
              )
              AND EXISTS (
                SELECT 1
                FROM sa_online_sales.src_online_sales src
                WHERE TRIM(src.item_id) = t.ITEM_SRC_ID
              );
            GET DIAGNOSTICS v_rows_current = ROW_COUNT;
            v_rows_updated := v_rows_updated + v_rows_current;
            p_rows := p_rows + v_rows_current;

            INSERT INTO BL_3NF.CE_ITEMS_SCD (
                ITEM_ID, ITEM_SRC_ID, ITEM_NAME, PACK, BOTTLE_VOLUME_ML,
                STATE_BOTTLE_RETAIL, STATE_BOTTLE_COST, CATEGORY_ID, VENDOR_ID,
                TA_START_DT, TA_END_DT, TA_IS_ACTIVE, TA_INSERT_DT, TA_SOURCE_SYSTEM, TA_SOURCE_ENTITY)
            SELECT DISTINCT ON (s.item_id)
                NEXTVAL('BL_3NF.SEQ_CE_ITEMS_SCD'),
                COALESCE(s.item_id, 'n.a.'),
                COALESCE(s.item_name, 'n.a.'),
                s.pack::INTEGER,
                s.bottle_volume_ml::INTEGER,
                REPLACE(s.state_bottle_retail, ',', '')::DECIMAL(10,2),
                REPLACE(s.state_bottle_cost,   ',', '')::DECIMAL(10,2),
                COALESCE(cat.CATEGORY_ID, -1),
                COALESCE(ven.VENDOR_ID, -1),
                CURRENT_DATE, '9999-12-31', 'Y', CURRENT_DATE,
                'sa_online_sales', 'src_online_sales'
            FROM sa_online_sales.src_online_sales s
            LEFT JOIN BL_3NF.CE_CATEGORIES cat ON s.category_id = cat.CATEGORY_SRC_ID
            LEFT JOIN BL_3NF.CE_VENDORS     ven ON s.vendor_id   = ven.VENDOR_SRC_ID
            WHERE NOT EXISTS (
                SELECT 1 FROM BL_3NF.CE_ITEMS_SCD t
                WHERE t.ITEM_SRC_ID = s.item_id
                  AND t.TA_IS_ACTIVE = 'Y'
                  AND t.TA_SOURCE_SYSTEM = 'sa_online_sales'
            )
            ORDER BY s.item_id;
            GET DIAGNOSTICS v_rows_current = ROW_COUNT;
            v_rows_inserted := v_rows_inserted + v_rows_current;
            p_rows := p_rows + v_rows_current;

        END IF;
    END LOOP;

    INSERT INTO BL_CL.MTA_LOGS (PROCEDURE_NAME, ROWS_AFFECTED, LOG_MESSAGE)
    VALUES (p_proc, p_rows, 'SUCCESS');

EXCEPTION WHEN OTHERS THEN

    INSERT INTO BL_CL.MTA_LOGS (PROCEDURE_NAME, ROWS_AFFECTED, LOG_MESSAGE)
    VALUES (p_proc, 0, 'ERROR: ' || SQLERRM);
    RAISE NOTICE 'Error during loading CE_ITEMS_SCD: %', SQLERRM;
END;
$$;

COMMIT;

CREATE OR REPLACE PROCEDURE BL_CL.LOAD_ALL_3NF_DATA()
LANGUAGE plpgsql AS $$
DECLARE
    p_proc VARCHAR := 'BL_CL.LOAD_ALL_3NF_DATA';
BEGIN
	CALL BL_CL.LOAD_CE_DATES();
    CALL BL_CL.LOAD_CE_COUNTIES();
    CALL BL_CL.LOAD_CE_CATEGORIES();
    CALL BL_CL.LOAD_CE_VENDORS();
    CALL BL_CL.LOAD_CE_PROMOTIONS();
    CALL BL_CL.LOAD_CE_STREETS();
	CALL BL_CL.LOAD_CE_CITIES();    
    CALL BL_CL.LOAD_CE_ADDRESSES(); 
    CALL BL_CL.LOAD_CE_ITEMS_SCD(); 
	CALL BL_CL.LOAD_CE_STORES();
	CALL BL_CL.LOAD_CE_SALES();
CALL BL_CL.PRC_WRITE_LOG(p_proc, 0, 'SUCCESS: All procedures completed');

EXCEPTION WHEN OTHERS THEN
    CALL BL_CL.PRC_WRITE_LOG(p_proc, 0, 'CRITICAL ERROR: ' || SQLERRM);
    RAISE NOTICE 'Master Procedure failed: %', SQLERRM;
END;
$$;

CALL BL_CL.LOAD_ALL_3NF_DATA();
SELECT * FROM BL_CL.MTA_LOGS;
DROP TABLE IF EXISTS BL_CL.MTA_LOGS CASCADE;
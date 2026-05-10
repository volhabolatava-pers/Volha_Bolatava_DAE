GRANT USAGE ON SCHEMA BL_DM  TO BL_CL;
GRANT SELECT, INSERT, UPDATE ON ALL TABLES IN SCHEMA BL_DM TO BL_CL;
GRANT USAGE  ON ALL SEQUENCES IN SCHEMA BL_DM TO BL_CL;
COMMIT;

ALTER TABLE BL_DM.DIM_VENDORS 
ADD CONSTRAINT dim_vendors_src_id_unique UNIQUE (VENDOR_SRC_ID);
ALTER TABLE BL_DM.DIM_PROMOTIONS
ADD CONSTRAINT dim_promotions_src_id_unique UNIQUE (PROMO_SRC_ID);
ALTER TABLE BL_DM.DIM_STORES
ADD CONSTRAINT dim_stores_src_id_unique UNIQUE (STORE_SRC_ID);


--DIM_DATES_DAY

CREATE OR REPLACE PROCEDURE BL_CL.LOAD_DIM_DATES_DAY()
LANGUAGE plpgsql AS $$
DECLARE
    p_rows  INTEGER := 0;
    p_proc  VARCHAR := 'BL_CL.LOAD_DIM_DATES_DAY';
BEGIN
INSERT INTO BL_DM.DIM_DATES_DAY (date_id, date_dt, day, day_of_week,is_weekend, month,quarter,year)
SELECT 
to_char (datum, 'YYYYMMDD'):: bigint,
datum::date,
extract(day from datum),
to_char(datum, 'TMDay'),
CASE 
        WHEN extract(isodow from datum) IN (6, 7) THEN TRUE 
        ELSE FALSE
END,
extract(month from datum),
    extract(quarter from datum),
    extract(year from datum)
FROM 
	generate_series('2024-01-01'::date, 
        '2025-12-31'::date, 
        '1 day'::interval) AS datum
WHERE NOT EXISTS (
        SELECT 1 FROM BL_DM.DIM_DATES_DAY t 
        WHERE t.date_id = TO_CHAR(datum, 'YYYYMMDD')::BIGINT
    );
GET DIAGNOSTICS p_rows = ROW_COUNT;
CALL BL_CL.PRC_WRITE_LOG(p_proc, p_rows, 'SUCCESS');
EXCEPTION WHEN OTHERS THEN
    CALL BL_CL.PRC_WRITE_LOG(p_proc, 0, 'ERROR: ' || SQLERRM);
END;
$$;

--DIM_VENDORS

--Define a composite type for DIM_VENDORS
DO $$ 
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'vendor_row_type') 
THEN
        CREATE TYPE BL_CL.vendor_row_type AS (
            vendor_src_id    VARCHAR(10),
            vendor_name      VARCHAR(100));
    END IF;
END $$;

--Loading DIM_VENDORS 
CREATE OR REPLACE PROCEDURE BL_CL.LOAD_DIM_VENDORS()
LANGUAGE plpgsql AS $$
DECLARE
    v_rec       RECORD;
    v_vendor    BL_CL.vendor_row_type;
    p_rows      INTEGER := 0;
    p_proc      VARCHAR := 'BL_CL.LOAD_DIM_VENDORS';
BEGIN
		FOR v_rec IN (SELECT VENDOR_ID, VENDOR_NAME FROM BL_3NF.CE_VENDORS WHERE VENDOR_ID <> -1)
		LOOP
			v_vendor.vendor_src_id := v_rec.VENDOR_ID::VARCHAR(10);
			v_vendor.vendor_name := COALESCE(v_rec.VENDOR_NAME, 'n.a.');
			INSERT INTO BL_DM.DIM_VENDORS (VENDOR_SURR_ID, VENDOR_SRC_ID, VENDOR_NAME, TA_INSERT_DT, TA_UPDATE_DT, TA_SOURCE_SYSTEM, TA_SOURCE_ENTITY)
			VALUES (
			NEXTVAL('BL_DM.SEQ_DIM_VENDORS'),
	    	v_vendor.vendor_src_id,
	    	v_vendor.vendor_name,
	    	CURRENT_DATE,
	    	CURRENT_DATE,
	    	'BL_3NF',
	    	'CE_VENDORS')
	    	ON CONFLICT (VENDOR_SRC_ID) DO UPDATE
	    	SET VENDOR_NAME = EXCLUDED.VENDOR_NAME, 
	    	TA_UPDATE_DT = CURRENT_DATE
	    	WHERE BL_DM.DIM_VENDORS.VENDOR_NAME IS DISTINCT FROM EXCLUDED.VENDOR_NAME;
			IF FOUND THEN
            	p_rows := p_rows + 1;
        	END IF;
		END LOOP;
	CALL BL_CL.PRC_WRITE_LOG(p_proc, p_rows, 'SUCCESS');
EXCEPTION WHEN OTHERS THEN
    CALL BL_CL.PRC_WRITE_LOG(p_proc, 0, 'ERROR: ' || SQLERRM);
END;
$$;

--Loading DIM_PROMOTIONS 
CREATE OR REPLACE PROCEDURE BL_CL.LOAD_DIM_PROMOTIONS()
LANGUAGE plpgsql AS $$
DECLARE
    p_rows INTEGER := 0;
    p_proc VARCHAR := 'BL_CL.LOAD_DIM_PROMOTIONS';
BEGIN
	INSERT INTO BL_DM.DIM_PROMOTIONS (PROMO_SURR_ID, PROMO_SRC_ID, PROMO_NAME, DISCOUNT_PCT, TA_INSERT_DT, TA_UPDATE_DT, TA_SOURCE_SYSTEM, TA_SOURCE_ENTITY)
 SELECT 
    NEXTVAL('BL_DM.SEQ_DIM_PROMOTIONS'),
    p.PROMO_ID::VARCHAR,
    COALESCE(p.PROMO_NAME, 'n.a.'),
    COALESCE(p.DISCOUNT_PCT, -1),
    CURRENT_DATE,
    CURRENT_DATE,
    'BL_3NF',
    'CE_PROMOTIONS'   
    FROM BL_3NF.CE_PROMOTIONS AS p 
    WHERE p.PROMO_ID <> -1 
    ON CONFLICT (PROMO_SRC_ID) DO UPDATE 
    SET PROMO_NAME = EXCLUDED.PROMO_NAME,
        DISCOUNT_PCT = EXCLUDED.DISCOUNT_PCT,
        TA_UPDATE_DT = CURRENT_DATE
    WHERE BL_DM.DIM_PROMOTIONS.PROMO_NAME IS DISTINCT FROM EXCLUDED.PROMO_NAME 
       OR BL_DM.DIM_PROMOTIONS.DISCOUNT_PCT IS DISTINCT FROM EXCLUDED.DISCOUNT_PCT;
GET DIAGNOSTICS p_rows = ROW_COUNT;
    CALL BL_CL.PRC_WRITE_LOG(p_proc, p_rows, 'SUCCESS');
EXCEPTION WHEN OTHERS THEN
    CALL BL_CL.PRC_WRITE_LOG(p_proc, 0, 'ERROR: ' || SQLERRM);
END;
$$;

--Loading DIM_STORES 
CREATE OR REPLACE PROCEDURE BL_CL.LOAD_DIM_STORES()
LANGUAGE plpgsql AS $$
DECLARE
	cur_stores REFCURSOR;
    v_rec      RECORD;
    p_rows     INTEGER := 0;
    p_proc     VARCHAR := 'BL_CL.LOAD_DIM_STORES';
BEGIN
	OPEN cur_stores FOR
	SELECT 
	s.STORE_ID::VARCHAR AS src_id,
    COALESCE(s.STORE_NAME,   'n.a.') AS STORE_NAME,
    COALESCE(s.STORE_TYPE,   'n.a.') AS STORE_TYPE,
    COALESCE(a.HOUSE_NUMBER, -1) AS HOUSE_NUMBER,
    COALESCE(cit.CITY_NAME,    'n.a.') AS CITY_NAME,
    COALESCE(cit.ZIP_CODE,     'n.a.') AS ZIP_CODE,
    COALESCE(c.COUNTY_NAME, 'n.a.') AS COUNTY_NAME
    FROM BL_3NF.CE_STORES as s 
    LEFT JOIN BL_3NF.CE_ADDRESSES a ON s.ADDRESS_ID=a.ADDRESS_ID
	LEFT JOIN BL_3NF.CE_CITIES cit ON a.CITY_ID= cit.CITY_ID
	LEFT JOIN BL_3NF.CE_COUNTIES c ON cit.COUNTY_ID= c.COUNTY_ID
	WHERE s.STORE_ID <> -1;
	LOOP
        FETCH cur_stores INTO v_rec;
        EXIT WHEN NOT FOUND;
INSERT INTO BL_DM.DIM_STORES(STORE_SURR_ID, STORE_SRC_ID, STORE_NAME, STORE_TYPE, HOUSE_NUMBER, CITY_NAME, ZIP_CODE, COUNTY_NAME, TA_INSERT_DT, TA_UPDATE_DT, TA_SOURCE_SYSTEM, TA_SOURCE_ENTITY)
VALUES(
NEXTVAL('BL_DM.SEQ_DIM_STORES'),
v_rec.src_id,
v_rec.STORE_NAME,
v_rec.STORE_TYPE,
v_rec.HOUSE_NUMBER,
v_rec.CITY_NAME,
v_rec.ZIP_CODE,
v_rec.COUNTY_NAME,
CURRENT_DATE,
CURRENT_DATE,
'BL_3NF',
'CE_STORES')
ON CONFLICT (STORE_SRC_ID) DO UPDATE
        SET 
            STORE_NAME   = EXCLUDED.STORE_NAME, 
            STORE_TYPE   = EXCLUDED.STORE_TYPE,
            HOUSE_NUMBER = EXCLUDED.HOUSE_NUMBER,
            CITY_NAME    = EXCLUDED.CITY_NAME, 
            ZIP_CODE     = EXCLUDED.ZIP_CODE,
            COUNTY_NAME  = EXCLUDED.COUNTY_NAME,
            TA_UPDATE_DT = CURRENT_DATE
            WHERE 
            BL_DM.DIM_STORES.STORE_NAME   IS DISTINCT FROM EXCLUDED.STORE_NAME OR
            BL_DM.DIM_STORES.STORE_TYPE   IS DISTINCT FROM EXCLUDED.STORE_TYPE OR
            BL_DM.DIM_STORES.HOUSE_NUMBER   IS DISTINCT FROM EXCLUDED.HOUSE_NUMBER OR
            BL_DM.DIM_STORES.CITY_NAME    IS DISTINCT FROM EXCLUDED.CITY_NAME OR
            BL_DM.DIM_STORES.ZIP_CODE    IS DISTINCT FROM EXCLUDED.ZIP_CODE OR
            BL_DM.DIM_STORES.COUNTY_NAME    IS DISTINCT FROM EXCLUDED.COUNTY_NAME;
	IF FOUND THEN
            	p_rows := p_rows + 1;
        	END IF;
END LOOP;
CLOSE cur_stores;
CALL BL_CL.PRC_WRITE_LOG(p_proc, p_rows, 'SUCCESS');
EXCEPTION WHEN OTHERS THEN
    CALL BL_CL.PRC_WRITE_LOG(p_proc, 0, 'ERROR: ' || SQLERRM);
END;
$$;

--Loading DIM_ITEMS_SCD

CREATE OR REPLACE PROCEDURE BL_CL.LOAD_DIM_ITEMS_SCD()
LANGUAGE plpgsql AS $$
DECLARE
    p_rows_upd INTEGER := 0;
    p_rows_ins INTEGER := 0;
    p_proc     VARCHAR := 'BL_CL.LOAD_DIM_ITEMS_SCD';
BEGIN
	-- Close records that have changed in 3NF
	UPDATE BL_DM.DIM_ITEMS_SCD scd
		SET TA_END_DT = CURRENT_DATE - 1,
			TA_IS_ACTIVE = 'N',
			TA_INSERT_DT = CURRENT_DATE
		 WHERE scd.TA_IS_ACTIVE = 'Y'
			AND scd.ITEM_SURR_ID <> -1
  			AND NOT EXISTS (
          SELECT 1 FROM BL_3NF.CE_ITEMS_SCD i
          WHERE i.ITEM_ID::VARCHAR = scd.ITEM_SRC_ID
			AND i.TA_IS_ACTIVE = 'Y');
	GET DIAGNOSTICS p_rows_upd = ROW_COUNT;

-- Insert new active records
	INSERT INTO BL_DM.DIM_ITEMS_SCD
    (ITEM_SURR_ID, ITEM_SRC_ID, ITEM_NAME, PACK, BOTTLE_VOLUME_ML, STATE_BOTTLE_RETAIL, STATE_BOTTLE_COST, ITEM_CATEGORY_ID, ITEM_CATEGORY_NAME, TA_START_DT, TA_END_DT, TA_IS_ACTIVE, TA_INSERT_DT, TA_SOURCE_SYSTEM, TA_SOURCE_ENTITY)
		SELECT 
		    NEXTVAL('BL_DM.SEQ_DIM_ITEMS_SCD'),
		    i.ITEM_ID::VARCHAR, 
		    COALESCE(i.ITEM_NAME,'n.a.'),
		    i.PACK,
		    i.BOTTLE_VOLUME_ML,
		    i.STATE_BOTTLE_RETAIL,
		    i.STATE_BOTTLE_COST,
		    COALESCE(cat.CATEGORY_ID,   -1),
		    COALESCE(cat.CATEGORY_NAME, 'n.a.'),
		    i.TA_START_DT,
		    '9999-12-31',
		    'Y',
		    CURRENT_DATE,
		    'BL_3NF',
		    'CE_ITEMS_SCD' 
   		 FROM BL_3NF.CE_ITEMS_SCD AS i 
    	LEFT JOIN BL_3NF.CE_CATEGORIES cat ON i.CATEGORY_ID = cat.CATEGORY_ID
		WHERE i.ITEM_ID <> -1
  			AND i.TA_IS_ACTIVE = 'Y'
  			AND NOT EXISTS (
      			SELECT 1 FROM BL_DM.DIM_ITEMS_SCD t
      			WHERE t.ITEM_SRC_ID  = i.ITEM_ID::VARCHAR
        		AND t.TA_IS_ACTIVE = 'Y');
	GET DIAGNOSTICS p_rows_ins = ROW_COUNT;

    CALL BL_CL.PRC_WRITE_LOG(p_proc, p_rows_upd + p_rows_ins, 'SUCCESS');
END;
$$;

ALTER TABLE BL_DM.FCT_SALES_DD RENAME TO FCT_SALES_DD_OLD;
CREATE TABLE BL_DM.FCT_SALES_DD (
    LIKE BL_DM.FCT_SALES_DD_OLD INCLUDING ALL
) PARTITION BY RANGE (EVENT_DT);
CALL BL_CL.MAINTAIN_FCT_PARTITIONS();
INSERT INTO BL_DM.FCT_SALES_DD SELECT * FROM BL_DM.FCT_SALES_DD_OLD;
DROP TABLE BL_DM.FCT_SALES_DD_OLD;
SELECT
    nmsp_parent.nspname AS parent_schema,
    parent.relname      AS parent_table,
    nmsp_child.nspname  AS child_schema,
    child.relname       AS partition_name
FROM pg_inherits
JOIN pg_class parent      ON pg_inherits.inhparent = parent.oid
JOIN pg_class child       ON pg_inherits.inhrelid  = child.oid
JOIN pg_namespace nmsp_parent ON nmsp_parent.oid = parent.relnamespace
JOIN pg_namespace nmsp_child  ON nmsp_child.oid = child.relnamespace
WHERE parent.relname = 'fct_sales_dd';

--Creating partitions 

CREATE OR REPLACE PROCEDURE BL_CL.MAINTAIN_FCT_PARTITIONS()
LANGUAGE plpgsql AS $$
DECLARE
    v_start_date DATE;
    v_end_date   DATE;
    v_curr_date  DATE;
    v_part_name  TEXT;
BEGIN
	BEGIN
        EXECUTE 'CREATE TABLE IF NOT EXISTS BL_DM.FCT_SALES_DD_DEFAULT 
                 PARTITION OF BL_DM.FCT_SALES_DD DEFAULT';
	END;
	SELECT 
	DATE_TRUNC('month', MIN(DATE_DT)), 
	DATE_TRUNC('month', MAX(DATE_DT))
    INTO v_start_date, v_end_date
    FROM BL_3NF.CE_DATES
	WHERE DATE_DT >= '2024-01-01';

	v_curr_date := v_start_date;

	WHILE v_curr_date <= v_end_date LOOP
        v_part_name := LOWER('fct_sales_dd_' || TO_CHAR(v_curr_date, 'YYYY_MM'));

		EXECUTE format('CREATE TABLE IF NOT EXISTS BL_DM.%I (LIKE BL_DM.FCT_SALES_DD INCLUDING ALL)', v_part_name);

BEGIN
    EXECUTE format('ALTER TABLE BL_DM.FCT_SALES_DD ATTACH PARTITION BL_DM.%I FOR VALUES FROM (%L) TO (%L)', v_part_name, v_curr_date, (v_curr_date + INTERVAL '1 month')::DATE);
EXCEPTION 
   WHEN object_not_in_prerequisite_state OR duplicate_table THEN NULL;
END;

v_curr_date := v_curr_date + INTERVAL '1 month';
    END LOOP;
END;
$$;

--Loading FCT_SALES_DD

CREATE OR REPLACE PROCEDURE BL_CL.LOAD_FCT_SALES_DD()
LANGUAGE plpgsql AS $$
DECLARE
	p_rows          INTEGER := 0;
    p_rows_current  INTEGER := 0;
    p_proc          VARCHAR := 'BL_CL.LOAD_FCT_SALES_DD';
    v_month_start   DATE;
    v_month_end     DATE;
    v_partition     TEXT;
    v_window_end    DATE;
BEGIN

SELECT 
      DATE_TRUNC('month', MIN(DATE_DT)), 
      DATE_TRUNC('month', MAX(DATE_DT))
INTO v_month_start, v_window_end
FROM BL_3NF.CE_DATES
WHERE DATE_DT >= '2024-01-01';

WHILE  v_month_start <= v_window_end LOOP
	v_month_end := (v_month_start + INTERVAL '1 month')::DATE;
    v_partition := LOWER('fct_sales_dd_' || TO_CHAR(v_month_start, 'YYYY_MM'));

 IF EXISTS (
        SELECT 1 FROM pg_inherits pi
        JOIN pg_class c ON pi.inhrelid = c.oid
        JOIN pg_namespace n ON c.relnamespace = n.oid
        WHERE n.nspname = 'bl_dm'
        AND c.relname = v_partition
    ) THEN
		EXECUTE format('ALTER TABLE BL_DM.FCT_SALES_DD DETACH PARTITION BL_DM.%I', v_partition);
	EXECUTE format('TRUNCATE TABLE BL_DM.%I', v_partition);
	ELSIF EXISTS (
        SELECT 1 FROM pg_tables
        WHERE schemaname = 'bl_dm'
        AND tablename = v_partition
    ) THEN
	EXECUTE format('TRUNCATE TABLE BL_DM.%I', v_partition);
ELSE
EXECUTE format(
            'CREATE TABLE BL_DM.%I (LIKE BL_DM.FCT_SALES_DD INCLUDING ALL)',
            v_partition
        );
END IF;

EXECUTE format(
            'INSERT INTO BL_DM.%I (EVENT_DT, DATE_ID,STORE_SURR_ID, VENDOR_SURR_ID, ITEM_SURR_ID, PROMO_SURR_ID,FCT_BOTTLES_SOLD_QTY, FCT_SALE_AMT_USD,FCT_VOLUME_SOLD_LTR,  FCT_PROFIT_AMT_USD,TA_INSERT_DT)
SELECT 
	COALESCE(dd.DATE_DT,d.DATE_DT),
    COALESCE(dd.DATE_ID,d.DATE_ID),
    COALESCE(ds.STORE_SURR_ID,  -1),
    COALESCE(dv.VENDOR_SURR_ID, -1),
    COALESCE(di.ITEM_SURR_ID,   -1),
    COALESCE(dp.PROMO_SURR_ID,  -1),
    SUM(COALESCE(cs.BOTTLES_SOLD,       0)),
    SUM(COALESCE(cs.SALE_DOLLARS,       0)),
    SUM(COALESCE(cs.VOLUME_SOLD_LITERS, 0)),
	SUM(COALESCE(cs.SALE_DOLLARS, 0)- COALESCE(i3.STATE_BOTTLE_COST, 0) * COALESCE(cs.BOTTLES_SOLD, 0)),
    CURRENT_DATE
FROM BL_3NF.CE_SALES cs
JOIN  BL_3NF.CE_DATES d   ON cs.DATE_ID  = d.DATE_ID
LEFT JOIN BL_DM.DIM_DATES_DAY dd ON d.DATE_ID  = dd.DATE_ID
JOIN  BL_3NF.CE_STORES  s3   ON cs.STORE_ID = s3.STORE_ID
LEFT JOIN BL_DM.DIM_STORES         ds   ON s3.STORE_ID::VARCHAR = ds.STORE_SRC_ID
LEFT JOIN  BL_3NF.CE_ITEMS_SCD          i3   ON cs.ITEM_ID  = i3.ITEM_ID
                                        AND i3.TA_IS_ACTIVE = ''Y''
LEFT JOIN BL_DM.DIM_ITEMS_SCD      di   ON i3.ITEM_ID::VARCHAR  = di.ITEM_SRC_ID
                                        AND di.TA_IS_ACTIVE = ''Y''
LEFT JOIN BL_3NF.CE_VENDORS        v3   ON i3.VENDOR_ID    = v3.VENDOR_ID
LEFT JOIN BL_DM.DIM_VENDORS        dv   ON v3.VENDOR_ID::VARCHAR = dv.VENDOR_SRC_ID
JOIN  BL_3NF.CE_PROMOTIONS         p3   ON cs.PROMO_ID  = p3.PROMO_ID
LEFT JOIN BL_DM.DIM_PROMOTIONS     dp   ON p3.PROMO_ID::VARCHAR = dp.PROMO_SRC_ID
WHERE d.DATE_DT >= %L
              AND d.DATE_DT <  %L
GROUP BY
    dd.DATE_DT,  dd.DATE_ID,
	d.DATE_DT,   d.DATE_ID,
    ds.STORE_SURR_ID,
    dv.VENDOR_SURR_ID,
    di.ITEM_SURR_ID,
    dp.PROMO_SURR_ID', v_partition,v_month_start, v_month_end);

GET DIAGNOSTICS p_rows_current = ROW_COUNT;
        p_rows := p_rows + p_rows_current;

EXECUTE format('ALTER TABLE BL_DM.FCT_SALES_DD ATTACH PARTITION BL_DM.%I FOR VALUES FROM (%L) TO (%L)', 
                        v_partition, v_month_start, v_month_end);

v_month_start := v_month_end;

END LOOP;

CALL BL_CL.PRC_WRITE_LOG(p_proc, p_rows, 'SUCCESS');
	EXCEPTION WHEN OTHERS THEN
    	CALL BL_CL.PRC_WRITE_LOG(p_proc, 0, 'ERROR: ' || SQLERRM);
END;
$$;

--Master Load Procedure

CREATE OR REPLACE PROCEDURE BL_CL.LOAD_ALL_DM_DATA()
LANGUAGE plpgsql AS $$
DECLARE 
    p_proc   VARCHAR := 'BL_CL.LOAD_ALL_DM_DATA';
BEGIN
	CALL BL_CL.LOAD_DIM_DATES_DAY();
    CALL BL_CL.LOAD_DIM_VENDORS();
    CALL BL_CL.LOAD_DIM_PROMOTIONS();
    CALL BL_CL.LOAD_DIM_STORES();
    CALL BL_CL.LOAD_DIM_ITEMS_SCD();
    CALL BL_CL.LOAD_FCT_SALES_DD();
    
    CALL BL_CL.PRC_WRITE_LOG(p_proc, 0, 'SUCCESS: DM loading process completed');

EXCEPTION WHEN OTHERS THEN
    CALL BL_CL.PRC_WRITE_LOG(p_proc, 0, 'CRITICAL ERROR: ' || SQLERRM);
    RAISE NOTICE 'Master Procedure failed: %', SQLERRM;
END;
$$;

CALL BL_CL.LOAD_ALL_DM_DATA();
SELECT * FROM BL_CL.MTA_LOGS ORDER BY log_id DESC ;

--testing ITEMS_SCD
ALTER FOREIGN TABLE sa_offline_sales.ext_offline_sales 
OPTIONS ( SET filename 'C:/Users/Volha_Bolatava/Projects/Data Warehousing and ETL. Part 1-lab/Offline_sales_test.csv' );

SELECT item_id ,item_src_id ,item_name, ta_start_dt, ta_end_dt, ta_is_active, ta_insert_dt, ta_source_system , ta_source_system   FROM BL_3NF.CE_ITEMS_SCD WHERE ITEM_SRC_ID = '322';

SELECT item_id,item_name , state_bottle_retail, bottles_sold , sale_dollars  FROM sa_offline_sales.src_offline_sales WHERE item_id = '322'

SELECT ITEM_ID, ITEM_SRC_ID, ITEM_NAME, STATE_BOTTLE_RETAIL, 
       TA_IS_ACTIVE, TA_START_DT
FROM BL_3NF.CE_ITEMS_SCD
WHERE ITEM_SRC_ID = '322'
ORDER BY TA_START_DT;
SELECT ITEM_SURR_ID, ITEM_SRC_ID, ITEM_NAME, STATE_BOTTLE_RETAIL,
       TA_IS_ACTIVE, TA_START_DT, TA_END_DT
FROM BL_DM.DIM_ITEMS_SCD
WHERE ITEM_SRC_ID IN (
    SELECT ITEM_ID::VARCHAR FROM BL_3NF.CE_ITEMS_SCD 
    WHERE ITEM_SRC_ID = '322'
);
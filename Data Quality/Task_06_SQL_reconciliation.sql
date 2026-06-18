-- creating an extension for connection to dwh_src_hw_db
CREATE EXTENSION IF NOT EXISTS dblink; 

--creating table for tracking the results of reconciliation 
CREATE TABLE lnd.reconciliation_results (
    table_name VARCHAR(256),
    key_column VARCHAR(256),
    src_id VARCHAR(256),
    trg_id VARCHAR(256),
    reconciliation_status VARCHAR(256)
);



--Source_s1 

--s1_channels -> lnd_s1_channels
INSERT INTO lnd.reconciliation_results (table_name, key_column, src_id, trg_id, reconciliation_status)
WITH reconciliation_results AS (
SELECT     
    's1_channels' AS table_name,
    'channel_id' AS key_column,
    src.channel_id AS src_id,
    trg.channel_id AS trg_id,
    CASE
        WHEN src.channel_id IS NULL THEN 'Only in target'
        WHEN trg.channel_id IS NULL THEN 'Only in source'
        WHEN src.channel_name <> trg.channel_name THEN 'Mismatch in channel_name'
        WHEN src.channel_location <> trg.channellocation THEN 'Mismatch in channel_location'
    END AS reconciliation_status
FROM lnd.lnd_s1_channels trg
FULL OUTER JOIN (
    SELECT * FROM dblink(
        'dbname=dwh_src_hw_db 
         user=postgres 
         password=postgres 
         host=localhost',
        'SELECT * FROM s1.s1_channels'
    ) AS src(channel_id VARCHAR(256), channel_name VARCHAR(256), channel_location VARCHAR(256))
) src
ON src.channel_id = trg.channel_id
)
SELECT table_name, key_column, src_id, trg_id, reconciliation_status 
FROM reconciliation_results
WHERE reconciliation_status IS NOT NULL;

--s1_clients -> lnd_s1_clients

INSERT INTO lnd.reconciliation_results (table_name, key_column, src_id, trg_id, reconciliation_status)
SELECT     
    's1_clients' AS table_name,
    'client_id' AS key_column,
    src.client_id AS src_id,
    trg.client_id AS trg_id,
    CASE
        WHEN trg.client_id IS NULL THEN 'Only in source'
        WHEN src.client_id IS NULL THEN 'Only in target'
        WHEN src.first_name <> trg.first_name THEN 'Mismatch in first_name'
        WHEN src.middle_name <> trg.middle_name THEN 'Mismatch in middle_name'
        WHEN src.last_name <> trg.last_name THEN 'Mismatch in last_name'
        WHEN src.email <> trg.email THEN 'Mismatch in email'
        WHEN src.phone <> trg.phone THEN 'Mismatch in phone'
        WHEN src.first_purchase <> trg.first_purchase THEN 'Mismatch in first_purchase'
    END AS reconciliation_status
FROM lnd.lnd_s1_clients trg
FULL OUTER JOIN (
    SELECT * FROM dblink(
        'dbname=dwh_src_hw_db 
         user=postgres 
         password=postgres 
         host=localhost',
        'SELECT client_id, first_name, middle_name, last_name, email, phone, first_purchase FROM s1.s1_clients'
    ) AS src(client_id VARCHAR(256), first_name VARCHAR(256), middle_name VARCHAR(256), last_name VARCHAR(256), email VARCHAR(256), phone VARCHAR(256), first_purchase VARCHAR(256))
) src ON src.client_id = trg.client_id
WHERE trg.client_id IS NULL 
   OR src.client_id IS NULL 
   OR src.first_name <> trg.first_name 
   OR src.middle_name <> trg.middle_name 
   OR src.last_name <> trg.last_name 
   OR src.email <> trg.email 
   OR src.phone <> trg.phone 
   OR src.first_purchase <> trg.first_purchase;


--s1_products -> lnd_s1_products

INSERT INTO lnd.reconciliation_results (table_name, key_column, src_id, trg_id, reconciliation_status)
SELECT     
    's1_products' AS table_name,
    'product_id' AS key_column,
    src.product_id AS src_id,
    trg.product_id AS trg_id,
    CASE
        WHEN trg.product_id IS NULL THEN 'Only in source'
        WHEN src.product_id IS NULL THEN 'Only in target'
        WHEN src.cost <> trg.cost THEN 'Mismatch in cost'
        WHEN src.product_name <> trg.product_name THEN 'Mismatch in product_name'
    END AS reconciliation_status
FROM lnd.lnd_s1_products trg
FULL OUTER JOIN (
    SELECT * FROM dblink(
        'dbname=dwh_src_hw_db user=postgres password=postgres host=localhost',
        'SELECT product_id, cost, product_name FROM s1.s1_products'
    ) AS src(product_id VARCHAR(256), cost VARCHAR(256), product_name VARCHAR(256))
) src ON src.product_id = trg.product_id
WHERE trg.product_id IS NULL 
   OR src.product_id IS NULL 
   OR src.cost <> trg.cost 
   OR src.product_name <> trg.product_name;

--s1_sales -> lnd_s1_sales
INSERT INTO lnd.reconciliation_results (table_name, key_column, src_id, trg_id, reconciliation_status)
SELECT     
    's1_sales' AS table_name,
    'client_id' AS key_column,
    src.client_id AS src_id,
    trg.client_id AS trg_id,
    CASE
        WHEN trg.client_id IS NULL THEN 'Only in source'
        WHEN src.client_id IS NULL THEN 'Only in target'
        WHEN src.channel_id <> trg.channel_id THEN 'Mismatch in channel_id'
        WHEN src.sale_date <> trg.sale_date THEN 'Mismatch in sale_date'
        WHEN src.units <> trg.units THEN 'Mismatch in units'
        WHEN src.product_id <> trg.product_id THEN 'Mismatch in product_id'
        WHEN src.purchase_date <> trg.purchase_date THEN 'Mismatch in purchase_date'
    END AS reconciliation_status
FROM lnd.lnd_s1_sales trg
FULL OUTER JOIN (
    SELECT * FROM dblink(
        'dbname=dwh_src_hw_db user=postgres password=postgres host=localhost',
        'SELECT client_id, channel_id, sale_date, units, product_id, purchase_date FROM s1.s1_sales'
    ) AS src(client_id VARCHAR(256), channel_id VARCHAR(256), sale_date VARCHAR(256), units VARCHAR(256), product_id VARCHAR(256), purchase_date VARCHAR(256))
) src ON src.client_id = trg.client_id AND src.product_id = trg.product_id AND src.sale_date = trg.sale_date
WHERE trg.client_id IS NULL 
   OR src.client_id IS NULL 
   OR src.channel_id <> trg.channel_id 
   OR src.sale_date <> trg.sale_date 
   OR src.units <> trg.units 
   OR src.product_id <> trg.product_id 
   OR src.purchase_date <> trg.purchase_date;


--Source_s2

--s2_channels -> lnd_s2_channels
INSERT INTO lnd.reconciliation_results (table_name, key_column, src_id, trg_id, reconciliation_status)
SELECT     
    's2_channels' AS table_name,
    'channel_id' AS key_column,
    src.channel_id AS src_id,
    trg.channel_id AS trg_id,
    CASE
        WHEN trg.channel_id IS NULL THEN 'Only in source'
        WHEN src.channel_id IS NULL THEN 'Only in target'
        WHEN src.channel_name <> trg.channel_name THEN 'Mismatch in channel_name'
        WHEN src.location_id <> trg.location_id THEN 'Mismatch in location_id'
    END AS reconciliation_status
FROM lnd.lnd_s2_channels trg
FULL OUTER JOIN (
    SELECT * FROM dblink(
        'dbname=dwh_src_hw_db user=postgres password=postgres host=localhost',
        'SELECT channel_id, channel_name, location_id FROM s2.s2_channels'
    ) AS src(channel_id VARCHAR(256), channel_name VARCHAR(256), location_id VARCHAR(256))
) src ON src.channel_id = trg.channel_id
WHERE trg.channel_id IS NULL 
   OR src.channel_id IS NULL 
   OR src.channel_name <> trg.channel_name 
   OR src.location_id <> trg.location_id;

--s2_clients -> lnd_s2_clients

INSERT INTO lnd.reconciliation_results (table_name, key_column, src_id, trg_id, reconciliation_status)
SELECT     
    's2_clients' AS table_name,
    'client_id' AS key_column,
    src.client_id AS src_id,
    trg.client_id AS trg_id,
    CASE
        WHEN trg.client_id IS NULL THEN 'Only in source'
        WHEN src.client_id IS NULL THEN 'Only in target'
        WHEN src.first_name <> trg.first_name THEN 'Mismatch in first_name'
        WHEN src.last_name <> trg.last_name THEN 'Mismatch in last_name'
        WHEN src.email <> trg.email THEN 'Mismatch in email'
        WHEN src.phone_code <> trg.phone_code THEN 'Mismatch in phone_code'
        WHEN src.phone_number <> trg.phone_number THEN 'Mismatch in phone_number'
        WHEN src.first_purchase <> trg.first_purchase THEN 'Mismatch in first_purchase'
        WHEN src.valid_from <> trg.valid_from THEN 'Mismatch in valid_from'
        WHEN src.valid_to <> trg.valid_to THEN 'Mismatch in valid_to'
    END AS reconciliation_status
FROM lnd.lnd_s2_clients trg
FULL OUTER JOIN (
    SELECT * FROM dblink(
        'dbname=dwh_src_hw_db 
         user=postgres 
         password=postgres 
         host=localhost',
        'SELECT client_id, first_name,last_name, email, phone_code,phone_number, first_purchase,valid_from, valid_to FROM s2.s2_clients'
    ) AS src(client_id VARCHAR(256), first_name VARCHAR(256), last_name VARCHAR(256), email VARCHAR(256), phone_code VARCHAR(256), phone_number VARCHAR(256), first_purchase VARCHAR(256),valid_from VARCHAR(256), valid_to VARCHAR(256))
) src ON src.client_id = trg.client_id
WHERE trg.client_id IS NULL 
   OR src.client_id IS NULL 
   OR src.first_name <> trg.first_name 
   OR src.last_name <> trg.last_name 
   OR src.email <> trg.email 
   OR src.phone_code <> trg.phone_code 
   OR src.phone_number <> trg.phone_number 
   OR src.first_purchase <> trg.first_purchase 
   OR src.valid_from <> trg.valid_from 
   OR src.valid_to <> trg.valid_to;

--s2_locations -> lnd_s2_locations

INSERT INTO lnd.reconciliation_results (table_name, key_column, src_id, trg_id, reconciliation_status)
SELECT     
    's2_locations' AS table_name,
    'location_id' AS key_column,
    src.location_id AS src_id,
    trg.location_id AS trg_id,
    CASE
        WHEN trg.location_id IS NULL THEN 'Only in source'
        WHEN src.location_id IS NULL THEN 'Only in target'
        WHEN src.location_name <> trg.location_name THEN 'Mismatch in location_name'
    END AS reconciliation_status
FROM lnd.lnd_s2_locations trg
FULL OUTER JOIN (
    SELECT * FROM dblink(
        'dbname=dwh_src_hw_db user=postgres password=postgres host=localhost',
        'SELECT location_id, location_name FROM s2.s2_locations'
    ) AS src(location_id VARCHAR(256), location_name VARCHAR(256))
) src ON src.location_id = trg.location_id
WHERE trg.location_id IS NULL 
   OR src.location_id IS NULL 
   OR src.location_name <> trg.location_name;

--s2_client_sales -> lnd_s2_client_sales

INSERT INTO lnd.reconciliation_results (table_name, key_column, src_id, trg_id, reconciliation_status)
SELECT     
    's2_client_sales' AS table_name,
    'client_id' AS key_column,
    src.client_id AS src_id,
    trg.client_id AS trg_id,
    CASE
        WHEN trg.client_id IS NULL THEN 'Only in source'
        WHEN src.client_id IS NULL THEN 'Only in target'
        WHEN src.channel_id <> trg.channel_id THEN 'Mismatch in channel_id'
        WHEN src.saled_at <> trg.saled_at THEN 'Mismatch in saled_at'
        WHEN src.product_id <> trg.product_id THEN 'Mismatch in product_id'
        WHEN src.product_name <> trg.product_name THEN 'Mismatch in product_name'
        WHEN src.product_price <> trg.product_price THEN 'Mismatch in product_price'
        WHEN src.product_amount <> trg.product_amount THEN 'Mismatch in product_amount'
        WHEN src.sold_date <> trg.sold_date THEN 'Mismatch in sold_date'
    END AS reconciliation_status
FROM lnd.lnd_s2_client_sales trg
FULL OUTER JOIN (
    SELECT * FROM dblink(
        'dbname=dwh_src_hw_db user=postgres password=postgres host=localhost',
        'SELECT client_id, channel_id, saled_at, product_id, product_name, product_price, product_amount, sold_date FROM s2.s2_client_sales'
    ) AS src(client_id VARCHAR(256), channel_id VARCHAR(256), saled_at VARCHAR(256), product_id VARCHAR(256), product_name VARCHAR(256), product_price VARCHAR(256), product_amount VARCHAR(256), sold_date VARCHAR(256))
) src ON src.client_id = trg.client_id AND src.product_id = trg.product_id AND src.saled_at = trg.saled_at
WHERE trg.client_id IS NULL 
   OR src.client_id IS NULL 
   OR src.channel_id <> trg.channel_id 
   OR src.saled_at <> trg.saled_at 
   OR src.product_id <> trg.product_id 
   OR src.product_name <> trg.product_name 
   OR src.product_price <> trg.product_price 
   OR src.product_amount <> trg.product_amount 
   OR src.sold_date <> trg.sold_date;

SELECT * FROM lnd.reconciliation_results;
SELECT count(*) FROM lnd.reconciliation_results;

SELECT 
    table_name,
    reconciliation_status,
    COUNT(*) AS discrepancy_count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 2) AS percentage_of_total_errors
FROM lnd.reconciliation_results
GROUP BY table_name, reconciliation_status
ORDER BY table_name, discrepancy_count DESC;
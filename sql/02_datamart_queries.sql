-- ============================================================
-- Task 2b - Datamart Queries (self-contained version)
-- 02_datamart_queries.sql
--
-- This file is fully standalone. It:
--   1. Creates the _clean tables from _raw tables (Task 2a logic in SQL)
--   2. Builds the two datamart report tables     (Task 2b logic)
--
-- Run order:
--   Step 1: 01_setup_raw_tables.sql   (creates + seeds raw tables)
--   Step 2: This file                 (cleans + builds datamarts)
--
-- Safe to re-run: all tables are dropped and recreated each time.
-- ============================================================

USE astraworld;


-- ============================================================
-- PART 1: CLEAN TABLES  (Task 2a logic in SQL)
-- ============================================================


-- ------------------------------------------------------------
-- 1A. customers_clean
--     Fixes: 3 dob formats, sentinel 1900-01-01, adds customer_type
-- ------------------------------------------------------------
DROP TABLE IF EXISTS customers_clean;

CREATE TABLE customers_clean AS
SELECT
    id,
    name,

    CASE
        -- Sentinel: clearly not a real birthday
        WHEN dob = '1900-01-01'
                                    THEN NULL

        -- Format: YYYY-MM-DD  (ISO standard)
        WHEN dob REGEXP '^[0-9]{4}-[0-9]{2}-[0-9]{2}$'
                                    THEN STR_TO_DATE(dob, '%Y-%m-%d')

        -- Format: YYYY/MM/DD  (slash separator, year first)
        WHEN dob REGEXP '^[0-9]{4}/[0-9]{2}/[0-9]{2}$'
                                    THEN STR_TO_DATE(dob, '%Y/%m/%d')

        -- Format: DD/MM/YYYY  (day first, common in Indonesia)
        WHEN dob REGEXP '^[0-9]{2}/[0-9]{2}/[0-9]{4}$'
                                    THEN STR_TO_DATE(dob, '%d/%m/%Y')

        ELSE NULL
    END                             AS dob,

    CASE
        WHEN name LIKE 'PT %'
          OR name LIKE 'CV %'
          OR name LIKE 'UD %'      THEN 'COMPANY'
        ELSE                            'INDIVIDUAL'
    END                             AS customer_type,

    created_at

FROM customers_raw;


-- ------------------------------------------------------------
-- 1B. sales_clean
--     Fixes: price string "350.000.000" to integer, flags duplicates
-- ------------------------------------------------------------
DROP TABLE IF EXISTS sales_clean;

CREATE TABLE sales_clean AS
SELECT
    vin,
    customer_id,
    model,
    invoice_date,

    -- Remove Indonesian thousands-separator dots, cast to integer
    CAST(REPLACE(price, '.', '') AS SIGNED)     AS price,

    -- Window function: count rows with same customer+model+date
    -- If count > 1, this row belongs to a duplicate group
    CASE
        WHEN COUNT(*) OVER (
            PARTITION BY customer_id, model, invoice_date
        ) > 1       THEN 1
        ELSE             0
    END                                         AS is_duplicate_flag,

    created_at

FROM sales_raw;


-- ------------------------------------------------------------
-- 1C. after_sales_clean
--     Fixes: flag VINs not found in sales_raw (orphan records)
-- ------------------------------------------------------------
DROP TABLE IF EXISTS after_sales_clean;

CREATE TABLE after_sales_clean AS
SELECT
    a.service_ticket,
    a.vin,
    a.customer_id,
    a.model,
    a.service_date,
    a.service_type,

    -- LEFT JOIN: if no matching VIN in sales_raw, s.vin will be NULL = orphan
    CASE
        WHEN s.vin IS NULL  THEN 1
        ELSE                     0
    END                         AS is_orphan_vin,

    a.created_at

FROM after_sales_raw a
LEFT JOIN sales_raw s ON a.vin = s.vin;


-- ------------------------------------------------------------
-- 1D. customer_addresses_clean
--     Fixes: inconsistent city/province casing
-- ------------------------------------------------------------
DROP TABLE IF EXISTS customer_addresses_clean;

CREATE TABLE customer_addresses_clean AS
SELECT
    id,
    customer_id,
    address,

    CASE
        WHEN UPPER(city) = 'JAKARTA PUSAT'     THEN 'Jakarta Pusat'
        WHEN UPPER(city) = 'TANGERANG SELATAN' THEN 'Tangerang Selatan'
        WHEN UPPER(city) = 'JAKARTA UTARA'     THEN 'Jakarta Utara'
        WHEN UPPER(city) = 'JAKARTA SELATAN'   THEN 'Jakarta Selatan'
        WHEN UPPER(city) = 'JAKARTA BARAT'     THEN 'Jakarta Barat'
        WHEN UPPER(city) = 'JAKARTA TIMUR'     THEN 'Jakarta Timur'
        ELSE CONCAT(
                UPPER(SUBSTRING(LOWER(city), 1, 1)),
                LOWER(SUBSTRING(city, 2))
             )
    END                         AS city,

    CASE
        WHEN UPPER(province) = 'DKI JAKARTA'   THEN 'DKI Jakarta'
        WHEN UPPER(province) = 'JAWA BARAT'    THEN 'Jawa Barat'
        WHEN UPPER(province) = 'JAWA TENGAH'   THEN 'Jawa Tengah'
        WHEN UPPER(province) = 'JAWA TIMUR'    THEN 'Jawa Timur'
        ELSE CONCAT(
                UPPER(SUBSTRING(LOWER(province), 1, 1)),
                LOWER(SUBSTRING(province, 2))
             )
    END                         AS province,

    created_at

FROM customer_addresses_raw;


-- ============================================================
-- PART 2: DATAMART TABLES  (Task 2b)
-- ============================================================


-- ------------------------------------------------------------
-- 2A. dm_sales_summary
--     Sales grouped by month / price class / model
-- ------------------------------------------------------------
DROP TABLE IF EXISTS dm_sales_summary;

CREATE TABLE dm_sales_summary (
    periode     CHAR(7)     NOT NULL,
    class       VARCHAR(10) NOT NULL,
    model       VARCHAR(50) NOT NULL,
    total       BIGINT      NOT NULL,
    updated_at  DATETIME    DEFAULT CURRENT_TIMESTAMP
);

INSERT INTO dm_sales_summary (periode, class, model, total)
SELECT
    DATE_FORMAT(invoice_date, '%Y-%m')          AS periode,

    CASE
        WHEN price >= 100000000
         AND price <  250000000                 THEN 'LOW'
        WHEN price >= 250000000
         AND price <  400000000                 THEN 'MEDIUM'
        WHEN price >= 400000000                 THEN 'HIGH'
        ELSE                                         'UNKNOWN'
    END                                         AS class,

    model,
    SUM(price)                                  AS total

FROM   sales_clean
WHERE  is_duplicate_flag = 0
GROUP  BY periode, class, model
ORDER  BY periode, class, model;


-- ------------------------------------------------------------
-- 2B. dm_aftersales_activity
--     After-sales activity per customer / vehicle / year
-- ------------------------------------------------------------
DROP TABLE IF EXISTS dm_aftersales_activity;

CREATE TABLE dm_aftersales_activity (
    periode         CHAR(4)      NOT NULL,
    vin             VARCHAR(20)  NOT NULL,
    customer_name   VARCHAR(100),
    address         VARCHAR(500),
    count_service   INT          NOT NULL,
    priority        VARCHAR(10)  NOT NULL,
    updated_at      DATETIME     DEFAULT CURRENT_TIMESTAMP
);

INSERT INTO dm_aftersales_activity
    (periode, vin, customer_name, address, count_service, priority)
SELECT
    DATE_FORMAT(a.service_date, '%Y')           AS periode,
    a.vin,
    c.name                                      AS customer_name,

    COALESCE(
        CONCAT_WS(', ', addr.address, addr.city, addr.province),
        'No address on file'
    )                                           AS address,

    COUNT(a.service_ticket)                     AS count_service,

    CASE
        WHEN COUNT(a.service_ticket) >  10      THEN 'HIGH'
        WHEN COUNT(a.service_ticket) >= 5       THEN 'MED'
        ELSE                                         'LOW'
    END                                         AS priority

FROM  after_sales_clean             a
LEFT JOIN customers_clean           c    ON a.customer_id = c.id
LEFT JOIN customer_addresses_clean  addr ON a.customer_id = addr.customer_id

GROUP BY
    DATE_FORMAT(a.service_date, '%Y'),
    a.vin,
    c.name,
    addr.address,
    addr.city,
    addr.province

ORDER BY periode, priority DESC, count_service DESC;

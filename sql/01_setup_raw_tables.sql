-- ============================================================
-- AstraWorld Technical Test
-- 01_setup_raw_tables.sql
-- Run this ONCE to create raw tables and seed sample data
-- ============================================================

CREATE DATABASE IF NOT EXISTS astraworld;
USE astraworld;

-- -------------------------------------------------------
-- customers_raw
-- Intentionally dirty: mixed dob formats, NULL, sentinel
-- -------------------------------------------------------
DROP TABLE IF EXISTS customers_raw;
CREATE TABLE customers_raw (
    id          INT PRIMARY KEY,
    name        VARCHAR(100),
    dob         VARCHAR(20),        -- stored as VARCHAR because formats are inconsistent
    created_at  DATETIME(3)
);

INSERT INTO customers_raw VALUES
(1, 'Antonio',       '1998-08-04',   '2025-03-01 14:24:40.012'),
(2, 'Brandon',       '2001-04-21',   '2025-03-02 08:12:54.003'),
(3, 'Charlie',       '1980/11/15',   '2025-03-02 11:20:02.391'),
(4, 'Dominikus',     '14/01/1995',   '2025-03-03 09:50:41.852'),
(5, 'Erik',          '1900-01-01',   '2025-03-03 17:22:03.198'),
(6, 'PT Black Bird', NULL,           '2025-03-04 12:52:16.122');

-- -------------------------------------------------------
-- sales_raw
-- Dirty: price as string with dots, potential duplicates
-- -------------------------------------------------------
DROP TABLE IF EXISTS sales_raw;
CREATE TABLE sales_raw (
    vin           VARCHAR(20),
    customer_id   INT,
    model         VARCHAR(50),
    invoice_date  DATE,
    price         VARCHAR(20),       -- stored as VARCHAR: "350.000.000"
    created_at    DATETIME(3)
);

INSERT INTO sales_raw VALUES
('JIS8135SAD', 1, 'RAIZA', '2025-03-01', '350.000.000', '2025-03-01 14:24:40.012'),
('MAS8160POE', 3, 'RANGGO','2025-05-19', '430.000.000', '2025-05-19 14:29:21.003'),
('JLK1368KDE', 4, 'INNAVO','2025-05-22', '600.000.000', '2025-05-22 16:10:28.120'),
('JLK1869KDF', 6, 'VELOS', '2025-08-02', '390.000.000', '2025-08-02 14:04:31.021'),
('JLK1962KOP', 6, 'VELOS', '2025-08-02', '390.000.000', '2025-08-02 15:21:04.201');

-- -------------------------------------------------------
-- after_sales_raw
-- Dirty: one orphan VIN not in sales_raw
-- -------------------------------------------------------
DROP TABLE IF EXISTS after_sales_raw;
CREATE TABLE after_sales_raw (
    service_ticket  VARCHAR(20),
    vin             VARCHAR(20),
    customer_id     INT,
    model           VARCHAR(50),
    service_date    DATE,
    service_type    VARCHAR(10),     -- BP=Body Paint, PM=Periodic Maintenance, GR=General Repair
    created_at      DATETIME(3)
);

INSERT INTO after_sales_raw VALUES
('T124-kgu1', 'MAS8160POE', 3, 'RANGGO', '2025-07-11', 'BP', '2025-07-11 09:24:40.012'),
('T560-jga1', 'JLK1368KDE', 4, 'INNAVO', '2025-08-04', 'PM', '2025-08-04 10:12:54.003'),
('T521-oai8', 'POI1059IIK', 5, 'RAIZA',  '2026-09-10', 'GR', '2026-09-10 12:45:02.391');
-- ^ POI1059IIK does not exist in sales_raw — this is an orphan record

-- -------------------------------------------------------
-- customer_addresses_raw
-- Landing table for ingested CSVs (Task 1 writes here)
-- -------------------------------------------------------
DROP TABLE IF EXISTS customer_addresses_raw;
CREATE TABLE customer_addresses_raw (
    id            INT,
    customer_id   INT,
    address       VARCHAR(255),
    city          VARCHAR(100),
    province      VARCHAR(100),
    created_at    DATETIME(3),
    ingested_at   DATETIME DEFAULT CURRENT_TIMESTAMP,   -- added by pipeline
    source_file   VARCHAR(100)                          -- tracks which CSV this came from
);

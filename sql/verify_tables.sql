SELECT 'customers_clean'         AS table_name, COUNT(*) AS `rows` FROM customers_clean
UNION ALL
SELECT 'sales_clean',              COUNT(*) FROM sales_clean
UNION ALL
SELECT 'after_sales_clean',        COUNT(*) FROM after_sales_clean
UNION ALL
SELECT 'customer_addresses_clean', COUNT(*) FROM customer_addresses_clean
UNION ALL
SELECT 'dm_sales_summary',         COUNT(*) FROM dm_sales_summary
UNION ALL
SELECT 'dm_aftersales_activity',   COUNT(*) FROM dm_aftersales_activity;

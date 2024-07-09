SELECT country_code, COUNT(*) AS total_no_stores
FROM dim_store_details
GROUP BY country_code;

SELECT locality, COUNT(*) AS total_no_stores
FROM dim_store_details
GROUP BY locality
ORDER BY total_no_stores DESC;

SELECT SUM(product_quantity * product_price) AS total_sales, month
FROM orders_table
JOIN dim_products ON orders_table.product_code = dim_products.product_code
JOIN dim_date_times ON orders_table.date_uuid = dim_date_times.date_uuid
GROUP BY month
ORDER BY total_sales DESC;

SELECT 
	COUNT(*) AS number_of_sales, 
	SUM(product_quantity) AS product_quantity_count,
    CASE 
       WHEN store_type = 'Web Portal' THEN 'Web'
       ELSE 'Offline'
    END AS location
FROM orders_table
JOIN dim_products ON orders_table.product_code = dim_products.product_code
JOIN dim_store_details ON orders_table.store_code = dim_store_details.store_code
GROUP BY location
ORDER by number_of_sales DESC;


WITH sales_table AS (SELECT
	store_type,
	SUM(product_quantity * product_price) AS total_sales
FROM orders_table
JOIN dim_products ON orders_table.product_code = dim_products.product_code
JOIN dim_store_details ON orders_table.store_code = dim_store_details.store_code
GROUP BY store_type
ORDER by total_sales DESC)
SELECT 
	store_type, 
	total_sales,
	(total_sales * 100.0)/(SUM(total_sales) OVER ()) AS "percentage_total(%)"
FROM sales_table;


SELECT
	SUM(product_quantity * product_price) AS total_sales,
	year,
	month
FROM orders_table
JOIN dim_products ON orders_table.product_code = dim_products.product_code
JOIN dim_date_times ON orders_table.date_uuid = dim_date_times.date_uuid
GROUP BY year, month
ORDER BY total_sales DESC
LIMIT 10;


SELECT
	SUM(staff_numbers) AS total_staff_numbers,
	country_code
FROM dim_store_details
GROUP BY country_code
ORDER BY total_staff_numbers DESC;


SELECT
	SUM(product_quantity * product_price) AS total_sales,
	store_type,
	country_code
FROM orders_table
JOIN dim_products ON orders_table.product_code = dim_products.product_code
JOIN dim_store_details ON orders_table.store_code = dim_store_details.store_code
WHERE country_code = 'DE'
GROUP BY store_type, country_code;



WITH date_time_table AS (SELECT date_uuid, year, make_timestamp(
    CAST(year AS INTEGER),
    CAST(month AS INTEGER),
    CAST(day AS INTEGER),
	EXTRACT(HOUR FROM to_timestamp(timestamp, 'HH24:MI:SS'))::INTEGER,
    EXTRACT(MINUTE FROM to_timestamp(timestamp, 'HH24:MI:SS'))::INTEGER,
    EXTRACT(SECOND FROM to_timestamp(timestamp, 'HH24:MI:SS'))::INTEGER) AS date_time
FROM dim_date_times),
	sales_times AS (SELECT
    year, orders_table.date_uuid, date_time_table.date_time
FROM
    orders_table
JOIN date_time_table ON orders_table.date_uuid = date_time_table.date_uuid
	ORDER BY date_time),

    time_diff_table AS
(SELECT 
	year,
	 LEAD(date_time) OVER (PARTITION BY year) - date_time AS time_difference	
FROM sales_times)

SELECT year, 
       AVG(time_difference) AS  actual_time_taken
FROM time_diff_table
GROUP BY year
ORDER BY actual_time_taken DESC
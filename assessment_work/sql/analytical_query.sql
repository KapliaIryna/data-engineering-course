-- Аналітичний запит: в якому штаті найбільше TV-покупок від клієнтів 20-30 років
-- за першу декаду вересня 2022?
-- Відповідь: Idaho (179 покупок)

WITH customer_sales AS (
    SELECT
        s.client_id,
        s.purchase_date,
        s.product_name,
        s.price,
        c.state,
        c.birth_date,
        DATEDIFF(year, c.birth_date, s.purchase_date) AS age
    FROM silver.sales s
    JOIN gold.user_profiles_enriched c ON s.client_id = c.client_id
    WHERE s.purchase_date BETWEEN '2022-09-01' AND '2022-09-10'
      AND LOWER(s.product_name) LIKE '%tv%'
      AND c.birth_date IS NOT NULL
      AND c.state IS NOT NULL
)
SELECT
    state,
    COUNT(*) AS tv_purchases,
    COUNT(DISTINCT client_id) AS unique_customers,
    SUM(price) AS total_revenue
FROM customer_sales
WHERE age BETWEEN 20 AND 30
GROUP BY state
ORDER BY tv_purchases DESC
LIMIT 1;

-- Data Cleaning: After reviewing, the dataset is quite clear so I keep the original format.

-- Data Exploratory:


--Query 1: Calculate total visit, pageview, transaction and revenue for Jan, Feb and March 2017 

SELECT
  format_date ('%Y%m', parse_date('%Y%m%d', date)) as month,
  sum(totals.visits) as visits,
  sum(totals.pageviews) as pageviews,
  sum(totals.transactions) as transactions,
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_2017*`
WHERE _table_suffix between '0101' and '0331'
group by month  --group by 1
order by month; --order by 1


--Query 2: Bounce rate per traffic source in July 2017 (Bounce_rate = num_bounce/total_visit) 

SELECT 
  trafficsource.source as source,
  SUM(totals.visits) as total_visits,
  SUM(totals.bounces) as total_no_of_bounces,
  SUM(totals.bounces)/SUM(totals.visits)*100 as Bounce_rate
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_201707*`
GROUP BY trafficsource.source
ORDER BY total_visits DESC;


--Query 3: Revenue by traffic source by week, by month in June 2017

SELECT
  "Month" as time_type,
  format_date ('%Y%m', parse_date('%Y%m%d', date)) as time,
  trafficsource.source as source,
  sum(product.productRevenue/Power(10,6))  as Revenue
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_201706*`,
UNNEST (hits) AS hits,
UNNEST (hits.product) AS product
GROUP BY time, source

UNION ALL

SELECT
  "Week" as time_type,
  format_date ('%Y%W', parse_date('%Y%m%d', date)) as time,
  trafficsource.source as source,
  sum(product.productRevenue/Power(10,6)) as Revenue 
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_201706*`,
UNNEST (hits) AS hits,
UNNEST (hits.product) AS product
GROUP BY time, source
ORDER BY Revenue DESC;


--Query 04: Average number of pageviews by purchaser type (purchasers vs non-purchasers) in June, July 2017

WITH Purchaser AS (
    SELECT 
        format_date('%Y%m', parse_date('%Y%m%d', date)) as month,
        SUM(totals.pageviews)/count (distinct fullvisitorID) as avg_pageviews_purchase
    FROM `bigquery-public-data.google_analytics_sample.ga_sessions_2017*`,
      UNNEST (hits) AS hits,
      UNNEST (hits.product) AS product
    WHERE _table_suffix between '0601' and '0731'
      AND totals.transactions >=1 AND product.productRevenue is not null
    GROUP BY month),

Non_Purchaser AS (
    SELECT 
        format_date('%Y%m', parse_date('%Y%m%d', date)) as month,
        SUM(totals.pageviews)/count (distinct fullvisitorID) as avg_pageviews_non_purchase
    FROM `bigquery-public-data.google_analytics_sample.ga_sessions_2017*`,
      UNNEST (hits) AS hits,
      UNNEST (hits.product) AS product
    WHERE _table_suffix between '0601' and '0731'
      AND totals.transactions is null AND product.productRevenue is null
    GROUP BY month)

SELECT Month, 
      avg_pageviews_purchase, 
      avg_pageviews_non_purchase
FROM Purchaser
LEFT JOIN Non_Purchaser
USING (month);


--Query 05: Average number of transactions per user that made a purchase in July 2017 

SELECT format_date('%Y%m', parse_date('%Y%m%d', date)) as month,
      sum (totals.transactions)/ count (distinct fullvisitorID) as Avg_total_transactions_per_user
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_201707*`,
UNNEST (hits) AS hits,
UNNEST (hits.product) AS product
WHERE totals.transactions >=1 
and product.productRevenue is not null
GROUP BY month;


--Query 06: Average amount of money spent per session. Only include purchaser data in July 2017

SELECT format_date('%Y%m', parse_date('%Y%m%d', date)) as month, 
  ((sum(product.productRevenue)/sum(totals.visits))/power(10,6)) as avg_revenue_by_user_per_visit
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_201707*`,
UNNEST (hits) AS hits,
UNNEST (hits.product) AS product
WHERE totals.transactions IS NOT NULL AND totals.transactions >=1
and product.productRevenue is not null
GROUP BY month;


--Query 07: Other products purchased by customers who purchased product "YouTube Men's Vintage Henley" in July 2017. 

SELECT product.v2ProductName as other_purchased_products, 
      sum(product.productQuantity) as Quantity
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_201707*`,
  UNNEST (hits) AS hits,
  UNNEST (hits.product) AS product
WHERE fullvisitorID 
IN (
    SELECT fullvisitorID
    FROM `bigquery-public-data.google_analytics_sample.ga_sessions_201707*`,
    UNNEST (hits) AS hits,
    UNNEST (hits.product) AS product
    WHERE product.v2productname = "YouTube Men's Vintage Henley"
    and product.productRevenue is not null)
and product.v2productname <> "YouTube Men's Vintage Henley"
and product.productRevenue is not null
GROUP BY other_purchased_products
ORDER BY Quantity DESC;


--Query 08: Calculate cohort map from product view to addtocart to purchase in Jan, Feb and March 2017. 
--hits.eCommerceAction.action_type = '2' is view product page; hits.eCommerceAction.action_type = '3' is add to cart; hits.eCommerceAction.action_type = '6' is purchase

WITH product_data AS (
  SELECT 
   format_date('%Y%m', parse_date('%Y%m%d',date)) as month,
   COUNT (CASE WHEN eCommerceAction.action_type = '2' THEN product.v2ProductName END) as num_product_view,
   COUNT (CASE WHEN eCommerceAction.action_type = '3' THEN product.v2ProductName END) as num_add_to_cart,
   COUNT (CASE WHEN eCommerceAction.action_type = '6' AND product.productRevenue IS NOT NULL THEN product.v2ProductName END) as num_purchase
  FROM `bigquery-public-data.google_analytics_sample.ga_sessions_*`,
  UNNEST(hits) AS hits,
  UNNEST (hits.product) AS product
  WHERE _table_suffix BETWEEN '20170101' and '20170331'
  AND eCommerceAction.action_type in ('2','3','6')
group by month
order by month
)

SELECT *,
ROUND(num_add_to_cart/num_product_view*100,2) AS add_to_cart_rate,
ROUND(num_purchase/num_product_view*100,2) AS purchase_rate,
FROM product_data

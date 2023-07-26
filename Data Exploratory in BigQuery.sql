-- Data Cleaning: After reviewing, the dataset is quite clear so I keep the original format.

-- Data Exploratory:


--Query 1: Calculate total visit, pageview, transaction and revenue for Jan, Feb and March 2017 (order by month)
SELECT
  format_date ('%Y%m', parse_date('%Y%m%d', date)) as month,
  sum(totals.visits) as visits,
  sum(totals.pageviews) as pageviews,
  sum(totals.transactions) as transactions,
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_2017*`
WHERE _table_suffix between '0101' and '0331'
group by month  --group by 1
order by month; --order by 1


--Query 02: Bounce rate per traffic source in July 2017 (Bounce_rate = num_bounce/total_visit) (order by total_visit DESC) -- DONE
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


--Query 06: Average amount of money spent per session. Only include purchaser data in July 2017 -- Ket qua chua ra dung
-- Why dung fullvisitorID dc ma fullvisitID ko duoc
--câu này yêu cầu mình tính "per session", session là 1 phiên đăng nhập, hay đc hiểu như là 1 visit, vậy mình sẽ count visitorID hoặc sum total visit
SELECT format_date('%Y%m', parse_date('%Y%m%d', date)) as month,
      --SUM(product.productRevenue)/ count (distinct visitID) as avg_revenue_by_user_per_visit
      ((sum(product.productRevenue)/sum(totals.visits))/power(10,6)) as avg_revenue_by_user_per_visit

FROM `bigquery-public-data.google_analytics_sample.ga_sessions_201707*`,
UNNEST (hits) AS hits,
UNNEST (hits.product) AS product
WHERE totals.transactions IS NOT NULL AND totals.transactions >=1
and product.productRevenue is not null
GROUP BY month;

--Query 07: Other products purchased by customers who purchased product "YouTube Men's Vintage Henley" in July 2017. Output should show product name and the quantity was ordered. -- KQ chua dung
--> kqua chưa đúng do thiếu đk productRevenue is not null, nên nó tính luôn cả những item k có purchase
SELECT product.v2ProductName as other_purchased_products, 
      count(product.productQuantity) as Quantity
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
and product.productRevenue is not null
GROUP BY other_purchased_products
ORDER BY Quantity DESC;

--Query 08: Calculate cohort map from product view to addtocart to purchase in Jan, Feb and March 2017. For example, 100% product view then 40% add_to_cart and 10% purchase.
--Y product.productRevenue is not null to remove duplicate data for purchase rate

WITH Number AS (
SELECT format_date ('%Y%m%d', parse_date ('%Y%m%d', date)) as month,
COUNT (CASE WHEN hits.eCommerceAction.action_type = '2' THEN fullvisitorID END) AS num_product_view,
COUNT (CASE WHEN hits.eCommerceAction.action_type = '3' THEN fullvisitorID END) AS num_addtocart,
COUNT (CASE WHEN hits.eCommerceAction.action_type = '6' THEN fullvisitorID END) AS num_purchase   --thiếu đk productRevenue is not null để lấy ra những product có purchase
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_2017*`,
    UNNEST (hits) AS hits,
    --UNNEST (hits.eCommerceAction) AS eCommerceAction,     --k cần unnest eCommerAction
    UNNEST (hits.product) AS product
WHERE _table_suffix between '0101' and '0331'
  AND product.productRevenue is not null
GROUP BY month
ORDER BY month
)
SELECT *,
    (num_addtocart/num_product_view)*100 as add_to_cart_rate,
    (num_purchase/num_product_view)*100 as purchase_rate
FROM Number
ORDER BY month;
--incorrect
/* Với mỗi sản phẩm, nó sẽ trải qua 3 stage, view -> add to cart -> purchase
thì để bài đang yêu cầu mình tính theo kiểu cohort map, qua từng stage như vậy, số sản phầm rớt dần còn bao nhiêu %
ví dụ có mình xem 10 sản phẩm, xong bỏ 4 sản phẩm vào giỏ hàng, rồi quyết định chỉ mua 1 cái thôi
*/
--dùng CTE
with
product_view as(
SELECT
  format_date("%Y%m", parse_date("%Y%m%d", date)) as month,
  count(product.productSKU) as num_product_view
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_*`
, UNNEST(hits) AS hits
, UNNEST(hits.product) as product
WHERE _TABLE_SUFFIX BETWEEN '20170101' AND '20170331'
AND hits.eCommerceAction.action_type = '2'
GROUP BY 1
),

add_to_cart as(
SELECT
  format_date("%Y%m", parse_date("%Y%m%d", date)) as month,
  count(product.productSKU) as num_addtocart
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_*`
, UNNEST(hits) AS hits
, UNNEST(hits.product) as product
WHERE _TABLE_SUFFIX BETWEEN '20170101' AND '20170331'
AND hits.eCommerceAction.action_type = '3'
GROUP BY 1
),

purchase as(
SELECT
  format_date("%Y%m", parse_date("%Y%m%d", date)) as month,
  count(product.productSKU) as num_purchase
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_*`
, UNNEST(hits) AS hits
, UNNEST(hits.product) as product
WHERE _TABLE_SUFFIX BETWEEN '20170101' AND '20170331'
AND hits.eCommerceAction.action_type = '6'
and product.productRevenue is not null   --phải thêm điều kiện này để đảm bảo có revenue
group by 1
)

select
    pv.*,
    num_addtocart,
    num_purchase,
    round(num_addtocart*100/num_product_view,2) as add_to_cart_rate,
    round(num_purchase*100/num_product_view,2) as purchase_rate
from product_view pv
join add_to_cart a on pv.month = a.month
join purchase p on pv.month = p.month
order by pv.month;


--Cách 2: bài này mình có thể dùng count(case when) hoặc sum(case when)

with product_data as(
select
    format_date('%Y%m', parse_date('%Y%m%d',date)) as month,
    count(CASE WHEN eCommerceAction.action_type = '2' THEN product.v2ProductName END) as num_product_view,
    count(CASE WHEN eCommerceAction.action_type = '3' THEN product.v2ProductName END) as num_add_to_cart,
    count(CASE WHEN eCommerceAction.action_type = '6' and product.productRevenue is not null THEN product.v2ProductName END) as num_purchase
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_*`
,UNNEST(hits) as hits
,UNNEST (hits.product) as product
where _table_suffix between '20170101' and '20170331'
and eCommerceAction.action_type in ('2','3','6')
group by month
order by month
)

select
    *,
    round(num_add_to_cart/num_product_view * 100, 2) as add_to_cart_rate,
    round(num_purchase/num_product_view * 100, 2) as purchase_rate
from product_data;














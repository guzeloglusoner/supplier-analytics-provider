// SUPPLIER_SCORE_METRICS TABLE

SELECT f.supplier_id,
    EXTRACT(DATE FROM TIMESTAMP(f.timestamp)) as calculated_at,
    "average_rating" as metric,
    AVG((CAST(NULLIF(f.REVIEW_VALUE_SPEED,'null')  AS FLOAT64) + CAST(NULLIF(f.REVIEW_VALUE_PRINT_QUALITY,'null')  AS FLOAT64)) / 2) as value
 FROM (
     SELECT *
      FROM  `supplier_analytics.REVIEW_EVENTS` r1
      WHERE r1.order_id NOT IN (SELECT r2.order_id FROM `supplier_analytics.REVIEW_EVENTS` r2 WHERE r2.event_name = "DELETED") -- Exclude deleted reviews
      and r1.timestamp = (SELECT MAX(timestamp) FROM `supplier_analytics.REVIEW_EVENTS` r3 WHERE r1.order_id = r3.order_id) -- Get the latest review for each review sequence
 ) f
 GROUP BY f.supplier_id, EXTRACT(DATE FROM TIMESTAMP(f.timestamp))
 
 UNION ALL

 SELECT supplier_id, 
    EXTRACT(DATE FROM TIMESTAMP(timestamp)) as calculated_at,
    --SUM(CASE WHEN EVENT_NAME = 'order/execute/customer/status/printing' THEN 1 ELSE 0 END) as ACCEPTED_ORDERS,
    --SUM(CASE WHEN EVENT_NAME in ('order/execute/customer/status/processing','order/execute/customer/status/printing') THEN 1 ELSE 0 END) AS TOTAL_ORDERS,
    "acceptance_ratio" as metric,
    100 * (SUM(CASE WHEN EVENT_NAME = 'order/execute/customer/status/printing' THEN 1 ELSE 0 END) / SUM(CASE WHEN EVENT_NAME in ('order/execute/customer/status/processing','order/execute/customer/status/printing') THEN 1 ELSE 0 END))  AS value,
FROM `supplier_analytics.ORDER_EVENTS`
WHERE EVENT_NAME in ('order/execute/customer/status/printing','order/execute/customer/status/processing')
GROUP BY supplier_id, EXTRACT(DATE FROM TIMESTAMP(timestamp));
SELECT f.supplier_id,
    EXTRACT(DATE FROM CURRENT_TIMESTAMP()) as calculated_at,
    "average_rating" as metric,
    AVG((CAST(NULLIF(f.REVIEW_VALUE_SPEED,'null')  AS NUMERIC) + CAST(NULLIF(f.REVIEW_VALUE_PRINT_QUALITY,'null')  AS NUMERIC)) / 2) as value
 FROM (
     SELECT *
      FROM  `venue-organization.supplier_analytics.REVIEW_EVENTS` r1
      WHERE r1.order_id NOT IN (SELECT r2.order_id FROM `venue-organization.supplier_analytics.REVIEW_EVENTS` r2 WHERE r2.event_name = "DELETED") -- Exclude deleted reviews
      and r1.timestamp = (SELECT MAX(timestamp) FROM `venue-organization.supplier_analytics.REVIEW_EVENTS` r3 WHERE r1.order_id = r3.order_id) -- Get the latest review for each review sequence
 ) f
 GROUP BY f.supplier_id, EXTRACT(DATE FROM CURRENT_TIMESTAMP())

 UNION ALL

  SELECT supplier_id,
    EXTRACT(DATE FROM CURRENT_TIMESTAMP()) as calculated_at,
    "acceptance_ratio" as metric,
    ROUND(
    100 *
    SAFE_DIVIDE(
        CAST(
            (SUM(CASE WHEN t1.order_id in (SELECT t2.order_id
                                    FROM `venue-organization.supplier_analytics.ORDER_EVENTS` t2
                                    WHERE t2.EVENT_NAME = 'order/execute/customer/status/printing') THEN 1 ELSE 0 END) )  AS FLOAT64),
        CAST((SUM(CASE WHEN EVENT_NAME in ('order/execute/customer/status/processing','order/execute/customer/status/printing') THEN 1 ELSE 0 END)) AS FLOAT64)
    ), 2) AS value
FROM `venue-organization.supplier_analytics.ORDER_EVENTS` t1
WHERE EVENT_NAME in ('order/execute/customer/status/printing','order/execute/customer/status/processing')
GROUP BY supplier_id,  EXTRACT(DATE FROM CURRENT_TIMESTAMP());
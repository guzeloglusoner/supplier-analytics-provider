-- ORDER ACCEPTANCE RATE
SELECT supplier_id,
       EXTRACT(DATE
               FROM TIMESTAMP(TIMESTAMP)) AS TIMESTAMP,
       SUM(CASE
               WHEN EVENT_NAME = 'order/execute/customer/status/printing' THEN 1
               ELSE 0
           END) AS ACCEPTED_ORDERS,
       SUM(CASE
               WHEN EVENT_NAME in ('order/execute/customer/status/processing', 'order/execute/customer/status/printing') THEN 1
               ELSE 0
           END) AS TOTAL_ORDERS,
       100 * (SUM(CASE
                      WHEN EVENT_NAME = 'order/execute/customer/status/printing' THEN 1
                      ELSE 0
                  END) / SUM(CASE
                                 WHEN EVENT_NAME in ('order/execute/customer/status/processing', 'order/execute/customer/status/printing') THEN 1
                                 ELSE 0
                             END)) AS ACCEPTANCE_RATIO,
FROM `venue-organization.supplier_analytics.ORDERS_NEW`
WHERE EVENT_NAME in ('order/execute/customer/status/printing',
                     'order/execute/customer/status/processing')
GROUP BY supplier_id,
         TIMESTAMP
ORDER BY TIMESTAMP DESC;


-- AVERAGE REVIEW SCORE
 SELECT f.supplier_id, f.timestamp, AVG(review_value_speed + review_value_print_quality)
 FROM (
     SELECT *,
      FROM REVIEWS r1
      WHERE r1.order_id NOT IN (SELECT r2.order_id FROM REVIEWS r2 WHERE r2.event = "DELETED") -- Exclude deleted reviews
      and r1.timestamp = (SELECT MAX(timestamp) FROM REVIEWS r3 WHERE r1.order_id = r2.order_id) -- Get the latest review for each review sequence
 ) FILTERED_REVIEWS f
 GROUP BY f.supplier_id, f.timestamp


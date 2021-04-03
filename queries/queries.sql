CREATE OR REPLACE STREAM EVENTS (
    card_data_id VARCHAR,
    authorized_amount BIGINT,
    ip_address VARCHAR,
    status VARCHAR,
    created VARCHAR
    ) WITH (
        KAFKA_TOPIC = 'fraud-streaming-aggregator-production-post_auth',
        VALUE_FORMAT = 'JSON',
        timestamp='created',
        timestamp_format='yyyy-MM-dd''T''HH:mm:ss.SSSz'
    );

CREATE STREAM EVENTS WITH (
    kafka_topic = 'dbserver1.public.MY_TABLE',
    value_format = 'avro',
    timestamp='after -> timestamp',
    timestamp_format='yyyy-MM-dd''T''HH:mm:ss.SSSz'
);

-- Null value modification for STREAM
CREATE STREAM ORDERS AS
    SELECT EXTRACTJSONFIELD(after -> data, '$.event') AS EVENT_NAME,
    EXTRACTJSONFIELD(after -> data, '$.hub_id') AS SUPPLIER_ID,
    EXTRACTJSONFIELD(after -> data, '$.order_id') AS ORDER_ID,
    EXTRACTJSONFIELD(after -> data, '$.timestamp') AS TIMESTAMP,
    EXTRACTJSONFIELD(after -> data, '$.price_customer') AS PRICE_CUSTOMER,
    EXTRACTJSONFIELD(after -> data, '$.orderStationType') AS ORDER_STATION_TYPE,
    EXTRACTJSONFIELD(after -> data, '$.orderStationModel') AS ORDER_STATION_MODEL,
    EXTRACTJSONFIELD(after -> data, '$.context_traits_uid') AS CONTEXT_TRAITS_UID,
    EXTRACTJSONFIELD(after -> data, '$.review_value_speed') AS REVIEW_VALUE_SPEED,
    EXTRACTJSONFIELD(after -> data, '$.context_traits_persona') AS CONTEXT_TRAITS_PERSONA,
    EXTRACTJSONFIELD(after -> data, '$.orderStationManufacterer') AS ORDER_STATION_MANUFACTURER
    FROM EVENTS;


-- NOT YET IMPLEMENTED WILL BE HANDLED ON BIGQUERY
 CREATE TABLE ORDER_ACCEPTANCE_RATE AS
     SELECT supplier_id,
         SUM(CASE WHEN EVENT_NAME = 'order/execute/customer/status/processing' THEN 1 ELSE 0 END),
         SUM(CASE WHEN EVENT_NAME in ('order/execute/customer/status/processing','order/execute/customer/status/printing') THEN 1 ELSE 0 END) AS ACCEPTANCE_RATE
     FROM ORDERS_NEW
     GROUP BY supplier_id
     EMIT CHANGES;

 SELECT f.supplier_id, f.timestamp, AVG(review_value_speed + review_value_print_quality)
 FROM (
     SELECT *,
      FROM REVIEWS r1
      WHERE r1.order_id NOT IN (SELECT r2.order_id FROM REVIEWS r2 WHERE r2.event = "DELETED") -- Exclude deleted reviews
      and r1.timestamp = (SELECT MAX(timestamp) FROM REVIEWS r3 WHERE r1.order_id = r2.order_id) -- Get the latest review for each review sequence
 ) FILTERED_REVIEWS f
 GROUP BY f.supplier_id, f.timestamp

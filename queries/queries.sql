CREATE STREAM EVENTS WITH (
    kafka_topic = 'dbserver1.public.MY_TABLE',
    value_format = 'avro'
);

CREATE STREAM EVENTS_EXPLODED AS
    SELECT EXTRACTJSONFIELD(after -> data, '$.event') AS EVENT_NAME, after
    FROM EVENTS;

-- ORDERS TO BE FILTERED.
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
    FROM EVENTS_EXPLODED
    WHERE EVENT_NAME =  'order/execute/customer/status/processing' OR EVENT_NAME = 'order/execute/customer/status/payment' OR EVENT_NAME = 'order/execute/customer/status/printing' OR EVENT_NAME = 'order/execute/customer/status/successful';



-- NOT YET IMPLEMENTED WILL BE HANDLED ON BIGQUERY
 CREATE TABLE ORDER_ACCEPTANCE_RATE AS
     SELECT supplier_id,
         SUM(CASE WHEN EVENT_NAME = 'order/execute/customer/status/processing' THEN 1 ELSE 0 END),
         SUM(CASE WHEN EVENT_NAME in ('order/execute/customer/status/processing','order/execute/customer/status/printing') THEN 1 ELSE 0 END) AS ACCEPTANCE_RATE
     FROM ORDERS
     GROUP BY supplier_id
     EMIT CHANGES;

WITH session_data AS (
    SELECT 
         FORMAT_TIMESTAMP('%Y-%m-%d %H:%M:%S', TIMESTAMP_MICROS(event_timestamp)) as event_timestamp,
         user_pseudo_id,
         (SELECT value.int_value FROM UNNEST(event_params) WHERE key='ga_session_id') as session_id,
         event_name,
         geo.country as country,
         device.category as device_category,
         device.operating_system as device_os,
         device.language as device_language,
         REGEXP_REPLACE(traffic_source.source, r'^[<>\\(\\)]+|[<>\\(\\)]+$', '') as source,
         REGEXP_REPLACE(traffic_source.medium, r'^[<>\\(\\)]+|[<>\\(\\)]+$', '') as medium,
         (SELECT item_id FROM UNNEST(items) LIMIT 1) as item_id,
         (SELECT item_name FROM UNNEST(items) LIMIT 1) as item_name,
         (SELECT quantity FROM UNNEST(items) LIMIT 1) as item_quantity,
         (SELECT price_in_usd FROM UNNEST(items) LIMIT 1) as item_price,
         (SELECT item_revenue_in_usd FROM UNNEST(items) LIMIT 1) as item_revenue,
         (SELECT item_refund FROM UNNEST(items) LIMIT 1) as item_refund,
         REGEXP_REPLACE((SELECT item_category FROM UNNEST(items) LIMIT 1), '/$', '') as item_category
    FROM bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_*
    WHERE _TABLE_SUFFIX BETWEEN '20210101' and '20213101'
        AND event_name IN ('session_start', 'view_item', 'add_to_cart', 'begin_checkout', 'add_shipping_info', 'add_payment_info', 'purchase', 'in_app_purchase')
  )
  , aggregated_data AS (
    SELECT
      event_timestamp,
      event_name,
      user_pseudo_id,
      session_id,
      country,
      device_category,
      device_os,
      device_language,
      source,
      medium,
      item_id,
      item_name,
      item_category,
      item_quantity,
      item_price,
      item_revenue,
      item_refund,
      COUNT(DISTINCT CONCAT(user_pseudo_id, session_id)) AS user_sessions_count,
      COUNT(DISTINCT CASE WHEN event_name = 'view_item' THEN CONCAT(user_pseudo_id, session_id) ELSE NULL END) AS visit_to_view_item,
      COUNT(DISTINCT CASE WHEN event_name = 'add_to_cart' THEN CONCAT(user_pseudo_id, session_id) ELSE NULL END) AS visit_to_add_to_cart,
      COUNT(DISTINCT CASE WHEN event_name = 'begin_checkout' THEN CONCAT(user_pseudo_id, session_id) ELSE NULL END) AS visit_to_begin_checkout,
      COUNT(DISTINCT CASE WHEN event_name = 'add_shipping_info' THEN CONCAT(user_pseudo_id, session_id) ELSE NULL END) AS visit_to_add_shipping_info,
      COUNT(DISTINCT CASE WHEN event_name = 'add_payment_info' THEN CONCAT(user_pseudo_id, session_id) ELSE NULL END) AS visit_to_add_payment_info,
      COUNT(DISTINCT CASE WHEN event_name IN ('purchase', 'in_app_purchase') THEN CONCAT(user_pseudo_id, session_id) ELSE NULL END) AS visit_to_purchase,
      ROW_NUMBER() OVER (ORDER BY event_timestamp) AS row_number
    FROM session_data
    GROUP BY 
      event_timestamp,
      event_name,
      user_pseudo_id,
      session_id,
      country,
      device_category,
      device_os,
      device_language,
      source,
      medium,
      item_id,
      item_name,
      item_category,
      item_quantity,
      item_price,
      item_revenue,
      item_refund
  )

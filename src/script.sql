WITH
cte_sessions AS (
  SELECT
    e.user_pseudo_id,
    COALESCE(
      (SELECT ep.value.int_value     FROM UNNEST(e.event_params) ep WHERE ep.key = 'ga_session_id'),
      SAFE_CAST((SELECT ep.value.string_value FROM UNNEST(e.event_params) ep WHERE ep.key = 'ga_session_id') AS INT64),
      SAFE_CAST((SELECT ep.value.float_value  FROM UNNEST(e.event_params) ep WHERE ep.key = 'ga_session_id') AS INT64)
    ) AS session_id,

    TIMESTAMP_MICROS(e.event_timestamp) AS session_start_ts,
    DATE(TIMESTAMP_MICROS(e.event_timestamp)) AS session_start_date,

    REGEXP_EXTRACT(
      (SELECT ep.value.string_value FROM UNNEST(e.event_params) ep WHERE ep.key = 'page_location'),
      r'(?:https:\/\/)?[^\/]+\/(.*)'
    ) AS landing_page_location,

    e.traffic_source.source   AS source,
    e.traffic_source.medium   AS medium,
    e.traffic_source.name     AS campaign,

    e.device.category         AS device_category,
    e.device.language         AS device_language,
    e.device.operating_system AS device_os,

    e.geo.country             AS country
  FROM `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_*` e
  WHERE e.event_name = 'session_start'
),

cte_funnel_events AS (
  SELECT
    e.user_pseudo_id,
    COALESCE(
      (SELECT ep.value.int_value     FROM UNNEST(e.event_params) ep WHERE ep.key = 'ga_session_id'),
      SAFE_CAST((SELECT ep.value.string_value FROM UNNEST(e.event_params) ep WHERE ep.key = 'ga_session_id') AS INT64),
      SAFE_CAST((SELECT ep.value.float_value  FROM UNNEST(e.event_params) ep WHERE ep.key = 'ga_session_id') AS INT64)
    ) AS session_id,

    TIMESTAMP_MICROS(e.event_timestamp) AS event_timestamp_ts,
    e.event_name,
    e.ecommerce.purchase_revenue_in_usd AS purchase_revenue_usd
  FROM `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_*` e
  WHERE e.event_name IN (
    'session_start',
    'view_item',
    'add_to_cart',
    'begin_checkout',
    'add_shipping_info',
    'add_payment_info',
    'purchase'
  )
)

SELECT
  s.user_pseudo_id,
  s.session_id,
  CONCAT(CAST(s.user_pseudo_id AS STRING), '.', CAST(s.session_id AS STRING)) AS user_session_id,
  s.session_start_ts,
  s.session_start_date,
  s.landing_page_location,
  s.source, s.medium, s.campaign,
  s.device_category, s.device_language, s.device_os,
  s.country,
  f.event_timestamp_ts,
  f.event_name,
  f.purchase_revenue_usd,
  
  CASE WHEN f.event_name = 'session_start'  THEN 1 ELSE 0 END AS is_visit_event,
  CASE WHEN f.event_name = 'begin_checkout' THEN 1 ELSE 0 END AS is_order_event,
  CASE WHEN f.event_name = 'purchase'       THEN 1 ELSE 0 END AS is_purchase_event,

  CASE WHEN f.event_name = 'view_item'         THEN 1 ELSE 0 END AS step_view_item,
  CASE WHEN f.event_name = 'add_to_cart'       THEN 1 ELSE 0 END AS step_add_to_cart,
  CASE WHEN f.event_name = 'begin_checkout'    THEN 1 ELSE 0 END AS step_begin_checkout,
  CASE WHEN f.event_name = 'add_shipping_info' THEN 1 ELSE 0 END AS step_add_shipping_info,
  CASE WHEN f.event_name = 'add_payment_info'  THEN 1 ELSE 0 END AS step_add_payment_info,
  CASE WHEN f.event_name = 'purchase'          THEN 1 ELSE 0 END AS step_purchase

FROM cte_sessions s
LEFT JOIN cte_funnel_events f
  ON  f.user_pseudo_id = s.user_pseudo_id
  AND f.session_id     = s.session_id;
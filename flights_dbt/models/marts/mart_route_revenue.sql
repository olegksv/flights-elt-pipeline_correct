{{
    config(
        materialized='incremental',
        unique_key=['flight_date', 'departure_airport', 'arrival_airport', 'fare_conditions']
    )
}}

SELECT
    f.flight_date,
    f.departure_airport,
    f.arrival_airport,
    tf.fare_conditions,
    COUNT(tf.ticket_no) AS total_tickets,
    SUM(tf.amount_rub) AS total_revenue_rub,
    ROUND(SUM(tf.amount_rub) / COALESCE(c.usd_rate, 60.0), 2) AS total_revenue_usd
FROM {{ ref('stg_flights') }} f
JOIN {{ ref('stg_ticket_flights') }} tf
    ON f.flight_id = tf.flight_id
LEFT JOIN {{ source('external_api', 'raw_currency_rates') }} c
    ON f.flight_date = c.rate_date

{% if is_incremental() %}
  WHERE f.flight_date >= (SELECT MAX(flight_date) FROM {{ this }})
{% endif %}

GROUP BY 1, 2, 3, 4, c.usd_rate

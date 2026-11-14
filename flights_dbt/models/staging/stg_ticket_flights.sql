SELECT
    ticket_no,
    flight_id,
    fare_conditions,
    amount AS amount_rub
FROM {{ source('bookings_raw', 'ticket_flights') }}

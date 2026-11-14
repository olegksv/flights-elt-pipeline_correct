SELECT
    flight_id,
    flight_no,
    scheduled_departure::date AS flight_date,
    departure_airport,
    arrival_airport,
    status
FROM {{ source('bookings_raw', 'flights') }}
WHERE status = 'Arrived'

with base as (
    select
        'yellow' as cab_type,
        vendor_id,
        pickup_datetime,
        dropoff_datetime,
        passenger_count,
        trip_distance,
        (trip_distance * e.co2_grams_per_mile / 1000.0) as trip_co2_kgs,
        case 
            when (epoch(dropoff_datetime) - epoch(pickup_datetime)) > 0
            then (trip_distance / ((epoch(dropoff_datetime) - epoch(pickup_datetime)) / 3600.0))
            else null
        end as avg_mph,
        extract(hour from pickup_datetime) as hour_of_day,
        extract(dow from pickup_datetime) as day_of_week,
        extract(week from pickup_datetime) as week_of_year,
        extract(month from pickup_datetime) as month_of_year
    from {{ source('main','yellow_trips_2024') }} t
    join {{ source('main','vehicle_emissions') }} e
      on e.vehicle_type = 'yellow_taxi'
)
select * from base

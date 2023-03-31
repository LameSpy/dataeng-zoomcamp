https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/yellow_tripdata_2019-01.csv.gz
https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/green_tripdata_2019-01.csv.gz



StructType([
    StructField('VendorID', LongType(), True), 
    StructField('lpep_pickup_datetime', StringType(), True), 
    StructField('lpep_dropoff_datetime', StringType(), True), 
    StructField('store_and_fwd_flag', StringType(), True), 
    StructField('RatecodeID', LongType(), True), 
    StructField('PULocationID', LongType(), True), 
    StructField('DOLocationID', LongType(), True), 
    StructField('passenger_count', LongType(), True), 
    StructField('trip_distance', DoubleType(), True), 
    StructField('fare_amount', DoubleType(), True), 
    StructField('extra', DoubleType(), True), 
    StructField('mta_tax', DoubleType(), True), 
    StructField('tip_amount', DoubleType(), True), 
    StructField('tolls_amount', DoubleType(), True), 
    StructField('ehail_fee', DoubleType(), True), 
    StructField('improvement_surcharge', DoubleType(), True), 
    StructField('total_amount', DoubleType(), True), 
    StructField('payment_type', LongType(), True), 
    StructField('trip_type', LongType(), True), 
    StructField('congestion_surcharge', DoubleType(), True)
    ])




select 
    -- Reveneue grouping 
    pickup_zone as revenue_zone,
    date_trunc(pickup_datetime, month) as revenue_month, 
    --Note: For BQ use instead: date_trunc(pickup_datetime, month) as revenue_month, 

    service_type, 

    -- Revenue calculation 
    sum(fare_amount) as revenue_monthly_fare,
    sum(extra) as revenue_monthly_extra,
    sum(mta_tax) as revenue_monthly_mta_tax,
    sum(tip_amount) as revenue_monthly_tip_amount,
    sum(tolls_amount) as revenue_monthly_tolls_amount,
    sum(improvement_surcharge) as revenue_monthly_improvement_surcharge,
    sum(total_amount) as revenue_monthly_total_amount,
    sum(congestion_surcharge) as revenue_monthly_congestion_surcharge,

    -- Additional calculations
    count(tripid) as total_monthly_trips,
    avg(passenger_count) as avg_montly_passenger_count,
    avg(trip_distance) as avg_montly_trip_distance

    from trips_data
    group by 1,2,3
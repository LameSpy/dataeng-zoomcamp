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


    python 4_spark_sql.py \
        --input_green=/home/Дмитрий/datacamp/dataeng-zoomcamp/week_5_batch_processing/data/pq/green/2020/* \
        --input_yellow=/home/Дмитрий/datacamp/dataeng-zoomcamp/week_5_batch_processing/data/pq/yellow/2020/* \
        --output=/home/Дмитрий/datacamp/dataeng-zoomcamp/week_5_batch_processing/data/report/revenue2020/

URL="spark://sparl-datacamp.asia-east2-a.c.datacamp-378414.internal:7077"

spark-submit \
    --master="${URL}" \
    4_spark_sql.py \
    --input_green=/home/Дмитрий/datacamp/dataeng-zoomcamp/week_5_batch_processing/data/pq/green/2021/* \
    --input_yellow=/home/Дмитрий/datacamp/dataeng-zoomcamp/week_5_batch_processing/data/pq/yellow/2021/* \
    --output=/home/Дмитрий/datacamp/dataeng-zoomcamp/week_5_batch_processing/data/report/revenue2021/

# For google dataproc
    --input_green=gs://dtc_data_lake_datacamp-378414/data/green/* \
    --input_yellow=gs://dtc_data_lake_datacamp-378414/data/yellow/* \
    --output=gs://dtc_data_lake_datacamp-378414/data/report/dataproc/revenue/

# Run dataproc from gcloud sdk
gcloud dataproc jobs submit pyspark \
    --cluster=de-zoomcapm-cluster \
    --region=asia-east2 \
    gs://dtc_data_lake_datacamp-378414/code/4_spark_sql.py \
    -- \
    --input_green=gs://dtc_data_lake_datacamp-378414/data/green/*/*/* \
    --input_yellow=gs://dtc_data_lake_datacamp-378414/data/yellow/*/*/* \
    --output=gs://dtc_data_lake_datacamp-378414/data/report/dataproc/revenue/

# Save table to bigquery
gcloud dataproc jobs submit pyspark \
    --cluster=de-zoomcapm-cluster \
    --region=asia-east2 \
    --jars=gs://spark-lib/bigquery/spark-bigquery-latest_2.12.jar \
    gs://dtc_data_lake_datacamp-378414/code/4_spark_sql_big_query.py \
    -- \
    --input_green=gs://dtc_data_lake_datacamp-378414/data/green/*/*/* \
    --input_yellow=gs://dtc_data_lake_datacamp-378414/data/yellow/*/*/* \
    --output=trips_data_all.reports



gsutil -m cp -r /home/Дмитрий/datacamp/dataeng-zoomcamp/week_5_batch_processing/data/pq/green gs://dtc_data_lake_datacamp-378414/data/green
gsutil cp /home/Дмитрий/datacamp/dataeng-zoomcamp/week_5_batch_processing/code/4_spark_sql_big_query.py gs://dtc_data_lake_datacamp-378414/code
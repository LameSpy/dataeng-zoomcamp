import findspark
findspark.init()

import pyspark
from pyspark.sql import SparkSession
from pyspark.sql import  types
from pyspark.sql import functions as F
import pandas as pd
import argparse

# config parser cli varibles for a python script
parser = argparse.ArgumentParser()

# create parameters with argparse library
parser.add_argument('--input_green', required=True, default='/home/Дмитрий/datacamp/dataeng-zoomcamp/week_5_batch_processing/data/green/*/*')
parser.add_argument('--input_yellow', required=True, default='/home/Дмитрий/datacamp/dataeng-zoomcamp/week_5_batch_processing/data/yellow/*/*')
parser.add_argument('--output', required=True, default='/home/Дмитрий/datacamp/dataeng-zoomcamp/week_5_batch_processing/data/report/revenue/')

args = parser.parse_args()

input_green = args.input_green
input_yellow = args.input_yellow
output = args.output
#######

spark = SparkSession.builder \
    .appName('test') \
    .getOrCreate()
    #.master("spark://sparl-datacamp.asia-east2-a.c.datacamp-378414.internal:7077") \

df_green = spark.read.parquet(input_green)
df_yellow = spark.read.parquet(input_yellow)


df_green = df_green \
    .withColumnRenamed('lpep_pickup_datetime', 'pickup_datetime') \
    .withColumnRenamed('lpep_dropoff_datetime', 'dropoff_datetime') 


df_yellow = df_yellow \
    .withColumnRenamed('lpep_pickup_datetime', 'pickup_datetime') \
    .withColumnRenamed('lpep_dropoff_datetime', 'dropoff_datetime') 

common_columns = [
    'VendorID',
    'pickup_datetime',
    'dropoff_datetime',
    'store_and_fwd_flag',
    'RatecodeID',
    'PULocationID',
    'DOLocationID',
    'passenger_count',
    'trip_distance',
    'fare_amount',
    'extra',
    'mta_tax',
    'tip_amount',
    'tolls_amount',
    'improvement_surcharge',
    'total_amount',
    'payment_type',
    'congestion_surcharge'
]


df_green_sel = df_green \
    .select(common_columns) \
    .withColumn('service_type', F.lit('green'))


df_yellow_sel = df_yellow \
    .select(common_columns) \
    .withColumn('service_type', F.lit('yellow')) 


df_trips_data = df_green_sel.unionAll(df_yellow_sel)


# convert spark table in spark SQL table
df_trips_data.registerTempTable('trips_data')

df_result = spark.sql("""
      select 
    -- Reveneue grouping 
    PULocationID as revenue_zone,
    date_trunc('month', pickup_datetime) as revenue_month, 
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
    avg(passenger_count) as avg_montly_passenger_count,
    avg(trip_distance) as avg_montly_trip_distance

    from trips_data
    group by 1,2,3
""")


df_result \
    .coalesce(1) \
    .write.parquet(output, mode='overwrite')



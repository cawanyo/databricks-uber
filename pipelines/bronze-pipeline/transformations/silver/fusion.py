from pyspark import pipelines as dp
import pyspark.sql.types as T
from pyspark.sql.functions import *


dp.create_streaming_table(name='stg_rides')


@dp.append_flow(target="stg_rides",)
def rides_static():
    df = spark.readStream.table("dev.bronze.bulk_rides")
    return df


@dp.append_flow(target="stg_rides",)
def rides_stream():
    df = spark.readStream.table("dev.bronze.ride_stream")

    ride_schema = spark.read.table("dev.bronze.bulk_rides").schema
    df_parsed = df.withColumn('parsed_rides', from_json(col('rides'), ride_schema)).select('parsed_rides.*')
    return df_parsed

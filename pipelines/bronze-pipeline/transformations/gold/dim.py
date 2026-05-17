from pyspark import pipelines as dp
from pyspark.sql.functions import col, expr

@dp.view
def dim_passenger_view():
  df = spark.readStream.table("dev.silver.silver_obt ")
  df =  df.select("passenger_id", "passenger_name", "passenger_email", "passenger_phone")
  df = df.dropDuplicates(subset=["passenger_id"])
  return df


dp.create_streaming_table("dim_passenger")

dp.create_auto_cdc_flow(
  target = "dim_passenger",
  source = "dim_passenger_view ",
  keys = ["passenger_id"],
  sequence_by = col("passenger_id"),
  stored_as_scd_type = 1
)





@dp.view
def dim_driver_view():
  df = spark.readStream.table("dev.silver.silver_obt")
  df =  df.select("driver_id", "driver_name", "driver_license", "driver_phone", "driver_rating")
  df = df.dropDuplicates(subset=["driver_id"])
  return df


dp.create_streaming_table("dim_driver")

dp.create_auto_cdc_flow(
  target = "dim_driver",
  source = "dim_driver_view ",
  keys = ["driver_id"],
  sequence_by = col("driver_id"),
  stored_as_scd_type = 1
)
  


@dp.view
def dim_vehicle_view():
  df = spark.readStream.table("dev.silver.silver_obt")
  df =  df.select("vehicle_id", "vehicle_make_id", "vehicle_model", "vehicle_type", "license_plate")
  df = df.dropDuplicates(subset=["vehicle_id"])
  return df


dp.create_streaming_table("dim_vehicle")

dp.create_auto_cdc_flow(
  target = "dim_vehicle",
  source = "dim_vehicle_view ",
  keys = ["vehicle_id"],
  sequence_by = col("vehicle_id"),
  stored_as_scd_type = 1
)


@dp.view
def dim_location_view():
  df = spark.readStream.table("dev.silver.silver_obt")
  df =  df.select("pickup_city_id", "city", "region", "state")
  df = df.dropDuplicates(subset=["pickup_city_id"])
  return df


dp.create_streaming_table("dim_location")

dp.create_auto_cdc_flow(
  target = "dim_location",
  source = "dim_location_view ",
  keys = ["pickup_city_id"],
  sequence_by = col("pickup_city_id"),
  stored_as_scd_type = 2
)



@dp.view 
def fact_view():
  df = spark.readStream.table("dev.silver.silver_obt")
  df = df.select( "pickup_location_id", "driver_id", "passenger_id", "vehicle_id", "total_fare", "tip_amount",   "duration_minutes", "distance_miles", "rating", "ride_id")
  return df


dp.create_streaming_table("fact")

dp.create_auto_cdc_flow(
  target = "fact",
  source = "fact_view ",
  keys = ["ride_id"],
  sequence_by = col("ride_id"),
  stored_as_scd_type = 1
)
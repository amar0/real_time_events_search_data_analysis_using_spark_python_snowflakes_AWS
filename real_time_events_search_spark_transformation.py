import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
  
sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
from pyspark.sql.functions import *
from datetime import datetime
from awsglue.dynamicframe import DynamicFrame
s3_path = "s3://real-time-events-search-amar/raw_data/to_process/"
source_dyf = glueContext.create_dynamic_frame.from_options(
    connection_type="s3",
    format="json",
    connection_options={"paths": [s3_path]},
    format_options={"withHeader": True},
    transformation_ctx="source_dyf"
)
real_time_events_df = source_dyf.toDF()
real_time_events_df.show(2)
test_df = real_time_events_df
test_df.withColumn("data", explode("data")).show(5, False)
test_df.withColumn("data", explode("data")).select(
    col("data.event_id").alias("event_id"),
    col("data.name").alias("artist_name"),
    col("data.description").alias("description"),
    col("data.start_time").cast("timestamp").alias("event_start_time"),
    regexp_replace(col("data.venue.full_address"), ",", "").alias("venue_address"),
    col("data.venue.name").alias("venue_name"),
    col("data.venue.phone_number").alias("venue_phone_number"),
    col("data.link").alias("link")
).show(1, False)
def real_time_events_data(df):
    df = df.withColumn("data", explode("data"))
    df = df.select(
        col("data.event_id").alias("event_id"),
        col("data.name").alias("artist_name"),
        col("data.description").alias("description"),
        col("data.start_time").cast("timestamp").alias("event_start_time"),
        regexp_replace(col("data.venue.full_address"), ",", "").alias("venue_address"),
        col("data.venue.name").alias("venue_name"),
        col("data.venue.phone_number").alias("venue_phone_number"),
        col("data.link").alias("link")
    )
    return df
real_time_events_processed_df = real_time_events_data(test_df)
def write_to_s3(df, s3_path, format_type="csv"):
    dynamic_frame = DynamicFrame.fromDF(df, glueContext, "dynamic_frame")
    
    glueContext.write_dynamic_frame.from_options(
        frame=dynamic_frame,
        connection_type="s3",
        connection_options={"path": f"s3://real-time-events-search-amar/transformed_data/{s3_path}/"},
        format=format_type
    )
write_to_s3(real_time_events_processed_df, "transformed_data_{}".format(datetime.now().strftime("%Y-%m-%d_%H-%M-%S")), "csv")
job.commit()
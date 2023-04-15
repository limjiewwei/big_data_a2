#########################################
# IMPORT LIBRARIES AND SET VARIABLES
#########################################
# Import Python modules
import sys
import boto3
from datetime import datetime

# Import pyspark modules
import pyspark.sql.functions as f
from pyspark.context import SparkContext

# Import glue modules
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from awsglue.dynamicframe import DynamicFrame
from awsglue.context import GlueContext
from awsglue.job import Job

# @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

# Initialize contexts and session
sc = SparkContext()
sc.setLogLevel('DEBUG')
glue_context = GlueContext(sc.getOrCreate())
logger = glue_context.get_logger()
spark = glue_context.spark_session
job = Job(glue_context)
job.init(args['JOB_NAME'], args)

# Parameters for data extraction
glue_db = "is459_a2_spotify_db"
glue_tbl = "partitioned_data"  # data catalog table

#########################################
# EXTRACT (READ DATA)
#########################################
df = glue_context.create_dynamic_frame.from_catalog(
    database=glue_db,
    table_name=glue_tbl
)

# Resolving data type issue using data catalog data types
dynamic_frame = ResolveChoice.apply(
    df, choice="match_catalog", database=glue_db, table_name=glue_tbl)

# # Convert the DynamicFrame to a DataFrame
df = dynamic_frame.toDF().coalesce(1)

# #########################################
# # TRANSFORM (MODIFY DATA)
# #########################################
dfs_to_aggregate = []

# Replacing all 0 values to "No Genre" for artist_genre column
df = df.withColumn("artist_genre",
                   f.when(df["artist_genre"] == 0, "No Genre").otherwise(df["artist_genre"]))

# Aggregating data for business qn 1
df_by_genre_country_streams = df.groupBy("artist_genre", "country").agg(
    f.sum('streams').alias('total_streams')
)

# Aggregating data for business qn 2
df_by_artist_streams_weeks_on_charts = df.groupBy("artist_individual").agg(
    f.sum('streams').alias('total_streams'),
    f.avg('weeks_on_chart').alias('average_weeks_on_chart')
)

# Aggregating data for business qn 3
df_by_artist_genre_collaboration = df.filter(f.col("collab") == 1).groupBy("artist_names", "artist_genre").agg(
    f.sum('streams').alias('total_streams')
)

# Aggregating data for business qn 4
new_df = df.filter(df["danceability"].isNotNull() &
                   df["acousticness"].isNotNull())
new_df = new_df.withColumn("danceability",
                           f.when(df["danceability"] >= 0.5, "High Danceability").when(df["danceability"] < 0.5, "Low Danceability"))

new_df = new_df.withColumn("acousticness",
                           f.when(df["acousticness"] >= 0.5, "High Acousticness").when(df["acousticness"] < 0.5, "Low Acousticness"))

df_by_danceability_acousticness_streams = new_df.groupBy("danceability", "acousticness").agg(
    f.avg('streams').alias('total_streams')
)

# Aggregating data for business qn 5
df_by_danceability_acousticness_weeks_on_chart = df.groupBy("source").agg(
    f.avg('danceability').alias('avg_danceability'),
    f.avg('acousticness').alias('avg_acousticness'),
    f.avg('weeks_on_chart').alias('avg_weeks_on_chart')
)

dfs_to_aggregate.append(df_by_genre_country_streams)
dfs_to_aggregate.append(df_by_artist_streams_weeks_on_charts)
dfs_to_aggregate.append(df_by_artist_genre_collaboration)
dfs_to_aggregate.append(df_by_danceability_acousticness_streams)
dfs_to_aggregate.append(df_by_danceability_acousticness_weeks_on_chart)

#########################################
# LOAD (WRITE DATA)
#########################################
# Parameters for data loading
current_datetime = datetime.now().strftime("%d-%m-%Y_%H:%M:%S")
BUCKET_NAME = "is459-a2-spotify-data"
PREFIX = f"aggregate_data/{current_datetime}/aggregate"
OUTPUT_FILENAMES = ['_by_genre_country_streams', '_by_artist_streams_weeks_on_charts', '_by_artist_genre_collaboration',
                    '_by_danceability_acousticness_streams', '_by_danceability_acousticness_weeks_on_chart']
s3_client = boto3.client('s3')


# Function to convert back to dynamic frame
def convert_to_dynamic_frame(df):
    dynamic_frame_write = DynamicFrame.fromDF(
        df,
        glue_context,
        "dynamic_frame_write"
    )
    return dynamic_frame_write


# Function to write DynamicFrame to S3 as a CSV file
def save_df_s3(dynamic_frame):
    write_csv = glue_context.write_dynamic_frame.from_options(
        frame=dynamic_frame,
        connection_type="s3",
        connection_options={"path": "s3://" + BUCKET_NAME + "/" + PREFIX},
        format="csv",
        transformation_ctx="write_csv"
    )
    return

# Function to rename glue output file in s3


def rename_csv(file_name, output_filename):
    copy_source = {'Bucket': BUCKET_NAME, 'Key': file_name}
    copy_key = PREFIX + f'{output_filename}.csv'

    s3_client.copy_object(Bucket=BUCKET_NAME,
                          CopySource=copy_source, Key=copy_key)
    s3_client.delete_object(Bucket=BUCKET_NAME, Key=file_name)


# Saving aggregated results to csv in s3 bucket
for df in dfs_to_aggregate:
    dynamic_frame = convert_to_dynamic_frame(df)
    save_df_s3(dynamic_frame)

# Iterating through the s3 bucket to rename the files
response = s3_client.list_objects(
    Bucket=BUCKET_NAME,
    Prefix=PREFIX,
)["Contents"]

for idx, obj in enumerate(response):
    file_name = obj.get("Key")
    rename_csv(file_name, OUTPUT_FILENAMES[idx])

job.commit()

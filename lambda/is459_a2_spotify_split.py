import boto3
import pandas as pd
import numpy as np
from io import StringIO

s3_client = boto3.client('s3')
glue_client = boto3.client('glue')


def split_csv(source_file, chunk_size=50000):
    bucket = "is459-a2-spotify-data"
    folder = "partitioned_data"

    intial_df = pd.read_csv(source_file,  usecols=lambda x: x != 'Unnamed: 0')
    final_df = intial_df[(intial_df != intial_df.columns).all(axis=1)]

    for index, chunk in final_df.groupby(np.arange(len(final_df))//chunk_size):
        csv_buffer = StringIO()
        chunk.to_csv(csv_buffer, header=True, index=True)
        file_key = 'spotify_part_{}.csv'.format(index)
        s3_client.put_object(
            Bucket=bucket, Body=csv_buffer.getvalue(), Key=f"{folder}/{file_key}")

    try:
        glue_client.start_workflow_run(Name='is459_a2_etl_workflow')
        print("Job Status : Running ETL ")
    except Exception as e:
        print(e)
        raise


def lambda_handler(event, context):
    bucket = "is459-a2-spotify-data"
    folder = "raw_data"
    file_key = f"{folder}/spotify.csv"
    url = s3_client.generate_presigned_url(
        ClientMethod='get_object',
        Params={
            'Bucket': bucket,
            'Key': file_key
        },
        ExpiresIn=1000  # seconds
    )
    split_csv(url)

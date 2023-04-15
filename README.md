# IS459 Big Data A2

## Folder Structure
- Lambda
  
  Contains the code for splitting up the spotify kaggle dataset into different partition of 50,000 records per file
  
- Glue

  Contains the code for running AWS Glue Job for cleaning the data and aggregating the datasets for quicksight

## Steps to constructure pipeline
Steps to creating pipeline:
1. Create S3 Bucket wth the following name and subfolders:<br/>
   <b>Name:</b> is459-a2-spotify-data<br/>
   <b>Subfolders:</b> raw_data, partitioned_data, aggregate_data
   ![image](https://user-images.githubusercontent.com/80034115/232238941-5356a231-8178-4f9d-bfaf-4f4ba89d9a8a.png)

2. Create Lambda Function with the following name and runtime:<br/>
    <b>Name:</b> is459_a2_spotify_split<br/>
    <b>Runtime:</b> Python 3.9

3. Attach the following AWS layer to Lamda Function: 
   <img width="844" alt="image" src="https://user-images.githubusercontent.com/80034115/232238564-93e4e0bb-dba5-49dc-bb35-11ee93941668.png">
 
4. Attach IAM role to Lambda Function:<br/>
   <b>IAM Role:</b> Is459_a2_lambda_role<br/>
   ![image](https://user-images.githubusercontent.com/80034115/232239461-a09c7666-9504-47b7-9125-ec682083c441.png)

5. Change Memory & Execution Time for Lambda Function:
   <b>Memory:</b> 3008MB
   <b>Execution Time:</b> 15Mins
![image](https://user-images.githubusercontent.com/80034115/232239163-29cb6411-fe0e-4246-98a9-f501c4f890c8.png)
 
6.	Add S3 trigger to Lambda Function<br/>
![image](https://user-images.githubusercontent.com/80034115/232241480-3780fd4f-56de-4fb3-b8fa-cf984767fa64.png)

7.	Create Glue CSV Classifier with the following settings and name:<br/>
    <b>Name:</b> is459_a2_spotify_classifier<br/>
    <b>Headers:</b> id,uri,rank,artist_names,artists_num,artist_individual,artist_id,artist_genre,artist_img,collab,track_name,release_date,album_num_tracks,album_cover,source,peak_rank,previous_rank,weeks_on_chart,streams,week,danceability,energy,key,mode,loudness,speechiness,acousticness,instrumentalness,liveness,valence,tempo,duration,country,region,language,pivot<br/>
    <b>Settings:</b><br/>
    ![image](https://user-images.githubusercontent.com/80034115/232242278-007a0b32-3b2c-4589-8a8b-f405f74d14cd.png)

8.	Create Data Catalog Database with the following name:<br/>
    <b>Name:</b> is459_a2_spotify_db<br/>

9.	Create Glue Crawler with the following name, data source and IAM role:<br/>
    <b>Name:</b> is459_a2_spotify_crawler<br/>
    <b>Data Source:</b><br/>
![image](https://user-images.githubusercontent.com/80034115/232242558-b6476d6b-4f5d-4e5e-9528-816bbbd61b90.png)
    <b>IAM Role: AWSGlueServiceRole-crawler</b><br/>
    ![image](https://user-images.githubusercontent.com/80034115/232242930-1ba41cd7-9411-4909-86a5-76952de9ab52.png)

10.	Create Glue Job with the following name:<br/>
    <b>Name:</b> is459_a2_spotify_glue_job

11.	Attach IAM role to Glue Job:<br/>
    <b>IAM Role: Is459_a2_glue_role</b><br/>
![image](https://user-images.githubusercontent.com/80034115/232243234-eea468c6-124c-4504-ac4c-801107c0a955.png)

12.	Create Glue Workflow with the following name and workflow:<br/>
    <b>Name:</b> is459_a2_etl_workflow
![image](https://user-images.githubusercontent.com/80034115/232244033-52308095-78d4-42b4-a256-06e9c56b1c27.png)

13.	Create 5 manifest.json file for each aggregated dataset by copying the s3 URI of each dataset in the aggregate_data folder in the is459-a2-spotify-data s3 bucket

14.	Allow access to s3 bucket(is459-a2-spotify-data) in AWS Quicksight
![image](https://user-images.githubusercontent.com/80034115/232244598-916fdc80-3f4d-4810-9c68-01a8a119fa3d.png)

15.	Load data sources into Quicksight using manifest files<br/>
![image](https://user-images.githubusercontent.com/80034115/232244782-0af4cee3-c4d6-4ab6-a52c-d85e0157d2db.png)

16.	Create Dashboard and add the 5 data sources to the dashboard

17.	Create visualization for each business question
![image](https://user-images.githubusercontent.com/80034115/232244755-e6a39ba8-b04d-4864-9b10-f6cf710f119e.png)

18.	Rename each of the visualization accordingly<br/>
    a. Total Streams By Country and Artist Genre<br/>
    b. Artist By Total Streams and Average Number of Weeks On Chart<br/>
    c. Artist Genre Collaborations by Total Streams<br/>
    d. Danceability Vs Acousticeness By Streams<br/>
    e. Music Acousticness & Danceability by Source

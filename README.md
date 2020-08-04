# Data-Warehouse-AWS-Redshift
Data warehousing project on Amazon Redshift

## Project description
Sparkify is a music streaming startup with a growing user base and song database.

Their user activity and songs metadata data resides in json files in S3. The goal of the current project is to build an ETL pipeline that extracts their data from S3, stages them in Redshift, and transforms data into a set of dimensional tables for their analytics team to continue finding insights in what songs their users are listening to.

In this project most of ETL is done with SQL & Python, Amazon S3 is used for Data storage 
and Amazon Redshift is used as Data warehouse.


## ETL Pipeline

### Source Data
The source data is available at public Amazon s3 buckets ```s3://udacity-dend/log_data``` and ```s3://udacity-dend/song_data``` which contains log data and songs data respectively.

Log files contains songplay events of the users in json format while song_data contains list of songs details.

### Staging Tables

staging_events <br /> 
staging_songs

### Fact Table
**songplays** - records in event data associated with song plays i.e. records with page 
**NextSong** - songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent

### Dimension Tables
**users - users in the app -** user_id, first_name, last_name, gender, level
<br/> **songs - songs in music database -** song_id, title, artist_id, year, duration
<br /> **artists - artists in music database -** artist_id, name, location, lattitude, longitude
<br /> **time - timestamps of records in songplays broken down into specific units -** start_time, hour, day, week, month, year, weekday


## To run the project:

Update the dwh.cfg file with you Amazon Redshift cluster credentials and IAM role that can access the cluster.
   
   1. create_tables.py
      ```
         $ python3 create_tables.py
      ```
   2. etl.py
      ```
         $ python3 etl.py
      ```

## Project structure

**create_tables.py -** This script will drop old tables (if exist) ad re-create new tables
<br/> **etl.py -** This script executes the queries that extract JSON data from the S3 bucket and ingest them to Redshift
<br/> **sql_queries.py -** This file contains variables with SQL statement in String formats, partitioned by CREATE, DROP, COPY and INSERT statements
<br/> **dhw.cfg -** Configuration file used that contains info about Redshift, IAM and S3

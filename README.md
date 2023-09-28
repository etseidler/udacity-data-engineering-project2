# Sparkify Data Warehouse

## Project Summary

A music streaming startup, Sparkify, has grown their user base and song database and want to move their processes and data onto the cloud. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

Sparkify's analytics team wants to continue finding insights into what songs their users are listening to. The Redshift tables created by the code in this project will enable that team to perform queries on the data and gain those valuable business insights.

## Project Design

### Database Schema Design

Staging tables were built to hold the raw data in a more structured format. Columns were created and named based on existing column names in the raw data.

The main fact table, `songplays` was created to hold information about each song play in addition to foreign keys tying this fact table to the other dimension tables.

`time`, `users`, `artists`, and `songs` were created to hold the dimension data. The first attempt at the column data types and constraints were based on looking at the sample data and guidance from the project instructions. Subsequent changes were made in an iterative fashion as the staging and final table insert queries were executed one-by-one during ETL (see below).

### ETL Design

`# STAGING TABLES` in `sql_queries.py` takes care of moving the data from S3 into the staging tables. In both queries, JSON was the source data format, and region was specified since the raw data was located in a region that was different from the cluster region. A JSON metadata was specified as needed for the `staging_events` table.

`songplays` was the big lift here in terms of getting the ETL right. Filtering the song plays based on `page=NextSong` *before* doing the join was crucial for having a reasonable number of final results in that table. My initial take at these columns include more `NOT NULL` conditions, but those were removed as a result of getting errors during the ETL. These errors were related to null data in columns. Ideally, null data would have already been identified before running the ETL, and that's one thing I would have done differently. Lastly, my initial take at ETL for song plays included fewer fields in the join. E.g., I figured matching on only song title would have been enough. But after some thought, I decided that it wouldn't hurt to also match on artist name. The match on duration seems redudant, but shouldn't hurt anything.

`users`,`songs`, and `artists` were fairly straight-forward and originated from the raw data. There's not much to point out there except ensuring `DISTINCT` values on the primary keys for those tables.

`time` was populated from `songplays` mainly as a convenience for not having to repeatedly convert epoch seconds into a timestamp.

## How to run the scripts
### Prerequisites

#### `requirements.txt`

Make sure to create a virtual environment and install the prod `requirements.txt` before doing anything. If you're developing, you can install the `requirements-dev.txt`, which includes some linting tools.

#### Config file

You will need a file called `dwh.cfg` which resides in the same place as the rest of the scripts, see section below for more details.

#### You need a cluster!

If you don't already have a cluster, you can use the file `cluster_iac_helper.py` to create a cluster for you. This file will read from `dwh.cfg` and also requires a pre-existing user+role in AWS that has the following access and permissions:
- AmazonRedshiftFullAccess
- AmazonS3ReadOnlyAccess
- iam:AttachRolePolicy
- iam:CreateRole
- iam:GetRole
- iam:PassRole

You can use the following switches with this script:
- `-c`: create cluster
- `-v`: verify existing cluster
- `-d`: destroy cluster
- `-x`: exterminate cluster, which includes destroying the cluster and any roles created by the script

### Main Flow

Once you've got all the values in your `dwh.cfg` file populated with values from the cluster you had lying around or the one you created, *and your cluster is running*, then you're ready to begin. Run the scripts in the following order.

#### `create_tables.py`

Creates all the tables in Redshift. Will drop any existing tables if necessary, so *be careful with this one*.

#### `etl.py`

Loads the raw data from S3 into staging tables. Then uses the staging tables to populate the final tables in star schema format.

Hint: the raw data can take a little while to load, so make sure you have a beefy cluster.


## Files in the repository

Several have already been covered, but will be listed again for comprehensiveness.

- `README.md`: this file. primary source of documentation
- `cluster_iac_helper.py`: used to create and destroy clusters if you don't want to do that manually
- `create_tables.py`: creates staging and final tables in star schema
- `dwh.cfg`: actually NOT in the repo. it's necessary, but you have to create it. (See section below.)
- `etl.py`: transfers raw data from S3 into staging tables. then uses staging tables to populate final tables in star schema format
- `requirements.txt` and `requirements-dev.txt`: Python packages needed for running the script and doing development, respectively
- `sql_queries.py`: contains all the SQL queries used in `create_tables.py` and `etl.py`. Should not be called directly.

## Config File NOT in the repository

The file `dwh.cfg` will need to be created and populated. Some of the data in this config file is sensitive; that's why the file wasn't checked in to source control. You can copy/paste the text below and fill in the `???` values with the correct ones when you run the code locally. You'll need your own Amazon creds and cluster config settings to do this.

```
[AWS]
KEY=???
SECRET=???

[CLUSTER]
IDENTIFIER=???
DB_HOST=???
DB_NAME=???
DB_USER=???
DB_PASSWORD=???
DB_PORT=???
TYPE=multi-node
NUM_NODES=4
NODE_TYPE=dc2.large

[IAM_ROLE]
ARN=???
NAME=???

[S3]
LOG_DATA='s3://udacity-dend/log_data'
LOG_JSONPATH='s3://udacity-dend/log_json_path.json'
SONG_DATA='s3://udacity-dend/song_data'
```

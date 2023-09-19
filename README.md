# Sparkify Data Warehouse

## Project Summary

Discuss the purpose of this database in context of the startup, Sparkify, and their analytical goals.

## Project Design

State and justify your database schema design and ETL pipeline.

## How to run the scripts

## Example queries

Provide example queries and results for song play analysis. We do not provide you any of these. You, as part of the Data Engineering team were tasked to build this ETL. Thorough study has gone into the star schema, tables, and columns required. The ETL will be effective and provide the data and in the format required. However, as an exercise, it seems almost silly to NOT show SOME examples of potential queries that could be ran by the users. PLEASE use your imagination here. For example, what is the most played song? When is the highest usage time of day by hour for songs? It would not take much to imagine what types of questions that corporate users of the system would find interesting. Including those queries and the answers makes your project far more compelling when using it as an example of your work to people / companies that would be interested. You could simply have a section of sql_queries.py that is executed after the load is done that prints a question and then the answer.

## Files in the repository

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

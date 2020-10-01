# Overview

ETL jobs to move Sparkify's data describing songs and logging user actions into the cloud and prepare this data for analytical queries.

# Usage

Create empty tables in the Redshift cluster:

```
>>> python create_tables.py
```

ETL the data from S3 into the staging tables and then transform it and populate the dimensional tables:

```
>>> python etl.py
```

Configurations for the S3 buckets and Redshift cluster are set in a non-version-controlled file, `dwh.cfg`. All SQL queries used in the process are in `sql_queries.py`.

# Infrastructure

Our company stores raw data about songs and user activity on S3 in:

* `s3://udacity-dend/song_data`
* `s3://udacity-dend/log_data`

We provide JSON paths for the log data in: `s3://udacity-dend/log_json_path.json`

This script loads that data in a raw format into staging tables on redshift. Then, in a second step, it inserts the data into dimensional tables, in the following format:

**Fact Table**

* Songplays

**Dimensional Tables**

* Songs
* Artists
* Users
* Time

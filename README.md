# AWS-Glue-Pyspark-ETL-Job


This module performs statistical analysis on the noval corona virus dataset. The implementation is specifically
designed for AWS Glue environment. Can be used as a Glue Pyspark Job.
The dataset being used was last updated on May 02, 2020. 
The Module performs the following Functions:
* Reads data from csv files stored on AWS S3
* Perfroms Extract, Transform, Load (ETL) operations. 
* Lists max Cases for each country/region and provice/state
* Lists max Deaths for each country/region and provice/state
* List max Recoveries for each country/region and provice/state
* stores the aggregated output in parquet format
*


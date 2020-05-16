"""
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
"""

import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql.functions import col
from awsglue.job import Job

# getting the Job Name
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

# Creating Glue and Spark Contexts
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# catalog: database and table names, s3 output bucket
db_name = "glue-catalog-dbname"
tbl_name = "glue-catalog-table"
s3_write_bucket = "s3 bucket path"

############################
#        EXTRACT           #
############################

# creating datasource using the catalog table
datasource0 = glueContext.create_dynamic_frame.from_catalog(
    database=db_name, table_name=tbl_name)

# converting from Glue DynamicFrame to Spark Dataframe
dataframe = datasource0.toDF()

############################
#        TRANSFORM         #
############################

# dropping the last update column
datasource_df = dataframe.drop('last update')

# dropping rows if a row contains more than 4 null values
corona_df = datasource_df.dropna(thresh=4)

# replacing the missing value in Province/State column and populating with a default value
cleansed_data_df = corona_df.fillna(
    value='na_province_state', subset='province/state')


# Grouping the records by Province/State and Country/Region column, aggregating with max(Confirmed) column
# and sorting them in descending order of max of Confirmed cases.

most_cases_province_state_df = cleansed_data_df.groupBy('province/state', 'country/region').max('confirmed')\
    .select('province/state', 'country/region', col("max(confirmed)").alias("Most_Cases"))\
    .orderBy('max(confirmed)', ascending=False)

# Grouping the records by Province/State and Country/Region column, aggregating with max(Deaths) column
# and sorting them in descending order of max of Deaths.

most_deaths_province_state_df = cleansed_data_df.groupBy('province/state', 'country/region').max('deaths')\
    .select('province/state', 'country/region', col("max(deaths)").alias("Most_Deaths"))\
    .orderBy('max(deaths)', ascending=False)

# Grouping the records by Province/State and Country/Region column, aggregating with max(Recovered) column
# and sorting them in descending order of max of Recovered.

most_recoveries_province_state_df = cleansed_data_df.groupBy('province/state', 'country/region').max('recovered')\
    .select('province/state', 'country/region', col("max(recovered)").alias("Most_Recovered"))\
    .orderBy('max(recovered)', ascending=False)

# transforming Spark Dataframes back to Glue DynamicFrames
transform1 = DynamicFrame.fromDF(
    most_cases_province_state_df, glueContext, 'transform1')
transform2 = DynamicFrame.fromDF(
    most_deaths_province_state_df, glueContext, 'transform2')
transform3 = DynamicFrame.fromDF(
    most_recoveries_province_state_df, glueContext, 'transform3')

############################
#        LOAD              #
############################

# Storing the data on s3 specified path in parquet format
datasink1 = glueContext.write_dynamic_frame.from_options(frame=transform1, connection_type="s3", connection_options={
                                                         "path": s3_write_bucket+'/most-cases'}, format="parquet", transformation_ctx="datasink1")
datasink2 = glueContext.write_dynamic_frame.from_options(frame=transform2, connection_type="s3", connection_options={
                                                         "path": s3_write_bucket+'/most-deaths'}, format="parquet", transformation_ctx="datasink2")
datasink3 = glueContext.write_dynamic_frame.from_options(frame=transform3, connection_type="s3", connection_options={
                                                         "path": s3_write_bucket+'/most-recoveries'}, format="parquet", transformation_ctx="datasink3")

job.commit()

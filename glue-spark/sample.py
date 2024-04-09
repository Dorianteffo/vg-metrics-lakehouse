import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import SparkSession
from awsglue.context import GlueContext
from awsglue.job import Job


args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)

spark = glueContext.sparkSession.builder \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")\
    .getOrCreate()
    
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

df = spark.read.csv('s3://vg-sales-raw-data/')

df.write \
.format("delta") \
.mode("overwrite") \
.saveAsTable("bronze.vg_sales_bronze") 


job.commit()




""" 

Specify the jar for delta storage and delta core
Create database (with location) in Glue Database

"""
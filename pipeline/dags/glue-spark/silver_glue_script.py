import sys
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
import logging

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)

spark = (
    glueContext.sparkSession.builder.config(
        "spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension"
    )
    .config(
        "spark.sql.catalog.spark_catalog",
        "org.apache.spark.sql.delta.catalog.DeltaCatalog",
    )
    .getOrCreate()
)

job = Job(glueContext)
job.init(args['JOB_NAME'], args)


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)

logger = logging.getLogger(__name__)


def read_delta_table(table: str, database: str) -> DataFrame:
    """
    Read delta table stored in s3

    :param table : the table name
    :param database : the database name
    """

    df = spark.read.format('delta').load(
        f"s3://vg-lakehouse/lakehouse/{database}/{table}"
    )
    logger.info(f"Table {database}.{table} successfully loaded from delta lake!!")

    return df


def clean_games_table(games_df: DataFrame) -> DataFrame:
    """
    Clean games info table : normalized Rating column

    :param games_df : games spark dataframe

    :return spark dataframe
    """
    games_df = games_df.dropDuplicates(['GameID'])

    # normalize rating column
    games_df = games_df.withColumn(
        "Rating",
        F.when(F.col("Rating") == "E", "Everyone")
        .when(F.col("Rating") == "T", "Teen")
        .when(F.col("Rating") == "M", "Mature")
        .otherwise("NoRating"),
    )
    return games_df


def clean_activities_table(activities_df: DataFrame) -> DataFrame:
    """
    Clean activities table : normalized Level column, create session duration column

    :param activities_df : activites spark dataframe

    :return spark dataframe
    """

    # normalize level column
    activities_df = activities_df.withColumn(
        "Level",
        F.when(F.col("Level") < 30, "Beginner")
        .when((F.col("Level") > 30) & (F.col("Level") < 60), "Mid-Level")
        .when(F.col("Level") > 60, "Advanced")
        .otherwise("Unknown"),
    )

    activities_df = activities_df.withColumn(
        'StartTime', F.col('StartTime').cast('timestamp')
    )
    activities_df = activities_df.withColumn(
        'EndTime', F.col('EndTime').cast('timestamp')
    )

    activities_df = activities_df.withColumn(
        'SessionDuration',
        (F.col('EndTime').cast('long') - F.col('StartTime').cast('long')) / 60,
    )

    return activities_df


def write_delta_tables(table: str, database: str, df: DataFrame):
    """
    Write to delta lake on S3 and glue catalog

    :param table : delta table name (will be use in Glue datacatalog)
    :param database : glue database name
    :param df : spark dataframe
    """
    df.write.format("delta").mode("overwrite").saveAsTable(f"{database}.{table}")

    logger.info(f"Table {table} successfully loaded to {database} database!!")


def main():
    games_table = 'games_info'
    activities_table = 'players_activity'

    bronze_database = 'bronze'
    silver_database = 'silver'

    games_df = read_delta_table(games_table, bronze_database)
    activities_df = read_delta_table(activities_table, bronze_database)

    games_clean_df = clean_games_table(games_df)
    activities_clean_df = clean_activities_table(activities_df)

    write_delta_tables(games_table, silver_database, games_clean_df)
    write_delta_tables(activities_table, silver_database, activities_clean_df)


if __name__ == '__main__':
    main()
    job.commit()

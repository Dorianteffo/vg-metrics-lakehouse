import sys
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import DataFrame
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


def read_raw_data(path: str) -> DataFrame:
    """
    Read data from s3

    :param path : path to the file stored in S3

    :return spark dataframe
    """

    df = spark.read.option("multiline", "true").json(path)
    return df


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
    games_info_path = "s3://vg-raw-data/Games/*/*.json"
    players_activity_path = "s3://vg-raw-data/Players/*/*.json"

    games_df = read_raw_data(games_info_path)
    players_df = read_raw_data(players_activity_path)

    games_info_table = "games_info"
    players_activity_table = "players_activity"

    database = "bronze"

    write_delta_tables(games_info_table, database, games_df)
    write_delta_tables(players_activity_table, database, players_df)


if __name__ == '__main__':
    main()
    job.commit()

import sys
from awsglue.transforms import *
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

spark = glueContext.sparkSession.builder \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")\
    .getOrCreate()
    
job = Job(glueContext)
job.init(args['JOB_NAME'], args)



logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)

logger = logging.getLogger(__name__)



def read_delta_table(table : str,
                     database : str) -> DataFrame: 
    
    df = spark.read.format('delta') \
                   .load(f"s3://vg-lakehouse/lakehouse/{database}/{table}")
    logger.info(f"Table {database}.{table} successfully loaded from delta lake!!")

    return df 


def session_metrics(activites_df : DataFrame) -> DataFrame: 
    session_metrics_df = activites_df.select(
                                         F.mean(F.col("SessionDuration")).alias("Avg_SessionDuration"),
                                         F.mean(F.col("ExperiencePoints")).alias("Avg_ExperiencePoints"),
                                         F.mean(F.col("AchievementsUnlocked")).alias("Avg_AchievementsUnlocked"),
                                         F.mean(F.col("CurrencyEarned")).alias("Avg_CurrencyEarned"),
                                         F.mean(F.col("CurrencySpent")).alias("Avg_CurrencySpent"),
                                     ).orderBy(F.col('Avg_SessionDuration').desc())
    return session_metrics_df



def game_genre_metrics(activities_df : DataFrame,
                                    games_df : DataFrame
                                    )-> DataFrame : 
    
    game_genre_metrics_df = activities_df.join(games_df, on ='GameID', how='inner') \
                                                      .groupBy('Genre') \
                                                      .agg(
                                                          F.mean(F.col("SessionDuration")).alias("Avg_SessionDuration")
                                                      )
    return game_genre_metrics_df


def write_delta_tables(table : str,
                       database : str, 
                       df : DataFrame):
    df.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable(f"{database}.{table}") 
    
    logger.info(f"Table {table} successfully loaded to {database} database!!")


def main(): 
    games_table = 'games_info'
    activities_table = 'players_activity'

    silver_database = 'silver'
    gold_database = 'gold'


    games_df = read_delta_table(games_table, silver_database)
    activities_df = read_delta_table(activities_table, silver_database)

    session_metrics_df = session_metrics(activities_df)
    game_genre_metrics_df = game_genre_metrics(activities_df, games_df)

    write_delta_tables("Session_metrics", gold_database,session_metrics_df)
    write_delta_tables("Games_genre_metrics", gold_database,game_genre_metrics_df)


if __name__ == '__main__': 
    main()
    job.commit()
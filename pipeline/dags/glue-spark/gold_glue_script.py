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


def session_metrics(activites_df: DataFrame) -> DataFrame:
    """
    Calculate game session metrics : avg session duration, avg currency earned,...

    :param activities_df : activities spark dataframe

    :return spark dataframe
    """
    session_metrics_df = activites_df.select(
        F.round(F.mean(F.col("SessionDuration"))).alias("Avg_SessionDuration_minutes"),
        F.round(F.mean(F.col("ExperiencePoints"))).alias("Avg_ExperiencePoints"),
        F.round(F.mean(F.col("AchievementsUnlocked"))).alias("Avg_AchievementsUnlocked"),
        F.round(F.mean(F.col("CurrencyEarned"))).alias("Avg_CurrencyEarned"),
        F.round(F.mean(F.col("CurrencySpent"))).alias("Avg_CurrencySpent"),
        F.round(F.mean(F.col("QuestsCompleted"))).alias("Avg_QuestsCompleted"),
    ).orderBy(F.col('Avg_SessionDuration_minutes').desc())
    return session_metrics_df


def game_genre_metrics(activities_df: DataFrame, games_df: DataFrame) -> DataFrame:
    """
    Calculate metrics by game genre : avg session duration, total quests completed,..;

    :param activities_df : activities spark dataframe
    :param games_df : games spark dataframe

    :return spark dataframe
    """

    game_genre_metrics_df = (
        activities_df.join(games_df, on='GameID', how='inner')
        .groupBy('Genre')
        .agg(
            F.round(F.mean(F.col("SessionDuration"))).alias(
                "Avg_SessionDuration_minutes"
            ),
            F.sum(F.col("QuestsCompleted")).alias("Total_QuestsCompleted"),
            F.round(F.mean(F.col("Game_Length"))).alias("Avg_Game_Length_minutes"),
        )
    )
    return game_genre_metrics_df


def player_level_metrics(activities_df: DataFrame) -> DataFrame:
    """
    Caculate metrics by player level during a session

    :param activities_df : activities spark dataframe

    :return spark dataframe
    """
    player_lvl_metrics_df = activities_df.groupBy('Level').agg(
        F.round(F.mean(F.col("EnemiesDefeated"))).alias("Avg_EnemiesDefeated"),
        F.round(F.mean(F.col("QuestsCompleted"))).alias("Avg_QuestsCompleted"),
    )

    return player_lvl_metrics_df


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

    silver_database = 'silver'
    gold_database = 'gold'

    games_df = read_delta_table(games_table, silver_database)
    activities_df = read_delta_table(activities_table, silver_database)

    session_metrics_df = session_metrics(activities_df)
    game_genre_metrics_df = game_genre_metrics(activities_df, games_df)
    player_level_metrics_df = player_level_metrics(activities_df)

    write_delta_tables("Session_metrics", gold_database, session_metrics_df)
    write_delta_tables("Games_genre_metrics", gold_database, game_genre_metrics_df)
    write_delta_tables("Player_level_metrics", gold_database, player_level_metrics_df)

if __name__ == '__main__':
    main()
    job.commit()

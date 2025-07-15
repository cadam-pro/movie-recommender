from pyspark.sql import SparkSession


def build_spark_session() -> SparkSession:
    """
    Build a Spark session with specified configurations.

    Returns:
        SparkSession: A configured Spark session.
    """
    spark = (
        SparkSession.builder.appName("Movie Recommender")
        .config("spark.driver.memory", "8g")
        .getOrCreate()
    )

    print("Spark session created.")

    return spark

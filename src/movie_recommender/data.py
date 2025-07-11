"""
Load, preprocess, prepare, and save the Movie dataset.
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, when, split, array, year
from functools import reduce

from utils import build_spark_session
from params import DATA_PATH


def load_data(spark: SparkSession) -> DataFrame:
    """
    To be completed with Alpha and Clément's code.

    Load the movie dataset.

    Returns:
        dataframe: A Spark DataFrame containing the movie dataset.
    """
    df = spark.read.csv(
        DATA_PATH,
        header=True,
        inferSchema=True,
        sep=",",
        quote='"',
        escape='"',
        multiLine=True,
    )

    return df


def add_completeness_score_column(df: DataFrame) -> DataFrame:
    """
    Add a completeness score column to the movie dataset.

    Args:
        df: The movie dataset.

    Returns:
        dataframe: A Spark DataFrame with the completeness score column added.
    """
    cols_to_check = [
        "title",
        "original_title",
        "overview",
        "release_date",
        "genres",
        "production_countries",
        "production_companies",
        "spoken_languages",
        "cast",
        "director",
        "writers",
    ]
    completeness_expr = reduce(
        lambda acc, c: acc + when(col(c).isNotNull(), 1).otherwise(0),
        cols_to_check[1:],
        when(col(cols_to_check[0]).isNotNull(), 1).otherwise(0),
    )

    return df.withColumn("completeness_score", completeness_expr)


def change_column_types(df: DataFrame) -> DataFrame:
    """
    Change the data types of specific columns in the movie dataset.

    Args:
        df: The movie dataset.

    Returns:
        dataframe: A Spark DataFrame with updated column types.
    """
    df = (
        df.withColumn(
            "vote_average", df["vote_average"].try_cast("double").try_cast("float")
        )
        .withColumn("vote_count", df["vote_count"].try_cast("double").try_cast("int"))
        .withColumn("release_date", df["release_date"].try_cast("date"))
        .withColumn("runtime", df["runtime"].try_cast("double").try_cast("float"))
        .withColumn("budget", df["budget"].try_cast("double").try_cast("float"))
        .withColumn("popularity", df["popularity"].try_cast("double").try_cast("float"))
        .withColumn(
            "genres_array",
            when(col("genres").isNotNull(), split(col("genres"), ",\\s*")).otherwise(
                array()
            ),
        )
        .withColumn(
            "production_countries_array",
            when(
                col("production_countries").isNotNull(),
                split(col("production_countries"), ",\\s*"),
            ).otherwise(array()),
        )
        .withColumn(
            "production_companies_array",
            when(
                col("production_companies").isNotNull(),
                split(col("production_companies"), ",\\s*"),
            ).otherwise(array()),
        )
        .withColumn(
            "spoken_languages_array",
            when(
                col("spoken_languages").isNotNull(),
                split(col("spoken_languages"), ",\\s*"),
            ).otherwise(array()),
        )
        .withColumn(
            "cast_array",
            when(col("cast").isNotNull(), split(col("cast"), ",\\s*")).otherwise(
                array()
            ),
        )
        .withColumn(
            "director_array",
            when(
                col("director").isNotNull(), split(col("director"), ",\\s*")
            ).otherwise(array()),
        )
        .withColumn(
            "writers_array",
            when(col("writers").isNotNull(), split(col("writers"), ",\\s*")).otherwise(
                array()
            ),
        )
        .withColumn(
            "release_year", year("release_date").try_cast("double").try_cast("int")
        )
        .withColumn(
            "original_language_array",
            when(
                col("original_language").isNotNull(),
                split(col("original_language"), ",\\s*"),
            ).otherwise(array()),
        )
    )

    df = df.drop(
        "genres",
        "production_countries",
        "production_companies",
        "spoken_languages",
        "cast",
        "director",
        "writers",
        "original_language",
    )

    return df


def clean_data(df):
    """
    Clean the movie dataset.

    Args:
        df: The raw movie dataset.

    Returns:
        dataframe: A cleaned Spark DataFrame.
    """
    pass


def prepare_data(df):
    """
    Prepare the movie dataset.

    Args:
        df: The cleaned movie dataset.

    Returns:
        dataframe: A prepared Spark DataFrame ready for analysis.
    """
    pass


def save_data(df, path):
    """
    To be completed with Alpha's code.

    Save the movie dataset to a specified path.

    Args:
        df: The prepared movie dataset.
    """
    pass


if __name__ == "__main__":
    spark = build_spark_session()
    print("Spark session created.")
    df = load_data(spark)
    print("Data loaded successfully.")
    df = add_completeness_score_column(df)
    print("Completeness score column added.")
    df = change_column_types(df)
    print(df.printSchema())
    print("Column types changed successfully.")

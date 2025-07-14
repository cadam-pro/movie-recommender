from utils import build_spark_session
from data import load_data, clean_data, prepare_data
from pyspark.sql.functions import col
from pyspark.sql import DataFrame
from pyspark.sql.functions import udf
from pyspark.sql.types import DoubleType


def train(df: DataFrame, movie_id: int) -> DataFrame:
    """
    Train the movie recommender model.
    Args:
        df (DataFrame): The DataFrame containing movie data.
        movie_id (int): The ID of the movie to use as a target for recommendations.
    Returns:
        DataFrame: A DataFrame with cosine similarity scores for each movie.
    """
    target_vector_row = df.filter(col("id") == movie_id).select("norm_features").first()

    if target_vector_row is None:
        raise ValueError(f"Aucun film avec id={movie_id}")

    target_vector = target_vector_row["norm_features"]

    @udf(returnType=DoubleType())
    def cosine_similarity_udf(vec):
        """
        Calculate the cosine similarity between two vectors.
        Args:
            vec (Vector): The vector from the DataFrame.
            target_vector (Vector): The target vector to compare against.
        Returns:
            float: The cosine similarity score.
        """
        return float(vec.dot(target_vector))

    df_with_similarity = df.withColumn(
        "cosine_similarity", cosine_similarity_udf(col("content_features"))
    )

    print(f"Cosine similarity calculated for movie ID {movie_id}.")

    return df_with_similarity


def get_top_recommendations(df: DataFrame, movie_id: int) -> DataFrame:
    """
    Get the top 20 movie recommendations based on cosine similarity.
    Args:
        df (DataFrame): The DataFrame containing movie data with cosine similarity scores.
        target_id (int): The ID of the target movie for recommendations.
    Returns:
        DataFrame: A DataFrame with the top 20 recommended movies.
    """
    top_20 = (
        df.filter(col("id") != movie_id)
        .orderBy(col("cosine_similarity").desc())
        .limit(20)
    )

    popular = top_20.orderBy(col("popularity").desc()).limit(1).toJSON()
    underground = top_20.orderBy(col("popularity").asc()).limit(1).toJSON()
    newest = top_20.orderBy(col("release_date").desc()).limit(1).toJSON()

    # top_20.show(truncate=True)

    return {
        "popular": popular.collect(),
        "underground": underground.collect(),
        "newest": newest.collect(),
    }


if __name__ == "__main__":
    spark = build_spark_session()
    df = load_data(spark)
    df_clean = clean_data(df)
    df_vec = prepare_data(df_clean)
    df_train = train(df_vec, movie_id=62)
    get_top_recommendations(df_train, movie_id=62)
    spark.stop()

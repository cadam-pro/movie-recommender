from registry import load_data, save_data
from data import clean_data, prepare_data, save_json_data
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


def get_top_recommendations(df: DataFrame, movie_id: int):
    """
    Get the movie recommendations based on cosine similarity.
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
    print("Get the top 20 movie recommendations based on cosine similarity")

    # Collecter les données une fois pour toutes
    top_20_list = top_20.collect()
    print("Collected top 20 movies")

    # Transformer en dicts Python
    top_20_dicts = [row.asDict() for row in top_20_list]

    # Popularité max
    popular = max(top_20_dicts, key=lambda x: x["popularity"])
    print("Get the most popular movie recommended")

    # Popularité min
    underground = min(top_20_dicts, key=lambda x: x["popularity"])
    print("Get the most underground movie recommended")

    # Sortie la plus récente
    newest = max(
        (movie for movie in top_20_dicts if movie["release_date"] is not None),
        key=lambda x: x["release_date"],
    )
    print("Get the newest movie recommended")

    save_json_data(movie_id, popular, underground, newest)

    return {
        "popular": popular,
        "underground": underground,
        "newest": newest,
    }


if __name__ == "__main__":
    df = load_data("update")
    df_clean = clean_data(df)
    df_vec = prepare_data(df_clean)
    save_data(df_vec, "clean", "parquet")
    df_train = train(df_vec, movie_id=62)
    get_top_recommendations(df_train, movie_id=62)

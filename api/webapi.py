from fastapi import FastAPI
from src.movie_recommender.registry import load_data, load_json_from_gcs
from src.movie_recommender.train import get_top_recommendations, train

mr_api = FastAPI(
    title="Movie Recommender API",
    description="API for the web application : Movie Recommender!",
)


@mr_api.get("/")
def read_root():
    """
    Root endpoint of the Web API.
    """
    return {"message": "Welcome to the Web API!"}


@mr_api.get("/recommendations")
def get_recommendations(movie_id: int):
    data = load_json_from_gcs()
    if recommendations := get_recommendations_by_json(movie_id, data):
        return {"recommendations": recommendations}

    df_vec = load_data("clean", "parquet")
    df_train = train(df_vec, movie_id)
    get_top_recommendations(df_train, movie_id, data)

    data = load_json_from_gcs()
    recommendations = get_recommendations_by_json(movie_id, data)

    return {"recommendations": recommendations}


def get_recommendations_by_json(movie_id, data):
    return data.get(str(movie_id))

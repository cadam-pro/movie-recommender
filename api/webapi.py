from fastapi import FastAPI
from src.movie_recommender.params import JSON_PATH
from src.movie_recommender.registry import load_data
from src.movie_recommender.train import get_top_recommendations, train
import json
import os

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
def get_recommendations_by_jsonmmendations(movie_id: int):
    if recommendations := get_recommendations_by_json(movie_id):
        return {"recommendations": recommendations}

    df_vec = load_data("clean", "parquet")
    df_train = train(df_vec, movie_id)
    get_top_recommendations(df_train, movie_id)

    recommendations = get_recommendations_by_json(movie_id)

    return {"recommendations": recommendations}


def get_recommendations_by_json(movie_id, filename=JSON_PATH):
    if not os.path.exists(filename):
        return None

    with open(filename, "r", encoding="utf-8") as f:
        data = json.load(f)

    return data.get(str(movie_id))

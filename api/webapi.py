from fastapi import FastAPI
from src.movie_recommender.data import clean_data, load_data, prepare_data
from src.movie_recommender.train import get_top_recommendations, train
from src.movie_recommender.utils import build_spark_session

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
    # get data prepared
    # temp
    spark = build_spark_session()
    df = load_data(spark)
    df_clean = clean_data(df)
    df_vec = prepare_data(df_clean)
    df_train = train(df_vec, movie_id)
    recommendations = get_top_recommendations(df_train, movie_id)

    return {"recommendations": recommendations}

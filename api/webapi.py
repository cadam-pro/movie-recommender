from fastapi import FastAPI
from src.movie_recommender.registry import load_data
from src.movie_recommender.train import get_top_recommendations, train
from google.cloud import bigquery

# PROJECT= "inner-synapse-461715-n2"
# DATASET = "reccomander"
# TABLE = "reco_20"


client = bigquery.Client()

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
    # if recommendations := get_reco(movie_id) :
    #     return {"recommendations": recommendations}
    df_vec = load_data("clean", "parquet")
    df_train = train(df_vec, movie_id)
    recommendations = get_top_recommendations(df_train, movie_id)

    return {"recommendations": recommendations}
    # print(get_reco(3060))
    # return 1


# def get_reco(movie_id) :
#     # Perform a query.
#     QUERY = (
#         f'''SELECT * FROM `{PROJECT}.{DATASET}.{TABLE}`
#         WHERE movie_id="{movie_id}"
#         LIMIT 1''')
#     print(QUERY)
#     query_job = client.query(QUERY,location="EU")  # API request
#     rows = query_job.result()  # Waits for query to finish
#     for row in rows :
#         return row
#     return False


# def insert_reco(movie_id, recommadation):
#     # Perform a query.
#     QUERY = (
#         f'''INSERT  INTO   `{PROJECT}.{DATASET}.{TABLE}`
#         VALUES (
#         {movie_id},  -- movie_id
#         FORMAT('%04d', CAST(FLOOR(RAND() * 10000) AS INT64)),  -- reco_1
#         FORMAT('%04d', CAST(FLOOR(RAND() * 10000) AS INT64)),
#         FORMAT('%04d', CAST(FLOOR(RAND() * 10000) AS INT64)),
#         FORMAT('%04d', CAST(FLOOR(RAND() * 10000) AS INT64)),
#         FORMAT('%04d', CAST(FLOOR(RAND() * 10000) AS INT64)),
#         FORMAT('%04d', CAST(FLOOR(RAND() * 10000) AS INT64)),
#         FORMAT('%04d', CAST(FLOOR(RAND() * 10000) AS INT64)),
#         FORMAT('%04d', CAST(FLOOR(RAND() * 10000) AS INT64)),
#         FORMAT('%04d', CAST(FLOOR(RAND() * 10000) AS INT64)),
#         FORMAT('%04d', CAST(FLOOR(RAND() * 10000) AS INT64)),
#         FORMAT('%04d', CAST(FLOOR(RAND() * 10000) AS INT64)),
#         FORMAT('%04d', CAST(FLOOR(RAND() * 10000) AS INT64)),
#         FORMAT('%04d', CAST(FLOOR(RAND() * 10000) AS INT64)),
#         FORMAT('%04d', CAST(FLOOR(RAND() * 10000) AS INT64)),
#         FORMAT('%04d', CAST(FLOOR(RAND() * 10000) AS INT64)),
#         FORMAT('%04d', CAST(FLOOR(RAND() * 10000) AS INT64)),
#         FORMAT('%04d', CAST(FLOOR(RAND() * 10000) AS INT64)),
#         FORMAT('%04d', CAST(FLOOR(RAND() * 10000) AS INT64)),
#         FORMAT('%04d', CAST(FLOOR(RAND() * 10000) AS INT64)),
#         FORMAT('%04d', CAST(FLOOR(RAND() * 10000) AS INT64))
#     );''')
#     print(QUERY)
#     query_job = client.query(QUERY,location="EU")  # API request
#     rows = query_job.result()  # Waits for query to finish
#     for row in rows :
#         return row
#     return False

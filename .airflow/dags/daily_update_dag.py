from airflow import DAG
from datetime import datetime
from airflow.decorators import task
from dotenv import load_dotenv
from datetime import date, timedelta, datetime
import os

def init_spark_session():
    from SparkSessionSingleton import SparkSessionSingleton
    load_dotenv()
    key_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
    SparkSessionSingleton.initialize(key_path)

with DAG(
    dag_id="daily_update",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False
) as dag:

    @task()
    def get_update():
        from registry import save_data
        from updater import retrieve_updates
        init_spark_session()
        df_update = retrieve_updates()
        save_data(df_update, "update")
    update = get_update()

    @task()
    def get_new_movies():
    #step 3
        from registry import save_data
        from updater import list_new_movies_ids, map_movies, get_movies, associate_imdb_rating
        init_spark_session()
        today = date.today()
        yesterday = today - timedelta(days=1)
        today_str = today.strftime("%m_%d_%Y")
        yesterday_str = yesterday.strftime("%m_%d_%Y")
        new_movies_ids = list_new_movies_ids(today_str, yesterday_str)
        df_new_movies = map_movies(get_movies(new_movies_ids))
        df_new_movies_with_imdb_rating = associate_imdb_rating(df_new_movies)
        save_data(df_new_movies_with_imdb_rating, "news")
    new_movies = get_new_movies()

    @task()
    def merge_update():
    #step 4
        from registry import save_data, load_data, copy_final_file
        from updater import apply_update, clean_dataframe
        init_spark_session()
        df_movie = load_data("data")
        df_update = load_data("update")
        df_new_movies_with_imdb_rating = load_data("news")
        df_movie_with_update = clean_dataframe(apply_update(df_movie, df_update))
        final_df_updated_and_with_new_movies = df_movie_with_update.unionByName(clean_dataframe(df_new_movies_with_imdb_rating))
        save_data(clean_dataframe(final_df_updated_and_with_new_movies), "tmp")
        copy_final_file("tmp", "data")

    merge = merge_update()


update >> new_movies >> merge
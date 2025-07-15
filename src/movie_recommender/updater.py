#!/usr/bin/env python
# coding: utf-8

# !pip install requests pandas scikit-learn umap-learn plotly numpy pyspark kaggle


## IMPORT
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import expr, lit, col, when, count, coalesce, udf, to_date, date_format
import pyspark.sql.functions as sf
from pyspark.errors import PySparkValueError
from requests import HTTPError
from datetime import date, timedelta, datetime
import gzip
from io import BytesIO
import json
from SparkSessionSingleton import SparkSessionSingleton
import shutil
import kaggle
import time
from dotenv import load_dotenv
from pyspark.sql import DataFrame
from params import API_KEY, CSV_PATH_LOAD, CHANGE_URL, GET_MOVIE_URL, URL_IMDB, CSV_PATH_SAVE
from registry import load_data, create_save, cleanup_temp_files
from mapper_movie import map_cast, map_crew, update_cast, update_crew, update_genre, update_production_company, update_release_date, update_tagline
from session_updater import call_api
from http_requester import get_update, get_tmdb_movie_ids, get_movie, get_imdb_rating
from pyspark.sql.types import StringType
from http_requester import get_credit
from schema import schema_movie, schemaCSV
import os


##RETRIEVE CSV FROM KAGGLE
print("retrieve file kaggle")
#kaggle.api.authenticate()
#kaggle.api.dataset_download_files("alanvourch/tmdb-movies-daily-updates", path=DEST_FILE, unzip=True)


### Open csv
def load_data_local():
    df = SparkSessionSingleton.get_instance().read.csv(CSV_PATH_LOAD, header=True, inferSchema=True, sep=",", quote='"', escape='"', multiLine=True)
    #cast double column
    df = df.withColumn("id", df["id"].try_cast('int'))
    df = df.withColumn("vote_average", df["vote_average"].try_cast('double'))
    df = df.withColumn("vote_count", df["vote_count"].try_cast('double'))
    df = df.withColumn("revenue", df["revenue"].try_cast('double'))
    df = df.withColumn("runtime", df["runtime"].try_cast('double'))
    df = df.withColumn("budget", df["budget"].try_cast('double'))
# retrieve movie ID
    return df



            ### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -  ###
            ### - - - - - - - - - - - - - - - - - - MOVIE UPDATE - - - - - - - - - - - - - - - - - - ###
            ### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -  ###
            

### - - - - - - - - RETRIEVE IDS OF MODIFY MOVIES DURING THE PAST 24 HOURS - - - - - - - - ###
def get_last_movies_change():
   id_movies = []
   # page max 500 : api limitation to page 500
   for page in range(1, 5):
      data = call_api(CHANGE_URL, {
        "page": page
      })
      if data["results"] != []:
         id_movies += [movie["id"] for movie in data["results"] if (not movie["adult"]) and (movie["adult"] is not None) ]
      else:
         break
   return id_movies






### - - - - - - DATAFRAME MODIFICATION CREATION - - - - - - ###

def update_movie(changes, row):
    empty = False
    columns = [
        "id", "title", "vote_average", "vote_count", "status", "release_dates", "revenue", "runtime",
        "budget", "imdb_id", "original_language", "original_title", "overview", "popularity",
        "tagline", "genres", "production_companies", "production_countries", "spoken_languages",
        "cast", "director", "director_of_photography", "writers", "producers", "music_composer",
        "imdb_rating", "imdb_votes", "poster_path"
    ]
    credits = []
    for change in changes:
        if change.get("key") != "status" :
            if (change.get("key") in columns) or (change.get("key") == "crew"):
                match change.get("key"):
                    case "release_dates":
                        row[columns.index(change.get("key"))] = update_release_date(change.get("items")[0])
                    case "tagline":
                        row[columns.index(change.get("key"))] = update_tagline(change.get("items"))
                    case "cast":
                        if credits == []:
                            credits = get_credit(row[0])
                            row = update_cast(change, credits, row, columns)
                    case "crew":
                        if credits == []:
                            credits = get_credit(row[0])
                            row = update_crew(change, credits, row, columns)
                            pass
                    case "production_companies":
                        row[columns.index(change.get("key"))] = update_production_company(change.get("items"))
                    case "genres":
                        row[columns.index(change.get("key"))] = update_genre(change.get("items"))
                    case _:
                        value = change.get("items")[0].get("value")
                        if isinstance(value, int):
                            value = float(value)
                        row[columns.index(change.get("key"))] = value   
    return row, empty

def prepare_update(to_update_ids_list):
    rows = []
    start_time = time.time()
    num_requests = 0
    dataframe_size = len(schemaCSV.fields)
    for movie_id in to_update_ids_list:
        response = get_update(movie_id)
        num_requests += 1
        if num_requests % 100 == 0:
            print(str(num_requests) + " requests done")

        if response.status_code == 200:
            changes = response.json().get("changes", [])
            row = [None] * (dataframe_size)
            row[0] = movie_id
            row_update, empty = update_movie(changes, row)
            if(empty == False):
                rows.append(row_update)
        else:
            print(f"Erreur pour le film {movie_id} : {response.status_code}")
    end_time = time.time()
    duration = end_time - start_time
    print(str(num_requests) + " " + str(duration))
    print(num_requests / duration)
    return SparkSessionSingleton.get_instance().createDataFrame(rows, schema=schemaCSV)


### - - - - - - END DATAFRAME MODIFICATION CREATION - - - - - - ###

### - - - - - - APPLY UPDATE - - - - - - ###
def apply_update(df_movies, update_df):
    joined = df_movies.alias("a").join(update_df.alias("b"), on="id", how="left")

    columns = [c for c in df_movies.columns if c != "id"]
    df_merged = joined.select(
    col("id"),
    *[
        coalesce(col(f"b.{c}"), col(f"a.{c}")).alias(c)
        for c in columns
    ]
)
    return df_merged



def retrieve_updates():
    movie_ids = get_last_movies_change()
    return prepare_update(movie_ids)


            ### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -  ###
            ### - - - - - - - - - - - - - - - - - -  NEW MOVIES  - - - - - - - - - - - - - - - - - - ###
            ### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -  ###


def list_new_movies_ids(today_str, yesterday_str):
    new_movies_ids = list(set(retrieve_tmdb_movie_id(today_str)) - set(retrieve_tmdb_movie_id(yesterday_str)))
    return new_movies_ids

def retrieve_tmdb_movie_id(date_str):
    response = get_tmdb_movie_ids(date_str)
    movie_ids = []
    with gzip.open(BytesIO(response.content), 'rt', encoding='utf-8') as f:
        for line in f:
            movie = json.loads(line)
            movie_ids.append(movie['id'])
    return movie_ids


### - - - - - - FUNCTIONS TO RETRIEVE MOVIES - - - - - - ###



def get_movies(movies):
    movie_data = []
    for movie in movies:
        try:
            data = get_movie(movie)
            movie_data.append(data)
        except HTTPError as e:
            print(e)
    return movie_data

### - - - - - - MOVIES MAPPERS - - - - - - ###




### - - - - - - - - IMDB RATTING - - - - - - - - ###

def associate_imdb_rating(df_new_movies) -> DataFrame:
    file_name = URL_IMDB.split('/')[-1]
    response = get_imdb_rating()
    if response.status_code == 200:
        with open(file_name, 'wb') as f:
            f.write(response.raw.read())
        with gzip.open(file_name, 'rb') as f_in:
            with open(file_name[:-3], 'wb') as f_out:
                shutil.copyfileobj(f_in, f_out)
    else:
        print(f"Failed to download {file_name}. Status code: {response.status_code}")

    imdb_df = SparkSessionSingleton.get_instance().read.csv("./title.ratings.tsv", header=True, inferSchema=True, sep='\t')
    imdb_df = imdb_df.withColumnRenamed("tconst", "imdb_id")
    imdb_df = imdb_df.withColumnRenamed("averageRating", "imdb_rating")
    imdb_df = imdb_df.withColumnRenamed("numVotes", "imdb_votes")

    print(imdb_df.show())
    df_new_movies_with_imdb_rating = df_new_movies.join(imdb_df, how='left', on="imdb_id")
    df_new_movies_with_imdb_rating = df_new_movies_with_imdb_rating.withColumn("release_date", df_new_movies_with_imdb_rating["release_date"].try_cast('date'))
    return df_new_movies_with_imdb_rating

def map_movies(data):
    map_castUDF = udf(lambda x: map_cast(x),StringType())
    map_crewUDF = udf(lambda x, y: map_crew(x, y),StringType())
    movieDF = SparkSessionSingleton.get_instance().createDataFrame(data, schema=schema_movie)
    movieDF = movieDF.drop("adult").drop("backdrop_path").drop("belongs_to_collection").drop("homepage").drop("homepage").drop("video")
    movieDF = movieDF.withColumn("id", movieDF["id"].try_cast('int'))
    movieDF = movieDF.withColumn("genres",expr("array_join(transform(genres, x -> x.name), ', ')"))
    movieDF = movieDF.withColumn("production_countries",expr("array_join(transform(production_countries, x -> x.name), ', ')"))
    movieDF = movieDF.withColumn("production_companies",expr("array_join(transform(production_companies, x -> x.name), ', ')"))
    movieDF = movieDF.withColumn("spoken_languages",expr("array_join(transform(spoken_languages, x -> x.english_name), ', ')"))

    movieDF = movieDF.withColumn("director", map_crewUDF(sf.to_json(movieDF.credits), lit(["Director"])))
    movieDF = movieDF.withColumn("director_of_photography", map_crewUDF(sf.to_json(movieDF.credits), lit(["Director of Photography"])))
    movieDF = movieDF.withColumn("writers", map_crewUDF(sf.to_json(movieDF.credits), lit(["Writer"])))
    movieDF = movieDF.withColumn("producers", map_crewUDF(sf.to_json(movieDF.credits), lit(['Producer', 'Executive Producer'])))
    movieDF = movieDF.withColumn("music_composer", map_crewUDF(sf.to_json(movieDF.credits), lit(["Original Music Composer"])))
    movieDF = movieDF.withColumn("cast", map_castUDF(sf.to_json(movieDF.credits)))
    movieDF = movieDF.drop("credits")
    print(movieDF.count())
    return movieDF

def principal_methode():

    #step 1
    load_dotenv()
    key_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
    SparkSessionSingleton.initialize(key_path)
    today = date.today()
    yesterday = today - timedelta(days=1)
    today_str = today.strftime("%m_%d_%Y")
    yesterday_str = yesterday.strftime("%m_%d_%Y")

    #step 2
    df_update = retrieve_updates()
    create_save(df_update, "update")

    #step 3
    new_movies_ids = list_new_movies_ids(today_str, yesterday_str)
    df_new_movies = map_movies(get_movies(new_movies_ids))
    df_new_movies_with_imdb_rating = associate_imdb_rating(df_new_movies)
    create_save(df_new_movies_with_imdb_rating, "news")

    #step 4
    df_movie = load_data("data")
    df_update = load_data("update")
    df_new_movies_with_imdb_rating = load_data("news")
    df_movie_with_update = apply_update(df_movie, df_update)
    final_df_updated_and_with_new_movies = df_movie_with_update.unionByName(df_new_movies_with_imdb_rating)
    create_save(final_df_updated_and_with_new_movies, "data")
    

principal_methode()
sys.exit()

print("Write New CSV")


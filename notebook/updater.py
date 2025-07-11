#!/usr/bin/env python
# coding: utf-8

# In[178]:


# !pip install requests pandas scikit-learn umap-learn plotly numpy pyspark kaggle


# # IMPORT

# In[179]:


import requests
from pyspark.sql import SparkSession
from pyspark.sql.functions import expr, lit, col, when, count, coalesce, udf, to_date, date_format
import pyspark.sql.functions as sf
from pyspark.sql.types import *
from pyspark.errors import PySparkValueError
from requests import HTTPError
from datetime import date, timedelta, datetime
import requests
import gzip
from io import BytesIO
import json
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import shutil
import kaggle
import time
from collections import deque


# # SCHEMA

# In[180]:


schema = StructType([
    StructField("adult", BooleanType(), True),
    StructField("backdrop_path", StringType(), True),
    StructField("belongs_to_collection", StringType(), True),
    StructField("budget", IntegerType(), True),
    StructField("genres", ArrayType(StructType([
        StructField("name", StringType(), True)
    ]))),
    StructField("homepage", StringType(), True),
    StructField("id", IntegerType(), True),
    StructField("imdb_id", StringType(), True),
    StructField("original_language", StringType(), True),
    StructField("original_title", StringType(), True),
    StructField("overview", StringType(), True),
    StructField("popularity", DoubleType(), True),
    StructField("poster_path", StringType(), True),
    StructField("production_companies", ArrayType(StructType([
        StructField("name", StringType(), True)
    ]))),
    StructField("production_countries", ArrayType(StructType([
        StructField("name", StringType(), True)
    ]))),
    StructField("release_date", StringType(), True),
    StructField("revenue", IntegerType(), True),
    StructField("runtime", IntegerType(), True),
    StructField("spoken_languages", ArrayType(StructType([
        StructField("english_name", StringType(), True)
    ]))),
    StructField("status", StringType(), True),
    StructField("tagline", StringType(), True),
    StructField("title", StringType(), True),
    StructField("video", BooleanType(), True),
    StructField("vote_average", DoubleType(), True),
    StructField("vote_count", IntegerType(), True),
    StructField("credits", StructType([
        StructField("cast", ArrayType(StructType([
            StructField("adult", BooleanType(), True),
            StructField("gender", IntegerType(), True),
            StructField("id", IntegerType(), True),
            StructField("known_for_department", StringType(), True),
            StructField("name", StringType(), True),
            StructField("original_name", StringType(), True),
            StructField("popularity", DoubleType(), True),
            StructField("profile_path", StringType(), True),
            StructField("cast_id", IntegerType(), True),
            StructField("character", StringType(), True),
            StructField("credit_id", StringType(), True),
            StructField("order", IntegerType(), True)
        ])), True),
        StructField("crew", ArrayType(StructType([
            StructField("adult", BooleanType(), True),
            StructField("gender", IntegerType(), True),
            StructField("id", IntegerType(), True),
            StructField("known_for_department", StringType(), True),
            StructField("name", StringType(), True),
            StructField("original_name", StringType(), True),
            StructField("popularity", DoubleType(), True),
            StructField("profile_path", StringType(), True),
            StructField("credit_id", StringType(), True),
            StructField("department", StringType(), True),
            StructField("job", StringType(), True)
        ])), True)
    ]), True)
])


# # VAR ENV

# In[181]:


API_KEY = "fb24501dcc147d8b2a12ae4312215c55"
BASE_URL = "https://api.themoviedb.org/3"
CHANGE_URL = "movie/changes"
GET_MOVIE_URL = "movie/"
CSV_PATH_LOAD = "./data/tmdb/load/TMDB_all_movies.csv"
CSV_PATH_SAVE = "./data/tmdb/save/"
DEST_FILE = "./data/tmdb/load"


# # UTILS

# In[182]:


retry_strategy = Retry(
        total=1,
        status_forcelist=[500, 502, 503, 504],
        allowed_methods=["GET"],
        backoff_factor=1,
        raise_on_status=False
    )
adapter = HTTPAdapter(pool_connections=10, pool_maxsize=100, max_retries=retry_strategy)

session = requests.Session()
session.mount("https://", adapter)
session.mount("http://", adapter)

def call_api(endpoint, params):
    url = f"{BASE_URL}/{endpoint}"
    params["api_key"] = API_KEY
    response = session.get(url, params=params)
    response.raise_for_status()
    return response.json()


def map_cast(data):
    cast = []
    json_data = json.loads(data)
    for people in json_data.get("cast"):
        cast.append(people.get("name"))
    return ','.join(cast)
    

def map_crew(data, job):
    crew = []
    json_data = json.loads(data)
    for people in json_data.get("crew"):
         if people.get("job") in job:
            crew.append(people.get("name"))
    return ','.join(crew)

map_castUDF = udf(lambda x: map_cast(x),StringType())
map_crewUDF = udf(lambda x, y: map_crew(x, y),StringType())


# # RETRIEVE CSV FROM KAGGLE

# In[183]:

print("retrieve file kaggle")
#kaggle.api.authenticate()
#kaggle.api.dataset_download_files("alanvourch/tmdb-movies-daily-updates", path=DEST_FILE, unzip=True)


# ## Open csv

# In[184]:


print("Start read csv")
spark1 = SparkSession.builder.appName("Updater").getOrCreate()
df = spark1.read.csv(CSV_PATH_LOAD, header=True, inferSchema=True, sep=",", quote='"', escape='"', multiLine=True)
#cast double column
df = df.withColumn("id", df["id"].try_cast('int'))
df = df.withColumn("vote_average", df["vote_average"].try_cast('double'))
df = df.withColumn("vote_count", df["vote_count"].try_cast('double'))
df = df.withColumn("revenue", df["revenue"].try_cast('double'))
df = df.withColumn("runtime", df["runtime"].try_cast('double'))
df = df.withColumn("budget", df["budget"].try_cast('double'))
schemaCSV = df.schema
# retrieve movie ID
id_list = df.select('id').rdd.flatMap(lambda x: x).collect()

print("End read csv")

# # UPADTE MOVIES
# ### RETRIEVE IDS OF MODIFY MOVIES DURING THE PAST 24 HOURS

# In[ ]:


def get_last_movies_change():
   id_movies = []
   # page max 500 : api limitation to page 500
   for page in range(1, 10):
      data = call_api(CHANGE_URL, {
        "page": page
      })
      if data["results"] != []:
         id_movies += [movie["id"] for movie in data["results"] if (not movie["adult"]) and (movie["adult"] is not None) ]
      else:
         break
   return id_movies


# ### CREATE DATAFRAME OF MODIFICATION

# In[186]:
movie_ids = get_last_movies_change()
dataframe_size = len(schemaCSV.fields)
#print(dataframe_size)
# retrieve unknow id to avoid useless call
unknow_id = list(set(movie_ids) - set(id_list))

update_ids_list_clean = list(set(movie_ids) - set(unknow_id))

def retrieve_credit(credit_movie_id):
    url = f"https://api.themoviedb.org/3/movie/{credit_movie_id}/credits?api_key={API_KEY}"
    response = requests.get(url)
    if response.status_code == 200:
        return response.json()
    else:
        print("Failed to download the file: HTTP %d", response.status_code)
        return ""
    
def update_credit(row, credit, columns):
    pass

def update_release_date(change):
    if "value" in change:
        return datetime.strptime(change.get("value").get("release_date", {}), '%Y-%m-%d').date()
    else:
        return datetime.strptime(change.get("original_value").get("release_date", {}) , '%Y-%m-%d').date()
    
def update_tagline(change):
    for item in change:
        if "value" in item:
            return item.get("value").get("tagline")
        else:
            return item.get("original_value").get("tagline")   

def update_production_company(change):
    added = []
    for item in change:
        if item.get("action") == "added":
            added.append(item.get("value").get("name"))
    return ",".join(added)


def update_cast(change, credits, row, columns):
    added = []
    cast = credits.get("cast")
    for actor in cast:
        added.append(actor.get("original_name"))
    row[columns.index(change.get("key"))] = ",".join(added)
    return row

def update_crew(change, credits, row, columns):
    added_producer = []
    added_writer = []
    crew = credits.get("crew")
    for member in crew:
        job = member.get("job")
        match job:
            case "Director":
                row[columns.index("director")] = member.get("name")
            case "Original Music Composer":
                row[columns.index("music_composer")] = member.get("name")
            case "Director of Photography":
                row[columns.index("director_of_photography")] = member.get("name")
            case "Writer":
                added_writer.append(member.get("name"))
            case "Executive Producer":
                added_producer.append(member.get("name"))
            case "Producer":
                added_producer.append(member.get("name"))
            case _:
                pass
    row[columns.index("writers")] = ",".join(added_writer)
    row[columns.index("producers")] = ",".join(added_producer)
    return row

def update_genre(change):
    added = []
    for item in change:
        if item.get("acion") == "added":
            added.append(item.get("value").get("name"))
    return ",".join(added)


def get_update(to_update_ids_list):
    rows = []
    columns = [
        "id", "title", "vote_average", "vote_count", "status", "release_dates", "revenue", "runtime",
        "budget", "imdb_id", "original_language", "original_title", "overview", "popularity",
        "tagline", "genres", "production_companies", "production_countries", "spoken_languages",
        "cast", "director", "director_of_photography", "writers", "producers", "music_composer",
        "imdb_rating", "imdb_votes", "poster_path"
    ]
    start_time = time.time()
    num_requests = 0
    for movie_id in to_update_ids_list:
        url = f"https://api.themoviedb.org/3/movie/{movie_id}/changes?api_key={API_KEY}"
        response = session.get(url)
        num_requests += 1
        credits = []

        if num_requests % 100 == 0:
            print(str(num_requests) + " requests done")

        if response.status_code == 200:
            empty = True
            changes = response.json().get("changes", [])
            row = [None] * (dataframe_size)
            row[0] = movie_id
            for change in changes:
                if change.get("key") != "status" :
                    if (change.get("key") in columns) or (change.get("key") == "crew"):
                        empty = False
                        match change.get("key"):
                            case "release_dates":
                                row[columns.index(change.get("key"))] = update_release_date(change.get("items")[0])
                            case "tagline":
                                row[columns.index(change.get("key"))] = update_tagline(change.get("items"))
                            case "cast":
                                if credits == []:
                                    credits = retrieve_credit(movie_id)
                                row = update_cast(change, credits, row, columns)
                            case "crew":
                                if credits == []:
                                    credits = retrieve_credit(movie_id)
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
            if empty == False :
                rows.append(row)
        else:
            print(f"Erreur pour le film {movie_id} : {response.status_code}")
    end_time = time.time()
    duration = end_time - start_time
    print(str(num_requests) + " " + str(duration))
    print(num_requests / duration)
    return spark1.createDataFrame(rows, schema=schemaCSV)

print("Start retrieve upadte")
df_modif = get_update(update_ids_list_clean)
print("End retrieve upadte")

# ### APPLY MODIFICATIONS

# In[187]:


def apply_update(df_movie, update_df):
    joined = df_movie.alias("a").join(update_df.alias("b"), on="id", how="left")

    columns = [c for c in df.columns if c != "id"]
    df_merged = joined.select(
    col("id"),
    *[
        coalesce(col(f"b.{c}"), col(f"a.{c}")).alias(c)
        for c in columns
    ]
)
    return df_merged

#df = apply_update(df, df_modif)
print("Update applied")

# # GET NEW MOVIES
# ### GET NEW MOVIES IDS

# In[188]:


today = date.today()
yesterday = today - timedelta(days=1)
today_str = today.strftime("%m_%d_%Y")
yesterday_str = yesterday.strftime("%m_%d_%Y")


def retrieve_tmdb_movie_id(date_str):
    url = f"http://files.tmdb.org/p/exports/movie_ids_{date_str}.json.gz"
    response = requests.get(url)

    movie_ids = []

    if response.status_code != 200:
        print("Failed to download the file: HTTP %d", response.status_code)

    with gzip.open(BytesIO(response.content), 'rt', encoding='utf-8') as f:
        for line in f:
            movie = json.loads(line)
            movie_ids.append(movie['id'])
    return movie_ids

new_movies_ids = list(set(retrieve_tmdb_movie_id(today_str)) - set(retrieve_tmdb_movie_id(yesterday_str)))


# ### REMOVE MOVIE IDS ALREADY IN THE CSV

# In[189]:


new_ids_list_clean = list(set(new_movies_ids) - set(id_list))


# ### FUNCTIONS TO RETRIEVE MOVIES

# In[190]:


def get_movie(id= 1):
    data = call_api(GET_MOVIE_URL+str(id), {
        "language": "en-EN",
        "append_to_response": "credits",
        "id": id
    })
    return data

def get_movies(movies):
    movie_data = []
    for movie in movies:
        try:
            data = get_movie(movie)
            movie_data.append(data)
        except HTTPError as e:
            print(e)
    return movie_data

# ### MOVIE MAPER

# In[191]:


def map_movies(data):
        movieDF = spark1.createDataFrame(data, schema=schema)
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
        return movieDF



# ### RETRIEVE THE MOVIES BY ID

# In[192]:

print("retrieve new novies")
df_new_movies = map_movies(get_movies(new_ids_list_clean))
df_new_movies = df_new_movies.drop("credits")


# # GET IMDB RATING

# In[193]:


today = date.today()
yesterday = today - timedelta(days=1)
date_str = today.strftime("%m_%d_%Y")
url_imdb = "https://datasets.imdbws.com/title.ratings.tsv.gz"
file_name = url_imdb.split('/')[-1]
print(f"Downloading {file_name}...")
response = requests.get(url_imdb, stream=True)
if response.status_code == 200:
    with open(file_name, 'wb') as f:
        f.write(response.raw.read())
    print(f"Extracting {file_name}...")
    with gzip.open(file_name, 'rb') as f_in:
        with open(file_name[:-3], 'wb') as f_out:
            shutil.copyfileobj(f_in, f_out)
    print(f"Downloaded and extracted {file_name}.")
else:
    print(f"Failed to download {file_name}. Status code: {response.status_code}")

imdb_df = spark1.read.csv("./title.ratings.tsv", header=True, inferSchema=True, sep='\t')
imdb_df = imdb_df.withColumnRenamed("tconst", "imdb_id")
imdb_df = imdb_df.withColumnRenamed("averageRating", "imdb_rating")
imdb_df = imdb_df.withColumnRenamed("numVotes", "imdb_votes")

df_new_movies_with_imdb_rating = df_new_movies.join(imdb_df, how='left', on="imdb_id")
df_new_movies_with_imdb_rating = df_new_movies_with_imdb_rating.withColumn("release_date", df_new_movies_with_imdb_rating["release_date"].try_cast('date'))


# ## APPEND NEW MOVIES

# In[194]:

print("Add new movies")
df = df.unionByName(df_new_movies_with_imdb_rating)


# # WRITE UPDATED CSV

# In[195]:

print("Write New CSV")
df.coalesce(1).write.mode('overwrite').option("header", "true").csv(CSV_PATH_SAVE)


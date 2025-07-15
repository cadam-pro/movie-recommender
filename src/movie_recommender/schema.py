from pyspark.sql.types import *


## SCHEMA TO MAP MOVIES
schema_movie = StructType([
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

schemaCSV = StructType([
    StructField("id", IntegerType(), True),
    StructField("title", StringType(), True),
    StructField("vote_average", DoubleType(), True),
    StructField("vote_count", DoubleType(), True),
    StructField("status", StringType(), True),
    StructField("release_date", DateType(), True),
    StructField("revenue", DoubleType(), True),
    StructField("runtime", DoubleType(), True),
    StructField("budget", DoubleType(), True),
    StructField("imdb_id", StringType(), True),
    StructField("original_language", StringType(), True),
    StructField("original_title", StringType(), True),
    StructField("overview", StringType(), True),
    StructField("popularity", DoubleType(), True),
    StructField("tagline", StringType(), True),
    StructField("genres", StringType(), True),
    StructField("production_companies", StringType(), True),
    StructField("production_countries", StringType(), True),
    StructField("spoken_languages", StringType(), True),
    StructField("cast", StringType(), True),
    StructField("director", StringType(), True),
    StructField("director_of_photography", StringType(), True),
    StructField("writers", StringType(), True),
    StructField("producers", StringType(), True),
    StructField("music_composer", StringType(), True),
    StructField("imdb_rating", DoubleType(), True),
    StructField("imdb_votes", DoubleType(), True),
    StructField("poster_path", StringType(), True),
])
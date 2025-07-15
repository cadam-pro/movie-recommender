"""
Load, preprocess, prepare, and save the Movie dataset.
"""

from registry import load_and_merge_data, save_merged_data, save_cleaned_data
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, when, split, array, year, size, row_number
from pyspark.sql import Window
from pyspark.ml import Pipeline
from pyspark.ml.feature import (
    CountVectorizer,
    Tokenizer,
    StopWordsRemover,
    HashingTF,
    IDF,
    VectorAssembler,
    StandardScaler,
    Normalizer,
)
from functools import reduce

from params import DATA_PATH, CHECK_DUPLICATION_FEATURES


def load_data():
    df, spark = load_and_merge_data()
    if df is None:
        print("❌ Failed to load data.")
        return None
            
    print("Data loaded successfully.")
    return df, spark

def save_data(df):
    if df is not None:
        save_merged_data(df)
        return df
    else:
        return None

def add_completeness_score_column(df: DataFrame) -> DataFrame:
    """
    Add a completeness score column to the movie dataset.

    Args:
        df: The movie dataset.

    Returns:
        dataframe: A Spark DataFrame with the completeness score column added.
    """
    cols_to_check = [
        "title",
        "original_title",
        "overview",
        "release_date",
        "genres",
        "production_countries",
        "production_companies",
        "spoken_languages",
        "cast",
        "director",
        "writers",
    ]
    completeness_expr = reduce(
        lambda acc, c: acc + when(col(c).isNotNull(), 1).otherwise(0),
        cols_to_check[1:],
        when(col(cols_to_check[0]).isNotNull(), 1).otherwise(0),
    )

    return df.withColumn("completeness_score", completeness_expr)


def change_column_types(df: DataFrame) -> DataFrame:
    """
    Change the data types of specific columns in the movie dataset.

    Args:
        df: The movie dataset.

    Returns:
        dataframe: A Spark DataFrame with updated column types.
    """
    df = (
        df.withColumn(
            "vote_average", df["vote_average"].try_cast("double").try_cast("float")
        )
        .withColumn("vote_count", df["vote_count"].try_cast("double").try_cast("int"))
        .withColumn("release_date", df["release_date"].try_cast("date"))
        .withColumn("runtime", df["runtime"].try_cast("double").try_cast("float"))
        .withColumn("budget", df["budget"].try_cast("double").try_cast("float"))
        .withColumn("popularity", df["popularity"].try_cast("double").try_cast("float"))
        .withColumn(
            "genres_array",
            when(col("genres").isNotNull(), split(col("genres"), ",\\s*")).otherwise(
                array()
            ),
        )
        .withColumn(
            "production_countries_array",
            when(
                col("production_countries").isNotNull(),
                split(col("production_countries"), ",\\s*"),
            ).otherwise(array()),
        )
        .withColumn(
            "production_companies_array",
            when(
                col("production_companies").isNotNull(),
                split(col("production_companies"), ",\\s*"),
            ).otherwise(array()),
        )
        .withColumn(
            "spoken_languages_array",
            when(
                col("spoken_languages").isNotNull(),
                split(col("spoken_languages"), ",\\s*"),
            ).otherwise(array()),
        )
        .withColumn(
            "cast_array",
            when(col("cast").isNotNull(), split(col("cast"), ",\\s*")).otherwise(
                array()
            ),
        )
        .withColumn(
            "director_array",
            when(
                col("director").isNotNull(), split(col("director"), ",\\s*")
            ).otherwise(array()),
        )
        .withColumn(
            "writers_array",
            when(col("writers").isNotNull(), split(col("writers"), ",\\s*")).otherwise(
                array()
            ),
        )
        .withColumn(
            "release_year", year("release_date").try_cast("double").try_cast("int")
        )
        .withColumn(
            "original_language_array",
            when(
                col("original_language").isNotNull(),
                split(col("original_language"), ",\\s*"),
            ).otherwise(array()),
        )
    )

    return df.drop(
        "genres",
        "production_countries",
        "production_companies",
        "spoken_languages",
        "cast",
        "director",
        "writers",
        "original_language",
    )


def clean_data(df: DataFrame) -> DataFrame:
    """
    Clean the movie dataset.

    Args:
        df: The raw movie dataset.

    Returns:
        dataframe: A cleaned Spark DataFrame.
    """
    df = add_completeness_score_column(df)
    print("Completeness score column added.")
    df = change_column_types(df)
    print("Column types changed successfully.")
    # remove movies not released
    df = df.filter(df["status"] == "Released")
    # drop columns that are not useful for the calculation
    df = df.drop(
        "status",
        "imdb_id",
        "tagline",
        "director_of_photography",
        "producers",
        "imdb_rating",
        "imdb_votes",
        "music_composer",
        "revenue",
    )
    # drop rows that have no title
    df = df.filter(df["title"].isNotNull() & (df["title"] != ""))
    # replace null values in overview with empty string and release_year with median year
    median_year = df.approxQuantile("release_year", [0.5], 0.01)[0]
    df = df.fillna({"overview": "", "release_year": median_year})
    # drop rows who have completeness_score < 4
    df = df.filter(df["completeness_score"] >= 4)
    # drop rows with release_date, production_companies, production_countries, spoken_languages, cast, director, writers, overview and genres null
    df = df.filter(
        ~(
            df.release_date.isNull()
            & df.overview.isNull()
            & (size(col("production_companies_array")) == 0)
            & (size(col("production_countries_array")) == 0)
            & (size(col("spoken_languages_array")) == 0)
            & (size(col("cast_array")) == 0)
            & (size(col("director_array")) == 0)
            & (size(col("writers_array")) == 0)
            & (size(col("genres_array")) == 0)
        )
    )

    for col_name in CHECK_DUPLICATION_FEATURES:
        # check for duplicate titles with release_date and overview, and delete the line with the lowest completeness_score
        window = Window.partitionBy("title", col_name).orderBy(
            col("completeness_score").desc()
        )

        # Keep the most complete line
        df = (
            df.withColumn("row_num", row_number().over(window))
            .filter(col("row_num") == 1)
            .drop("row_num")
        )

    print("Data cleaned successfully.")

    return df

def save_dataframe_to_gcs(df):
    if df is not None:
        save_cleaned_data(df)
        return df
    else:
        return None

def prepare_data(df: DataFrame) -> DataFrame:
    """
    Prepare the movie dataset.

    Args:
        df: The cleaned movie dataset.

    Returns:
        dataframe: A prepared Spark DataFrame ready for analysis.
    """
    # use CountVectorizer to vectorize the genres, production_countries, production_companies, spoken_languages, cast, director, writers columns and original_language
    vectorizer_genres = CountVectorizer(
        inputCol="genres_array", outputCol="genres_vector"
    )
    vectorizer_production_countries = CountVectorizer(
        inputCol="production_countries_array", outputCol="production_countries_vector"
    )
    vectorizer_production_companies = CountVectorizer(
        inputCol="production_companies_array", outputCol="production_companies_vector"
    )
    vectorizer_spoken_languages = CountVectorizer(
        inputCol="spoken_languages_array", outputCol="spoken_languages_vector"
    )
    vectorizer_cast = CountVectorizer(inputCol="cast_array", outputCol="cast_vector")
    vectorizer_director = CountVectorizer(
        inputCol="director_array", outputCol="director_vector"
    )
    vectorizer_writers = CountVectorizer(
        inputCol="writers_array", outputCol="writers_vector"
    )
    vectorizer_original_language = CountVectorizer(
        inputCol="original_language_array", outputCol="original_language_vector"
    )
    print("Vectorizers created.")

    # Overview vectorization pipeline
    # 1. Tokenisation
    tokenizer = Tokenizer(inputCol="overview", outputCol="overview_tokens")
    # 2. delete empty words
    remover = StopWordsRemover(
        inputCol="overview_tokens", outputCol="overview_filtered"
    )
    # 3. TF
    hashingTF = HashingTF(
        inputCol="overview_filtered", outputCol="overview_tf", numFeatures=10000
    )
    # 4. IDF
    idf = IDF(inputCol="overview_tf", outputCol="overview_vector")
    print("Overview vectorization pipeline created.")

    # scale the release_year
    release_year_assembler = VectorAssembler(
        inputCols=["release_year"], outputCol="release_year_vec", handleInvalid="keep"
    )
    release_year_scaler = StandardScaler(
        inputCol="release_year_vec", outputCol="release_year_scaled"
    )
    print("Release year scaling pipeline created.")

    pipeline = Pipeline(
        stages=[
            tokenizer,
            remover,
            hashingTF,
            idf,
            vectorizer_genres,
            vectorizer_production_countries,
            vectorizer_production_companies,
            vectorizer_spoken_languages,
            vectorizer_cast,
            vectorizer_director,
            vectorizer_writers,
            vectorizer_original_language,
            release_year_assembler,
            release_year_scaler,
        ]
    )
    pipeline_model = pipeline.fit(df)
    df_vec = pipeline_model.transform(df)
    print("Pipeline fitted and data transformed.")

    df_vec = df_vec.drop(
        "release_year_vec", "overview_tokens", "overview_filtered", "overview_tf"
    )

    final_assembler = VectorAssembler(
        inputCols=[
            "genres_vector",
            "production_countries_vector",
            "production_companies_vector",
            "spoken_languages_vector",
            "cast_vector",
            "director_vector",
            "writers_vector",
            "original_language_vector",
            "release_year_scaled",
            "overview_vector",
        ],
        outputCol="content_features",
    )

    df_vec_assembler = final_assembler.transform(df_vec)
    print("Final features assembled.")

    normalizer = Normalizer(
        inputCol="content_features", outputCol="norm_features", p=2.0
    )
    print("Normalizer created.")

    return normalizer.transform(df_vec_assembler)


# def save_data(df, path):
    """
    To be completed with Alpha's code.

    Save the movie dataset to a specified path.

    Args:
        df: The prepared movie dataset.
    """
    pass

if __name__ == "__main__":

    # Étape 1 : Charger les données
    df, spark = load_data()
    if df is None:
        print("❌ Impossible de charger les données.")
        exit()

    # Étape 2 : Nettoyage
    df_cleaned = clean_data(df)
    print("✅ Données nettoyées.")
    df_cleaned.show(5)

    # Étape 4 : Sauvegarde (optionnelle)
    save_dataframe_to_gcs(df_cleaned)
    print("✅ Données nettoyées sauvegardées dans GCS.")

    # Étape 3 : Préparation (vectorisation, normalisation)
    df_prepared = prepare_data(df_cleaned)
    print("✅ Données préparées et vectorisées.")
    df_prepared.select("norm_features").show(5, truncate=False)

    spark.stop()

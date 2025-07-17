import pytest
from pyspark.sql import SparkSession, Row
from src.movie_recommender.data import (
    add_completeness_score_column,
    change_column_types,
)
from pyspark.sql.types import StructType, StructField, StringType


@pytest.fixture(scope="module")
def spark():
    return SparkSession.builder.master("local[1]").appName("Test").getOrCreate()


def test_add_completeness_score_column(spark):
    schema = StructType(
        [
            StructField("title", StringType(), True),
            StructField("original_title", StringType(), True),
            StructField("overview", StringType(), True),
            StructField("release_date", StringType(), True),
            StructField("genres", StringType(), True),
            StructField("production_countries", StringType(), True),
            StructField("production_companies", StringType(), True),
            StructField("spoken_languages", StringType(), True),
            StructField("cast", StringType(), True),
            StructField("director", StringType(), True),
            StructField("writers", StringType(), True),
        ]
    )

    data = [
        (
            "Titre 1",
            "Titre 1",
            "Blabla",
            "2020-01-01",
            "Action",
            "USA",
            "Warner",
            "en",
            "Tom Hanks",
            "Zemeckis",
            "Koepp",
        ),
        ("Titre 2", None, None, None, None, None, None, None, None, None, None),
    ]

    df = spark.createDataFrame(data, schema=schema)

    result_df = add_completeness_score_column(df).select("completeness_score").collect()

    assert result_df[0]["completeness_score"] == 11  # tout est présent
    assert result_df[1]["completeness_score"] == 1  # seulement "title"


def test_change_column_types(spark):
    data = [
        Row(
            vote_average="7.5",
            vote_count="100",
            release_date="2021-06-01",
            runtime="120",
            budget="50000000",
            popularity="80.5",
            genres="Drama, War",
            production_countries="US, UK",
            production_companies="Warner",
            spoken_languages="en",
            cast="Actor1, Actor2",
            director="Spielberg",
            writers="Writer1, Writer2",
            original_language="en",
        )
    ]
    df = spark.createDataFrame(data)
    df_transformed = change_column_types(df)

    row = df_transformed.first()

    # Test des conversions de type
    assert isinstance(row.vote_average, float)
    assert isinstance(row.vote_count, int)
    assert isinstance(row.runtime, float)
    assert isinstance(row.popularity, float)
    assert isinstance(row.budget, float)
    assert isinstance(row.release_year, int)

    # Test des colonnes array
    assert row.genres_array == ["Drama", "War"]
    assert row.production_countries_array == ["US", "UK"]
    assert row.cast_array == ["Actor1", "Actor2"]

    # Vérifie que les colonnes d'origine ont bien été supprimées
    dropped_columns = {
        "genres",
        "production_countries",
        "cast",
        "writers",
        "director",
        "spoken_languages",
        "original_language",
    }
    for col_name in dropped_columns:
        assert col_name not in df_transformed.columns

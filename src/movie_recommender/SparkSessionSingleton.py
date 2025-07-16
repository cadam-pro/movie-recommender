from pyspark.sql import SparkSession


class SparkSessionSingleton:
    _instance = None
    _key_path = None

    @classmethod
    def initialize(cls, key_path):
        print(key_path)
        if cls._instance is None:
            cls._key_path = key_path
            cls._instance = (
                SparkSession.builder.appName("Movie Recommender")
                .config(
                    "spark.jars.packages",
                    "com.google.cloud.bigdataoss:gcs-connector:hadoop3-2.2.5",
                )
                .config(
                    "spark.hadoop.fs.gs.impl",
                    "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem",
                )
                .config("spark.hadoop.google.cloud.auth.service.account.enable", "true")
                .config(
                    "spark.hadoop.google.cloud.auth.service.account.json.keyfile",
                    key_path,
                )
                .config("spark.sql.adaptive.enabled", "true")
                .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
                .config("spark.submit.pyFiles", "src/movie_recommender/mapper_movie.py")
                .config("spark.driver.memory", "8g")
                .getOrCreate()
            )
            cls._instance.sparkContext.setLogLevel("ERROR")

    @classmethod
    def get_instance(cls):
        if cls._instance is None:
            raise ValueError(
                "SparkSession is not initialized. Please call initialize(key_path) first."
            )
        return cls._instance

    @classmethod
    def stop_instance(cls):
        if cls._instance is not None:
            print("Fermeture de SparkSession")
            cls._instance.stop()
            cls._instance = None
            cls._key_path = None

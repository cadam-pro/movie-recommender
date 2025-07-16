#!/usr/bin/env python

import os
from dotenv import load_dotenv
from google.cloud import storage
from SparkSessionSingleton import SparkSessionSingleton

spark = None


def validate_environment():
    """Valide les variables d'environnement nécessaires"""
    required_vars = ["GCS_BUCKET_NAME", "GOOGLE_APPLICATION_CREDENTIALS"]
    missing_vars = []

    for var in required_vars:
        if not os.getenv(var):
            missing_vars.append(var)

    if missing_vars:
        print(f"❌ Variables d'environnement manquantes : {', '.join(missing_vars)}")
        return False

    return True


def read_csv_file(file_path, description):
    """Lit un fichier CSV avec gestion d'erreur"""
    spark = SparkSessionSingleton.get_instance()
    try:
        print(f"📂 Lecture du fichier {description} : {file_path}")
        df = spark.read.csv(
            file_path,
            header=True,
            inferSchema=True,
            sep=",",
            quote='"',
            escape='"',
            multiLine=True,
        )
        print(f"✅ Fichier {description} chargé avec succès ({df.count()} lignes)")
        return df
    except Exception as e:
        print(f"❌ Erreur lors de la lecture du fichier {description} : {e}")
        return None


def read_parquet_file(file_path, description):
    """Lit un fichier parquet avec gestion d'erreur"""
    spark = SparkSessionSingleton.get_instance()
    try:
        print(f"📂 Lecture du fichier {description} : {file_path}")
        df = spark.read.parquet(
            file_path,
            header=True,
            inferSchema=True,
            sep=",",
            quote='"',
            escape='"',
            multiLine=True,
        )
        print(f"✅ Fichier {description} chargé avec succès ({df.count()} lignes)")
        return df
    except Exception as e:
        print(f"❌ Erreur lors de la lecture du fichier {description} : {e}")
        return None


def cleanup_temp_files(temp_prefix):
    """Nettoie les fichiers temporaires"""
    print("🧹 Nettoyage des fichiers temporaires")
    bucket_name = os.getenv("GCS_BUCKET_NAME")
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    try:
        blobs = list(bucket.list_blobs(prefix=temp_prefix))
        for blob in blobs:
            blob.delete()
        print(f"✅ {len(blobs)} fichiers temporaires supprimés")
    except Exception as e:
        print(f"⚠️ Erreur lors du nettoyage : {e}")


def load_data(folder="", format="csv"):
    """Fonction principale pour charger et fusionner les données"""
    key_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
    SparkSessionSingleton.initialize(key_path)

    print("📦 Chargement des variables d'environnement")
    load_dotenv()

    # Validation des variables d'environnement
    if not validate_environment():
        return False

    bucket_name = os.getenv("GCS_BUCKET_NAME")
    source_blob = "*.parquet" if format == "parquet" else "*.csv"

    # Définition des chemins
    gcs_path = f"gs://{bucket_name}/{folder}/{source_blob}"

    try:
        # Lecture des fichiers
        df1 = (
            read_parquet_file(gcs_path, "GCS")
            if format == "parquet"
            else read_csv_file(gcs_path, "GCS")
        )
        if df1 is None:
            print("test")
            return False
        return df1
    except Exception as e:
        print(f"❌ Erreur inattendue : {e}")
        return False


def save_data(df, folder, format="csv"):
    print("💾 Écriture du DataFrame fusionné dans un répertoire temporaire GCS")
    bucket_name = os.getenv("GCS_BUCKET_NAME")

    cleanup_temp_files(folder)
    # Définition des chemins
    gcs_path = f"gs://{bucket_name}/{folder}/"
    try:
        if format == "parquet":
            df.coalesce(1).write.option("header", "true").mode("overwrite").parquet(
                gcs_path
            )
        else:
            df.coalesce(1).write.option("header", "true").mode("overwrite").csv(
                gcs_path
            )
        print("✅ Fichier temporaire écrit dans GCS")
    except Exception as e:
        print(f"❌ Erreur lors de l'écriture dans GCS : {e}")
        return False
    except Exception as e:
        print(f"❌ Erreur inattendue : {e}")
        return False

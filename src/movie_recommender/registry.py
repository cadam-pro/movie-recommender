#!/usr/bin/env python

import os
from dotenv import load_dotenv
from google.cloud import storage
from pyspark.sql.functions import col
import json
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
    print("TEEEEEESSSST")
    try:
        blobs = list(bucket.list_blobs(prefix=temp_prefix))
        for blob in blobs:
            print(blob)
            blob.delete()
        print(f"✅ {len(blobs)} fichiers temporaires supprimés")
    except Exception as e:
        print(f"⚠️ Erreur lors du nettoyage : {e}")


def copy_final_file(temp_prefix, final_path):
    """Copie le fichier final depuis le dossier temporaire"""
    print("🔍 Recherche du fichier CSV généré dans le dossier temporaire")
    bucket_name = os.getenv("GCS_BUCKET_NAME")
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    cleanup_temp_files(final_path)
    try:
        blobs = list(bucket.list_blobs(prefix=temp_prefix))
        csv_blob = next((blob for blob in blobs if blob.name.endswith('.csv')), None)
        
        if csv_blob:
            print(f"✅ Fichier CSV généré trouvé : {csv_blob.name}")
            print(f"📁 Copie vers : {final_path}")
            print(f"📁 Copie vers : {bucket}")
            file_name = csv_blob.name.split("/")[1]
            final_name = f"{final_path}/{file_name}"
            bucket.copy_blob(csv_blob, bucket, final_name)
            return True
        else:
            print("❌ Aucun fichier .csv trouvé dans le dossier temporaire")
            return False
    except Exception as e:
        print(f"❌ Erreur lors de la copie du fichier final : {e}")
        return False

def load_data(folder=""):
    """Fonction principale pour charger et fusionner les données"""
    key_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
    SparkSessionSingleton.initialize(key_path)

    print("📦 Chargement des variables d'environnement")
    load_dotenv()

    # Validation des variables d'environnement
    if not validate_environment():
        return False

    bucket_name = os.getenv("GCS_BUCKET_NAME")
    source_blob = f"*.{format}"

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


def load_json_from_gcs():
    """Charge un fichier JSON depuis Google Cloud Storage."""
    try:
        client = storage.Client()
        bucket_name = os.getenv("GCS_BUCKET_NAME")
        bucket = client.bucket(bucket_name)

        # Définition des chemins
        file_path = "json/recommendations.json"
        blob = bucket.blob(file_path)

        if not blob.exists():
            print(f"❌ Le fichier json n'existe pas dans le bucket {bucket_name}.")
            return {}

        content = blob.download_as_text()
        return json.loads(content)
    except Exception as e:
        print(f"❌ Erreur lors du chargement du JSON depuis GCS : {e}")
        return None


def save_json_to_gcs(data):
    """Écrit un dictionnaire Python dans un fichier JSON sur Google Cloud Storage."""
    try:
        client = storage.Client()
        bucket_name = os.getenv("GCS_BUCKET_NAME")
        bucket = client.bucket(bucket_name)

        # Définition des chemins
        file_path = "json/recommendations.json"
        blob = bucket.blob(file_path)

        json_data = json.dumps(data, ensure_ascii=False, indent=2, default=str)
        blob.upload_from_string(json_data, content_type="application/json")

        print(f"✅ Fichier JSON sauvegardé sur gs://{bucket_name}/json/*.json")
    except Exception as e:
        print(f"❌ Erreur lors de l'enregistrement du JSON sur GCS : {e}")
        return False

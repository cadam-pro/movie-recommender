#!/usr/bin/env python

import os
import sys
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from google.cloud import storage
from pyspark.sql.types import StringType
from pyspark.sql.functions import col

def validate_environment():
    """Valide les variables d'environnement nécessaires"""
    required_vars = ["GCS_BUCKET_NAME", "GCS_BLOB_NAME", "GOOGLE_APPLICATION_CREDENTIALS"]
    missing_vars = []
    
    for var in required_vars:
        if not os.getenv(var):
            missing_vars.append(var)
    
    if missing_vars:
        print(f"❌ Variables d'environnement manquantes : {', '.join(missing_vars)}")
        return False
    
    return True

def create_spark_session(key_path):
    """Crée et configure la session Spark"""
    print("🚀 Initialisation de SparkSession")
    
    spark = SparkSession.builder \
        .appName("Movie Recommender") \
        .config("spark.jars.packages", "com.google.cloud.bigdataoss:gcs-connector:hadoop3-2.2.5") \
        .config("spark.hadoop.fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem") \
        .config("spark.hadoop.google.cloud.auth.service.account.enable", "true") \
        .config("spark.hadoop.google.cloud.auth.service.account.json.keyfile", key_path) \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .getOrCreate()
    
    # Réduire la verbosité des logs
    spark.sparkContext.setLogLevel("ERROR")
    
    return spark

def read_csv_file(spark, file_path, description):
    """Lit un fichier CSV avec gestion d'erreur"""
    try:
        print(f"📂 Lecture du fichier {description} : {file_path}")
        df = spark.read.csv(
            file_path, 
            header=True, 
            inferSchema=True, 
            sep=",", 
            quote='"', 
            escape='"', 
            multiLine=True
        )
        print(f"✅ Fichier {description} chargé avec succès ({df.count()} lignes)")
        return df
    except Exception as e:
        print(f"❌ Erreur lors de la lecture du fichier {description} : {e}")
        return None

def convert_to_string_type(df):
    """Convertit toutes les colonnes en StringType"""
    print("🧪 Conversion des colonnes en StringType")
    for col_name in df.columns:
        df = df.withColumn(col_name, col(col_name).cast(StringType()))
    return df

def cleanup_temp_files(bucket, temp_prefix):
    """Nettoie les fichiers temporaires"""
    print("🧹 Nettoyage des fichiers temporaires")
    try:
        blobs = list(bucket.list_blobs(prefix=temp_prefix))
        for blob in blobs:
            blob.delete()
        print(f"✅ {len(blobs)} fichiers temporaires supprimés")
    except Exception as e:
        print(f"⚠️ Erreur lors du nettoyage : {e}")

def copy_final_file(bucket, temp_prefix, final_path):
    """Copie le fichier final depuis le dossier temporaire"""
    print("🔍 Recherche du fichier CSV généré dans le dossier temporaire")
    
    try:
        blobs = list(bucket.list_blobs(prefix=temp_prefix))
        csv_blob = next((blob for blob in blobs if blob.name.endswith('.csv')), None)
        
        if csv_blob:
            print(f"✅ Fichier CSV généré trouvé : {csv_blob.name}")
            print(f"📁 Copie vers : {final_path}")
            bucket.copy_blob(csv_blob, bucket, final_path)
            return True
        else:
            print("❌ Aucun fichier .csv trouvé dans le dossier temporaire")
            return False
    except Exception as e:
        print(f"❌ Erreur lors de la copie du fichier final : {e}")
        return False

def load_data():
    """Fonction principale pour charger et fusionner les données"""
    print("📦 Chargement des variables d'environnement")
    load_dotenv()
    
    # Validation des variables d'environnement
    if not validate_environment():
        return False
    
    bucket_name = os.getenv("GCS_BUCKET_NAME")
    source_blob = os.getenv("GCS_BLOB_NAME")
    key_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
    
    # Vérification de l'existence du fichier de clés
    if not os.path.exists(key_path):
        print(f"❌ Fichier de clés GCS introuvable : {key_path}")
        return False
    
    # Définition des chemins
    gcs_path = f"gs://{bucket_name}/{source_blob}"
    temp_path = f"gs://{bucket_name}/temp/csv_output/"
    final_path = "movies_recommender.csv"
    local_path = "data/test.csv"
    
    # Vérification de l'existence du fichier local
    if not os.path.exists(local_path):
        print(f"❌ Fichier local introuvable : {local_path}")
        return False
    
    # Initialisation de Spark
    spark = create_spark_session(key_path)
    
    try:
        # Lecture des fichiers
        df1 = read_csv_file(spark, gcs_path, "GCS")
        if df1 is None:
            return False
        
        df2 = read_csv_file(spark, local_path, "local")
        if df2 is None:
            return False
        
        # Vérification de la compatibilité des schémas
        if df1.columns != df2.columns:
            print("⚠️ Avertissement : Les schémas des fichiers ne correspondent pas exactement")
            print(f"Colonnes GCS : {df1.columns}")
            print(f"Colonnes local : {df2.columns}")
        
        # Conversion en StringType
        df1 = convert_to_string_type(df1)
        df2 = convert_to_string_type(df2)
        
        # Fusion des DataFrames
        print("🔗 Fusion des DataFrames")
        df = df1.union(df2)
        total_rows = df.count()
        print(f"📊 Nombre total de lignes fusionnées : {total_rows}")
        
        # Écriture dans GCS
        print("💾 Écriture du DataFrame fusionné dans un répertoire temporaire GCS")
        try:
            df.coalesce(1) \
              .write \
              .option("header", "true") \
              .mode("overwrite") \
              .csv(temp_path)
            print("✅ Fichier temporaire écrit dans GCS")
        except Exception as e:
            print(f"❌ Erreur lors de l'écriture dans GCS : {e}")
            return False
        
        # Initialisation du client GCS
        print("🔐 Initialisation du client GCS")
        try:
            client = storage.Client()
            bucket = client.bucket(bucket_name)
        except Exception as e:
            print(f"❌ Erreur lors de l'initialisation du client GCS : {e}")
            return False
        
        # Copie du fichier final
        if copy_final_file(bucket, 'temp/csv_output/', final_path):
            print(f"✅ Fichier final sauvegardé sous : gs://{bucket_name}/{final_path}")
            
            # Nettoyage des fichiers temporaires
            cleanup_temp_files(bucket, 'temp/csv_output/')
            
            print("🎉 Processus terminé avec succès !")
            return True
        else:
            return False
            
    except Exception as e:
        print(f"❌ Erreur inattendue : {e}")
        return False
    
    finally:
        print("🛑 Arrêt de Spark")
        spark.stop()

if __name__ == "__main__":
    success = load_data()
    sys.exit(0 if success else 1)
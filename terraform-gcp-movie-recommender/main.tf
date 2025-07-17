provider "google" {
  project = "data-engineering-461712" # Remplace par le bon ID
  region  = "europe-west1"
}

resource "google_storage_bucket" "projet_groupe_bucket" {
  name     = "movie-recommender-batch-062-test"   # doit être unique globalement
  location = "EU"
  force_destroy = true
}

resource "google_storage_bucket_object" "fichier_csv" {
  name   = "data/movies_recommender.csv" # le nom dans le bucket
  bucket = google_storage_bucket.projet_groupe_bucket.name
  source = "../data/TMDB_all_movies.csv" # le fichier local à uploader
}

## je voudrais crééer un dataset BigQuery pour le projet Movie Recommender
## avec une table recommender
# storage_bucket_object "fichier_csv_recommender" {
#   name   = "data/recommender.csv" # le nom dans le bucket
#   bucket = google_storage_bucket.projet_groupe_bucket.name
#   source = "../data/recommender.csv" # le fichier local à uploader
# }

# ## Création du dataset BigQuery pour le projet Movie Recommender
# resource "google_s


# bigquery_dataset "movie_recommender_dataset" {
#   dataset_id = "movie_recommender"
#   location   = "EU"
# }
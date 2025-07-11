import os

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_FOLDER = os.path.abspath(os.path.join(BASE_DIR, "../..", "data"))
DATA_PATH = os.path.join(DATA_FOLDER, "TMDB_all_movies.csv")

CHECK_DUPLICATION_FEATURES = ["release_date", "overview"]

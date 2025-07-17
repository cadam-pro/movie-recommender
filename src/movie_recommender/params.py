CHECK_DUPLICATION_FEATURES = ["release_date", "overview"]
JSON_PATH = "data/recommendations.json"


API_KEY = "fb24501dcc147d8b2a12ae4312215c55"
BASE_URL = "https://api.themoviedb.org/3"
CHANGE_URL = "movie/changes"
GET_MOVIE_URL = "movie/"
CSV_PATH_LOAD = "../../data/tmdb/load/TMDB_all_movies.csv"
CSV_PATH_SAVE = "./data/tmdb/save/"
DEST_FILE = "./data/tmdb/load"
URL_IMDB = "https://datasets.imdbws.com/title.ratings.tsv.gz"
CREDIT_URL = (
    "https://api.themoviedb.org/3/movie/{credit_movie_id}/credits?api_key={API_KEY}"
)
MOVIE_CHANGE_URL = (
    "https://api.themoviedb.org/3/movie/{movie_id}/changes?api_key={API_KEY}"
)
MOVIE_IDS_FILE_URL = "http://files.tmdb.org/p/exports/movie_ids_{date_str}.json.gz"

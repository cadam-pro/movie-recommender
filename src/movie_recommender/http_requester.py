from session_updater import session, call_api
from params import API_KEY, CSV_PATH_LOAD, CHANGE_URL, \
        CREDIT_URL, MOVIE_CHANGE_URL, MOVIE_IDS_FILE_URL, GET_MOVIE_URL, URL_IMDB, CSV_PATH_SAVE

def get_update(movie_id):
    url = MOVIE_CHANGE_URL.format(movie_id=movie_id, API_KEY=API_KEY)
    return session.get(url)

def get_tmdb_movie_ids(date_str):
    url = MOVIE_IDS_FILE_URL.format(date_str=date_str)
    response = session.get(url)
    if response.status_code != 200:
        print("Failed to download the file with tmdb movies ids: HTTP %d", response.status_code)
    return response


def get_credit(credit_movie_id):
    url = CREDIT_URL.format(credit_movie_id=credit_movie_id, API_KEY=API_KEY)
    response = session.get(url)
    if response.status_code == 200:
        return response.json()
    else:
        print("Failed to retrieve credits for the movie %d: HTTP %d", credit_movie_id, response.status_code)
        return ""

def get_movie(id= 1):
    data = call_api(GET_MOVIE_URL+str(id), {
        "language": "en-EN",
        "append_to_response": "credits",
        "id": id
    })
    return data

def get_imdb_rating():
    response = session.get(URL_IMDB, stream=True)
    if response.status_code == 200:
        return response
    else:
        print("Failed to download the file imdb rating: HTTP %d", response.status_code)
        return ""
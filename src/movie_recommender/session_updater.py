from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import requests
from params import BASE_URL, API_KEY

retry_strategy = Retry(
        total=1,
        status_forcelist=[500, 502, 503, 504],
        allowed_methods=["GET"],
        backoff_factor=1,
        raise_on_status=False
    )
adapter = HTTPAdapter(pool_connections=10, pool_maxsize=100, max_retries=retry_strategy)
session = requests.Session()
session.mount("https://", adapter)
session.mount("http://", adapter)

def call_api(endpoint, params):
    url = f"{BASE_URL}/{endpoint}"
    params["api_key"] = API_KEY
    response = session.get(url, params=params)
    response.raise_for_status()
    return response.json()

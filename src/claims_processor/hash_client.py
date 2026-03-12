import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import hashlib

HASHIFY_BASE_URL = "https://api.hashify.net/hash/md4/hex"

def _build_session() -> requests.Session:
    session = requests.Session()
    retries = Retry(
        total=3,
        backoff_factor=0.5,
        status_forcelist=[500, 502, 503, 504]
    )
    session.mount("https://", HTTPAdapter(max_retries=retries))
    return session

def hash_md4_via_api(claim_id: str, timeout: int = 5, session: requests.Session | None = None) -> str:
    session = session or _build_session()
    response = session.get(
        HASHIFY_BASE_URL,
        params={"value":claim_id},
        timeout=timeout
    )
    response.raise_for_status()
    return response.json()['Digest']

def hash_md4_local(claim_id: str) -> str:
    return hashlib.new("md4", claim_id.encode()).hexdigest()
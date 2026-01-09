# utils/finnhub_client.py
from decouple import config
import finnhub

_api_key = config("FINNHUB_APIKEY", default="")
_client = finnhub.Client(api_key=_api_key) if _api_key else None

def get_client():
    if not _client:
        raise RuntimeError("FINNHUB_APIKEY no configurada")
    return _client

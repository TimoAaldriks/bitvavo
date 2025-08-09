import time

from python_bitvavo_api.bitvavo import Bitvavo
from src.config import secrets
from src.logger import logger

class BitvavoAPIClientWrapper:
    """
    A wrapper for the Bitvavo API client that monitors and logs the
    API rate limit after each call.
    """
    def __init__(self, api_key: str, api_secret: str):
        self._client = Bitvavo({
            'api_key': api_key,
            'api_secret': api_secret,
        })
        # On initialization, log the current rate limit.
        self._previous_limit = self._client.getRemainingLimit()
        self.log_rate_limit()

    def log_rate_limit(self, attribute=None):
        """Fetches and logs the remaining API rate limit."""
        try:
            limit = self._client.getRemainingLimit()
            # if attribute:
                # logger.debug(f"Limit weight for Bitvavo.{attribute}: {self._previous_limit} {limit} {self._previous_limit - limit}, remaining: {limit}")
            # else:
            logger.debug(f"Rate limit remaining: {limit}")

            self._previous_limit = limit

        except Exception as e:
            logger.warning(f"Could not retrieve API rate limit: {e}")

    def __getattr__(self, name):
        """
        Proxies attribute access to the underlying client. If the attribute
        is a callable method, it wraps it to log the rate limit after execution.
        """
        attr = getattr(self._client, name)
        
        self._previous_limit = self._client.getRemainingLimit()
        if self._previous_limit < 50:
            logger.warning("Approaching API rate limit, waiting for reset...")

        while self._previous_limit < 50:
            time.sleep(1)
            self._previous_limit = self._client.getRemainingLimit()

        if callable(attr):
            def wrapper(*args, **kwargs):
                response = attr(*args, **kwargs)
                self.log_rate_limit(name)
                return response
            return wrapper
        return attr

def get_bitvavo_client():
    """Initializes and returns a Bitvavo API client instance."""
    return BitvavoAPIClientWrapper(
        api_key=secrets.BITVAVO_API_KEY,
        api_secret=secrets.BITVAVO_API_SECRET
    )
    

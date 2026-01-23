import logging
from urllib.parse import urljoin

import requests
from requests.adapters import HTTPAdapter, Retry

from workers.config import config

logger = logging.getLogger(__name__)


class LogRetry(Retry):

    def increment(self,
                  method=None,
                  url=None,
                  response=None,
                  error=None,
                  _pool=None,
                  _stacktrace=None, ):
        """Override the increment method to log a warning when retries happen."""
        retries = super().increment(method=method, url=url, response=response, error=error, _pool=_pool,
                                    _stacktrace=_stacktrace)
        if retries:
            logger.warning(
                f"Retrying {method} request to {url} (retry number {len(self.history)}). Error: {error.args}")
        return retries


def make_retry_adapter():
    # https://stackoverflow.com/questions/15431044/can-i-set-max-retries-for-requests-request
    # https://majornetwork.net/2022/04/handling-retries-in-python-requests/
    # https://urllib3.readthedocs.io/en/latest/reference/urllib3.util.html#module-urllib3.util.retry
    # retry for all HTTP methods (even non-idempotent methods like POST)
    # retry for all connection failures
    # retry for transient HTTP error response codes:
    #   429 - Too Many Requests
    #   502 - Bad Gateway
    #   503 - Service Unavailable
    #   not including 500, 504, because retrying for all HTTP methods could be dangerous
    #   if some process has already happened
    # delays between retries follow exponential backoff pattern
    # A backoff factor to apply between attempts after the second try.
    # delay = {backoff factor} * (2 ** ({number of total retries} - 1))
    # backoff_factor=5, delays = [0, 10, 20, 40, 80, 120, 120, 120, 120]
    # max idle time is 10 min 30s
    return HTTPAdapter(max_retries=LogRetry(
        total=9,
        backoff_factor=5,
        allowed_methods=None,
        status_forcelist=[429, 502, 503]
    ))


# https://stackoverflow.com/a/51026159/2580077
class CMGAPISession(requests.Session):
    def __init__(self, enable_retry: bool = True, use_auth: bool = True):
        super().__init__()
        # Every step in the workflow calls this API at least twice
        # Failing a long run step because the API is momentarily down for maintenance is wasteful
        # Retry adapter will keep trying to re-connect on connection and other transient errors up to 10m30s
        if enable_retry:
            adapter = make_retry_adapter()
            # noinspection HttpUrlsUsage
            self.mount("http://", adapter)
            self.mount("https://", adapter)
        self.base_url = config['cmg_api']['base_url']
        self.timeout = (config['cmg_api']['conn_timeout'], config['cmg_api']['read_timeout'])
        self.use_auth = use_auth
        if use_auth:
            self.auth_token = config['cmg_api']['auth_token']
        else:
            self.auth_token = None

    def request(self, method, url, *args, **kwargs):
        joined_url = urljoin(self.base_url, url)
        if 'timeout' not in kwargs:
            kwargs['timeout'] = self.timeout

        # Add auth header if enabled
        headers = kwargs.pop('headers', {})
        if self.use_auth and self.auth_token:
            headers['Authorization'] = f'Bearer {self.auth_token}'
        kwargs['headers'] = headers

        return super().request(method, joined_url, *args, **kwargs)


def get_session(session_id: str, use_auth: bool = False):
    """
    Get a CMG session by ID.
    
    Args:
        session_id: The session ID to retrieve
        use_auth: Whether to include authentication (default: False for public endpoint)
    
    Returns:
        dict: Session data from the API
    """
    with CMGAPISession(use_auth=use_auth) as s:
        r = s.get(f'sessions/{session_id}')
        r.raise_for_status()
        return r.json()


if __name__ == '__main__':
    pass


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
        """Override increment method to log a warning when retries happen."""
        retries = super().increment(method=method, url=url, response=response, error=error, _pool=_pool,
                                    _stacktrace=_stacktrace)
        if retries:
            logger.warning(
                f"Retrying {method} request to {url} (retry number {len(self.history)}). Error: {error.args}")
        return retries


def make_retry_adapter():
    return HTTPAdapter(max_retries=LogRetry(
        total=9,
        backoff_factor=5,
        allowed_methods=None,
        status_forcelist=[429, 502, 503]
    ))


class XENIUMAPISession(requests.Session):
    def __init__(self, enable_retry: bool = True, use_auth: bool = True):
        super().__init__()
        if enable_retry:
            adapter = make_retry_adapter()
            self.mount("http://", adapter)
            self.mount("https://", adapter)
        self.base_url = config['xenium_api']['base_url']
        self.timeout = (config['xenium_api']['conn_timeout'], config['xenium_api']['read_timeout'])
        self.use_auth = use_auth
        self.auth_token = config['xenium_api']['auth_token'] if use_auth else None

        if not self.base_url:
            raise RuntimeError('xenium_api.base_url is not configured')
        if self.use_auth and not self.auth_token:
            raise RuntimeError('xenium_api.auth_token is not configured (set XENIUM_API_TOKEN)')

    def request(self, method, url, *args, **kwargs):
        joined_url = urljoin(self.base_url, url)
        if 'timeout' not in kwargs:
            kwargs['timeout'] = self.timeout

        headers = kwargs.pop('headers', {})
        if self.use_auth and self.auth_token:
            headers['Authorization'] = f'Bearer {self.auth_token}'
        kwargs['headers'] = headers

        return super().request(method, joined_url, *args, **kwargs)


def get_dataset_by_origin_path(origin_path: str, use_auth: bool = True):
    """
    Get a Xenium dataset by origin path from the legacy-migration endpoint.
    """
    with XENIUMAPISession(use_auth=use_auth) as s:
        r = s.get('legacy-migration/dataset', params={'origin_path': origin_path})
        r.raise_for_status()
        response = r.json()

        if response.get('success') and response.get('found'):
            return response.get('dataset')
        return None


if __name__ == '__main__':
    pass

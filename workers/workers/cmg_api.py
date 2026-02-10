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


def get_dataset_by_origin_path(origin_path: str, use_auth: bool = True):
    """
    Get a CMG dataset by origin path.
    
    Args:
        origin_path: The origin path to search for
        use_auth: Whether to include authentication (default: True for legacy-migration endpoint)
    
    Returns:
        dict | None: Dataset data from the API, or None if not found
        
    Example successful response:
        {
            "success": true,
            "found": true,
            "normalized_path": "/opt/sca/cmg/data/source/dataset_name",
            "dataset": {
                "_id": "69848419e62d88fe0f0b3766",
                "name": "dataset_name",
                "archived": false,
                "taken": null,
                ...
            }
        }
    """
    with CMGAPISession(use_auth=use_auth) as s:
        r = s.get('api/legacy-migration/datasets', params={'origin_path': origin_path})
        r.raise_for_status()
        response = r.json()
        
        # Check if request was successful and dataset was found
        if response.get('success') and response.get('found'):
            return response.get('dataset')
        return None


def get_dataproduct_by_origin_path(origin_path: str, use_auth: bool = True):
    """
    Get a CMG dataproduct by origin path.
    
    Args:
        origin_path: The origin path to search for
        use_auth: Whether to include authentication (default: True for legacy-migration endpoint)
    
    Returns:
        dict | None: Dataproduct data from the API, or None if not found
        
    Example successful response:
        {
            "success": true,
            "found": true,
            "normalized_path": "/opt/sca/cmg/data/uploads/user_upload_123",
            "dataproduct": {
                "_id": "507f191e810c19729de860eb",
                "name": "user_upload_123",
                "origin_path": "/opt/sca/cmg/data/uploads/user_upload_123",
                "paths": {
                    "archive": "archive/products/user_upload_123.tar",
                    "staged": ""
                },
                ...
            }
        }
    """
    with CMGAPISession(use_auth=use_auth) as s:
        r = s.get('api/legacy-migration/dataproducts', params={'origin_path': origin_path})
        r.raise_for_status()
        response = r.json()
        
        # Check if request was successful and dataproduct was found
        if response.get('success') and response.get('found'):
            return response.get('dataproduct')
        return None


def is_dataset_archived_in_cmg(origin_path: str, dataset_type: str, use_auth: bool = True):
    """
    Check if a dataset/dataproduct is archived in CMG by checking database conditions.
    
    For RAW_DATA (dataset collection): checks dataset.archived === true
    For DATA_PRODUCT (dataproduct collection): checks dataproduct.paths.archive exists and is not empty
    
    Args:
        origin_path: The origin path of the dataset (used to query CMG)
        dataset_type: Either 'RAW_DATA' or 'DATA_PRODUCT'
        use_auth: Whether to include authentication (default: True)
    
    Returns:
        bool: True if archived in CMG, False otherwise
        
    Example flow:
        1. Query CMG by origin_path
        2. If found, check archived status
        3. Return True if archived, False otherwise
    """
    if dataset_type == 'RAW_DATA':
        # Check dataset collection
        dataset = get_dataset_by_origin_path(origin_path, use_auth=use_auth)
        if dataset:
            return dataset.get('archived', False) is True
        return False
        
    elif dataset_type == 'DATA_PRODUCT':
        # Check dataproduct collection
        dataproduct = get_dataproduct_by_origin_path(origin_path, use_auth=use_auth)
        if dataproduct:
            archive_path = dataproduct.get('paths', {}).get('archive', '')
            return bool(archive_path and archive_path != '')
        return False
        
    else:
        raise ValueError(f'Unknown dataset_type: {dataset_type}. Expected RAW_DATA or DATA_PRODUCT')


if __name__ == '__main__':
    pass


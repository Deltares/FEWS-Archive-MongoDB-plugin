#  Copyright (c) 2026 INFISYS INC

import urllib3
import requests

from requests import adapters

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


def session(pool_maxsize=adapters.DEFAULT_POOLSIZE):
	s = requests.Session()
	s.mount('https://', adapters.HTTPAdapter(pool_maxsize=pool_maxsize, max_retries=urllib3.util.retry.Retry(total=5, backoff_factor=1, status_forcelist=[429, 500, 502, 503, 504], allowed_methods=None)))
	s.verify = False
	return s

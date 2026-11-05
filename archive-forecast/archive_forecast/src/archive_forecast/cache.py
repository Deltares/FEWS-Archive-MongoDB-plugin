#  Copyright (c) 2026 INFISYS INC

import os
import hashlib
import logging

from datetime import datetime, timedelta, timezone
from archive_forecast.log import log

_logger = logging.getLogger('archive_forecast')
_days = 7


def folder(name):
	path = os.path.join(os.path.expanduser('~'), f'.{name}')
	os.makedirs(path, exist_ok=True)
	return path


@log
def clean(path):
	cache_dates = {datetime.fromtimestamp(os.path.getctime(os.path.join(path, f)), tz=timezone.utc): f for f in os.listdir(path)}
	[os.remove(os.path.join(path, cache_dates[d])) for d in cache_dates if d < datetime.now(tz=timezone.utc) - timedelta(days=_days)]


def get(path, key, extension, load, put):
	filename = os.path.join(path, f'{hashlib.sha3_256(key.encode()).hexdigest()}.{extension}')

	_logger.info(f'get begin [{key} -> {filename}]')

	if os.path.exists(filename):
		try:
			data = load(filename)
		except Exception:
			put(filename)
			data = load(filename)
	else:
		put(filename)
		data = load(filename)

	_logger.info('get complete')

	return data

#  Copyright (c) 2024 INFISYS INC
import logging
import traceback
import functools
import time
from datetime import datetime, timezone

logger = logging.getLogger('tva_graphcast')

_retry_attempts = 5
_retry_pause_seconds = 5


def log(func):
	@functools.wraps(func)
	def wrapper(*args, **kwargs):
		then = datetime.now(timezone.utc)
		logger.info(f'{func.__name__} begin')
		try:
			result = func(*args, **kwargs)
			logger.info(f'{func.__name__} complete in {datetime.now(timezone.utc) - then}')
		except Exception as e:
			logger.error(traceback.format_exc())
			raise e
		return result
	return wrapper


def retry(fn=None, retry_attempts=_retry_attempts):
	if fn is None:
		return functools.partial(retry, retry_attempts=retry_attempts)

	@functools.wraps(fn)
	def wrapper(*args, **kwargs):
		for retry_attempt in range(1, retry_attempts + 1):
			try:
				return fn(*args, **kwargs)
			except Exception:
				if retry_attempt == _retry_attempts:
					raise
				time.sleep(_retry_pause_seconds * retry_attempt)
	return wrapper

#  Copyright (c) 2026 INFISYS INC
import logging
import traceback
import functools
from datetime import datetime, timezone


def log(func):
	logger = logging.getLogger(func.__module__.split('.')[0])

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

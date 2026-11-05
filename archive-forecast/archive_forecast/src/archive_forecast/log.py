#  Copyright (c) 2026 INFISYS INC
import logging
import traceback
import functools
from datetime import datetime, timezone

_logging_format = '{"time": "%(asctime)s", "level": "%(levelname)s", "name": "%(name)s", "message": "%(message)s"}'


def configure(name):
	logging.basicConfig(filename=f'{name}.log', level=logging.INFO, filemode='w+', format=_logging_format)
	stream_handler = logging.StreamHandler()
	stream_handler.setFormatter(logging.Formatter(_logging_format))
	stream_handler.setLevel(logging.INFO)
	logging.getLogger().addHandler(stream_handler)


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

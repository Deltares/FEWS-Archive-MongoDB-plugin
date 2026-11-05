#  Copyright (c) 2026 INFISYS INC
import functools
import time

_retry_attempts = 5
_retry_pause_seconds = 5


def retry(fn=None, retry_attempts=_retry_attempts):
	if fn is None:
		return functools.partial(retry, retry_attempts=retry_attempts)

	@functools.wraps(fn)
	def wrapper(*args, **kwargs):
		for retry_attempt in range(1, retry_attempts + 1):
			try:
				return fn(*args, **kwargs)
			except Exception:
				if retry_attempt == retry_attempts:
					raise
				time.sleep(_retry_pause_seconds * retry_attempt)
	return wrapper

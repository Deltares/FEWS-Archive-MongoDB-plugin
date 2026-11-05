#  Copyright (c) 2026 INFISYS INC

import pytest

from archive_forecast import retry as retry_module
from archive_forecast.retry import retry


@pytest.fixture(autouse=True)
def no_pause(monkeypatch):
	monkeypatch.setattr(retry_module, '_retry_pause_seconds', 0)


def _flaky(failures):
	calls = []

	def function():
		calls.append(1)
		if len(calls) <= failures:
			raise ValueError('fail')
		return 'ok'
	return function, calls


def test_retry_returns_after_transient_failures():
	function, calls = _flaky(2)
	assert retry(function)() == 'ok'
	assert len(calls) == 3


def test_retry_raises_after_all_attempts():
	function, calls = _flaky(10)
	with pytest.raises(ValueError):
		retry(function)()
	assert len(calls) == retry_module._retry_attempts


def test_retry_with_custom_attempts():
	function, calls = _flaky(10)
	with pytest.raises(ValueError):
		retry(retry_attempts=2)(function)()
	assert len(calls) == 2

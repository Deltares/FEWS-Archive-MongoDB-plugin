#  Copyright (c) 2026 INFISYS INC

from archive_forecast import web


def test_session_retries_transient_http_errors():
	with web.session() as s:
		retries = s.get_adapter('https://example.com').max_retries
		assert retries.total == 5
		assert set(retries.status_forcelist) == {429, 500, 502, 503, 504}
		assert retries.allowed_methods is None


def test_session_disables_certificate_verification():
	with web.session() as s:
		assert s.verify is False


def test_session_pool_size():
	with web.session(pool_maxsize=16) as s:
		assert s.get_adapter('https://example.com')._pool_maxsize == 16

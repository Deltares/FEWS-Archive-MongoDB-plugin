#  Copyright (c) 2026 INFISYS INC

import json

from contextlib import nullcontext
from datetime import datetime

import numpy as np
import pandas as pd
import pytest

from weathernext_3 import inputs

_corners = [[-89.5, 38.1], [-81.0, 33.4]]


def test_read_uses_latest_six_hour_init_before_t0_and_hourly_times(monkeypatch):
	calls = []
	monkeypatch.setattr(inputs.cache, 'clean', lambda path: None)
	monkeypatch.setattr(inputs, '_get_forecast', lambda *args: calls.append(args))

	inputs.read(datetime(2026, 11, 3, 7), 1, 3, ['temperature_2m'], _corners)

	init, times, fields, corners = calls[0]
	assert init == pd.Timestamp('2026-11-03 06:00')
	assert times == [datetime(2026, 11, 3, 7), datetime(2026, 11, 3, 8), datetime(2026, 11, 3, 9)]


@pytest.fixture
def pixels(monkeypatch):
	requests = []

	def query(session, url, request):
		requests.append((url, request))
		height, width = request['grid']['dimensions']['height'], request['grid']['dimensions']['width']
		hour = int(url[-4:-2])
		return {band: np.full((height, width), hour + (-1.0 if band.endswith('mean') else int(band.rsplit('_p', 1)[-1])), dtype='float32') for band in request['bandIds']}

	monkeypatch.setattr(inputs, '_get_session', lambda: nullcontext())
	monkeypatch.setattr(inputs, '_query', query)
	return requests


def test_get_forecast_requests_one_asset_per_hour_with_mean_and_percentile_bands(pixels):
	times = [datetime(2026, 11, 3, 7), datetime(2026, 11, 3, 8)]
	inputs._get_forecast(pd.Timestamp('2026-11-03 06:00'), times, ['temperature_2m'], _corners)

	assert [url.rsplit('/', 1)[-1] for url, _ in pixels] == ['202611030600_202611030700', '202611030600_202611030800']
	assert pixels[0][1]['bandIds'] == ['temperature_2m_mean', 'temperature_2m_p10', 'temperature_2m_p25', 'temperature_2m_p50', 'temperature_2m_p75', 'temperature_2m_p90']


def test_get_forecast_builds_mean_and_percentile_datasets(pixels):
	times = [datetime(2026, 11, 3, 7), datetime(2026, 11, 3, 8)]
	single_level, percentiles = inputs._get_forecast(pd.Timestamp('2026-11-03 06:00'), times, ['temperature_2m', 'total_precipitation_1hr'], _corners)

	assert single_level['temperature_2m'].dims == ('time', 'lat', 'lon')
	assert percentiles['temperature_2m'].dims == ('time', 'lat', 'lon', 'percentile')
	assert list(percentiles['percentile'].values) == [10, 25, 50, 75, 90]
	assert single_level['temperature_2m'].dtype == np.float64
	assert np.all(np.diff(single_level['lat'].values) > 0)
	assert single_level['lat'].min() >= 33.4 and single_level['lat'].max() <= 38.1
	assert single_level['lon'].min() >= 270.5 and single_level['lon'].max() <= 279.0
	assert list(single_level['temperature_2m'].values[:, 0, 0]) == [6.0, 7.0]
	assert list(percentiles['temperature_2m'].values[0, 0, 0]) == [17.0, 32.0, 57.0, 82.0, 97.0]


class _Response:
	def __init__(self, status_code, content=b'npy'):
		self.status_code, self.content, self.text = status_code, content, 'error'


class _Session:
	def __init__(self, response):
		self.response, self.posts = response, []

	def post(self, url, json):
		self.posts.append((url, json))
		return self.response


def test_put_data_cache_posts_get_pixels_and_writes_file(tmp_path):
	session = _Session(_Response(200))
	filename = tmp_path / 'data.npy'

	inputs._put_data_cache(session, 'https://earthengine/assets/x', {'bandIds': ['a']}, str(filename))

	assert session.posts == [('https://earthengine/assets/x:getPixels', {'bandIds': ['a']})]
	assert filename.read_bytes() == b'npy'


def test_put_data_cache_raises_on_http_error(tmp_path):
	with pytest.raises(ImportError):
		inputs._put_data_cache(_Session(_Response(403)), 'https://earthengine/assets/x', {}, str(tmp_path / 'data.npy'))


def test_query_caches_npy_by_url_and_request(monkeypatch):
	calls = []
	monkeypatch.setattr(inputs.cache, 'get', lambda path, key, extension, load, put: calls.append((path, json.loads(key), extension, load)))

	inputs._query('session', 'https://earthengine/assets/x', {'bandIds': ['a']})

	path, key, extension, load = calls[0]
	assert path == inputs._home_path
	assert key == {'https://earthengine/assets/x': {'bandIds': ['a']}}
	assert extension == 'npy'
	assert load is np.load

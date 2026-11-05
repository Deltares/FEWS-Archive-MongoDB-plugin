#  Copyright (c) 2026 INFISYS INC

import os
import json
import logging

import google.auth
import numpy as np
import xarray as xr
import pandas as pd

from google.auth.transport.requests import Request
from concurrent.futures import ThreadPoolExecutor
from datetime import timedelta
from archive_forecast import cache, web
from archive_forecast.log import log
from archive_forecast.retry import retry

_logger = logging.getLogger('weathernext_3')
_home_path = cache.folder('weathernext_3')
_workers = 16
_resolution = 0.1
_percentiles = [10, 25, 50, 75, 90]


@log
def read(t0, timestep, predictions, fields, corners):
	cache.clean(_home_path)
	init = pd.Timestamp(t0 - timedelta(hours=timestep)).floor('6h')
	times = [t0 + timedelta(hours=p * timestep) for p in range(predictions)]
	return _get_forecast(init, times, fields, corners)


@retry
def _query(session, url, request):
	return cache.get(_home_path, json.dumps({url: request}), 'npy', np.load, lambda filename: _put_data_cache(session, url, request, filename))


@log
def _get_session():
	credentials, _ = google.auth.load_credentials_from_file(os.environ['GOOGLE_APPLICATION_CREDENTIALS'], scopes=['https://www.googleapis.com/auth/cloud-platform'])
	session = web.session(pool_maxsize=_workers)
	credentials.refresh(Request(session))
	credentials.apply(session.headers)
	return session


def _put_data_cache(session, url, request, filename):
	response = session.post(f'{url}:getPixels', json=request)
	if response.status_code != 200:
		raise ImportError(response.text)
	with open(filename, 'wb') as f:
		f.write(response.content)


@log
def _get_forecast(init, times, fields, corners):
	x = [int(np.ceil((min(c[0] for c in corners) + 180) / _resolution)), int(np.floor((max(c[0] for c in corners) + 180) / _resolution))]
	y = [int(np.ceil((90 - max(c[1] for c in corners)) / _resolution)), int(np.floor((90 - min(c[1] for c in corners)) / _resolution))]
	bands = [f'{f}_{s}' for f in fields for s in ['mean'] + [f'p{p}' for p in _percentiles]]
	request = {
		'fileFormat': 'NPY',
		'bandIds': bands,
		'grid': {
			'crsCode': 'EPSG:4326',
			'dimensions': {'width': x[1] - x[0] + 1, 'height': y[1] - y[0] + 1},
			'affineTransform': {'scaleX': _resolution, 'shearX': 0, 'translateX': (x[0] - 0.5) * _resolution - 180, 'shearY': 0, 'scaleY': -_resolution, 'translateY': 90 - (y[0] - 0.5) * _resolution}}}

	with _get_session() as session, ThreadPoolExecutor(max_workers=_workers) as executor:
		pixels = list(executor.map(lambda t: _query(session, f'https://earthengine.googleapis.com/v1/projects/gcp-public-data-weathernext/assets/weathernext_3_0_0_0p1deg/{init:%Y%m%d%H%M}_{t:%Y%m%d%H%M}', request), times))

	coords = {
		'time': pd.DatetimeIndex(times).as_unit('ns'),
		'lat': np.round(90 - np.arange(y[0], y[1] + 1) * _resolution, 1),
		'lon': np.round((np.arange(x[0], x[1] + 1) * _resolution - 180) % 360, 1)}

	single_level = xr.Dataset({f: (('time', 'lat', 'lon'), np.stack([p[f'{f}_mean'] for p in pixels])) for f in fields}, coords=coords)
	percentiles = xr.Dataset({f: (('time', 'lat', 'lon', 'percentile'), np.stack([np.stack([p[f'{f}_p{q}'] for p in pixels]) for q in _percentiles], axis=-1)) for f in fields}, coords=coords | {'percentile': np.array(_percentiles, dtype='float64')})

	return single_level.sortby('lat').astype('float64'), percentiles.sortby('lat').astype('float64')
